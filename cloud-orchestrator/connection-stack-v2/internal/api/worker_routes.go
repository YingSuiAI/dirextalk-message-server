package api

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

type workerRoute struct {
	sessionID string
	taskID    string
	kind      string
}

func parseWorkerRoute(path string) (workerRoute, bool) {
	if !strings.HasPrefix(path, workerSessionsPathPrefix) {
		return workerRoute{}, false
	}
	parts := strings.Split(strings.TrimPrefix(path, workerSessionsPathPrefix), "/")
	if len(parts) < 2 || !contract.ValidID(parts[0]) {
		return workerRoute{}, false
	}
	switch {
	case len(parts) == 2 && parts[1] == "events":
		return workerRoute{sessionID: parts[0], kind: "heartbeat"}, true
	case len(parts) == 3 && parts[1] == "tasks" && parts[2] == "claim":
		return workerRoute{sessionID: parts[0], kind: "task_claim"}, true
	case len(parts) == 4 && parts[1] == "tasks" && contract.ValidID(parts[2]) && parts[3] == "events":
		return workerRoute{sessionID: parts[0], taskID: parts[2], kind: "task_event"}, true
	case len(parts) == 3 && parts[1] == "recipe-tasks" && parts[2] == "claim":
		return workerRoute{sessionID: parts[0], kind: "recipe_task_claim"}, true
	case len(parts) == 4 && parts[1] == "recipe-tasks" && contract.ValidRecipeTaskID(parts[2]) && parts[3] == "events":
		return workerRoute{sessionID: parts[0], taskID: parts[2], kind: "recipe_task_event"}, true
	case len(parts) == 3 && parts[1] == "service-readiness-tasks" && parts[2] == "claim":
		return workerRoute{sessionID: parts[0], kind: "service_readiness_claim"}, true
	case len(parts) == 4 && parts[1] == "service-readiness-tasks" && contract.ValidRecipeTaskID(parts[2]) && parts[3] == "events":
		return workerRoute{sessionID: parts[0], taskID: parts[2], kind: "service_readiness_event"}, true
	case len(parts) == 3 && parts[1] == "service-secrets" && parts[2] == "materialize":
		return workerRoute{sessionID: parts[0], kind: "service_secret_materialize"}, true
	default:
		return workerRoute{}, false
	}
}

func (b Broker) serveWorkerRoute(response http.ResponseWriter, request *http.Request, route workerRoute) {
	if request.URL.RawQuery != "" {
		writeError(response, http.StatusNotFound, "not_found")
		return
	}
	if request.Method != http.MethodPost {
		response.Header().Set("Allow", http.MethodPost)
		writeError(response, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		writeError(response, http.StatusUnsupportedMediaType, "unsupported_content_type")
		return
	}
	if !b.DeploymentEnabled || b.DeploymentStore == nil {
		writeError(response, http.StatusNotImplemented, "operation_not_enabled")
		return
	}
	if (route.kind == "heartbeat" && b.WorkerSessionEvents == nil) ||
		((route.kind == "task_claim" || route.kind == "task_event") && b.WorkerTasks == nil) ||
		((route.kind == "recipe_task_claim" || route.kind == "recipe_task_event") && b.RecipeTasks == nil) ||
		((route.kind == "service_readiness_claim" || route.kind == "service_readiness_event") && b.ServiceReadiness == nil) {
		writeError(response, http.StatusNotImplemented, "operation_not_enabled")
		return
	}
	if route.kind == "service_secret_materialize" && (!b.ServiceSecretsEnabled || b.ServiceSecretStore == nil || b.ServiceSecretProvider == nil || b.RecipeTasks == nil) {
		writeError(response, http.StatusNotImplemented, "operation_not_enabled")
		return
	}
	raw, ok := readWorkerBody(response, request)
	if !ok {
		return
	}
	switch route.kind {
	case "heartbeat":
		b.serveWorkerHeartbeat(response, request, route, raw)
	case "task_claim":
		b.serveWorkerTaskClaim(response, request, route, raw)
	case "task_event":
		b.serveWorkerTaskEvent(response, request, route, raw)
	case "recipe_task_claim":
		b.serveRecipeTaskClaim(response, request, route, raw)
	case "recipe_task_event":
		b.serveRecipeTaskEvent(response, request, route, raw)
	case "service_readiness_claim":
		b.serveServiceReadinessClaim(response, request, route, raw)
	case "service_readiness_event":
		b.serveServiceReadinessEvent(response, request, route, raw)
	case "service_secret_materialize":
		b.serveWorkerServiceSecret(response, request, route, raw)
	default:
		writeError(response, http.StatusNotFound, "not_found")
	}
}

func readWorkerBody(response http.ResponseWriter, request *http.Request) ([]byte, bool) {
	request.Body = http.MaxBytesReader(response, request.Body, contract.MaxWorkerTaskRequestBytes)
	raw, err := io.ReadAll(request.Body)
	if err == nil {
		return raw, true
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(response, http.StatusRequestEntityTooLarge, "request_too_large")
	} else {
		writeError(response, http.StatusBadRequest, "invalid_worker_request")
	}
	return nil, false
}

func (b Broker) serveWorkerHeartbeat(response http.ResponseWriter, request *http.Request, route workerRoute, raw []byte) {
	event, err := contract.ParseWorkerHeartbeatEvent(raw)
	if err != nil || event.BootstrapSessionID != route.sessionID {
		writeError(response, http.StatusBadRequest, "invalid_worker_heartbeat")
		return
	}
	authorization, err := b.authorizeWorker(request, route.sessionID, int64(event.LeaseEpoch))
	if err != nil {
		writeProviderError(response, err)
		return
	}
	if event.ConnectionID != authorization.Session.ConnectionID || event.DeploymentID != authorization.Session.DeploymentID {
		writeError(response, http.StatusForbidden, "worker_session_unauthorized")
		return
	}
	eventDigest := sha256.Sum256(raw)
	_, idempotent, err := b.WorkerSessionEvents.RecordWorkerSessionEvent(request.Context(), commandstore.WorkerSessionEvent{
		ConnectionID: event.ConnectionID, DeploymentID: event.DeploymentID, BootstrapSessionID: event.BootstrapSessionID,
		ExpectedInstanceID: authorization.Session.ExpectedInstanceID, LeaseEpoch: int64(event.LeaseEpoch),
		Sequence: int64(event.Sequence), TokenSHA256: authorization.TokenSHA256, EventSHA256: hex.EncodeToString(eventDigest[:]),
		OccurredAt: event.OccurredAt, Now: authorization.Now.Format("2006-01-02T15:04:05.000Z"),
	})
	if err != nil {
		writeStoreError(response, err)
		return
	}
	result, err := contract.MarshalWorkerHeartbeatEventReceipt(event, idempotent)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "worker_session_unavailable")
		return
	}
	writeRawJSON(response, http.StatusOK, result)
}
