package api

import (
	"net/http"
	"strconv"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

const workerLeaseEpochHeader = "X-Dirextalk-Worker-Lease-Epoch"

func (b Broker) serveWorkerServiceSecret(w http.ResponseWriter, r *http.Request, route workerRoute, raw []byte) {
	request, err := contract.ParseWorkerServiceSecretRequest(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, contract.Code(err))
		return
	}
	epochRaw := r.Header.Get(workerLeaseEpochHeader)
	epoch, err := strconv.ParseInt(epochRaw, 10, 64)
	if err != nil || epoch < 1 || strconv.FormatInt(epoch, 10) != epochRaw {
		writeError(w, http.StatusUnauthorized, "worker_session_unauthorized")
		return
	}
	authorization, err := b.authorizeWorker(r, route.sessionID, epoch)
	if err != nil {
		writeProviderError(w, err)
		return
	}
	task, found, err := b.RecipeTasks.LookupRecipeTask(r.Context(), authorization.Session.DeploymentID, request.TaskID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	manifest, matches := workerServiceSecretManifest(authorization.Session.ConnectionID, authorization.Session.DeploymentID, request, task)
	if !found || !matches {
		writeError(w, http.StatusForbidden, "worker_service_secret_unauthorized")
		return
	}
	session, found, err := b.ServiceSecretStore.LookupCompletedServiceSecret(r.Context(), authorization.Session.ConnectionID, authorization.Session.DeploymentID, manifest.RecipeDigest, request.ArtifactDigest, request.SlotID, request.SecretRef)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusTooEarly, "worker_service_secret_pending")
		return
	}
	if session.State != commandstore.ServiceSecretCompleted {
		writeError(w, http.StatusForbidden, "worker_service_secret_unauthorized")
		return
	}
	value, err := b.ServiceSecretProvider.GetServiceSecret(r.Context(), ServiceSecretReadBinding{ConnectionID: session.ConnectionID, DeploymentID: session.DeploymentID, SecretRef: session.SecretRef, ProviderVersion: session.ProviderVersion})
	if err != nil {
		writeProviderError(w, err)
		return
	}
	defer clear(value)
	if len(value) == 0 || len(value) > contract.MaxServiceSecretPlaintext {
		writeError(w, http.StatusBadGateway, "service_secret_provider_invalid")
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(value)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(value)
}

func workerServiceSecretManifest(connectionID, deploymentID string, request contract.WorkerServiceSecretRequest, task commandstore.RecipeTaskRecord) (contract.RecipeExecutionManifestV1, bool) {
	if task.ConnectionID != connectionID || task.DeploymentID != deploymentID || task.TaskID != request.TaskID || task.ExecutionID != request.ExecutionID || task.RecipeExecutionManifestDigest != request.ManifestDigest || (task.Status != "queued" && task.Status != "running") {
		return contract.RecipeExecutionManifestV1{}, false
	}
	manifest, err := contract.ParseRecipeExecutionManifestJSON(task.ManifestJSON)
	if err != nil {
		return contract.RecipeExecutionManifestV1{}, false
	}
	digest, err := manifest.Digest()
	if err != nil || digest != request.ManifestDigest || manifest.ExecutionID != request.ExecutionID || manifest.DeploymentID != deploymentID || manifest.ArtifactDigest != request.ArtifactDigest {
		return contract.RecipeExecutionManifestV1{}, false
	}
	for _, slot := range manifest.SecretSlots {
		if slot.SlotID == request.SlotID {
			return manifest, slot.SecretRef == request.SecretRef
		}
	}
	return contract.RecipeExecutionManifestV1{}, false
}
