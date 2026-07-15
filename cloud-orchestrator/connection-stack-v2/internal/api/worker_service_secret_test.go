package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

func TestWorkerServiceSecretMaterializationAuthorizesBeforeProvider(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	token := "worker-materialization-token"
	tokenHash := sha256.Sum256([]byte(token))
	deploymentStore := newMemoryCommandStore()
	bootstrapID := "bootstrap-secret-0001"
	deploymentStore.workerSessions[bootstrapID] = commandstore.WorkerSession{BootstrapSessionID: bootstrapID, ConnectionID: "connection-0001", DeploymentID: "deployment-0001", State: "active", LeaseEpoch: 4, LeaseExpiresAt: "2026-07-15T12:05:00.000Z", TokenSHA256: hex.EncodeToString(tokenHash[:])}
	request := contract.WorkerServiceSecretRequest{TaskID: "task-secret-0001", ExecutionID: "execution-0001", ArtifactDigest: "sha256:" + repeat("2", 64), SlotID: "model_token", SecretRef: "secret_ref:model-token-001"}
	proof := contract.ServiceSecretApprovalProof{DeploymentID: "deployment-0001", ExecutionID: request.ExecutionID, RecipeDigest: "sha256:" + repeat("1", 64), ArtifactDigest: request.ArtifactDigest, SlotID: request.SlotID, SecretRef: request.SecretRef}
	manifest := serviceSecretManifest(proof)
	request.ManifestDigest, _ = manifest.Digest()
	manifestJSON, _ := manifest.CanonicalJSON()
	task := commandstore.RecipeTaskRecord{ConnectionID: "connection-0001", DeploymentID: "deployment-0001", TaskID: request.TaskID, ExecutionID: request.ExecutionID, RecipeExecutionManifestDigest: request.ManifestDigest, ManifestJSON: manifestJSON, Status: "running"}
	tasks := &memoryRecipeTaskStore{tasks: map[string]commandstore.RecipeTaskRecord{"deployment-0001\x00" + request.TaskID: task}, receipts: newMemoryCommandStore()}
	secretStore := newMemoryServiceSecretStore()
	secretStore.sessions["secret-session-0001"] = commandstore.ServiceSecretSession{SessionID: "secret-session-0001", ConnectionID: "connection-0001", DeploymentID: "deployment-0001", TaskID: request.TaskID, ExecutionID: request.ExecutionID, ManifestDigest: request.ManifestDigest, RecipeDigest: proof.RecipeDigest, ArtifactDigest: request.ArtifactDigest, SlotID: request.SlotID, SecretRef: request.SecretRef, Purpose: "model inference", Delivery: "environment", ExpiresAt: "2026-07-15T12:10:00.000Z", State: commandstore.ServiceSecretCompleted, ProviderVersion: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", EnvelopeDigest: "sha256:" + repeat("a", 64)}
	canary := []byte("WORKER_SECRET_CANARY_BINARY")
	provider := &capturingServiceSecretProvider{readValue: canary}
	broker := Broker{DeploymentEnabled: true, ServiceSecretsEnabled: true, DeploymentStore: deploymentStore, RecipeTasks: tasks, ServiceSecretStore: secretStore, ServiceSecretProvider: provider, Now: func() time.Time { return now }}
	raw, _ := json.Marshal(request)
	success := serveWorkerSecret(t, broker, bootstrapID, token, "4", raw)
	if success.Code != 200 || success.Header().Get("Content-Type") != "application/octet-stream" || success.Header().Get("Cache-Control") != "no-store" || !bytes.Equal(success.Body.Bytes(), canary) || provider.getCalls != 1 {
		t.Fatalf("success=%d headers=%v calls=%d body=%q", success.Code, success.Header(), provider.getCalls, success.Body.Bytes())
	}
	for name, mutate := range map[string]func(*contract.WorkerServiceSecretRequest){"ref": func(v *contract.WorkerServiceSecretRequest) { v.SecretRef = "secret_ref:other-001" }, "artifact": func(v *contract.WorkerServiceSecretRequest) { v.ArtifactDigest = "sha256:" + repeat("9", 64) }} {
		t.Run(name, func(t *testing.T) {
			changed := request
			mutate(&changed)
			body, _ := json.Marshal(changed)
			response := serveWorkerSecret(t, broker, bootstrapID, token, "4", body)
			if response.Code == 200 || provider.getCalls != 1 {
				t.Fatalf("status=%d provider=%d", response.Code, provider.getCalls)
			}
		})
	}
	stale := serveWorkerSecret(t, broker, bootstrapID, token, "3", raw)
	if stale.Code != 401 || provider.getCalls != 1 {
		t.Fatalf("stale=%d provider=%d", stale.Code, provider.getCalls)
	}
	otherToken := "worker-other-deployment"
	otherHash := sha256.Sum256([]byte(otherToken))
	deploymentStore.workerSessions["bootstrap-secret-other"] = commandstore.WorkerSession{BootstrapSessionID: "bootstrap-secret-other", ConnectionID: "connection-0001", DeploymentID: "deployment-other", State: "active", LeaseEpoch: 4, LeaseExpiresAt: "2026-07-15T12:05:00.000Z", TokenSHA256: hex.EncodeToString(otherHash[:])}
	crossDeployment := serveWorkerSecret(t, broker, "bootstrap-secret-other", otherToken, "4", raw)
	if crossDeployment.Code == 200 || provider.getCalls != 1 {
		t.Fatalf("cross deployment=%d provider=%d", crossDeployment.Code, provider.getCalls)
	}
	stored := secretStore.sessions["secret-session-0001"]
	stored.State = commandstore.ServiceSecretUploaded
	secretStore.sessions[stored.SessionID] = stored
	pending := serveWorkerSecret(t, broker, bootstrapID, token, "4", raw)
	if pending.Code != http.StatusTooEarly || provider.getCalls != 1 {
		t.Fatalf("pending=%d provider=%d", pending.Code, provider.getCalls)
	}
	stored.State = commandstore.ServiceSecretCompleted
	stored.ExpiresAt = "2026-07-15T11:59:59.000Z"
	secretStore.sessions[stored.SessionID] = stored
	restartedBroker := broker
	materializedAfterBootstrapExpiry := serveWorkerSecret(t, restartedBroker, bootstrapID, token, "4", raw)
	if materializedAfterBootstrapExpiry.Code != 200 || provider.getCalls != 2 || !bytes.Equal(materializedAfterBootstrapExpiry.Body.Bytes(), canary) {
		t.Fatalf("completed after bootstrap expiry=%d provider=%d body=%q", materializedAfterBootstrapExpiry.Code, provider.getCalls, materializedAfterBootstrapExpiry.Body.Bytes())
	}
	stored.ExpiresAt = "2026-07-15T12:10:00.000Z"
	secretStore.sessions[stored.SessionID] = stored
	provider.getErr = errors.New("AccessDenied")
	denied := serveWorkerSecret(t, broker, bootstrapID, token, "4", raw)
	if denied.Code != 503 || provider.getCalls != 3 || bytes.Contains(denied.Body.Bytes(), canary) {
		t.Fatalf("denied=%d calls=%d", denied.Code, provider.getCalls)
	}
}

func TestWorkerServiceSecretRouteDefaultsOff(t *testing.T) {
	response := serveWorkerSecret(t, Broker{}, "bootstrap-secret-0001", "token-token-token1", "1", []byte(`{}`))
	if response.Code != http.StatusNotImplemented {
		t.Fatalf("status=%d", response.Code)
	}
}

func TestWorkerServiceSecretReusesDeploymentBindingForNewActiveTask(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	token := "worker-restart-token"
	tokenHash := sha256.Sum256([]byte(token))
	deploymentStore := newMemoryCommandStore()
	deploymentStore.workerSessions["bootstrap-restart-0001"] = commandstore.WorkerSession{BootstrapSessionID: "bootstrap-restart-0001", ConnectionID: "connection-0001", DeploymentID: "deployment-0001", State: "active", LeaseEpoch: 4, LeaseExpiresAt: "2026-07-15T12:05:00.000Z", TokenSHA256: hex.EncodeToString(tokenHash[:])}
	proof := contract.ServiceSecretApprovalProof{DeploymentID: "deployment-0001", ExecutionID: "execution-install-0001", RecipeDigest: "sha256:" + repeat("1", 64), ArtifactDigest: "sha256:" + repeat("2", 64), SlotID: "model_token", SecretRef: "secret_ref:model-token-001"}
	installManifest := serviceSecretManifest(proof)
	installDigest, _ := installManifest.Digest()
	installJSON, _ := installManifest.CanonicalJSON()
	restartManifest := installManifest
	restartManifest.ExecutionID = "execution-restart-0002"
	restartDigest, _ := restartManifest.Digest()
	restartJSON, _ := restartManifest.CanonicalJSON()
	tasks := &memoryRecipeTaskStore{tasks: map[string]commandstore.RecipeTaskRecord{
		"deployment-0001\x00task-install-0001": {ConnectionID: "connection-0001", DeploymentID: "deployment-0001", TaskID: "task-install-0001", ExecutionID: installManifest.ExecutionID, RecipeExecutionManifestDigest: installDigest, ManifestJSON: installJSON, Status: "succeeded"},
		"deployment-0001\x00task-restart-0002": {ConnectionID: "connection-0001", DeploymentID: "deployment-0001", TaskID: "task-restart-0002", ExecutionID: restartManifest.ExecutionID, RecipeExecutionManifestDigest: restartDigest, ManifestJSON: restartJSON, Status: "running"},
	}, receipts: newMemoryCommandStore()}
	secretStore := newMemoryServiceSecretStore()
	secretStore.sessions["secret-session-0001"] = commandstore.ServiceSecretSession{SessionID: "secret-session-0001", ConnectionID: "connection-0001", DeploymentID: "deployment-0001", TaskID: "task-install-0001", ExecutionID: installManifest.ExecutionID, ManifestDigest: installDigest, RecipeDigest: proof.RecipeDigest, ArtifactDigest: proof.ArtifactDigest, SlotID: proof.SlotID, SecretRef: proof.SecretRef, Purpose: "model inference", Delivery: "environment", ExpiresAt: "2026-07-15T11:59:00.000Z", State: commandstore.ServiceSecretCompleted, ProviderVersion: "version-restart-1", EnvelopeDigest: "sha256:" + repeat("a", 64)}
	provider := &capturingServiceSecretProvider{readValue: []byte("RESTART_SECRET")}
	broker := Broker{DeploymentEnabled: true, ServiceSecretsEnabled: true, DeploymentStore: deploymentStore, RecipeTasks: tasks, ServiceSecretStore: secretStore, ServiceSecretProvider: provider, Now: func() time.Time { return now }}
	request := contract.WorkerServiceSecretRequest{TaskID: "task-restart-0002", ExecutionID: restartManifest.ExecutionID, ManifestDigest: restartDigest, ArtifactDigest: proof.ArtifactDigest, SlotID: proof.SlotID, SecretRef: proof.SecretRef}
	raw, _ := json.Marshal(request)
	response := serveWorkerSecret(t, broker, "bootstrap-restart-0001", token, "4", raw)
	if response.Code != http.StatusOK || provider.getCalls != 1 {
		t.Fatalf("restart materialization=%d calls=%d body=%s", response.Code, provider.getCalls, response.Body.String())
	}
	request.ArtifactDigest = "sha256:" + repeat("9", 64)
	raw, _ = json.Marshal(request)
	if tampered := serveWorkerSecret(t, broker, "bootstrap-restart-0001", token, "4", raw); tampered.Code != http.StatusForbidden || provider.getCalls != 1 {
		t.Fatalf("artifact tamper=%d calls=%d", tampered.Code, provider.getCalls)
	}
}
func serveWorkerSecret(t *testing.T, b Broker, sessionID, token, epoch string, raw []byte) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v2/worker-sessions/"+sessionID+"/service-secrets/materialize", bytes.NewReader(raw))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set(workerLeaseEpochHeader, epoch)
	w := httptest.NewRecorder()
	b.ServeHTTP(w, r)
	return w
}
