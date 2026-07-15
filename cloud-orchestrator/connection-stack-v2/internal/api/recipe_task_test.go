package api

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

func TestRecipeTaskSignedIssueClaimProgressReplayAndObserve(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryCommandStore()
	recipes := &memoryRecipeTaskStore{tasks: map[string]commandstore.RecipeTaskRecord{}, receipts: store}
	token := "worker-token-0000000000000000000000000001"
	tokenDigest := sha256.Sum256([]byte(token))
	workerManifestDigest := "sha256:" + strings.Repeat("3", 64)
	session := commandstore.WorkerSession{BootstrapSessionID: "bootstrap-session-0001", ConnectionID: "connection-0001", DeploymentID: "deployment-0001", WorkerImageDigest: workerManifestDigest, ExpectedInstanceID: "i-0123456789abcdef0", State: "active", LeaseEpoch: 1, LeaseExpiresAt: "2026-07-15T01:07:03.000Z", TokenSHA256: hex.EncodeToString(tokenDigest[:])}
	store.workerSessions[session.BootstrapSessionID] = session
	store.deployments["connection-0001\x00deployment-0001"] = commandstore.DeploymentReservation{ConnectionID: "connection-0001", DeploymentID: "deployment-0001", BootstrapSessionID: session.BootstrapSessionID, State: "finalized", WorkerSession: session}
	now := time.Date(2026, 7, 15, 1, 3, 0, 0, time.UTC)
	broker := Broker{Resolver: StaticKeyResolver{ConnectionID: "connection-0001", NodeKeyID: "node-key-01", Generation: 1, PublicKey: publicKey}, Store: store, DeploymentStore: store, DeploymentEnabled: true, RecipeTasks: recipes, Now: func() time.Time { return now }}

	manifest := recipeManifestAPI(t)
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	issueRequest := contract.RecipeTaskIssueRequest{Schema: contract.RecipeTaskIssueSchema, TaskID: "recipe-task-0001", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskKind: contract.RecipeTaskKindExecution, RecipeExecutionManifestDigest: manifestDigest, InputDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Manifest: manifest}
	mismatched := issueRequest
	mismatched.Manifest.WorkerResourceManifestDigest = "sha256:" + strings.Repeat("9", 64)
	mismatched.RecipeExecutionManifestDigest, err = mismatched.Manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	mismatchedPayload, _ := json.Marshal(mismatched)
	mismatchedCommand := signedReadOnlyCommand(t, privateKey, "command-recipe-worker-mismatch-0001", 1, contract.ActionWorkerRecipeTaskIssue, mismatchedPayload)
	mismatchedResponse := serve(t, broker, http.MethodPost, commandPath, mismatchedCommand).Result()
	defer mismatchedResponse.Body.Close()
	if mismatchedResponse.StatusCode != http.StatusConflict {
		t.Fatalf("worker manifest mismatch status=%d", mismatchedResponse.StatusCode)
	}
	issuePayload, _ := json.Marshal(issueRequest)
	issue := signedReadOnlyCommand(t, privateKey, "command-recipe-issue-0001", 1, contract.ActionWorkerRecipeTaskIssue, issuePayload)
	assertRecipeTaskStatus(t, serve(t, broker, http.MethodPost, commandPath, issue), "recipe_task_issued", "queued")
	now = now.Add(6 * time.Minute)
	assertRecipeTaskStatus(t, serve(t, broker, http.MethodPost, commandPath, issue), "idempotent", "queued")
	now = now.Add(-6 * time.Minute)

	claim := `{"schema":"dirextalk.recipe-execution-task-claim/v1","lease_epoch":1}`
	claimResponse := serveWorker(t, broker, "/v2/worker-sessions/bootstrap-session-0001/recipe-tasks/claim", claim, token)
	if claimResponse.Code != http.StatusOK || !strings.Contains(claimResponse.Body.String(), `"status":"claimed"`) || !strings.Contains(claimResponse.Body.String(), `"manifest":{"schema_version":"dirextalk.recipe-execution-manifest/v1"`) {
		t.Fatalf("claim = %d %s", claimResponse.Code, claimResponse.Body.String())
	}

	running := recipeTaskEventJSON(t, "running", "artifact_verified", "", manifestDigest, 1)
	assertDisposition(t, serveWorker(t, broker, "/v2/worker-sessions/bootstrap-session-0001/recipe-tasks/recipe-task-0001/events", running, token), "accepted")
	assertDisposition(t, serveWorker(t, broker, "/v2/worker-sessions/bootstrap-session-0001/recipe-tasks/recipe-task-0001/events", running, token), "idempotent")
	second := recipeTaskEventJSON(t, "running", "install_complete", "", manifestDigest, 2)
	assertDisposition(t, serveWorker(t, broker, "/v2/worker-sessions/bootstrap-session-0001/recipe-tasks/recipe-task-0001/events", second, token), "accepted")
	terminal := recipeTaskEventJSON(t, "succeeded", "health_verified", "", manifestDigest, 3)
	assertDisposition(t, serveWorker(t, broker, "/v2/worker-sessions/bootstrap-session-0001/recipe-tasks/recipe-task-0001/events", terminal, token), "accepted")

	observePayload := []byte(`{"deployment_id":"deployment-0001","task_id":"recipe-task-0001"}`)
	observe := signedReadOnlyCommand(t, privateKey, "command-recipe-observe-0001", 2, contract.ActionWorkerRecipeTaskObserve, observePayload)
	assertRecipeTaskStatus(t, serve(t, broker, http.MethodPost, commandPath, observe), "recipe_task_observed", "succeeded")
}

func recipeTaskEventJSON(t *testing.T, status, checkpoint, errorCode, evidence string, sequence int) string {
	t.Helper()
	optional := func(value string) any {
		if value == "" {
			return nil
		}
		return value
	}
	raw, err := json.Marshal(map[string]any{"schema": contract.RecipeTaskEventV1Schema, "task_id": "recipe-task-0001", "attempt": 1, "lease_epoch": 1, "sequence": sequence, "status": status, "checkpoint": optional(checkpoint), "error_code": optional(errorCode), "evidence_digest": optional(evidence), "occurred_at": "2026-07-15T01:03:01.000Z"})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func recipeManifestAPI(t *testing.T) contract.RecipeExecutionManifestV1 {
	t.Helper()
	return contract.RecipeExecutionManifestV1{SchemaVersion: contract.RecipeExecutionManifestSchema, ExecutionID: "execution-0001", DeploymentID: "deployment-0001", PlanID: "plan-00000001", PlanHash: "sha256:" + strings.Repeat("1", 64), PlanRevision: 1, RecipeDigest: "sha256:" + strings.Repeat("2", 64), WorkerResourceManifestDigest: "sha256:" + strings.Repeat("3", 64), ArtifactDigest: "sha256:" + strings.Repeat("4", 64), ActionID: "install_service", RootRequired: true, TimeoutSeconds: 900, CheckpointSequence: []string{"artifact_verified", "install_complete", "health_verified"}}
}

func assertRecipeTaskStatus(t *testing.T, response interface{ Result() *http.Response }, status, taskStatus string) {
	t.Helper()
	resultHTTP := response.Result()
	defer resultHTTP.Body.Close()
	var result contract.RecipeTaskResult
	if err := json.NewDecoder(resultHTTP.Body).Decode(&result); err != nil || resultHTTP.StatusCode != http.StatusOK || result.Status != status || result.Task.Status != taskStatus {
		t.Fatalf("recipe result = %#v status=%d err=%v", result, resultHTTP.StatusCode, err)
	}
}

type memoryRecipeTaskStore struct {
	tasks    map[string]commandstore.RecipeTaskRecord
	receipts *memoryCommandStore
}

func (s *memoryRecipeTaskStore) LookupRecipeTask(_ context.Context, deploymentID, taskID string) (commandstore.RecipeTaskRecord, bool, error) {
	value, ok := s.tasks[deploymentID+"\x00"+taskID]
	return value, ok, nil
}
func (s *memoryRecipeTaskStore) IssueRecipeTask(ctx context.Context, receipt commandstore.Record, task commandstore.RecipeTaskRecord) (commandstore.Record, commandstore.RecipeTaskRecord, bool, error) {
	storedReceipt, created, err := s.receipts.Commit(ctx, receipt, nil)
	if err != nil {
		return commandstore.Record{}, commandstore.RecipeTaskRecord{}, false, err
	}
	key := task.DeploymentID + "\x00" + task.TaskID
	if existing, ok := s.tasks[key]; ok {
		return storedReceipt, existing, false, nil
	}
	s.tasks[key] = task
	return storedReceipt, task, created, nil
}
func (s *memoryRecipeTaskStore) ClaimRecipeTask(_ context.Context, auth commandstore.WorkerLeaseAuthorization) (commandstore.RecipeTaskRecord, bool, bool, error) {
	for key, task := range s.tasks {
		if task.DeploymentID == auth.DeploymentID && (task.Status == "queued" || task.Status == "running") {
			task.LeaseEpoch = auth.LeaseEpoch
			s.tasks[key] = task
			return task, true, true, nil
		}
	}
	return commandstore.RecipeTaskRecord{}, false, false, nil
}
func (s *memoryRecipeTaskStore) RecordRecipeTaskEvent(_ context.Context, _ commandstore.WorkerLeaseAuthorization, event commandstore.RecipeTaskEvent) (commandstore.RecipeTaskRecord, bool, error) {
	key := "deployment-0001\x00" + event.TaskID
	task, ok := s.tasks[key]
	if !ok {
		return commandstore.RecipeTaskRecord{}, false, commandstore.NewError("recipe_task_not_found")
	}
	if task.LastSequence == event.Sequence && task.LastEventSHA256 == event.EventSHA256 {
		return task, true, nil
	}
	if task.LastSequence+1 != event.Sequence {
		return commandstore.RecipeTaskRecord{}, false, commandstore.NewError("recipe_task_event_conflict")
	}
	task.Status, task.LastSequence, task.LastEventSHA256, task.UpdatedAt = event.Status, event.Sequence, event.EventSHA256, event.OccurredAt
	if event.Checkpoint != "" {
		task.LastCheckpoint = event.Checkpoint
	}
	task.ErrorCode, task.EvidenceDigest = event.ErrorCode, event.EvidenceDigest
	s.tasks[key] = task
	return task, false, nil
}
