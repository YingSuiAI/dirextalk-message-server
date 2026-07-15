package api

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

func TestExecutionProbeCommandAndWorkerEventRoundTrip(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryCommandStore()
	tasks := &memoryWorkerTaskStore{tasks: map[string]commandstore.WorkerTaskRecord{}, receipts: store}
	token := "worker-token-0000000000000000000000000001"
	tokenDigest := sha256.Sum256([]byte(token))
	session := commandstore.WorkerSession{BootstrapSessionID: "bootstrap-session-0001", ConnectionID: "connection-0001",
		DeploymentID: "deployment-0001", ExpectedInstanceID: "i-0123456789abcdef0", State: "active", LeaseEpoch: 1,
		LeaseExpiresAt: "2026-07-15T01:07:03.000Z", TokenSHA256: hex.EncodeToString(tokenDigest[:])}
	store.workerSessions[session.BootstrapSessionID] = session
	store.deployments["connection-0001\x00deployment-0001"] = commandstore.DeploymentReservation{
		ConnectionID: "connection-0001", DeploymentID: "deployment-0001", BootstrapSessionID: session.BootstrapSessionID,
		State: "finalized", WorkerSession: session,
	}
	now := time.Date(2026, 7, 15, 1, 3, 0, 0, time.UTC)
	broker := Broker{Resolver: StaticKeyResolver{ConnectionID: "connection-0001", NodeKeyID: "node-key-01", Generation: 1, PublicKey: publicKey},
		Store: store, DeploymentStore: store, DeploymentEnabled: true, WorkerTasks: tasks, WorkerSessionEvents: tasks,
		Now: func() time.Time { return now }}

	manifest := "sha256:" + strings.Repeat("a", 64)
	input := "sha256:" + strings.Repeat("b", 64)
	issuePayload, _ := json.Marshal(contract.WorkerTaskIssueRequest{Schema: contract.WorkerTaskIssueSchema,
		DeploymentID: "deployment-0001", TaskID: "task-0001", TaskKind: contract.WorkerTaskKindExecutionProbe,
		ExecutionManifestDigest: manifest, InputDigest: input})
	issue := signedReadOnlyCommand(t, privateKey, "command-task-issue-0001", 1, contract.ActionWorkerTaskIssue, issuePayload)
	assertWorkerTaskStatus(t, serve(t, broker, http.MethodPost, commandPath, issue), "worker_task_issued", "queued")
	now = time.Date(2026, 7, 15, 1, 8, 0, 0, time.UTC)
	assertWorkerTaskStatus(t, serve(t, broker, http.MethodPost, commandPath, issue), "idempotent", "queued")
	now = time.Date(2026, 7, 15, 1, 3, 0, 0, time.UTC)

	claim := `{"schema":"dirextalk.worker-task-claim/v1","lease_epoch":1}`
	claimResponse := serveWorker(t, broker, "/v2/worker-sessions/bootstrap-session-0001/tasks/claim", claim, token)
	if claimResponse.Code != http.StatusOK || !strings.Contains(claimResponse.Body.String(), `"status":"claimed"`) {
		t.Fatalf("claim = %d %s", claimResponse.Code, claimResponse.Body.String())
	}

	running := `{"schema":"dirextalk.worker-task-event/v1","task_id":"task-0001","attempt":1,"lease_epoch":1,"sequence":1,"status":"running","checkpoint":"execution_manifest_received","error_code":null,"evidence_digest":"` + manifest + `","occurred_at":"2026-07-15T01:03:01.000Z"}`
	assertDisposition(t, serveWorker(t, broker, "/v2/worker-sessions/bootstrap-session-0001/tasks/task-0001/events", running, token), "accepted")
	assertDisposition(t, serveWorker(t, broker, "/v2/worker-sessions/bootstrap-session-0001/tasks/task-0001/events", running, token), "idempotent")
	succeeded := `{"schema":"dirextalk.worker-task-event/v1","task_id":"task-0001","attempt":1,"lease_epoch":1,"sequence":2,"status":"succeeded","checkpoint":"task_transport_verified","error_code":null,"evidence_digest":"` + manifest + `","occurred_at":"2026-07-15T01:03:02.000Z"}`
	assertDisposition(t, serveWorker(t, broker, "/v2/worker-sessions/bootstrap-session-0001/tasks/task-0001/events", succeeded, token), "accepted")

	observePayload := []byte(`{"deployment_id":"deployment-0001","task_id":"task-0001"}`)
	observe := signedReadOnlyCommand(t, privateKey, "command-task-observe-0001", 2, contract.ActionWorkerTaskObserve, observePayload)
	assertWorkerTaskStatus(t, serve(t, broker, http.MethodPost, commandPath, observe), "worker_task_observed", "succeeded")

	heartbeat := `{"schema":"dirextalk.worker-event/v1","connection_id":"connection-0001","deployment_id":"deployment-0001","bootstrap_session_id":"bootstrap-session-0001","lease_epoch":1,"sequence":1,"kind":"heartbeat","occurred_at":"2026-07-15T01:03:03.000Z"}`
	assertDisposition(t, serveWorker(t, broker, "/v2/worker-sessions/bootstrap-session-0001/events", heartbeat, token), "accepted")
	assertDisposition(t, serveWorker(t, broker, "/v2/worker-sessions/bootstrap-session-0001/events", heartbeat, token), "idempotent")
}

func serveWorker(t *testing.T, broker Broker, path, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	broker.ServeHTTP(response, request)
	return response
}

func assertWorkerTaskStatus(t *testing.T, response *httptest.ResponseRecorder, status, taskStatus string) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
	var result contract.WorkerTaskResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.Status != status || result.Task.Status != taskStatus {
		t.Fatalf("result = %#v, %v", result, err)
	}
}

func assertDisposition(t *testing.T, response *httptest.ResponseRecorder, disposition string) {
	t.Helper()
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"disposition":"`+disposition+`"`) {
		t.Fatalf("disposition %s = %d %s", disposition, response.Code, response.Body.String())
	}
}

type memoryWorkerTaskStore struct {
	tasks             map[string]commandstore.WorkerTaskRecord
	receipts          *memoryCommandStore
	heartbeatSequence int64
	heartbeatHash     string
}

func (s *memoryWorkerTaskStore) IssueWorkerTask(ctx context.Context, receipt commandstore.Record, task commandstore.WorkerTaskRecord) (commandstore.Record, commandstore.WorkerTaskRecord, bool, error) {
	storedReceipt, receiptCreated, err := s.receipts.Commit(ctx, receipt, nil)
	if err != nil {
		return commandstore.Record{}, commandstore.WorkerTaskRecord{}, false, err
	}
	storedTask, taskCreated, err := s.EnsureWorkerTask(ctx, task)
	if err != nil {
		return commandstore.Record{}, commandstore.WorkerTaskRecord{}, false, err
	}
	return storedReceipt, storedTask, receiptCreated && taskCreated, nil
}

func (s *memoryWorkerTaskStore) LookupWorkerTask(_ context.Context, deploymentID, taskID string) (commandstore.WorkerTaskRecord, bool, error) {
	task, ok := s.tasks[deploymentID+"\x00"+taskID]
	return task, ok, nil
}

func (s *memoryWorkerTaskStore) EnsureWorkerTask(_ context.Context, task commandstore.WorkerTaskRecord) (commandstore.WorkerTaskRecord, bool, error) {
	key := task.DeploymentID + "\x00" + task.TaskID
	if existing, ok := s.tasks[key]; ok {
		return existing, false, nil
	}
	s.tasks[key] = task
	return task, true, nil
}

func (s *memoryWorkerTaskStore) ClaimWorkerTask(_ context.Context, authorization commandstore.WorkerLeaseAuthorization) (commandstore.WorkerTaskRecord, bool, bool, error) {
	for key, task := range s.tasks {
		if task.DeploymentID == authorization.DeploymentID && (task.Status == "queued" || task.Status == "running") {
			task.LeaseEpoch = authorization.LeaseEpoch
			s.tasks[key] = task
			return task, true, true, nil
		}
	}
	return commandstore.WorkerTaskRecord{}, false, false, nil
}

func (s *memoryWorkerTaskStore) RecordWorkerTaskEvent(_ context.Context, _ commandstore.WorkerLeaseAuthorization, event commandstore.WorkerTaskEvent) (commandstore.WorkerTaskRecord, bool, error) {
	key := "deployment-0001\x00" + event.TaskID
	task, ok := s.tasks[key]
	if !ok {
		return commandstore.WorkerTaskRecord{}, false, commandstore.NewError("worker_task_not_found")
	}
	if task.LastSequence == event.Sequence && task.LastEventSHA256 == event.EventSHA256 {
		return task, true, nil
	}
	if task.LastSequence+1 != event.Sequence {
		return commandstore.WorkerTaskRecord{}, false, commandstore.NewError("worker_task_event_conflict")
	}
	task.Status, task.LastSequence, task.LastEventSHA256, task.UpdatedAt = event.Status, event.Sequence, event.EventSHA256, event.OccurredAt
	task.Checkpoint, task.ErrorCode, task.EvidenceDigest = event.Checkpoint, event.ErrorCode, event.EvidenceDigest
	s.tasks[key] = task
	return task, false, nil
}

func (s *memoryWorkerTaskStore) RecordWorkerSessionEvent(_ context.Context, event commandstore.WorkerSessionEvent) (commandstore.WorkerSession, bool, error) {
	if s.heartbeatSequence == event.Sequence && s.heartbeatHash == event.EventSHA256 {
		return commandstore.WorkerSession{}, true, nil
	}
	if s.heartbeatSequence+1 != event.Sequence {
		return commandstore.WorkerSession{}, false, commandstore.NewError("worker_event_conflict")
	}
	s.heartbeatSequence, s.heartbeatHash = event.Sequence, event.EventSHA256
	return commandstore.WorkerSession{}, false, nil
}
