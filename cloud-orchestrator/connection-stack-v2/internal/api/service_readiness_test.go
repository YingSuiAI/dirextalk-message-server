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

func TestServiceReadinessSignedIssueExactReplayClaimAndStackObservation(t *testing.T) {
	publicKey, privateKey, _ := ed25519.GenerateKey(nil)
	store := newMemoryCommandStore()
	readiness := &memoryReadinessStore{tasks: map[string]commandstore.ServiceReadinessRecord{}, receipts: store}
	token := "worker-token-0000000000000000000000000001"
	tokenDigest := sha256.Sum256([]byte(token))
	session := commandstore.WorkerSession{BootstrapSessionID: "bootstrap-session-0001", ConnectionID: "connection-0001", DeploymentID: "deployment-0001", ExpectedInstanceID: "i-0123456789abcdef0", State: "active", LeaseEpoch: 1, LeaseExpiresAt: "2026-07-15T01:07:03.000Z", TokenSHA256: hex.EncodeToString(tokenDigest[:])}
	store.workerSessions[session.BootstrapSessionID] = session
	store.deployments["connection-0001\x00deployment-0001"] = commandstore.DeploymentReservation{ConnectionID: session.ConnectionID, DeploymentID: session.DeploymentID, BootstrapSessionID: session.BootstrapSessionID, State: "finalized", WorkerSession: session}
	now := time.Date(2026, 7, 15, 1, 3, 0, 0, time.UTC)
	challenge, _ := contract.NewServiceReadinessChallenge(make([]byte, 32), "2026-07-15T01:05:00.000Z")
	broker := Broker{Resolver: StaticKeyResolver{ConnectionID: "connection-0001", NodeKeyID: "node-key-01", Generation: 1, PublicKey: publicKey}, Store: store, DeploymentStore: store, DeploymentEnabled: true, ServiceReadiness: readiness, ReadinessChallenges: fixedReadinessChallenge{challenge}, Now: func() time.Time { return now }}

	digest := func(value string) string { return "sha256:" + strings.Repeat(value, 64) }
	issueRequest := contract.ServiceReadinessIssueRequest{Schema: contract.ServiceReadinessIssueSchema, ExecutionID: "execution-ready-0001", DeploymentID: "deployment-0001", ServiceID: "service-ready-0001", TaskID: "readiness-task-0001", ProbeKind: contract.ServiceReadinessProbeKind, RecipeExecutionManifestDigest: digest("a"), InstallEvidenceDigest: digest("b"), ArtifactDigest: digest("d"), SemanticProbe: contract.ServiceReadinessProbeV1{Scheme: "http", Port: 19191, Path: "/openclaw/semantic", ExpectedStatus: 200, BodySHA256: digest("c")}, SemanticExpectationDigest: digest("c")}
	payload, _ := json.Marshal(issueRequest)
	issue := signedReadOnlyCommand(t, privateKey, "command-ready-issue-0001", 1, contract.ActionServiceReadinessIssue, payload)
	first := serve(t, broker, http.MethodPost, commandPath, issue)
	if first.Code != http.StatusOK {
		t.Fatalf("issue = %d %s", first.Code, first.Body.String())
	}
	wantReplay := first.Body.String()
	key := "deployment-0001\x00readiness-task-0001"
	mutated := readiness.tasks[key]
	mutated.Status = "failed"
	mutated.ErrorCode = "mutated_after_receipt"
	mutated.LastSequence = 1
	readiness.tasks[key] = mutated
	replay := serve(t, broker, http.MethodPost, commandPath, issue)
	if replay.Code != http.StatusOK || replay.Body.String() != wantReplay {
		t.Fatalf("exact replay drifted: %d %s, want %s", replay.Code, replay.Body.String(), wantReplay)
	}
	mutated.Status, mutated.ErrorCode, mutated.LastSequence = "queued", "", 0
	readiness.tasks[key] = mutated

	claim := serveWorker(t, broker, "/v2/worker-sessions/bootstrap-session-0001/service-readiness-tasks/claim", `{"schema":"dirextalk.service-readiness-task-claim/v1","lease_epoch":1}`, token)
	if claim.Code != http.StatusOK || !strings.Contains(claim.Body.String(), challenge.ChallengeB64) || strings.Contains(claim.Body.String(), "url") {
		t.Fatalf("claim = %d %s", claim.Code, claim.Body.String())
	}
	event := `{"schema":"dirextalk.service-readiness-task-event/v1","task_id":"readiness-task-0001","attempt":1,"lease_epoch":1,"sequence":1,"status":"succeeded","challenge_digest":"` + challenge.ChallengeDigest + `","semantic_evidence_digest":"` + issueRequest.SemanticExpectationDigest + `","error_code":null,"occurred_at":"2026-07-15T01:03:01.000Z"}`
	eventResponse := serveWorker(t, broker, "/v2/worker-sessions/bootstrap-session-0001/service-readiness-tasks/readiness-task-0001/events", event, token)
	if eventResponse.Code != http.StatusOK || strings.Contains(eventResponse.Body.String(), challenge.ChallengeB64) {
		t.Fatalf("event receipt = %d %s", eventResponse.Code, eventResponse.Body.String())
	}
	stored := readiness.tasks[key]
	if stored.Status != "succeeded" || stored.StackObservationDigest == "" || stored.StackObservationDigest == stored.ChallengeDigest || stored.StackObservationDigest == stored.SemanticEvidenceDigest {
		t.Fatalf("stored readiness = %#v", stored)
	}
}

type fixedReadinessChallenge struct {
	value contract.ServiceReadinessChallengeV1
}

func (f fixedReadinessChallenge) Generate(time.Time) (contract.ServiceReadinessChallengeV1, error) {
	return f.value, nil
}

type memoryReadinessStore struct {
	tasks    map[string]commandstore.ServiceReadinessRecord
	receipts *memoryCommandStore
}

func (s *memoryReadinessStore) LookupServiceReadiness(_ context.Context, deploymentID, taskID string) (commandstore.ServiceReadinessRecord, bool, error) {
	value, ok := s.tasks[deploymentID+"\x00"+taskID]
	return value, ok, nil
}
func (s *memoryReadinessStore) IssueServiceReadiness(ctx context.Context, receipt commandstore.Record, task commandstore.ServiceReadinessRecord) (commandstore.Record, commandstore.ServiceReadinessRecord, bool, error) {
	stored, created, err := s.receipts.Commit(ctx, receipt, nil)
	if err != nil {
		return commandstore.Record{}, commandstore.ServiceReadinessRecord{}, false, err
	}
	key := task.DeploymentID + "\x00" + task.TaskID
	if existing, ok := s.tasks[key]; ok {
		return stored, existing, false, nil
	}
	s.tasks[key] = task
	return stored, task, created, nil
}
func (s *memoryReadinessStore) ClaimServiceReadiness(_ context.Context, auth commandstore.WorkerLeaseAuthorization, grant commandstore.ServiceReadinessChallengeGrant) (commandstore.ServiceReadinessRecord, bool, error) {
	for key, task := range s.tasks {
		if task.DeploymentID == auth.DeploymentID && (task.Status == "queued" || task.Status == "running") {
			task.Status = "running"
			task.Checkpoint = "challenge_issued"
			task.LeaseEpoch = auth.LeaseEpoch
			task.ChallengeDigest = grant.Digest
			task.ChallengeExpiresAt = grant.ExpiresAt
			s.tasks[key] = task
			return task, true, nil
		}
	}
	return commandstore.ServiceReadinessRecord{}, false, nil
}
func (s *memoryReadinessStore) RecordServiceReadinessEvent(_ context.Context, _ commandstore.WorkerLeaseAuthorization, event commandstore.ServiceReadinessEvent) (commandstore.ServiceReadinessRecord, bool, error) {
	key := "deployment-0001\x00" + event.TaskID
	task := s.tasks[key]
	if task.LastSequence == event.Sequence && task.LastEventSHA256 == event.EventSHA256 {
		return task, true, nil
	}
	task.Status = event.Status
	task.LastSequence = event.Sequence
	task.LastEventSHA256 = event.EventSHA256
	task.ChallengeDigest = event.ChallengeDigest
	task.SemanticEvidenceDigest = event.SemanticEvidenceDigest
	task.StackObservationDigest = event.StackObservationDigest
	task.ErrorCode = event.ErrorCode
	if event.Status == "succeeded" {
		task.Checkpoint = "readiness_verified"
	}
	s.tasks[key] = task
	return task, false, nil
}
