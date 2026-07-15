package store

import (
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

func TestServiceReadinessPersistsOnlyChallengeDigestAndFencesEvidence(t *testing.T) {
	digest := func(value string) string { return "sha256:" + strings.Repeat(value, 64) }
	task := ServiceReadinessRecord{ConnectionID: "connection-ready-0001", DeploymentID: "deployment-ready-0001", ServiceID: "service-ready-0001", TaskID: "readiness-task-0001", RequestSHA256: strings.Repeat("f", 64), BootstrapSessionID: "bootstrap-ready-0001", ExpectedInstanceID: "i-0123456789abcdef0", ExecutionID: "execution-ready-0001", ProbeKind: contract.ServiceReadinessProbeKind, RecipeExecutionManifestDigest: digest("a"), InstallEvidenceDigest: digest("b"), ArtifactDigest: digest("9"), SemanticProbe: contract.ServiceReadinessProbeV1{Scheme: "http", Port: 19191, Path: "/knowledge/semantic", ExpectedStatus: 200, BodySHA256: digest("c")}, SemanticExpectationDigest: digest("c"), Status: "running", Checkpoint: "challenge_issued", Attempt: 1, LeaseEpoch: 3, ChallengeDigest: digest("d"), ChallengeExpiresAt: "2026-07-15T12:02:00.000Z", CreatedAt: "2026-07-15T12:00:00.000Z", UpdatedAt: "2026-07-15T12:00:00.000Z"}
	item := readinessItem(task)
	if _, exists := item["challenge_b64"]; exists {
		t.Fatal("Dynamo readiness item persisted challenge plaintext")
	}
	if _, exists := item["challenge_digest"]; exists {
		t.Fatal("initial item unexpectedly persisted a claim-time challenge")
	}
	auth := WorkerLeaseAuthorization{ConnectionID: task.ConnectionID, DeploymentID: task.DeploymentID, BootstrapSessionID: task.BootstrapSessionID, ExpectedInstanceID: task.ExpectedInstanceID, LeaseEpoch: 3, TokenSHA256: strings.Repeat("e", 64), Now: "2026-07-15T12:01:00.000Z"}
	success := ServiceReadinessEvent{TaskID: task.TaskID, Attempt: 1, LeaseEpoch: 3, Sequence: 1, Status: "succeeded", ChallengeDigest: task.ChallengeDigest, SemanticEvidenceDigest: task.SemanticExpectationDigest, StackObservationDigest: digest("e"), OccurredAt: auth.Now, EventSHA256: strings.Repeat("a", 64)}
	if !validReadinessEvent(task, auth, success) {
		t.Fatal("valid terminal readiness evidence was rejected")
	}
	success.SemanticEvidenceDigest = digest("f")
	if validReadinessEvent(task, auth, success) {
		t.Fatal("mismatched semantic evidence was accepted")
	}
	success.SemanticEvidenceDigest = task.SemanticExpectationDigest
	auth.Now = task.ChallengeExpiresAt
	if validReadinessEvent(task, auth, success) {
		t.Fatal("expired challenge was accepted")
	}
}
