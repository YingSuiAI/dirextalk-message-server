package runtime

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestServiceReadinessRequiresStackWitnessedEvidence(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	claim := serviceReadinessClaim(t, now)
	store := &readinessTestStore{claim: claim, found: true}
	transport := &readinessTestTransport{signed: readinessSigned(now), result: readinessSuccess(claim, now)}
	runner := NewServiceReadinessRunner(store, transport, Config{WorkerID: "readiness-test", Lease: 2 * time.Minute, AttemptTimeout: time.Minute, RetryDelay: time.Minute, Now: func() time.Time { return now }})

	run, err := runner.RunOnce(context.Background())
	if err != nil || !run || store.commits != 1 || store.defers != 0 || store.failures != 0 {
		t.Fatalf("RunOnce() run=%v err=%v commits=%d defers=%d failures=%d", run, err, store.commits, store.defers, store.failures)
	}

	// A Worker-local semantic claim is deliberately insufficient. The Stack
	// must add its own persisted observation digest after validating the fresh
	// challenge response.
	store = &readinessTestStore{claim: claim, found: true}
	localOnly := readinessSuccess(claim, now)
	localOnly.StackObservationDigest = nil
	transport.result = localOnly
	runner = NewServiceReadinessRunner(store, transport, Config{WorkerID: "readiness-test", Lease: 2 * time.Minute, AttemptTimeout: time.Minute, RetryDelay: time.Minute, Now: func() time.Time { return now }})
	run, err = runner.RunOnce(context.Background())
	if err != nil || !run || store.commits != 0 || store.defers != 1 || store.deferCode != "invalid_service_readiness_result" {
		t.Fatalf("local-only result run=%v err=%v commits=%d defers=%d code=%q", run, err, store.commits, store.defers, store.deferCode)
	}
}

func TestServiceReadinessClaimRejectsSelectableProbeTarget(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	claim := serviceReadinessClaim(t, now)
	claim.IssueRequest.ProbeKind = "https://worker.invalid/readyz"
	if err := ValidateServiceReadinessClaim(claim); err == nil {
		t.Fatal("ValidateServiceReadinessClaim accepted a selectable URL probe")
	}
}

func TestServiceReadinessAcceptsStackChallengeBeforeWorkerSequence(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	claim := serviceReadinessClaim(t, now)
	challenge := "sha256:" + strings.Repeat("d", 64)
	running := ServiceReadinessResult{ExecutionID: claim.ExecutionID, DeploymentID: claim.DeploymentID, ServiceID: claim.ServiceID,
		TaskID: claim.TaskID, Status: "running", Checkpoint: ServiceReadinessChallengeIssued, Attempt: 1, LastSequence: 0,
		ChallengeDigest: &challenge, UpdatedAt: now}
	if err := ValidateServiceReadinessResult(claim, running, now); err != nil {
		t.Fatalf("Stack challenge before Worker event: %v", err)
	}
	code := "fixed_probe_failed"
	failed := ServiceReadinessResult{ExecutionID: claim.ExecutionID, DeploymentID: claim.DeploymentID, ServiceID: claim.ServiceID,
		TaskID: claim.TaskID, Status: "failed", Attempt: 1, LastSequence: 1, ChallengeDigest: &challenge, ErrorCode: &code, UpdatedAt: now}
	if err := ValidateServiceReadinessResult(claim, failed, now); err == nil {
		t.Fatal("failed readiness accepted a challenge digest")
	}
}

func TestServiceReadinessObserveAcceptsNewerWorkerAttempt(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	claim := serviceReadinessClaim(t, now)
	issue := claim.IssueRequest
	issueDigest := claim.Command.RequestDigest
	observe := ServiceReadinessObserveRequest{DeploymentID: claim.DeploymentID, ServiceID: claim.ServiceID, TaskID: claim.TaskID}
	digest, err := observe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	claim.Phase, claim.OutboxID, claim.Kind, claim.AggregateType, claim.AggregateID = ServiceReadinessPhaseObserve, "", "", "", ""
	claim.IssueRequest = ServiceReadinessIssueRequest{}
	claim.ObserveRequest = observe
	claim.Command.Action, claim.Command.RequestDigest = ServiceReadinessObserveAction, digest
	result := readinessSuccess(claim, now)
	result.Attempt = claim.TaskAttempt + 1
	if err := ValidateServiceReadinessResult(claim, result, now); err != nil {
		t.Fatalf("newer Stack attempt should survive an Orchestrator outage: %v", err)
	}
	claim.Phase, claim.OutboxID, claim.Kind, claim.AggregateType, claim.AggregateID = ServiceReadinessPhaseIssue, "outbox-ready-0001", "cloud.service_readiness.requested", "service_readiness_task", claim.TaskID
	claim.IssueRequest, claim.ObserveRequest = issue, ServiceReadinessObserveRequest{}
	claim.Command.Action, claim.Command.RequestDigest = ServiceReadinessIssueAction, issueDigest
	if err := ValidateServiceReadinessResult(claim, result, now); err == nil {
		t.Fatal("initial issue accepted an unexpected task attempt")
	}
}

func serviceReadinessClaim(t *testing.T, now time.Time) ServiceReadinessClaim {
	t.Helper()
	digest := func(value string) string { return "sha256:" + strings.Repeat(value, 64) }
	issue := ServiceReadinessIssueRequest{
		Schema: ServiceReadinessIssueSchema, ExecutionID: "execution-ready-0001", DeploymentID: "deployment-ready-0001",
		ServiceID: "service-ready-0001", TaskID: "readiness-task-0001", ProbeKind: ServiceReadinessProbeKind,
		RecipeExecutionManifestDigest: digest("a"), InstallEvidenceDigest: digest("b"), SemanticExpectationDigest: digest("c"),
	}
	requestDigest, err := issue.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return ServiceReadinessClaim{
		Phase: ServiceReadinessPhaseIssue, OutboxID: "outbox-ready-0001", Kind: "cloud.service_readiness.requested",
		AggregateType: "service_readiness_task", AggregateID: issue.TaskID, LeaseToken: "lease-ready-0001",
		ExecutionID: issue.ExecutionID, DeploymentID: issue.DeploymentID, ServiceID: issue.ServiceID, ConnectionID: "connection-ready-0001",
		Region: "us-east-1", InstanceID: "i-0123456789abcdef0", TaskID: issue.TaskID, Purpose: "install", JobID: "job-ready-0001",
		BrokerEndpoint: "https://a1b2c3d4e5.execute-api.us-east-1.amazonaws.com/prod/v2/commands", NodeKeyID: "node-key-1",
		SemanticExpectationDigest: issue.SemanticExpectationDigest, ExpectedGeneration: 1, TaskAttempt: 1, IssueRequest: issue,
		Command: ServiceReadinessCommand{CommandID: "command-ready-0001", ExecutionID: issue.ExecutionID, DeploymentID: issue.DeploymentID,
			ServiceID: issue.ServiceID, TaskID: issue.TaskID, ConnectionID: "connection-ready-0001", NodeKeyID: "node-key-1",
			ExpectedGeneration: 1, NodeCounter: 1, Attempt: 1, Action: ServiceReadinessIssueAction, RequestDigest: requestDigest},
	}
}

func readinessSigned(now time.Time) SignedServiceReadinessCommand {
	return SignedServiceReadinessCommand{EnvelopeJSON: `{"schema":"dirextalk.aws.command/v2"}`, PayloadJSON: `{}`,
		PayloadSHA256: strings.Repeat("d", 64), RequestSHA256: strings.Repeat("e", 64), IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute)}
}

func readinessSuccess(claim ServiceReadinessClaim, now time.Time) ServiceReadinessResult {
	challenge, semantic, stack := "sha256:"+strings.Repeat("d", 64), claim.SemanticExpectationDigest, "sha256:"+strings.Repeat("e", 64)
	return ServiceReadinessResult{ExecutionID: claim.ExecutionID, DeploymentID: claim.DeploymentID, ServiceID: claim.ServiceID, TaskID: claim.TaskID,
		Status: "succeeded", Checkpoint: ServiceReadinessVerified, Attempt: 1, LastSequence: 2, ChallengeDigest: &challenge,
		SemanticEvidenceDigest: &semantic, StackObservationDigest: &stack, UpdatedAt: now}
}

type readinessTestStore struct {
	claim                     ServiceReadinessClaim
	found                     bool
	commits, defers, failures int
	deferCode                 string
}

func (s *readinessTestStore) ClaimServiceReadiness(context.Context, string, time.Duration) (ServiceReadinessClaim, bool, error) {
	return s.claim, s.found, nil
}
func (s *readinessTestStore) MarkServiceReadinessStarted(context.Context, ServiceReadinessClaim) error {
	return nil
}
func (s *readinessTestStore) PersistServiceReadinessCommand(context.Context, ServiceReadinessClaim, SignedServiceReadinessCommand) error {
	return nil
}
func (s *readinessTestStore) CommitServiceReadiness(_ context.Context, _ ServiceReadinessClaim, _ ServiceReadinessResult) error {
	s.commits++
	return nil
}
func (s *readinessTestStore) DeferServiceReadiness(_ context.Context, _ ServiceReadinessClaim, code string, _ time.Time) error {
	s.defers++
	s.deferCode = code
	return nil
}
func (s *readinessTestStore) ExpireServiceReadinessCommand(context.Context, ServiceReadinessClaim) error {
	return nil
}
func (s *readinessTestStore) FailServiceReadiness(_ context.Context, _ ServiceReadinessClaim, _ string) error {
	s.failures++
	return nil
}

type readinessTestTransport struct {
	signed SignedServiceReadinessCommand
	result ServiceReadinessResult
}

func (t *readinessTestTransport) BuildServiceReadinessIssueCommand(ServiceReadinessCommand, ServiceReadinessIssueRequest, time.Time) (SignedServiceReadinessCommand, error) {
	return t.signed, nil
}
func (t *readinessTestTransport) RequestServiceReadinessIssue(context.Context, string, ServiceReadinessCommand, SignedServiceReadinessCommand, ServiceReadinessIssueRequest) (ServiceReadinessResult, error) {
	return t.result, nil
}
func (t *readinessTestTransport) BuildServiceReadinessObserveCommand(ServiceReadinessCommand, ServiceReadinessObserveRequest, time.Time) (SignedServiceReadinessCommand, error) {
	return t.signed, nil
}
func (t *readinessTestTransport) RequestServiceReadinessObserve(context.Context, string, ServiceReadinessCommand, SignedServiceReadinessCommand, ServiceReadinessObserveRequest) (ServiceReadinessResult, error) {
	return t.result, nil
}
