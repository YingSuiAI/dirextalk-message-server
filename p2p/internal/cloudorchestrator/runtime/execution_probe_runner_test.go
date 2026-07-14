package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestExecutionProbeRunnerPersistsIssueThenCommitsDigestBoundResult(t *testing.T) {
	now := time.Date(2026, time.July, 15, 2, 0, 0, 0, time.UTC)
	claim := testExecutionProbeClaim(t, ExecutionProbePhaseIssue)
	store := &fakeExecutionProbeStore{claims: []ExecutionProbeClaim{claim}}
	transport := &fakeExecutionProbeTransport{}
	runner := NewExecutionProbeRunner(store, transport, executionProbeTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.started) != 1 || len(store.persisted) != 1 || len(store.committed) != 1 || len(store.deferred) != 0 {
		t.Fatalf("issue settlement started=%#v persisted=%#v committed=%#v deferred=%#v", store.started, store.persisted, store.committed, store.deferred)
	}
	if len(transport.issueBuilds) != 1 || len(transport.issueRequests) != 1 || len(transport.observeBuilds) != 0 || len(transport.observeRequests) != 0 {
		t.Fatalf("issue transport builds=%#v requests=%#v observe_builds=%#v observe_requests=%#v", transport.issueBuilds, transport.issueRequests, transport.observeBuilds, transport.observeRequests)
	}
	if store.trace.eventsString() != "started,persisted,committed" {
		t.Fatalf("issue order=%v", store.trace.events)
	}
}

func TestExecutionProbeRunnerReplaysPersistedObserveEnvelopeAfterResponseLoss(t *testing.T) {
	now := time.Date(2026, time.July, 15, 2, 0, 0, 0, time.UTC)
	claim := testExecutionProbeClaim(t, ExecutionProbePhaseObserve)
	signed := testSignedExecutionProbeCommand(now)
	claim.Command.PayloadJSON = signed.PayloadJSON
	claim.Command.PayloadSHA256 = signed.PayloadSHA256
	claim.Command.RequestSHA256 = signed.RequestSHA256
	claim.Command.SignedEnvelope = signed.EnvelopeJSON
	claim.Command.IssuedAt = signed.IssuedAt
	claim.Command.ExpiresAt = signed.ExpiresAt
	store := &fakeExecutionProbeStore{claims: []ExecutionProbeClaim{claim, claim}}
	transport := &fakeExecutionProbeTransport{}
	transport.observeRequest = func(_ context.Context, _ string, _ ExecutionProbeCommand, _ SignedExecutionProbeCommand, _ ExecutionProbeObserveRequest) (ExecutionProbeTaskResult, error) {
		if len(transport.observeRequests) == 1 {
			return ExecutionProbeTaskResult{}, ExecutionProbeRetryable("broker_unavailable", errors.New("lost response"))
		}
		return testExecutionProbeResult(claim, "running", now), nil
	}
	runner := NewExecutionProbeRunner(store, transport, executionProbeTestConfig(now))

	for attempt := 0; attempt < 2; attempt++ {
		processed, err := runner.RunOnce(context.Background())
		if err != nil || !processed {
			t.Fatalf("RunOnce attempt %d = processed:%v err:%v", attempt+1, processed, err)
		}
	}
	if len(transport.observeBuilds) != 0 || len(store.persisted) != 0 || len(transport.observeRequests) != 2 ||
		transport.observeRequests[0].signed.EnvelopeJSON != transport.observeRequests[1].signed.EnvelopeJSON || len(store.deferred) != 1 || len(store.committed) != 1 {
		t.Fatalf("persisted observe retry drifted: builds=%#v persisted=%#v requests=%#v deferred=%#v committed=%#v", transport.observeBuilds, store.persisted, transport.observeRequests, store.deferred, store.committed)
	}
}

func TestExecutionProbeRunnerDefersResultWithWrongManifestEvidence(t *testing.T) {
	now := time.Date(2026, time.July, 15, 2, 0, 0, 0, time.UTC)
	claim := testExecutionProbeClaim(t, ExecutionProbePhaseObserve)
	store := &fakeExecutionProbeStore{claims: []ExecutionProbeClaim{claim}}
	transport := &fakeExecutionProbeTransport{}
	transport.observeRequest = func(_ context.Context, _ string, _ ExecutionProbeCommand, _ SignedExecutionProbeCommand, _ ExecutionProbeObserveRequest) (ExecutionProbeTaskResult, error) {
		result := testExecutionProbeResult(claim, "succeeded", now)
		wrong := "sha256:" + strings.Repeat("b", 64)
		result.EvidenceDigest = &wrong
		return result, nil
	}
	runner := NewExecutionProbeRunner(store, transport, executionProbeTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.deferred) != 1 || store.deferred[0].code != executionProbeInvalidResultCode(ExecutionProbePhaseObserve) || len(store.committed) != 0 {
		t.Fatalf("invalid result must defer: deferred=%#v committed=%#v", store.deferred, store.committed)
	}
}

func TestExecutionProbeRunnerOnlyExpiresAfterExplicitStackResponse(t *testing.T) {
	now := time.Date(2026, time.July, 15, 2, 5, 0, 0, time.UTC)
	claim := testExecutionProbeClaim(t, ExecutionProbePhaseIssue)
	signed := testSignedExecutionProbeCommand(now.Add(-5 * time.Minute))
	claim.Command.PayloadJSON = signed.PayloadJSON
	claim.Command.PayloadSHA256 = signed.PayloadSHA256
	claim.Command.RequestSHA256 = signed.RequestSHA256
	claim.Command.SignedEnvelope = signed.EnvelopeJSON
	claim.Command.IssuedAt = signed.IssuedAt
	claim.Command.ExpiresAt = signed.ExpiresAt
	store := &fakeExecutionProbeStore{claims: []ExecutionProbeClaim{claim}}
	transport := &fakeExecutionProbeTransport{}
	transport.issueRequest = func(context.Context, string, ExecutionProbeCommand, SignedExecutionProbeCommand, ExecutionProbeIssueRequest) (ExecutionProbeTaskResult, error) {
		return ExecutionProbeTaskResult{}, ExecutionProbeCommandExpired(errors.New("expired_command"))
	}
	runner := NewExecutionProbeRunner(store, transport, executionProbeTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(transport.issueRequests) != 1 || len(store.expired) != 1 || len(store.deferred) != 0 {
		t.Fatalf("expired command handling issue_requests=%#v expired=%#v deferred=%#v", transport.issueRequests, store.expired, store.deferred)
	}
}

type fakeExecutionProbeStore struct {
	claims    []ExecutionProbeClaim
	started   []ExecutionProbeClaim
	persisted []executionProbePersistRecord
	committed []executionProbeCommitRecord
	deferred  []executionProbeDeferRecord
	expired   []ExecutionProbeClaim
	failed    []executionProbeFailRecord
	trace     executionProbeTrace
}

func (s *fakeExecutionProbeStore) ClaimExecutionProbe(context.Context, string, time.Duration) (ExecutionProbeClaim, bool, error) {
	if len(s.claims) == 0 {
		return ExecutionProbeClaim{}, false, nil
	}
	claim := s.claims[0]
	s.claims = s.claims[1:]
	return claim, true, nil
}

func (s *fakeExecutionProbeStore) PersistExecutionProbeCommand(_ context.Context, claim ExecutionProbeClaim, signed SignedExecutionProbeCommand) error {
	s.persisted = append(s.persisted, executionProbePersistRecord{claim: claim, signed: signed})
	s.trace.events = append(s.trace.events, "persisted")
	return nil
}

func (s *fakeExecutionProbeStore) MarkExecutionProbeStarted(_ context.Context, claim ExecutionProbeClaim) error {
	s.started = append(s.started, claim)
	s.trace.events = append(s.trace.events, "started")
	return nil
}

func (s *fakeExecutionProbeStore) CommitExecutionProbe(_ context.Context, claim ExecutionProbeClaim, result ExecutionProbeTaskResult) error {
	s.committed = append(s.committed, executionProbeCommitRecord{claim: claim, result: result})
	s.trace.events = append(s.trace.events, "committed")
	return nil
}

func (s *fakeExecutionProbeStore) DeferExecutionProbe(_ context.Context, claim ExecutionProbeClaim, code string, availableAt time.Time) error {
	s.deferred = append(s.deferred, executionProbeDeferRecord{claim: claim, code: code, availableAt: availableAt})
	return nil
}

func (s *fakeExecutionProbeStore) ExpireExecutionProbeCommand(_ context.Context, claim ExecutionProbeClaim) error {
	s.expired = append(s.expired, claim)
	return nil
}

func (s *fakeExecutionProbeStore) FailExecutionProbe(_ context.Context, claim ExecutionProbeClaim, code string) error {
	s.failed = append(s.failed, executionProbeFailRecord{claim: claim, code: code})
	return nil
}

type fakeExecutionProbeTransport struct {
	issueBuilds     []executionProbeIssueBuildRecord
	issueRequests   []executionProbeIssueRequestRecord
	observeBuilds   []executionProbeObserveBuildRecord
	observeRequests []executionProbeObserveRequestRecord
	issueRequest    func(context.Context, string, ExecutionProbeCommand, SignedExecutionProbeCommand, ExecutionProbeIssueRequest) (ExecutionProbeTaskResult, error)
	observeRequest  func(context.Context, string, ExecutionProbeCommand, SignedExecutionProbeCommand, ExecutionProbeObserveRequest) (ExecutionProbeTaskResult, error)
}

func (t *fakeExecutionProbeTransport) BuildExecutionProbeIssueCommand(command ExecutionProbeCommand, request ExecutionProbeIssueRequest, now time.Time) (SignedExecutionProbeCommand, error) {
	t.issueBuilds = append(t.issueBuilds, executionProbeIssueBuildRecord{command: command, request: request})
	return testSignedExecutionProbeCommand(now), nil
}

func (t *fakeExecutionProbeTransport) RequestExecutionProbeIssue(ctx context.Context, endpoint string, command ExecutionProbeCommand, signed SignedExecutionProbeCommand, request ExecutionProbeIssueRequest) (ExecutionProbeTaskResult, error) {
	t.issueRequests = append(t.issueRequests, executionProbeIssueRequestRecord{endpoint: endpoint, command: command, signed: signed, request: request})
	if t.issueRequest != nil {
		return t.issueRequest(ctx, endpoint, command, signed, request)
	}
	return testExecutionProbeResult(executionProbeClaimForCommand(command, request.DeploymentID, request.TaskID), "queued", signed.IssuedAt), nil
}

func (t *fakeExecutionProbeTransport) BuildExecutionProbeObserveCommand(command ExecutionProbeCommand, request ExecutionProbeObserveRequest, now time.Time) (SignedExecutionProbeCommand, error) {
	t.observeBuilds = append(t.observeBuilds, executionProbeObserveBuildRecord{command: command, request: request})
	return testSignedExecutionProbeCommand(now), nil
}

func (t *fakeExecutionProbeTransport) RequestExecutionProbeObserve(ctx context.Context, endpoint string, command ExecutionProbeCommand, signed SignedExecutionProbeCommand, request ExecutionProbeObserveRequest) (ExecutionProbeTaskResult, error) {
	t.observeRequests = append(t.observeRequests, executionProbeObserveRequestRecord{endpoint: endpoint, command: command, signed: signed, request: request})
	if t.observeRequest != nil {
		return t.observeRequest(ctx, endpoint, command, signed, request)
	}
	return testExecutionProbeResult(executionProbeClaimForCommand(command, request.DeploymentID, request.TaskID), "running", signed.IssuedAt), nil
}

func executionProbeClaimForCommand(command ExecutionProbeCommand, deploymentID, taskID string) ExecutionProbeClaim {
	return ExecutionProbeClaim{DeploymentID: deploymentID, TaskID: taskID, TaskAttempt: 1, ExecutionManifestDigest: "sha256:" + strings.Repeat("a", 64), Command: command}
}

type executionProbeTrace struct{ events []string }

func (t executionProbeTrace) eventsString() string { return strings.Join(t.events, ",") }

type executionProbePersistRecord struct {
	claim  ExecutionProbeClaim
	signed SignedExecutionProbeCommand
}
type executionProbeCommitRecord struct {
	claim  ExecutionProbeClaim
	result ExecutionProbeTaskResult
}
type executionProbeDeferRecord struct {
	claim       ExecutionProbeClaim
	code        string
	availableAt time.Time
}
type executionProbeFailRecord struct {
	claim ExecutionProbeClaim
	code  string
}
type executionProbeIssueBuildRecord struct {
	command ExecutionProbeCommand
	request ExecutionProbeIssueRequest
}
type executionProbeIssueRequestRecord struct {
	endpoint string
	command  ExecutionProbeCommand
	signed   SignedExecutionProbeCommand
	request  ExecutionProbeIssueRequest
}
type executionProbeObserveBuildRecord struct {
	command ExecutionProbeCommand
	request ExecutionProbeObserveRequest
}
type executionProbeObserveRequestRecord struct {
	endpoint string
	command  ExecutionProbeCommand
	signed   SignedExecutionProbeCommand
	request  ExecutionProbeObserveRequest
}

func executionProbeTestConfig(now time.Time) Config {
	return Config{WorkerID: "orchestrator-execution-probe-test", Lease: 2 * time.Minute, AttemptTimeout: time.Minute, RetryDelay: time.Minute, Now: func() time.Time { return now }}
}

func testExecutionProbeClaim(t *testing.T, phase string) ExecutionProbeClaim {
	t.Helper()
	claim := ExecutionProbeClaim{
		Phase: phase, DeploymentID: "deployment-probe-0001", PlanID: "plan-probe-0001", ConnectionID: "connection-probe-0001", Region: "us-east-1",
		InstanceID: "i-0123456789abcdef0", TaskID: "task-probe-0001", TaskAttempt: 1,
		ExecutionManifestDigest: "sha256:" + strings.Repeat("a", 64), InputDigest: "sha256:" + strings.Repeat("b", 64),
		BrokerEndpoint: "https://a1b2c3d4e5.execute-api.us-east-1.amazonaws.com/prod/v2/commands", NodeKeyID: "node-key-1", ExpectedGeneration: 1,
		JobID: "job-probe-0001", LeaseToken: "lease-probe-0001",
	}
	switch phase {
	case ExecutionProbePhaseIssue:
		claim.OutboxID = "outbox-probe-0001"
		claim.Kind = ExecutionProbeIssueRequested
		claim.AggregateType = "execution_probe_task"
		claim.AggregateID = claim.TaskID
		claim.IssueRequest = ExecutionProbeIssueRequest{Schema: ExecutionProbeIssueSchema, DeploymentID: claim.DeploymentID, TaskID: claim.TaskID, TaskKind: ExecutionProbeTaskKind, ExecutionManifestDigest: claim.ExecutionManifestDigest, InputDigest: claim.InputDigest}
		digest, err := claim.IssueRequest.Digest()
		if err != nil {
			t.Fatal(err)
		}
		claim.Command = ExecutionProbeCommand{CommandID: "command-probe-issue-0001", DeploymentID: claim.DeploymentID, TaskID: claim.TaskID, ConnectionID: claim.ConnectionID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: 1, NodeCounter: 1, Attempt: 1, Action: ExecutionProbeIssueAction, RequestDigest: digest}
	case ExecutionProbePhaseObserve:
		claim.ObserveRequest = ExecutionProbeObserveRequest{DeploymentID: claim.DeploymentID, TaskID: claim.TaskID}
		digest, err := claim.ObserveRequest.Digest()
		if err != nil {
			t.Fatal(err)
		}
		claim.Command = ExecutionProbeCommand{CommandID: "command-probe-observe-0001", DeploymentID: claim.DeploymentID, TaskID: claim.TaskID, ConnectionID: claim.ConnectionID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: 1, NodeCounter: 2, Attempt: 1, Action: ExecutionProbeObserveAction, RequestDigest: digest}
	default:
		t.Fatalf("unsupported phase %q", phase)
	}
	return claim
}

func testSignedExecutionProbeCommand(issuedAt time.Time) SignedExecutionProbeCommand {
	return SignedExecutionProbeCommand{EnvelopeJSON: `{"schema":"dirextalk.aws.command/v2","command_id":"command-probe"}`, PayloadJSON: `{}`,
		PayloadSHA256: strings.Repeat("a", 64), RequestSHA256: strings.Repeat("b", 64), IssuedAt: issuedAt.UTC(), ExpiresAt: issuedAt.UTC().Add(4 * time.Minute)}
}

func testExecutionProbeResult(claim ExecutionProbeClaim, status string, now time.Time) ExecutionProbeTaskResult {
	result := ExecutionProbeTaskResult{TaskID: claim.TaskID, DeploymentID: claim.DeploymentID, Status: status, Attempt: claim.TaskAttempt, UpdatedAt: now.UTC()}
	switch status {
	case "queued":
		return result
	case "running":
		checkpoint := ExecutionProbeReceived
		result.LastSequence, result.Checkpoint, result.EvidenceDigest = 1, &checkpoint, &claim.ExecutionManifestDigest
	case "succeeded":
		checkpoint := ExecutionProbeTransportPassed
		result.LastSequence, result.Checkpoint, result.EvidenceDigest = 2, &checkpoint, &claim.ExecutionManifestDigest
	case "failed", "interrupted":
		errorCode := "execution_probe_failed"
		result.LastSequence, result.ErrorCode = 1, &errorCode
	}
	return result
}
