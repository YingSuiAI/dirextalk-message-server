package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestWorkerBootstrapObservationRunnerPersistsThenCommitsVerifiedObservation(t *testing.T) {
	now := time.Date(2026, time.July, 15, 1, 0, 0, 0, time.UTC)
	claim := testWorkerBootstrapObservationClaim(t)
	store := &fakeWorkerBootstrapObservationStore{claims: []WorkerBootstrapObservationClaim{claim}}
	transport := &fakeWorkerBootstrapObservationTransport{}
	runner := NewWorkerBootstrapObservationRunner(store, transport, workerBootstrapObservationTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.started) != 1 || len(store.persisted) != 1 || len(store.committed) != 1 || len(store.deferred) != 0 {
		t.Fatalf("observation settlement started=%#v persisted=%#v committed=%#v deferred=%#v", store.started, store.persisted, store.committed, store.deferred)
	}
	if len(transport.built) != 1 || len(transport.requests) != 1 {
		t.Fatalf("observation transport built=%#v requests=%#v", transport.built, transport.requests)
	}
	if store.trace.eventsString() != "started,persisted,committed" {
		t.Fatalf("observation order=%v", store.trace.events)
	}
}

func TestWorkerBootstrapObservationRunnerDefersUnverifiedWorkerWithoutChangingOutcome(t *testing.T) {
	now := time.Date(2026, time.July, 15, 1, 0, 0, 0, time.UTC)
	claim := testWorkerBootstrapObservationClaim(t)
	store := &fakeWorkerBootstrapObservationStore{claims: []WorkerBootstrapObservationClaim{claim}}
	transport := &fakeWorkerBootstrapObservationTransport{}
	transport.request = func(_ context.Context, _ string, _ WorkerBootstrapObservationCommand, _ SignedWorkerBootstrapObservationCommand, _ WorkerBootstrapObservationRequest) (WorkerBootstrapObservation, error) {
		result := testWorkerBootstrapObservation(claim, now)
		result.WorkerSessionState = "bound"
		result.LeaseEpoch = 0
		result.LeaseExpiresAt = time.Time{}
		return result, nil
	}
	runner := NewWorkerBootstrapObservationRunner(store, transport, workerBootstrapObservationTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.deferred) != 1 || store.deferred[0].code != invalidWorkerBootstrapObservationResultCode || len(store.committed) != 0 || len(store.failed) != 0 {
		t.Fatalf("unverified worker must defer: deferred=%#v committed=%#v failed=%#v", store.deferred, store.committed, store.failed)
	}
}

func TestWorkerBootstrapObservationRunnerReplaysPersistedEnvelopeOnRetry(t *testing.T) {
	now := time.Date(2026, time.July, 15, 1, 0, 0, 0, time.UTC)
	claim := testWorkerBootstrapObservationClaim(t)
	signed := testSignedWorkerBootstrapObservationCommand()
	claim.Command.PayloadJSON = signed.PayloadJSON
	claim.Command.PayloadSHA256 = signed.PayloadSHA256
	claim.Command.RequestSHA256 = signed.RequestSHA256
	claim.Command.SignedEnvelope = signed.EnvelopeJSON
	claim.Command.IssuedAt = signed.IssuedAt
	claim.Command.ExpiresAt = signed.ExpiresAt
	store := &fakeWorkerBootstrapObservationStore{claims: []WorkerBootstrapObservationClaim{claim, claim}}
	transport := &fakeWorkerBootstrapObservationTransport{}
	transport.request = func(_ context.Context, _ string, _ WorkerBootstrapObservationCommand, _ SignedWorkerBootstrapObservationCommand, _ WorkerBootstrapObservationRequest) (WorkerBootstrapObservation, error) {
		if len(transport.requests) == 1 {
			return WorkerBootstrapObservation{}, WorkerBootstrapObservationRetryable("broker_unavailable", errors.New("lost response"))
		}
		return testWorkerBootstrapObservation(claim, now), nil
	}
	runner := NewWorkerBootstrapObservationRunner(store, transport, workerBootstrapObservationTestConfig(now))

	for attempt := 0; attempt < 2; attempt++ {
		processed, err := runner.RunOnce(context.Background())
		if err != nil || !processed {
			t.Fatalf("RunOnce attempt %d = processed:%v err:%v", attempt+1, processed, err)
		}
	}
	if len(transport.built) != 0 || len(store.persisted) != 0 || len(transport.requests) != 2 ||
		transport.requests[0].signed.EnvelopeJSON != transport.requests[1].signed.EnvelopeJSON || len(store.deferred) != 1 || len(store.committed) != 1 {
		t.Fatalf("persisted observe retry drifted: built=%#v persisted=%#v requests=%#v deferred=%#v committed=%#v", transport.built, store.persisted, transport.requests, store.deferred, store.committed)
	}
}

func TestWorkerBootstrapObservationRunnerRetiresCounterOnlyAfterExplicitStackExpiry(t *testing.T) {
	now := time.Date(2026, time.July, 15, 1, 10, 0, 0, time.UTC)
	claim := testWorkerBootstrapObservationClaim(t)
	signed := testSignedWorkerBootstrapObservationCommand()
	signed.IssuedAt = now.Add(-5 * time.Minute)
	signed.ExpiresAt = now.Add(-time.Minute)
	claim.Command.PayloadJSON = signed.PayloadJSON
	claim.Command.PayloadSHA256 = signed.PayloadSHA256
	claim.Command.RequestSHA256 = signed.RequestSHA256
	claim.Command.SignedEnvelope = signed.EnvelopeJSON
	claim.Command.IssuedAt = signed.IssuedAt
	claim.Command.ExpiresAt = signed.ExpiresAt
	store := &fakeWorkerBootstrapObservationStore{claims: []WorkerBootstrapObservationClaim{claim}}
	transport := &fakeWorkerBootstrapObservationTransport{}
	transport.request = func(context.Context, string, WorkerBootstrapObservationCommand, SignedWorkerBootstrapObservationCommand, WorkerBootstrapObservationRequest) (WorkerBootstrapObservation, error) {
		return WorkerBootstrapObservation{}, WorkerBootstrapObservationCommandExpired(errors.New("expired_command"))
	}
	runner := NewWorkerBootstrapObservationRunner(store, transport, workerBootstrapObservationTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(transport.requests) != 1 || len(store.expired) != 1 || len(store.deferred) != 0 {
		t.Fatalf("expired envelope handling requests=%#v expired=%#v deferred=%#v", transport.requests, store.expired, store.deferred)
	}
}

type fakeWorkerBootstrapObservationStore struct {
	claims    []WorkerBootstrapObservationClaim
	started   []WorkerBootstrapObservationClaim
	persisted []workerBootstrapObservationPersistRecord
	committed []workerBootstrapObservationCommitRecord
	deferred  []workerBootstrapObservationDeferRecord
	expired   []WorkerBootstrapObservationClaim
	failed    []workerBootstrapObservationFailRecord
	trace     workerBootstrapObservationTrace
}

func (s *fakeWorkerBootstrapObservationStore) ClaimWorkerBootstrapObservation(context.Context, string, time.Duration) (WorkerBootstrapObservationClaim, bool, error) {
	if len(s.claims) == 0 {
		return WorkerBootstrapObservationClaim{}, false, nil
	}
	claim := s.claims[0]
	s.claims = s.claims[1:]
	return claim, true, nil
}

func (s *fakeWorkerBootstrapObservationStore) PersistWorkerBootstrapObservationCommand(_ context.Context, claim WorkerBootstrapObservationClaim, signed SignedWorkerBootstrapObservationCommand) error {
	s.persisted = append(s.persisted, workerBootstrapObservationPersistRecord{claim: claim, signed: signed})
	s.trace.events = append(s.trace.events, "persisted")
	return nil
}

func (s *fakeWorkerBootstrapObservationStore) MarkWorkerBootstrapObservationStarted(_ context.Context, claim WorkerBootstrapObservationClaim) error {
	s.started = append(s.started, claim)
	s.trace.events = append(s.trace.events, "started")
	return nil
}

func (s *fakeWorkerBootstrapObservationStore) CommitWorkerBootstrapObservation(_ context.Context, claim WorkerBootstrapObservationClaim, observation WorkerBootstrapObservation) error {
	s.committed = append(s.committed, workerBootstrapObservationCommitRecord{claim: claim, observation: observation})
	s.trace.events = append(s.trace.events, "committed")
	return nil
}

func (s *fakeWorkerBootstrapObservationStore) DeferWorkerBootstrapObservation(_ context.Context, claim WorkerBootstrapObservationClaim, code string, availableAt time.Time) error {
	s.deferred = append(s.deferred, workerBootstrapObservationDeferRecord{claim: claim, code: code, availableAt: availableAt})
	return nil
}

func (s *fakeWorkerBootstrapObservationStore) ExpireWorkerBootstrapObservationCommand(_ context.Context, claim WorkerBootstrapObservationClaim) error {
	s.expired = append(s.expired, claim)
	return nil
}

func (s *fakeWorkerBootstrapObservationStore) FailWorkerBootstrapObservation(_ context.Context, claim WorkerBootstrapObservationClaim, code string) error {
	s.failed = append(s.failed, workerBootstrapObservationFailRecord{claim: claim, code: code})
	return nil
}

type fakeWorkerBootstrapObservationTransport struct {
	built    []workerBootstrapObservationBuildRecord
	requests []workerBootstrapObservationRequestRecord
	request  func(context.Context, string, WorkerBootstrapObservationCommand, SignedWorkerBootstrapObservationCommand, WorkerBootstrapObservationRequest) (WorkerBootstrapObservation, error)
}

func (t *fakeWorkerBootstrapObservationTransport) BuildWorkerBootstrapObservationCommand(command WorkerBootstrapObservationCommand, request WorkerBootstrapObservationRequest, _ time.Time) (SignedWorkerBootstrapObservationCommand, error) {
	t.built = append(t.built, workerBootstrapObservationBuildRecord{command: command, request: request})
	return testSignedWorkerBootstrapObservationCommand(), nil
}

func (t *fakeWorkerBootstrapObservationTransport) RequestWorkerBootstrapObservation(ctx context.Context, endpoint string, command WorkerBootstrapObservationCommand, signed SignedWorkerBootstrapObservationCommand, request WorkerBootstrapObservationRequest) (WorkerBootstrapObservation, error) {
	t.requests = append(t.requests, workerBootstrapObservationRequestRecord{endpoint: endpoint, command: command, signed: signed, request: request})
	if t.request != nil {
		return t.request(ctx, endpoint, command, signed, request)
	}
	return testWorkerBootstrapObservation(commandClaim(command, request), signed.IssuedAt), nil
}

func commandClaim(command WorkerBootstrapObservationCommand, request WorkerBootstrapObservationRequest) WorkerBootstrapObservationClaim {
	return WorkerBootstrapObservationClaim{DeploymentID: command.DeploymentID, InstanceID: "i-0123456789abcdef0", Request: request}
}

type workerBootstrapObservationTrace struct{ events []string }

func (t workerBootstrapObservationTrace) eventsString() string { return strings.Join(t.events, ",") }

type workerBootstrapObservationPersistRecord struct {
	claim  WorkerBootstrapObservationClaim
	signed SignedWorkerBootstrapObservationCommand
}

type workerBootstrapObservationCommitRecord struct {
	claim       WorkerBootstrapObservationClaim
	observation WorkerBootstrapObservation
}

type workerBootstrapObservationDeferRecord struct {
	claim       WorkerBootstrapObservationClaim
	code        string
	availableAt time.Time
}

type workerBootstrapObservationFailRecord struct {
	claim WorkerBootstrapObservationClaim
	code  string
}

type workerBootstrapObservationBuildRecord struct {
	command WorkerBootstrapObservationCommand
	request WorkerBootstrapObservationRequest
}

type workerBootstrapObservationRequestRecord struct {
	endpoint string
	command  WorkerBootstrapObservationCommand
	signed   SignedWorkerBootstrapObservationCommand
	request  WorkerBootstrapObservationRequest
}

func workerBootstrapObservationTestConfig(now time.Time) Config {
	return Config{WorkerID: "orchestrator-observe-test", Lease: 2 * time.Minute, AttemptTimeout: time.Minute, RetryDelay: time.Minute, Now: func() time.Time { return now }}
}

func testWorkerBootstrapObservationClaim(t *testing.T) WorkerBootstrapObservationClaim {
	t.Helper()
	request := WorkerBootstrapObservationRequest{DeploymentID: "deployment-observe-0001"}
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return WorkerBootstrapObservationClaim{
		DeploymentID: "deployment-observe-0001", PlanID: "plan-observe-0001", ConnectionID: "connection-observe-0001", Region: "us-east-1",
		InstanceID: "i-0123456789abcdef0", BrokerEndpoint: "https://a1b2c3d4e5.execute-api.us-east-1.amazonaws.com/prod/v2/commands",
		NodeKeyID: "node-key-1", ExpectedGeneration: 1, JobID: "job-observe-0001", LeaseToken: "lease-observe-0001", Attempt: 1, Request: request,
		Command: WorkerBootstrapObservationCommand{CommandID: "command-observe-0001", DeploymentID: request.DeploymentID, ConnectionID: "connection-observe-0001", NodeKeyID: "node-key-1", ExpectedGeneration: 1, NodeCounter: 1, Attempt: 1, RequestDigest: digest},
	}
}

func testSignedWorkerBootstrapObservationCommand() SignedWorkerBootstrapObservationCommand {
	issuedAt := time.Date(2026, time.July, 15, 1, 0, 0, 0, time.UTC)
	return SignedWorkerBootstrapObservationCommand{
		EnvelopeJSON:  `{"schema":"dirextalk.aws.command/v2","command_id":"command-observe-0001"}`,
		PayloadJSON:   `{"deployment_id":"deployment-observe-0001"}`,
		PayloadSHA256: strings.Repeat("a", 64), RequestSHA256: strings.Repeat("b", 64), IssuedAt: issuedAt, ExpiresAt: issuedAt.Add(4 * time.Minute),
	}
}

func testWorkerBootstrapObservation(claim WorkerBootstrapObservationClaim, now time.Time) WorkerBootstrapObservation {
	return WorkerBootstrapObservation{
		Schema: WorkerBootstrapObservationSchema, DeploymentID: claim.DeploymentID, ResourceStatus: "provisioning", InstanceID: claim.InstanceID,
		WorkerSessionState: "active", LeaseEpoch: 1, LeaseExpiresAt: now.Add(4 * time.Minute), LastSequence: 0, ObservedAt: now,
	}
}
