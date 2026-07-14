package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestConnectionRegistrationRunnerPersistsSignedEnvelopeBeforeVerificationAndCommit(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	claim := testConnectionRegistrationClaim(t)
	store := &fakeConnectionRegistrationStore{claims: []ConnectionRegistrationClaim{claim}}
	transport := &fakeConnectionRegistrationTransport{trace: &store.trace}
	transport.request = func(_ context.Context, _ string, command ConnectionRegistrationCommand, signed SignedConnectionRegistrationCommand, request ConnectionRegistrationRequest) (BrokerRegistration, error) {
		if len(store.persisted) != 1 {
			return BrokerRegistration{}, errors.New("broker verification occurred before durable envelope persistence")
		}
		if !reflect.DeepEqual(store.persisted[0].signed, signed) {
			return BrokerRegistration{}, errors.New("broker verification did not use the persisted envelope")
		}
		if command.CommandID != claim.Command.CommandID || request.BootstrapID != claim.BootstrapID {
			return BrokerRegistration{}, errors.New("broker verification lost its durable binding")
		}
		return testBrokerRegistration(t, claim, signed), nil
	}
	runner := NewConnectionRegistrationRunner(store, transport, connectionRegistrationTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if got, want := store.trace.events, []string{"started", "built", "persisted", "requested", "committed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("durable registration order = %#v, want %#v", got, want)
	}
	if len(store.persisted) != 1 || len(store.committed) != 1 || len(store.deferred) != 0 || len(store.expired) != 0 || len(store.failed) != 0 {
		t.Fatalf("registration settlement = persisted:%#v committed:%#v deferred:%#v expired:%#v failed:%#v", store.persisted, store.committed, store.deferred, store.expired, store.failed)
	}
	if got, want := store.committed[0].claim.Command.SignedEnvelope, store.persisted[0].signed.EnvelopeJSON; got != want {
		t.Fatalf("committed claim must retain the verified persisted envelope: got %q want %q", got, want)
	}
}

func TestConnectionRegistrationRunnerDefersTimedOutAttemptWithoutActivation(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	claim := testConnectionRegistrationClaim(t)
	store := &fakeConnectionRegistrationStore{claims: []ConnectionRegistrationClaim{claim}}
	transport := &fakeConnectionRegistrationTransport{}
	transport.request = func(ctx context.Context, _ string, _ ConnectionRegistrationCommand, _ SignedConnectionRegistrationCommand, _ ConnectionRegistrationRequest) (BrokerRegistration, error) {
		<-ctx.Done()
		return BrokerRegistration{}, ctx.Err()
	}
	config := connectionRegistrationTestConfig(now)
	config.Lease = time.Second
	config.AttemptTimeout = time.Millisecond
	runner := NewConnectionRegistrationRunner(store, transport, config)

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.deferred) != 1 || store.deferred[0].code != "connection_registration_attempt_timed_out" || !store.deferred[0].availableAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("timed out registration must only defer: %#v", store.deferred)
	}
	if len(store.committed) != 0 || len(store.expired) != 0 || len(store.failed) != 0 {
		t.Fatalf("timed out registration must not activate: committed=%#v expired=%#v failed=%#v", store.committed, store.expired, store.failed)
	}
}

func TestConnectionRegistrationRunnerDefersRetryableFailureWithoutActivation(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	claim := testConnectionRegistrationClaim(t)
	store := &fakeConnectionRegistrationStore{claims: []ConnectionRegistrationClaim{claim}}
	transport := &fakeConnectionRegistrationTransport{}
	transport.request = func(context.Context, string, ConnectionRegistrationCommand, SignedConnectionRegistrationCommand, ConnectionRegistrationRequest) (BrokerRegistration, error) {
		return BrokerRegistration{}, ConnectionRegistrationRetryable("broker_unavailable", errors.New("temporary broker outage"))
	}
	runner := NewConnectionRegistrationRunner(store, transport, connectionRegistrationTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.deferred) != 1 || store.deferred[0].code != "broker_unavailable" || !store.deferred[0].availableAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("retryable registration failure must only defer: %#v", store.deferred)
	}
	if len(store.committed) != 0 || len(store.expired) != 0 || len(store.failed) != 0 {
		t.Fatalf("retryable registration failure must not activate: committed=%#v expired=%#v failed=%#v", store.committed, store.expired, store.failed)
	}
}

func TestConnectionRegistrationRunnerExpiresOnlyExactExpiredBrokerCommand(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	claim := testConnectionRegistrationClaim(t)
	store := &fakeConnectionRegistrationStore{claims: []ConnectionRegistrationClaim{claim}}
	transport := &fakeConnectionRegistrationTransport{}
	transport.request = func(context.Context, string, ConnectionRegistrationCommand, SignedConnectionRegistrationCommand, ConnectionRegistrationRequest) (BrokerRegistration, error) {
		return BrokerRegistration{}, ConnectionRegistrationCommandExpired(errors.New("expired command"))
	}
	runner := NewConnectionRegistrationRunner(store, transport, connectionRegistrationTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.expired) != 1 || len(store.committed) != 0 || len(store.deferred) != 0 || len(store.failed) != 0 {
		t.Fatalf("expired command settlement = expired:%#v committed:%#v deferred:%#v failed:%#v", store.expired, store.committed, store.deferred, store.failed)
	}
}

func TestConnectionRegistrationRunnerRejectsTamperedBrokerResultWithoutActivation(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	claim := testConnectionRegistrationClaim(t)
	store := &fakeConnectionRegistrationStore{claims: []ConnectionRegistrationClaim{claim}}
	transport := &fakeConnectionRegistrationTransport{}
	transport.request = func(_ context.Context, _ string, _ ConnectionRegistrationCommand, signed SignedConnectionRegistrationCommand, _ ConnectionRegistrationRequest) (BrokerRegistration, error) {
		if signed.RequestSHA256 == signed.PayloadSHA256 {
			t.Fatal("test precondition: request and payload hashes must be distinct")
		}
		registration := testBrokerRegistration(t, claim, signed)
		registration.RequestSHA256 = signed.PayloadSHA256
		return registration, nil
	}
	runner := NewConnectionRegistrationRunner(store, transport, connectionRegistrationTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.failed) != 1 || store.failed[0].code != invalidConnectionRegistrationResultCode {
		t.Fatalf("tampered broker result must fail closed: %#v", store.failed)
	}
	if len(store.committed) != 0 || len(store.deferred) != 0 || len(store.expired) != 0 {
		t.Fatalf("tampered broker result must not activate: committed=%#v deferred=%#v expired=%#v", store.committed, store.deferred, store.expired)
	}
}

type fakeConnectionRegistrationStore struct {
	claims    []ConnectionRegistrationClaim
	started   []ConnectionRegistrationClaim
	persisted []connectionRegistrationPersistRecord
	committed []connectionRegistrationCommitRecord
	deferred  []connectionRegistrationDeferRecord
	expired   []ConnectionRegistrationClaim
	failed    []connectionRegistrationFailRecord
	trace     connectionRegistrationTrace
}

func (s *fakeConnectionRegistrationStore) ClaimConnectionRegistration(context.Context, string, time.Duration) (ConnectionRegistrationClaim, bool, error) {
	if len(s.claims) == 0 {
		return ConnectionRegistrationClaim{}, false, nil
	}
	claim := s.claims[0]
	s.claims = s.claims[1:]
	return claim, true, nil
}

func (s *fakeConnectionRegistrationStore) MarkConnectionRegistrationStarted(_ context.Context, claim ConnectionRegistrationClaim) error {
	s.started = append(s.started, claim)
	s.trace.events = append(s.trace.events, "started")
	return nil
}

func (s *fakeConnectionRegistrationStore) PersistConnectionRegistrationCommand(_ context.Context, claim ConnectionRegistrationClaim, signed SignedConnectionRegistrationCommand) error {
	s.persisted = append(s.persisted, connectionRegistrationPersistRecord{claim: claim, signed: signed})
	s.trace.events = append(s.trace.events, "persisted")
	return nil
}

func (s *fakeConnectionRegistrationStore) CommitConnectionRegistration(_ context.Context, claim ConnectionRegistrationClaim, registration BrokerRegistration) error {
	s.committed = append(s.committed, connectionRegistrationCommitRecord{claim: claim, registration: registration})
	s.trace.events = append(s.trace.events, "committed")
	return nil
}

func (s *fakeConnectionRegistrationStore) DeferConnectionRegistration(_ context.Context, claim ConnectionRegistrationClaim, code string, availableAt time.Time) error {
	s.deferred = append(s.deferred, connectionRegistrationDeferRecord{claim: claim, code: code, availableAt: availableAt})
	return nil
}

func (s *fakeConnectionRegistrationStore) ExpireConnectionRegistrationCommand(_ context.Context, claim ConnectionRegistrationClaim) error {
	s.expired = append(s.expired, claim)
	return nil
}

func (s *fakeConnectionRegistrationStore) FailConnectionRegistration(_ context.Context, claim ConnectionRegistrationClaim, code string) error {
	s.failed = append(s.failed, connectionRegistrationFailRecord{claim: claim, code: code})
	return nil
}

type fakeConnectionRegistrationTransport struct {
	built    []connectionRegistrationBuildRecord
	requests []connectionRegistrationRequestRecord
	build    func(ConnectionRegistrationCommand, ConnectionRegistrationRequest) (SignedConnectionRegistrationCommand, error)
	request  func(context.Context, string, ConnectionRegistrationCommand, SignedConnectionRegistrationCommand, ConnectionRegistrationRequest) (BrokerRegistration, error)
	trace    *connectionRegistrationTrace
}

func (t *fakeConnectionRegistrationTransport) BuildConnectionRegistrationCommand(command ConnectionRegistrationCommand, request ConnectionRegistrationRequest) (SignedConnectionRegistrationCommand, error) {
	t.built = append(t.built, connectionRegistrationBuildRecord{command: command, request: request})
	if t.trace != nil {
		t.trace.events = append(t.trace.events, "built")
	}
	if t.build != nil {
		return t.build(command, request)
	}
	return testSignedConnectionRegistrationCommand(), nil
}

func (t *fakeConnectionRegistrationTransport) RequestConnectionRegistration(ctx context.Context, endpoint string, command ConnectionRegistrationCommand, signed SignedConnectionRegistrationCommand, request ConnectionRegistrationRequest) (BrokerRegistration, error) {
	t.requests = append(t.requests, connectionRegistrationRequestRecord{endpoint: endpoint, command: command, signed: signed, request: request})
	if t.trace != nil {
		t.trace.events = append(t.trace.events, "requested")
	}
	if t.request != nil {
		return t.request(ctx, endpoint, command, signed, request)
	}
	return BrokerRegistration{}, errors.New("unexpected broker verification")
}

type connectionRegistrationTrace struct{ events []string }

type connectionRegistrationBuildRecord struct {
	command ConnectionRegistrationCommand
	request ConnectionRegistrationRequest
}

type connectionRegistrationRequestRecord struct {
	endpoint string
	command  ConnectionRegistrationCommand
	signed   SignedConnectionRegistrationCommand
	request  ConnectionRegistrationRequest
}

type connectionRegistrationPersistRecord struct {
	claim  ConnectionRegistrationClaim
	signed SignedConnectionRegistrationCommand
}

type connectionRegistrationCommitRecord struct {
	claim        ConnectionRegistrationClaim
	registration BrokerRegistration
}

type connectionRegistrationDeferRecord struct {
	claim       ConnectionRegistrationClaim
	code        string
	availableAt time.Time
}

type connectionRegistrationFailRecord struct {
	claim ConnectionRegistrationClaim
	code  string
}

func connectionRegistrationTestConfig(now time.Time) Config {
	return Config{
		WorkerID:       "orchestrator-registration-test",
		Lease:          2 * time.Minute,
		AttemptTimeout: time.Minute,
		RetryDelay:     time.Minute,
		Now:            func() time.Time { return now },
	}
}

func testConnectionRegistrationClaim(t *testing.T) ConnectionRegistrationClaim {
	t.Helper()
	request := ConnectionRegistrationRequest{
		BootstrapID:     "bootstrap-registration-1",
		RequestedRegion: "ap-south-1",
		StackARN:        "arn:aws:cloudformation:ap-south-1:123456789012:stack/dirextalk-registration-1/0123456789abcdef",
	}
	digest, err := request.Digest()
	if err != nil {
		t.Fatalf("registration request digest: %v", err)
	}
	return ConnectionRegistrationClaim{
		OutboxID:           "cloud-outbox-registration-1",
		Kind:               ConnectionRegistrationRequested,
		AggregateType:      "connection_bootstrap",
		AggregateID:        request.BootstrapID,
		BootstrapID:        request.BootstrapID,
		ConnectionID:       "connection-registration-1",
		RequestedRegion:    request.RequestedRegion,
		BrokerEndpoint:     "https://abcdefghij.execute-api.ap-south-1.amazonaws.com/prod/v2/commands",
		StackARN:           request.StackARN,
		NodeKeyID:          "node-key-1",
		ExpectedGeneration: 1,
		JobID:              "job-registration-1",
		LeaseToken:         "lease-registration-1",
		Attempt:            1,
		Request:            request,
		Command: ConnectionRegistrationCommand{
			CommandID:          "command-registration-1",
			BootstrapID:        request.BootstrapID,
			ConnectionID:       "connection-registration-1",
			NodeKeyID:          "node-key-1",
			ExpectedGeneration: 1,
			NodeCounter:        1,
			Attempt:            1,
			RequestDigest:      digest,
		},
	}
}

func testSignedConnectionRegistrationCommand() SignedConnectionRegistrationCommand {
	issuedAt := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	return SignedConnectionRegistrationCommand{
		EnvelopeJSON:  `{"schema":"dirextalk.aws.command/v2","command_id":"command-registration-1"}`,
		PayloadJSON:   `{"bootstrap_id":"bootstrap-registration-1","requested_region":"ap-south-1"}`,
		PayloadSHA256: strings.Repeat("a", 64),
		RequestSHA256: strings.Repeat("b", 64),
		IssuedAt:      issuedAt,
		ExpiresAt:     issuedAt.Add(4 * time.Minute),
	}
}

func testBrokerRegistration(t *testing.T, claim ConnectionRegistrationClaim, signed SignedConnectionRegistrationCommand) BrokerRegistration {
	t.Helper()
	registration := BrokerRegistration{
		Schema:               "dirextalk.aws.connection-registration/v1",
		BootstrapID:          claim.BootstrapID,
		ConnectionID:         claim.ConnectionID,
		AccountID:            "123456789012",
		Region:               claim.RequestedRegion,
		BrokerCommandURL:     claim.BrokerEndpoint,
		NodeKeyID:            claim.NodeKeyID,
		ConnectionGeneration: claim.ExpectedGeneration,
		StackARN:             claim.StackARN,
		CommandID:            claim.Command.CommandID,
		RequestSHA256:        signed.RequestSHA256,
		ReceiptJSON:          `{"disposition":"registered"}`,
	}
	if err := ValidateBrokerRegistration(claim, signed, registration); err != nil {
		t.Fatalf("test broker registration must be valid: %v", err)
	}
	return registration
}
