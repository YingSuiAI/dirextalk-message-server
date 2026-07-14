package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDeploymentProvisionRunnerPersistsExactEnvelopeBeforeBrokerCreate(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	claim := testDeploymentProvisionClaim(t)
	store := &fakeDeploymentProvisionStore{claims: []DeploymentProvisionClaim{claim}}
	transport := &fakeDeploymentProvisionTransport{trace: &store.trace}
	transport.request = func(_ context.Context, _ string, command DeploymentCreateCommand, signed SignedDeploymentCreateCommand, request DeploymentCreateRequest) (BrokerDeployment, error) {
		if len(store.persisted) != 1 {
			return BrokerDeployment{}, errors.New("broker create occurred before durable envelope persistence")
		}
		if !reflect.DeepEqual(store.persisted[0].signed, signed) {
			return BrokerDeployment{}, errors.New("broker create did not use the persisted envelope")
		}
		if command.CommandID != claim.Command.CommandID || request.DeploymentID != claim.DeploymentID || request.ManifestDigest == "" {
			return BrokerDeployment{}, errors.New("broker create lost its approved deployment binding")
		}
		return testBrokerDeployment(t, claim, signed), nil
	}
	runner := NewDeploymentProvisionRunner(store, transport, deploymentProvisionTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if got, want := store.trace.events, []string{"started", "built", "persisted", "requested", "committed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("durable provision order = %#v, want %#v", got, want)
	}
	if len(store.persisted) != 1 || len(store.committed) != 1 || len(store.deferred) != 0 || len(store.expired) != 0 || len(store.failed) != 0 {
		t.Fatalf("provision settlement = persisted:%#v committed:%#v deferred:%#v expired:%#v failed:%#v", store.persisted, store.committed, store.deferred, store.expired, store.failed)
	}
}

func TestDeploymentProvisionRunnerReplaysPersistedEnvelopeAfterIndeterminateFailure(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	first := testDeploymentProvisionClaim(t)
	second := testDeploymentProvisionClaim(t)
	store := &fakeDeploymentProvisionStore{claims: []DeploymentProvisionClaim{first, second}}
	store.persistHook = func(_ DeploymentProvisionClaim, signed SignedDeploymentCreateCommand) {
		for index := range store.claims {
			store.claims[index].Command.PayloadJSON = signed.PayloadJSON
			store.claims[index].Command.PayloadSHA256 = signed.PayloadSHA256
			store.claims[index].Command.RequestSHA256 = signed.RequestSHA256
			store.claims[index].Command.SignedEnvelope = signed.EnvelopeJSON
			store.claims[index].Command.IssuedAt = signed.IssuedAt
			store.claims[index].Command.ExpiresAt = signed.ExpiresAt
		}
	}
	transport := &fakeDeploymentProvisionTransport{trace: &store.trace}
	requestCount := 0
	transport.request = func(_ context.Context, _ string, _ DeploymentCreateCommand, signed SignedDeploymentCreateCommand, _ DeploymentCreateRequest) (BrokerDeployment, error) {
		requestCount++
		if requestCount == 1 {
			return BrokerDeployment{}, DeploymentProvisionRetryable("broker_unavailable", errors.New("response lost after broker create"))
		}
		return testBrokerDeployment(t, second, signed), nil
	}
	runner := NewDeploymentProvisionRunner(store, transport, deploymentProvisionTestConfig(now))

	for attempt := 0; attempt < 2; attempt++ {
		processed, err := runner.RunOnce(context.Background())
		if err != nil || !processed {
			t.Fatalf("RunOnce attempt %d = processed:%v err:%v", attempt+1, processed, err)
		}
	}
	if len(transport.built) != 1 || len(store.persisted) != 1 {
		t.Fatalf("indeterminate retry must not re-sign or re-persist: builds=%#v persisted=%#v", transport.built, store.persisted)
	}
	if len(transport.requests) != 2 || transport.requests[0].signed.EnvelopeJSON != transport.requests[1].signed.EnvelopeJSON ||
		transport.requests[0].signed.PayloadSHA256 != transport.requests[1].signed.PayloadSHA256 ||
		transport.requests[0].signed.RequestSHA256 != transport.requests[1].signed.RequestSHA256 {
		t.Fatalf("retry must replay the exact persisted create envelope: %#v", transport.requests)
	}
	if len(store.deferred) != 1 || store.deferred[0].code != "broker_unavailable" || !store.deferred[0].availableAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("retryable create failure must only defer: %#v", store.deferred)
	}
	if len(store.committed) != 1 || len(store.expired) != 0 || len(store.failed) != 0 {
		t.Fatalf("retry settlement = committed:%#v expired:%#v failed:%#v", store.committed, store.expired, store.failed)
	}
}

func TestDeploymentProvisionRunnerRejectsReceiptWithDifferentSignedRequestHash(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	claim := testDeploymentProvisionClaim(t)
	store := &fakeDeploymentProvisionStore{claims: []DeploymentProvisionClaim{claim}}
	transport := &fakeDeploymentProvisionTransport{}
	transport.request = func(_ context.Context, _ string, _ DeploymentCreateCommand, signed SignedDeploymentCreateCommand, _ DeploymentCreateRequest) (BrokerDeployment, error) {
		result := testBrokerDeployment(t, claim, signed)
		result.RequestSHA256 = strings.Repeat("f", 64)
		return result, nil
	}
	runner := NewDeploymentProvisionRunner(store, transport, deploymentProvisionTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.failed) != 1 || store.failed[0].code != invalidDeploymentProvisionResultCode || len(store.committed) != 0 {
		t.Fatalf("unbound request receipt must fail: failed=%#v committed=%#v", store.failed, store.committed)
	}
}

func TestDeploymentProvisionRunnerDoesNotSignOrSendExpiredQuote(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	claim := testDeploymentProvisionClaim(t)
	claim.QuoteValidUntil = now
	store := &fakeDeploymentProvisionStore{claims: []DeploymentProvisionClaim{claim}}
	transport := &fakeDeploymentProvisionTransport{}
	runner := NewDeploymentProvisionRunner(store, transport, deploymentProvisionTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.failed) != 1 || store.failed[0].code != "quote_expired_before_provision" || len(store.started) != 0 || len(transport.built) != 0 || len(transport.requests) != 0 {
		t.Fatalf("expired quote must not start, sign, or send: failed=%#v started=%#v builds=%#v requests=%#v", store.failed, store.started, transport.built, transport.requests)
	}
}

func TestDeploymentProvisionRunnerExpiresOnlyExplicitBrokerExpiredCommand(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	claim := testDeploymentProvisionClaim(t)
	store := &fakeDeploymentProvisionStore{claims: []DeploymentProvisionClaim{claim}}
	transport := &fakeDeploymentProvisionTransport{}
	transport.request = func(context.Context, string, DeploymentCreateCommand, SignedDeploymentCreateCommand, DeploymentCreateRequest) (BrokerDeployment, error) {
		return BrokerDeployment{}, DeploymentCreateCommandExpired(errors.New("expired_command"))
	}
	runner := NewDeploymentProvisionRunner(store, transport, deploymentProvisionTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.expired) != 1 || len(store.committed) != 0 || len(store.deferred) != 0 || len(store.failed) != 0 {
		t.Fatalf("expired command settlement = expired:%#v committed:%#v deferred:%#v failed:%#v", store.expired, store.committed, store.deferred, store.failed)
	}
}

type fakeDeploymentProvisionStore struct {
	claims      []DeploymentProvisionClaim
	started     []DeploymentProvisionClaim
	persisted   []deploymentPersistRecord
	committed   []deploymentCommitRecord
	deferred    []deploymentDeferRecord
	expired     []DeploymentProvisionClaim
	failed      []deploymentFailRecord
	persistHook func(DeploymentProvisionClaim, SignedDeploymentCreateCommand)
	trace       deploymentTrace
}

func (s *fakeDeploymentProvisionStore) ClaimDeploymentProvision(context.Context, string, time.Duration) (DeploymentProvisionClaim, bool, error) {
	if len(s.claims) == 0 {
		return DeploymentProvisionClaim{}, false, nil
	}
	claim := s.claims[0]
	s.claims = s.claims[1:]
	return claim, true, nil
}

func (s *fakeDeploymentProvisionStore) MarkDeploymentProvisionStarted(_ context.Context, claim DeploymentProvisionClaim) error {
	s.started = append(s.started, claim)
	s.trace.events = append(s.trace.events, "started")
	return nil
}

func (s *fakeDeploymentProvisionStore) PersistDeploymentCreateCommand(_ context.Context, claim DeploymentProvisionClaim, signed SignedDeploymentCreateCommand) error {
	s.persisted = append(s.persisted, deploymentPersistRecord{claim: claim, signed: signed})
	s.trace.events = append(s.trace.events, "persisted")
	if s.persistHook != nil {
		s.persistHook(claim, signed)
	}
	return nil
}

func (s *fakeDeploymentProvisionStore) CommitDeploymentProvision(_ context.Context, claim DeploymentProvisionClaim, result BrokerDeployment) error {
	s.committed = append(s.committed, deploymentCommitRecord{claim: claim, result: result})
	s.trace.events = append(s.trace.events, "committed")
	return nil
}

func (s *fakeDeploymentProvisionStore) DeferDeploymentProvision(_ context.Context, claim DeploymentProvisionClaim, code string, availableAt time.Time) error {
	s.deferred = append(s.deferred, deploymentDeferRecord{claim: claim, code: code, availableAt: availableAt})
	return nil
}

func (s *fakeDeploymentProvisionStore) ExpireDeploymentCreateCommand(_ context.Context, claim DeploymentProvisionClaim) error {
	s.expired = append(s.expired, claim)
	return nil
}

func (s *fakeDeploymentProvisionStore) FailDeploymentProvision(_ context.Context, claim DeploymentProvisionClaim, code string) error {
	s.failed = append(s.failed, deploymentFailRecord{claim: claim, code: code})
	return nil
}

type fakeDeploymentProvisionTransport struct {
	built    []deploymentBuildRecord
	requests []deploymentRequestRecord
	build    func(DeploymentCreateCommand, DeploymentCreateRequest) (SignedDeploymentCreateCommand, error)
	request  func(context.Context, string, DeploymentCreateCommand, SignedDeploymentCreateCommand, DeploymentCreateRequest) (BrokerDeployment, error)
	trace    *deploymentTrace
}

func (t *fakeDeploymentProvisionTransport) BuildDeploymentCreateCommand(command DeploymentCreateCommand, request DeploymentCreateRequest) (SignedDeploymentCreateCommand, error) {
	t.built = append(t.built, deploymentBuildRecord{command: command, request: request})
	if t.trace != nil {
		t.trace.events = append(t.trace.events, "built")
	}
	if t.build != nil {
		return t.build(command, request)
	}
	return testSignedDeploymentCreateCommand(), nil
}

func (t *fakeDeploymentProvisionTransport) RequestDeploymentCreate(ctx context.Context, endpoint string, command DeploymentCreateCommand, signed SignedDeploymentCreateCommand, request DeploymentCreateRequest) (BrokerDeployment, error) {
	t.requests = append(t.requests, deploymentRequestRecord{endpoint: endpoint, command: command, signed: signed, request: request})
	if t.trace != nil {
		t.trace.events = append(t.trace.events, "requested")
	}
	if t.request != nil {
		return t.request(ctx, endpoint, command, signed, request)
	}
	return BrokerDeployment{}, errors.New("unexpected broker request")
}

type deploymentTrace struct{ events []string }

type deploymentBuildRecord struct {
	command DeploymentCreateCommand
	request DeploymentCreateRequest
}

type deploymentRequestRecord struct {
	endpoint string
	command  DeploymentCreateCommand
	signed   SignedDeploymentCreateCommand
	request  DeploymentCreateRequest
}

type deploymentPersistRecord struct {
	claim  DeploymentProvisionClaim
	signed SignedDeploymentCreateCommand
}

type deploymentCommitRecord struct {
	claim  DeploymentProvisionClaim
	result BrokerDeployment
}

type deploymentDeferRecord struct {
	claim       DeploymentProvisionClaim
	code        string
	availableAt time.Time
}

type deploymentFailRecord struct {
	claim DeploymentProvisionClaim
	code  string
}

func deploymentProvisionTestConfig(now time.Time) Config {
	return Config{
		WorkerID:       "orchestrator-deployment-test",
		Lease:          2 * time.Minute,
		AttemptTimeout: time.Minute,
		RetryDelay:     time.Minute,
		Now:            func() time.Time { return now },
	}
}

func testDeploymentProvisionClaim(t *testing.T) DeploymentProvisionClaim {
	t.Helper()
	request := DeploymentCreateRequest{
		DeploymentID:   "deployment-create-0001",
		PlanRevision:   4,
		PlanHash:       testDeploymentDigest('a'),
		QuoteID:        "quote-create-0001",
		QuoteDigest:    testDeploymentDigest('b'),
		CandidateID:    "recommended-create-0001",
		ManifestDigest: testDeploymentDigest('c'),
		WorkerArtifact: WorkerArtifactReferenceV1{Kind: "ami", AMIID: "ami-0123456789abcdef0"},
		Network: DeploymentNetworkReference{
			VPCID: "vpc-0123456789abcdef0", SubnetID: "subnet-0123456789abcdef0", AvailabilityZone: "us-east-1a",
		},
	}
	digest, err := request.Digest()
	if err != nil {
		t.Fatalf("deployment request digest: %v", err)
	}
	return DeploymentProvisionClaim{
		OutboxID:           "cloud-outbox-deployment-0001",
		Kind:               DeploymentProvisionRequested,
		AggregateType:      "deployment",
		AggregateID:        request.DeploymentID,
		DeploymentID:       request.DeploymentID,
		PlanID:             "plan-create-0001",
		ConnectionID:       "connection-create-0001",
		Region:             "us-east-1",
		PlanRevision:       int64(request.PlanRevision),
		QuoteValidUntil:    time.Date(2026, time.July, 14, 12, 15, 0, 0, time.UTC),
		BrokerEndpoint:     "https://a1b2c3d4e5.execute-api.us-east-1.amazonaws.com/prod/v2/commands",
		ExpectedGeneration: 1,
		NodeKeyID:          "node-key-1",
		JobID:              "cloud-job-provision-0001",
		LeaseToken:         "lease-deployment-0001",
		Attempt:            1,
		Request:            request,
		Command: DeploymentCreateCommand{
			CommandID:          "command-deployment-0001",
			DeploymentID:       request.DeploymentID,
			ConnectionID:       "connection-create-0001",
			NodeKeyID:          "node-key-1",
			ExpectedGeneration: 1,
			NodeCounter:        1,
			Attempt:            1,
			RequestDigest:      digest,
		},
	}
}

func testSignedDeploymentCreateCommand() SignedDeploymentCreateCommand {
	issuedAt := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	return SignedDeploymentCreateCommand{
		EnvelopeJSON:  `{"schema":"dirextalk.aws.command/v2","command_id":"command-deployment-0001"}`,
		PayloadJSON:   `{"deployment_id":"deployment-create-0001"}`,
		PayloadSHA256: strings.Repeat("a", 64),
		RequestSHA256: strings.Repeat("b", 64),
		IssuedAt:      issuedAt,
		ExpiresAt:     issuedAt.Add(4 * time.Minute),
	}
}

func testBrokerDeployment(t *testing.T, claim DeploymentProvisionClaim, signed SignedDeploymentCreateCommand) BrokerDeployment {
	t.Helper()
	result := BrokerDeployment{
		Schema:              deploymentReceiptSchema,
		DeploymentID:        claim.DeploymentID,
		ConnectionID:        claim.ConnectionID,
		CommandID:           claim.Command.CommandID,
		RequestSHA256:       signed.RequestSHA256,
		ResourceStatus:      "provisioning",
		InstanceID:          "i-0123456789abcdef0",
		VolumeIDs:           []string{"vol-0123456789abcdef0"},
		NetworkInterfaceIDs: []string{"eni-0123456789abcdef0"},
		ReceiptJSON:         `{"schema":"dirextalk.aws.command-receipt/v2","disposition":"committed"}`,
	}
	if err := ValidateBrokerDeployment(claim, signed, result); err != nil {
		t.Fatalf("test broker deployment must be valid: %v", err)
	}
	return result
}

func testDeploymentDigest(character rune) string {
	return "sha256:" + strings.Repeat(string(character), 64)
}
