package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestQuoteRunnerPersistsSignedEnvelopeBeforeBrokerRequest(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	claim := testQuoteClaim(t)
	store := &fakeQuoteStore{claims: []QuoteClaim{claim}}
	transport := &fakeQuoteTransport{trace: &store.trace}
	transport.request = func(_ context.Context, _ string, command QuoteCommand, signed SignedQuoteCommand, request cloudcontracts.QuoteRequestV1) (BrokerQuote, error) {
		if len(store.persisted) != 1 {
			return BrokerQuote{}, errors.New("broker request occurred before durable envelope persistence")
		}
		if !reflect.DeepEqual(store.persisted[0].signed, signed) {
			return BrokerQuote{}, errors.New("broker request did not use the persisted envelope")
		}
		if command.CommandID != claim.Command.CommandID || request.QuoteRequestID != claim.Request.QuoteRequestID {
			return BrokerQuote{}, errors.New("broker request lost its durable binding")
		}
		return testBrokerQuote(t, claim, signed), nil
	}
	runner := NewQuoteRunner(store, transport, quoteTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if got, want := store.trace.events, []string{"started", "built", "persisted", "requested", "committed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("durable quote order = %#v, want %#v", got, want)
	}
	if len(store.persisted) != 1 || len(store.committed) != 1 || len(store.deferred) != 0 || len(store.failed) != 0 || len(store.expired) != 0 {
		t.Fatalf("quote settlement = persisted:%#v committed:%#v deferred:%#v failed:%#v expired:%#v", store.persisted, store.committed, store.deferred, store.failed, store.expired)
	}
}

func TestQuoteRunnerDefersRetryableFailureAndReplaysPersistedEnvelope(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	first := testQuoteClaim(t)
	second := testQuoteClaim(t)
	store := &fakeQuoteStore{claims: []QuoteClaim{first, second}}
	store.persistHook = func(_ QuoteClaim, signed SignedQuoteCommand) {
		for index := range store.claims {
			store.claims[index].Command.PayloadJSON = signed.PayloadJSON
			store.claims[index].Command.PayloadSHA256 = signed.PayloadSHA256
			store.claims[index].Command.RequestSHA256 = signed.RequestSHA256
			store.claims[index].Command.SignedEnvelope = signed.EnvelopeJSON
			store.claims[index].Command.IssuedAt = signed.IssuedAt
			store.claims[index].Command.ExpiresAt = signed.ExpiresAt
		}
	}
	transport := &fakeQuoteTransport{trace: &store.trace}
	requestCount := 0
	transport.request = func(_ context.Context, _ string, _ QuoteCommand, signed SignedQuoteCommand, _ cloudcontracts.QuoteRequestV1) (BrokerQuote, error) {
		requestCount++
		if requestCount == 1 {
			return BrokerQuote{}, QuoteRetryable("broker_unavailable", errors.New("temporary broker outage"))
		}
		return testBrokerQuote(t, second, signed), nil
	}
	runner := NewQuoteRunner(store, transport, quoteTestConfig(now))

	for attempt := 0; attempt < 2; attempt++ {
		processed, err := runner.RunOnce(context.Background())
		if err != nil || !processed {
			t.Fatalf("RunOnce attempt %d = processed:%v err:%v", attempt+1, processed, err)
		}
	}
	if len(transport.built) != 1 || len(store.persisted) != 1 {
		t.Fatalf("indeterminate retry must not re-sign: builds=%#v persisted=%#v", transport.built, store.persisted)
	}
	if len(transport.requests) != 2 || transport.requests[0].signed.EnvelopeJSON != transport.requests[1].signed.EnvelopeJSON ||
		transport.requests[0].signed.PayloadSHA256 != transport.requests[1].signed.PayloadSHA256 ||
		transport.requests[0].signed.RequestSHA256 != transport.requests[1].signed.RequestSHA256 {
		t.Fatalf("retry must replay the exact persisted envelope: %#v", transport.requests)
	}
	if len(store.deferred) != 1 || store.deferred[0].code != "broker_unavailable" || !store.deferred[0].availableAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("retryable failure must only defer: %#v", store.deferred)
	}
	if len(store.expired) != 0 || len(store.failed) != 0 || len(store.committed) != 1 {
		t.Fatalf("retry settlement = committed:%#v expired:%#v failed:%#v", store.committed, store.expired, store.failed)
	}
}

func TestQuoteRunnerExpiresOnlyExactExpiredBrokerCommand(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	claim := testQuoteClaim(t)
	store := &fakeQuoteStore{claims: []QuoteClaim{claim}}
	transport := &fakeQuoteTransport{trace: &store.trace}
	transport.request = func(context.Context, string, QuoteCommand, SignedQuoteCommand, cloudcontracts.QuoteRequestV1) (BrokerQuote, error) {
		return BrokerQuote{}, QuoteCommandExpired(errors.New("expired_command"))
	}
	runner := NewQuoteRunner(store, transport, quoteTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.expired) != 1 || len(store.committed) != 0 || len(store.deferred) != 0 || len(store.failed) != 0 {
		t.Fatalf("expired command settlement = expired:%#v committed:%#v deferred:%#v failed:%#v", store.expired, store.committed, store.deferred, store.failed)
	}
}

func TestQuoteRunnerRejectsBrokerQuoteBoundToPayloadHash(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	claim := testQuoteClaim(t)
	store := &fakeQuoteStore{claims: []QuoteClaim{claim}}
	transport := &fakeQuoteTransport{trace: &store.trace}
	transport.request = func(_ context.Context, _ string, _ QuoteCommand, signed SignedQuoteCommand, _ cloudcontracts.QuoteRequestV1) (BrokerQuote, error) {
		if signed.RequestSHA256 == signed.PayloadSHA256 {
			t.Fatal("test precondition: request and payload hashes must be distinct")
		}
		quote := testBrokerQuote(t, claim, signed)
		quote.RequestSHA256 = signed.PayloadSHA256
		return quote, nil
	}
	runner := NewQuoteRunner(store, transport, quoteTestConfig(now))

	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
	}
	if len(store.failed) != 1 || store.failed[0].code != invalidQuoteResultCode || len(store.committed) != 0 {
		t.Fatalf("payload-hash-bound quote must fail: failed=%#v committed=%#v", store.failed, store.committed)
	}
}

func TestQuoteRunnerRejectsMismatchedBrokerCandidatesAndCoverage(t *testing.T) {
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name   string
		mutate func(*BrokerQuote)
	}{
		{
			name: "candidate",
			mutate: func(quote *BrokerQuote) {
				quote.Candidates[0].InstanceType = "m7i.2xlarge"
			},
		},
		{
			name: "coverage",
			mutate: func(quote *BrokerQuote) {
				quote.UnincludedItems = quote.UnincludedItems[:len(quote.UnincludedItems)-1]
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			claim := testQuoteClaim(t)
			store := &fakeQuoteStore{claims: []QuoteClaim{claim}}
			transport := &fakeQuoteTransport{trace: &store.trace}
			transport.request = func(_ context.Context, _ string, _ QuoteCommand, signed SignedQuoteCommand, _ cloudcontracts.QuoteRequestV1) (BrokerQuote, error) {
				quote := testBrokerQuote(t, claim, signed)
				testCase.mutate(&quote)
				return quote, nil
			}
			runner := NewQuoteRunner(store, transport, quoteTestConfig(now))

			processed, err := runner.RunOnce(context.Background())
			if err != nil || !processed {
				t.Fatalf("RunOnce = processed:%v err:%v", processed, err)
			}
			if len(store.failed) != 1 || store.failed[0].code != invalidQuoteResultCode || len(store.committed) != 0 {
				t.Fatalf("mismatched broker quote must fail: failed=%#v committed=%#v", store.failed, store.committed)
			}
		})
	}
}

type fakeQuoteStore struct {
	claims      []QuoteClaim
	started     []QuoteClaim
	persisted   []quotePersistRecord
	committed   []quoteCommitRecord
	deferred    []quoteDeferRecord
	expired     []QuoteClaim
	failed      []quoteFailRecord
	persistHook func(QuoteClaim, SignedQuoteCommand)
	trace       quoteTrace
}

func (s *fakeQuoteStore) ClaimQuoteRequest(context.Context, string, time.Duration) (QuoteClaim, bool, error) {
	if len(s.claims) == 0 {
		return QuoteClaim{}, false, nil
	}
	claim := s.claims[0]
	s.claims = s.claims[1:]
	return claim, true, nil
}

func (s *fakeQuoteStore) MarkQuoteStarted(_ context.Context, claim QuoteClaim) error {
	s.started = append(s.started, claim)
	s.trace.events = append(s.trace.events, "started")
	return nil
}

func (s *fakeQuoteStore) PersistQuoteCommand(_ context.Context, claim QuoteClaim, signed SignedQuoteCommand) error {
	s.persisted = append(s.persisted, quotePersistRecord{claim: claim, signed: signed})
	s.trace.events = append(s.trace.events, "persisted")
	if s.persistHook != nil {
		s.persistHook(claim, signed)
	}
	return nil
}

func (s *fakeQuoteStore) CommitQuote(_ context.Context, claim QuoteClaim, quote BrokerQuote) error {
	s.committed = append(s.committed, quoteCommitRecord{claim: claim, quote: quote})
	s.trace.events = append(s.trace.events, "committed")
	return nil
}

func (s *fakeQuoteStore) DeferQuote(_ context.Context, claim QuoteClaim, code string, availableAt time.Time) error {
	s.deferred = append(s.deferred, quoteDeferRecord{claim: claim, code: code, availableAt: availableAt})
	return nil
}

func (s *fakeQuoteStore) ExpireQuoteCommand(_ context.Context, claim QuoteClaim) error {
	s.expired = append(s.expired, claim)
	return nil
}

func (s *fakeQuoteStore) FailQuote(_ context.Context, claim QuoteClaim, code string) error {
	s.failed = append(s.failed, quoteFailRecord{claim: claim, code: code})
	return nil
}

type fakeQuoteTransport struct {
	built    []quoteBuildRecord
	requests []quoteRequestRecord
	build    func(QuoteCommand, cloudcontracts.QuoteRequestV1) (SignedQuoteCommand, error)
	request  func(context.Context, string, QuoteCommand, SignedQuoteCommand, cloudcontracts.QuoteRequestV1) (BrokerQuote, error)
	trace    *quoteTrace
}

func (t *fakeQuoteTransport) BuildQuoteCommand(command QuoteCommand, request cloudcontracts.QuoteRequestV1) (SignedQuoteCommand, error) {
	t.built = append(t.built, quoteBuildRecord{command: command, request: request})
	if t.trace != nil {
		t.trace.events = append(t.trace.events, "built")
	}
	if t.build != nil {
		return t.build(command, request)
	}
	return testSignedQuoteCommand(), nil
}

func (t *fakeQuoteTransport) RequestQuote(ctx context.Context, endpoint string, command QuoteCommand, signed SignedQuoteCommand, request cloudcontracts.QuoteRequestV1) (BrokerQuote, error) {
	t.requests = append(t.requests, quoteRequestRecord{endpoint: endpoint, command: command, signed: signed, request: request})
	if t.trace != nil {
		t.trace.events = append(t.trace.events, "requested")
	}
	if t.request != nil {
		return t.request(ctx, endpoint, command, signed, request)
	}
	return BrokerQuote{}, errors.New("unexpected broker request")
}

type quoteTrace struct{ events []string }

type quoteBuildRecord struct {
	command QuoteCommand
	request cloudcontracts.QuoteRequestV1
}

type quoteRequestRecord struct {
	endpoint string
	command  QuoteCommand
	signed   SignedQuoteCommand
	request  cloudcontracts.QuoteRequestV1
}

type quotePersistRecord struct {
	claim  QuoteClaim
	signed SignedQuoteCommand
}

type quoteCommitRecord struct {
	claim QuoteClaim
	quote BrokerQuote
}

type quoteDeferRecord struct {
	claim       QuoteClaim
	code        string
	availableAt time.Time
}

type quoteFailRecord struct {
	claim QuoteClaim
	code  string
}

func quoteTestConfig(now time.Time) Config {
	return Config{
		WorkerID:       "orchestrator-quote-test",
		Lease:          2 * time.Minute,
		AttemptTimeout: time.Minute,
		RetryDelay:     time.Minute,
		Now:            func() time.Time { return now },
	}
}

func testQuoteClaim(t *testing.T) QuoteClaim {
	t.Helper()
	request := cloudcontracts.QuoteRequestV1{
		SchemaVersion:     cloudcontracts.SchemaVersionV1,
		QuoteRequestID:    "quote-request-1",
		PlanID:            "plan-1",
		PlanRevision:      2,
		CloudConnectionID: "connection-1",
		RecipeDigest:      "sha256:" + strings.Repeat("a", 64),
		Region:            "ap-south-1",
		Candidates: []cloudcontracts.QuoteRequestCandidateV1{{
			CandidateID: "recommended", Tier: cloudcontracts.QuoteTierRecommended, InstanceType: "m7i.xlarge",
			PurchaseOption: cloudcontracts.PurchaseOnDemand, EstimatedDiskGiB: 80,
		}},
	}
	digest, err := request.Digest()
	if err != nil {
		t.Fatalf("quote request digest: %v", err)
	}
	return QuoteClaim{
		OutboxID:           "cloud-outbox-quote-1",
		Kind:               QuotePlanRequested,
		AggregateType:      "plan",
		AggregateID:        request.PlanID,
		PlanID:             request.PlanID,
		ConnectionID:       request.CloudConnectionID,
		PlanRevision:       int64(request.PlanRevision),
		LeaseToken:         "lease-quote-1",
		BrokerEndpoint:     "https://broker.example.test/v2/commands",
		ExpectedGeneration: 1,
		NodeKeyID:          "node-key-1",
		Request:            request,
		Command: QuoteCommand{
			CommandID:          "broker-command-1",
			ConnectionID:       request.CloudConnectionID,
			NodeKeyID:          "node-key-1",
			ExpectedGeneration: 1,
			NodeCounter:        1,
			Attempt:            1,
			RequestDigest:      digest,
		},
	}
}

func testSignedQuoteCommand() SignedQuoteCommand {
	issuedAt := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	return SignedQuoteCommand{
		EnvelopeJSON:  `{"schema":"dirextalk.aws.command/v2","command_id":"broker-command-1"}`,
		PayloadJSON:   `{"quote_request_id":"quote-request-1"}`,
		PayloadSHA256: strings.Repeat("a", 64),
		RequestSHA256: strings.Repeat("b", 64),
		IssuedAt:      issuedAt,
		ExpiresAt:     issuedAt.Add(4 * time.Minute),
	}
}

func testBrokerQuote(t *testing.T, claim QuoteClaim, signed SignedQuoteCommand) BrokerQuote {
	t.Helper()
	digest, err := claim.Request.Digest()
	if err != nil {
		t.Fatalf("quote request digest: %v", err)
	}
	candidates := make([]cloudcontracts.QuoteCandidateV1, len(claim.Request.Candidates))
	for index, candidate := range claim.Request.Candidates {
		candidates[index] = cloudcontracts.QuoteCandidateV1{
			CandidateID: candidate.CandidateID, Tier: candidate.Tier, InstanceType: candidate.InstanceType,
			PurchaseOption: candidate.PurchaseOption, Architecture: cloudcontracts.ArchitectureAMD64, VCPU: 4, MemoryMiB: 16384,
			GPUCount: 0, GPUMemoryMiB: 0, HourlyMinor: 128, ThirtyDayMinor: 92160,
			StartupUpperMinor: 0, EstimatedDiskGiB: candidate.EstimatedDiskGiB,
		}
	}
	quotedAt := signed.IssuedAt.Add(time.Second)
	quote := BrokerQuote{
		Schema:          "dirextalk.aws.quote/v1",
		QuoteID:         "quote-" + signed.RequestSHA256[:32],
		ConnectionID:    claim.ConnectionID,
		CommandID:       claim.Command.CommandID,
		RequestSHA256:   signed.RequestSHA256,
		QuoteRequestID:  claim.Request.QuoteRequestID,
		PlanDigest:      digest,
		Region:          claim.Request.Region,
		Currency:        "USD",
		QuotedAt:        quotedAt,
		ValidUntil:      quotedAt.Add(15 * time.Minute),
		Candidates:      candidates,
		IncludedItems:   append([]string(nil), requiredQuoteIncludedItems...),
		UnincludedItems: append([]string(nil), requiredQuoteUnincludedItems...),
		ReceiptJSON:     `{"disposition":"committed"}`,
	}
	if err := ValidateBrokerQuote(claim, signed, quote); err != nil {
		t.Fatalf("test broker quote must be valid: %v", err)
	}
	return quote
}
