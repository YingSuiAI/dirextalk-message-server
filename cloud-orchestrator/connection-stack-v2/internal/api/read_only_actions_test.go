package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

func TestBrokerCommitsRegistrationAndReplaysAfterExpiry(t *testing.T) {
	privateKey := testPrivateKey()
	now := time.Date(2026, 7, 15, 1, 2, 4, 0, time.UTC)
	store := newMemoryCommandStore()
	attestor := &recordingRegistrationAttestor{}
	broker := readOnlyTestBroker(privateKey, store, attestor, &recordingQuoteProvider{}, func() time.Time { return now })
	raw := signedRegistrationCommand(t, privateKey, "command-registration-0001", 7)

	first := serveWithGatewayRuntime(t, broker, raw)
	assertResultBinding(t, first, "connection_registered", "committed", contract.ActionRegistrationVerify)
	if attestor.calls != 1 {
		t.Fatalf("registration attestor calls = %d, want 1", attestor.calls)
	}

	now = now.Add(10 * time.Minute)
	replay := serveWithGatewayRuntime(t, broker, raw)
	assertResultBinding(t, replay, "idempotent", "idempotent", contract.ActionRegistrationVerify)
	if attestor.calls != 1 {
		t.Fatalf("expired exact replay called attestor %d times, want 1", attestor.calls)
	}
}

func TestBrokerRejectsCommandIdentityAndCounterConflicts(t *testing.T) {
	privateKey := testPrivateKey()
	store := newMemoryCommandStore()
	broker := readOnlyTestBroker(
		privateKey,
		store,
		&recordingRegistrationAttestor{},
		&recordingQuoteProvider{},
		func() time.Time { return time.Date(2026, 7, 15, 1, 2, 4, 0, time.UTC) },
	)

	first := serveWithGatewayRuntime(t, broker, signedRegistrationCommand(t, privateKey, "command-registration-0001", 7))
	if first.Code != 200 {
		t.Fatalf("first status = %d; body %s", first.Code, first.Body.String())
	}

	commandConflict := serveWithGatewayRuntime(t, broker, signedRegistrationCommand(t, privateKey, "command-registration-0001", 8))
	assertHTTPError(t, commandConflict, 409, "command_id_conflict")

	staleCounter := serve(t, broker, "POST", "/v2/commands", signedQuoteCommand(t, privateKey, "command-quote-0002", 6))
	assertHTTPError(t, staleCounter, 409, "stale_node_counter")
}

func TestBrokerCommitsQuoteAndNeverPersistsProviderFailure(t *testing.T) {
	privateKey := testPrivateKey()
	store := newMemoryCommandStore()
	provider := &recordingQuoteProvider{err: NewError("quote_provider_unavailable", 503)}
	broker := readOnlyTestBroker(
		privateKey,
		store,
		&recordingRegistrationAttestor{},
		provider,
		func() time.Time { return time.Date(2026, 7, 15, 1, 2, 4, 0, time.UTC) },
	)
	raw := signedQuoteCommand(t, privateKey, "command-quote-0001", 1)

	failed := serve(t, broker, "POST", "/v2/commands", raw)
	assertHTTPError(t, failed, 503, "quote_provider_unavailable")
	if len(store.records) != 0 || len(store.lastCounters) != 0 {
		t.Fatalf("provider failure persisted state: records=%d counters=%d", len(store.records), len(store.lastCounters))
	}

	provider.err = nil
	succeeded := serve(t, broker, "POST", "/v2/commands", raw)
	assertResultBinding(t, succeeded, "quote_issued", "committed", contract.ActionQuoteRequest)
	var result contract.QuoteResult
	if err := json.Unmarshal(succeeded.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode quote result: %v", err)
	}
	if result.Receipt.Quote == nil || !reflect.DeepEqual(*result.Receipt.Quote, result.Quote) {
		t.Fatalf("top-level quote and receipt quote differ: %#v", result)
	}
	if provider.calls != 2 {
		t.Fatalf("quote provider calls = %d, want failed attempt plus success", provider.calls)
	}

	replay := serve(t, broker, "POST", "/v2/commands", raw)
	assertResultBinding(t, replay, "idempotent", "idempotent", contract.ActionQuoteRequest)
	if provider.calls != 2 {
		t.Fatalf("idempotent replay called quote provider, calls=%d", provider.calls)
	}
}

func TestBrokerConcurrentExactReplayCreatesOneDurableReceipt(t *testing.T) {
	privateKey := testPrivateKey()
	store := newMemoryCommandStore()
	broker := readOnlyTestBroker(
		privateKey,
		store,
		&recordingRegistrationAttestor{},
		&recordingQuoteProvider{},
		func() time.Time { return time.Date(2026, 7, 15, 1, 2, 4, 0, time.UTC) },
	)
	raw := signedQuoteCommand(t, privateKey, "command-quote-concurrent", 11)

	const requests = 12
	responses := make(chan *httptest.ResponseRecorder, requests)
	var wait sync.WaitGroup
	for range requests {
		wait.Add(1)
		go func() {
			defer wait.Done()
			responses <- serveConcurrent(broker, raw)
		}()
	}
	wait.Wait()
	close(responses)
	committed, idempotent := 0, 0
	for response := range responses {
		if response.Code != http.StatusOK {
			t.Fatalf("concurrent status = %d; body %s", response.Code, response.Body.String())
		}
		var result struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		switch result.Status {
		case "quote_issued":
			committed++
		case "idempotent":
			idempotent++
		default:
			t.Fatalf("unexpected concurrent result: %s", response.Body.String())
		}
	}
	if committed != 1 || idempotent != requests-1 || len(store.records) != 1 || store.lastCounters["connection-0001"] != 11 {
		t.Fatalf("concurrent outcomes committed=%d idempotent=%d records=%d counters=%#v", committed, idempotent, len(store.records), store.lastCounters)
	}
}

func TestBrokerNeverReturnsCorruptStoredReceipt(t *testing.T) {
	privateKey := testPrivateKey()
	store := newMemoryCommandStore()
	now := time.Date(2026, 7, 15, 1, 2, 4, 0, time.UTC)
	raw := signedQuoteCommand(t, privateKey, "command-quote-corrupt", 3)
	command, err := contract.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		t.Fatal(err)
	}
	store.records[command.ConnectionID+"\x00"+command.CommandID] = commandstore.Record{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: requestSHA,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Action: command.Action,
		ResultJSON: []byte(`{"status":"quote_issued","secret_ref":"must-not-leak"}`),
	}
	broker := readOnlyTestBroker(privateKey, store, &recordingRegistrationAttestor{}, &recordingQuoteProvider{}, func() time.Time { return now })

	response := serve(t, broker, "POST", "/v2/commands", raw)
	assertHTTPError(t, response, http.StatusInternalServerError, "receipt_store_invalid")
	if bytes.Contains(response.Body.Bytes(), []byte("must-not-leak")) {
		t.Fatalf("corrupt receipt leaked in response: %s", response.Body.String())
	}
}

func readOnlyTestBroker(
	privateKey ed25519.PrivateKey,
	store commandstore.Repository,
	registration RegistrationAttestor,
	quote QuoteProvider,
	now func() time.Time,
) Broker {
	return Broker{
		Resolver: StaticKeyResolver{
			ConnectionID: "connection-0001",
			NodeKeyID:    "node-key-01",
			Generation:   1,
			PublicKey:    privateKey.Public().(ed25519.PublicKey),
		},
		Store:        store,
		Registration: registration,
		Quote:        quote,
		Now:          now,
	}
}

func serveWithGatewayRuntime(t *testing.T, broker Broker, raw []byte) *httptest.ResponseRecorder {
	t.Helper()
	return serveWithContext(t, WithGatewayRuntime(context.Background(), GatewayRuntime{
		DomainName: "abcdefghij.execute-api.ap-northeast-1.amazonaws.com",
		Stage:      "prod",
	}), broker, raw)
}

func serveWithContext(t *testing.T, ctx context.Context, broker Broker, raw []byte) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v2/commands", bytes.NewReader(raw)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	broker.ServeHTTP(response, request)
	return response
}

func serveConcurrent(broker Broker, raw []byte) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v2/commands", bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	broker.ServeHTTP(response, request)
	return response
}

func signedRegistrationCommand(t *testing.T, privateKey ed25519.PrivateKey, commandID string, counter int64) []byte {
	t.Helper()
	return signedReadOnlyCommand(t, privateKey, commandID, counter, contract.ActionRegistrationVerify,
		[]byte(`{"bootstrap_id":"bootstrap-0001","requested_region":"ap-northeast-1","stack_arn":"arn:aws:cloudformation:ap-northeast-1:123456789012:stack/dirextalk-test/12345678-1234-1234-1234-123456789012"}`))
}

func signedQuoteCommand(t *testing.T, privateKey ed25519.PrivateKey, commandID string, counter int64) []byte {
	t.Helper()
	return signedReadOnlyCommand(t, privateKey, commandID, counter, contract.ActionQuoteRequest,
		[]byte(`{"quote_request_id":"quote-request-0001","plan_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","region":"ap-northeast-1","candidates":[{"candidate_id":"candidate-0001","tier":"recommended","instance_type":"t3.large","purchase_option":"on_demand","estimated_disk_gib":40}]}`))
}

func signedReadOnlyCommand(t *testing.T, privateKey ed25519.PrivateKey, commandID string, counter int64, action string, payload []byte) []byte {
	t.Helper()
	sum := sha256.Sum256(payload)
	command := contract.Command{
		Schema:             contract.CommandSchema,
		ConnectionID:       "connection-0001",
		CommandID:          commandID,
		NodeKeyID:          "node-key-01",
		IssuedAt:           "2026-07-15T01:02:03.000Z",
		ExpiresAt:          "2026-07-15T01:07:03.000Z",
		ExpectedGeneration: 1,
		NodeCounter:        counter,
		Action:             action,
		PayloadB64:         base64.StdEncoding.EncodeToString(payload),
		PayloadSHA256:      hex.EncodeToString(sum[:]),
	}
	signatureBase, err := command.SignatureBase()
	if err != nil {
		t.Fatalf("signature base: %v", err)
	}
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(signatureBase)))
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	return raw
}

type recordingRegistrationAttestor struct{ calls int }

func (a *recordingRegistrationAttestor) Attest(
	_ context.Context,
	runtime GatewayRuntime,
	command contract.Command,
	request contract.RegistrationRequest,
) (contract.Registration, error) {
	a.calls++
	requestSHA, _ := command.RequestSHA256()
	return contract.Registration{
		Schema:                       contract.RegistrationSchema,
		BootstrapID:                  request.BootstrapID,
		ConnectionID:                 command.ConnectionID,
		AccountID:                    "123456789012",
		Region:                       request.RequestedRegion,
		BrokerCommandURL:             "https://" + runtime.DomainName + "/" + runtime.Stage + "/v2/commands",
		NodeKeyID:                    command.NodeKeyID,
		ConnectionGeneration:         command.ExpectedGeneration,
		WorkerArtifact:               contract.WorkerArtifactReference{Kind: "fixed_ami", AMIID: "ami-0123456789abcdef0"},
		WorkerNetwork:                contract.WorkerNetworkReference{VPCID: "vpc-0123456789abcdef0", SubnetID: "subnet-0123456789abcdef0", AvailabilityZone: "ap-northeast-1a"},
		WorkerResourceManifestDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		StackARN:                     request.StackARN,
		CommandID:                    command.CommandID,
		RequestSHA256:                requestSHA,
	}, nil
}

type recordingQuoteProvider struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (p *recordingQuoteProvider) Quote(
	_ context.Context,
	command contract.Command,
	request contract.QuoteRequest,
	now time.Time,
) (contract.Quote, error) {
	p.mu.Lock()
	p.calls++
	providerErr := p.err
	p.mu.Unlock()
	if providerErr != nil {
		return contract.Quote{}, providerErr
	}
	requestSHA, _ := command.RequestSHA256()
	requested := request.Candidates[0]
	return contract.Quote{
		Schema:         contract.QuoteSchema,
		QuoteID:        "quote-" + requestSHA[:32],
		ConnectionID:   command.ConnectionID,
		CommandID:      command.CommandID,
		RequestSHA256:  requestSHA,
		QuoteRequestID: request.QuoteRequestID,
		PlanDigest:     request.PlanDigest,
		Region:         request.Region,
		Currency:       "USD",
		QuotedAt:       contract.CanonicalInstant(now),
		ValidUntil:     contract.CanonicalInstant(now.Add(15 * time.Minute)),
		Candidates: []contract.QuotedCandidate{{
			CandidateID:       requested.CandidateID,
			Tier:              requested.Tier,
			InstanceType:      requested.InstanceType,
			PurchaseOption:    requested.PurchaseOption,
			EstimatedDiskGiB:  requested.EstimatedDiskGiB,
			Architecture:      "amd64",
			VCPU:              2,
			MemoryMiB:         8192,
			GPUCount:          0,
			GPUMemoryMiB:      0,
			HourlyMinor:       10,
			ThirtyDayMinor:    7200,
			AvailabilityZones: []string{"ap-northeast-1a"},
		}},
		IncludedItems:   []string{"ec2_linux_ondemand"},
		UnincludedItems: []string{"cloudwatch_logs", "data_transfer", "ebs_gp3", "public_ipv4", "snapshots", "taxes"},
	}, nil
}

type memoryCommandStore struct {
	mu             sync.Mutex
	records        map[string]commandstore.Record
	lastCounters   map[string]int64
	quotes         map[string]commandstore.IssuedQuote
	deployments    map[string]commandstore.DeploymentReservation
	approvalUses   map[string]string
	challengeUses  map[string]string
	workerSessions map[string]commandstore.WorkerSession
}

func newMemoryCommandStore() *memoryCommandStore {
	return &memoryCommandStore{records: map[string]commandstore.Record{}, lastCounters: map[string]int64{}, quotes: map[string]commandstore.IssuedQuote{}, deployments: map[string]commandstore.DeploymentReservation{}, approvalUses: map[string]string{}, challengeUses: map[string]string{}, workerSessions: map[string]commandstore.WorkerSession{}}
}

func (s *memoryCommandStore) Lookup(_ context.Context, connectionID, commandID string) (commandstore.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[connectionID+"\x00"+commandID]
	return record, ok, nil
}

func (s *memoryCommandStore) LookupIssuedQuote(_ context.Context, connectionID, quoteID string) (commandstore.IssuedQuote, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.quotes[connectionID+"\x00"+quoteID]
	return q, ok, nil
}
func (s *memoryCommandStore) LookupDeployment(_ context.Context, connectionID, deploymentID string) (commandstore.DeploymentReservation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.deployments[connectionID+"\x00"+deploymentID]
	return r, ok, nil
}
func (s *memoryCommandStore) ReserveDeployment(_ context.Context, r commandstore.DeploymentReservation) (commandstore.DeploymentReservation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := r.ConnectionID + "\x00" + r.DeploymentID
	if existing, ok := s.deployments[key]; ok {
		if !existing.SameIdentity(r) {
			return commandstore.DeploymentReservation{}, false, commandstore.NewError("deployment_id_conflict")
		}
		return existing, false, nil
	}
	if _, ok := s.approvalUses[r.ConnectionID+"\x00"+r.ApprovalID]; ok {
		return commandstore.DeploymentReservation{}, false, commandstore.NewError("approval_already_consumed")
	}
	if _, ok := s.challengeUses[r.ConnectionID+"\x00"+r.ChallengeID]; ok {
		return commandstore.DeploymentReservation{}, false, commandstore.NewError("challenge_already_consumed")
	}
	if last, ok := s.lastCounters[r.ConnectionID]; ok && r.NodeCounter <= last {
		return commandstore.DeploymentReservation{}, false, commandstore.NewError("stale_node_counter")
	}
	s.deployments[key] = r
	s.workerSessions[r.BootstrapSessionID] = r.WorkerSession
	s.approvalUses[r.ConnectionID+"\x00"+r.ApprovalID] = r.DeploymentID
	s.challengeUses[r.ConnectionID+"\x00"+r.ChallengeID] = r.DeploymentID
	s.lastCounters[r.ConnectionID] = r.NodeCounter
	return r, true, nil
}
func (s *memoryCommandStore) FinalizeDeployment(_ context.Context, r commandstore.DeploymentReservation, receipt commandstore.Record) (commandstore.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := r.ConnectionID + "\x00" + r.DeploymentID
	existing, ok := s.deployments[key]
	if !ok || !existing.SameIdentity(r) {
		return commandstore.Record{}, false, commandstore.NewError("deployment_reservation_conflict")
	}
	receiptKey := receipt.ConnectionID + "\x00" + receipt.CommandID
	if stored, ok := s.records[receiptKey]; ok {
		return stored, false, nil
	}
	existing.State = "finalized"
	existing.ResultJSON = append([]byte(nil), receipt.ResultJSON...)
	s.deployments[key] = existing
	session := s.workerSessions[r.BootstrapSessionID]
	var result contract.DeploymentResult
	_ = json.Unmarshal(receipt.ResultJSON, &result)
	session.State = "bound"
	session.ExpectedInstanceID = result.Deployment.InstanceID
	s.workerSessions[r.BootstrapSessionID] = session
	s.records[receiptKey] = receipt
	return receipt, true, nil
}

func (s *memoryCommandStore) LookupWorkerSession(_ context.Context, bootstrapSessionID string) (commandstore.WorkerSession, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.workerSessions[bootstrapSessionID]
	return session, ok, nil
}

func (s *memoryCommandStore) ActivateWorkerSession(_ context.Context, claim commandstore.WorkerSessionClaim) (commandstore.WorkerSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.workerSessions[claim.Session.BootstrapSessionID]
	if !ok || current.ConnectionID != claim.Session.ConnectionID || current.DeploymentID != claim.Session.DeploymentID || current.ExpectedInstanceID != claim.Session.ExpectedInstanceID || current.LeaseEpoch != claim.Session.LeaseEpoch || (current.State != "bound" && current.State != "active") {
		return commandstore.WorkerSession{}, commandstore.NewError("worker_session_conflict")
	}
	current.State = "active"
	current.LeaseEpoch++
	current.LeaseExpiresAt = claim.LeaseExpiresAt
	current.TokenSHA256 = claim.TokenSHA256
	current.LastSequence = 0
	current.LastEventAt = ""
	s.workerSessions[current.BootstrapSessionID] = current
	return current, nil
}

func (s *memoryCommandStore) Commit(_ context.Context, record commandstore.Record, _ *commandstore.IssuedQuote) (commandstore.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := record.ConnectionID + "\x00" + record.CommandID
	if existing, ok := s.records[key]; ok {
		if !existing.SameIdentity(record) {
			return commandstore.Record{}, false, commandstore.NewError("command_id_conflict")
		}
		return existing, false, nil
	}
	if last, ok := s.lastCounters[record.ConnectionID]; ok && record.NodeCounter <= last {
		return commandstore.Record{}, false, commandstore.NewError("stale_node_counter")
	}
	s.records[key] = record
	s.lastCounters[record.ConnectionID] = record.NodeCounter
	return record, true, nil
}

func assertResultBinding(t *testing.T, response *httptest.ResponseRecorder, status, disposition, action string) {
	t.Helper()
	if response.Code != 200 {
		t.Fatalf("status = %d, want 200; body %s", response.Code, response.Body.String())
	}
	var result struct {
		Status  string `json:"status"`
		Receipt struct {
			Schema      string `json:"schema"`
			Disposition string `json:"disposition"`
			Action      string `json:"action"`
		} `json:"receipt"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.Status != status || result.Receipt.Schema != contract.ReceiptSchema || result.Receipt.Disposition != disposition || result.Receipt.Action != action {
		t.Fatalf("result binding = %#v", result)
	}
}

func assertHTTPError(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("status = %d, want %d; body %s", response.Code, status, response.Body.String())
	}
	assertErrorCode(t, response, code)
}
