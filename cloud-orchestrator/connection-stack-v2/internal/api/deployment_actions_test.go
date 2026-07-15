package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

func TestDeploymentCreateCommitsReadBackAndReplaysWithoutProvider(t *testing.T) {
	broker, store, provider, raw := deploymentTestBroker(t)
	first := serve(t, broker, http.MethodPost, "/v2/commands", raw)
	assertResultBinding(t, first, "deployment_created", "committed", contract.ActionDeploymentCreate)
	var result contract.DeploymentResult
	if err := json.Unmarshal(first.Body.Bytes(), &result); err != nil || result.Deployment.InstanceID != "i-0123456789abcdef0" {
		t.Fatalf("deployment result=%#v err=%v", result, err)
	}
	replay := serve(t, broker, http.MethodPost, "/v2/commands", raw)
	assertResultBinding(t, replay, "idempotent", "idempotent", contract.ActionDeploymentCreate)
	if provider.ensureCalls != 1 || provider.readCalls != 1 || len(store.approvalUses) != 1 || len(store.challengeUses) != 1 {
		t.Fatalf("provider/consumption ensure=%d read=%d approvals=%d challenges=%d", provider.ensureCalls, provider.readCalls, len(store.approvalUses), len(store.challengeUses))
	}
}

func TestDeploymentReservationPersistsExactApprovedRecipeAndSecretScope(t *testing.T) {
	broker, store, _, raw := deploymentTestBroker(t)
	scope := []contract.ApprovalSecretReference{{SecretRef: "secret_ref:model-token-001", Purpose: "model inference", Delivery: "environment"}, {SecretRef: "secret_ref:github-app-001", Purpose: "source checkout", Delivery: "file"}}
	raw = resignDeploymentApprovalScope(t, raw, scope)
	response := serve(t, broker, http.MethodPost, "/v2/commands", raw)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	reservation := store.deployments["connection-create-0001\x00deployment-create-0001"]
	proof, err := contract.ParseApprovalProof(mustCommand(t, raw).ApprovalProof)
	if err != nil {
		t.Fatal(err)
	}
	if reservation.PlanHash != proof.PlanHash || reservation.RecipeDigest != proof.RecipeDigest || len(reservation.SecretScope) != len(proof.SecretScope) {
		t.Fatalf("reservation scope=%#v proof=%#v", reservation, proof.SecretScope)
	}
	want := []commandstore.ApprovedSecretReference{{SecretRef: "secret_ref:github-app-001", Purpose: "source checkout", Delivery: "file"}, {SecretRef: "secret_ref:model-token-001", Purpose: "model inference", Delivery: "environment"}}
	for index := range want {
		if reservation.SecretScope[index] != want[index] {
			t.Fatalf("secret scope[%d]=%#v want %#v", index, reservation.SecretScope[index], want[index])
		}
	}
}

func resignDeploymentApprovalScope(t *testing.T, raw []byte, scope []contract.ApprovalSecretReference) []byte {
	t.Helper()
	command := mustCommand(t, raw)
	proof, err := command.Approval()
	if err != nil {
		t.Fatal(err)
	}
	proof.SecretScope = append([]contract.ApprovalSecretReference(nil), scope...)
	payload, err := proof.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	deviceKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x4f}, ed25519.SeedSize))
	proof.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(deviceKey, payload))
	command.ApprovalProof, err = json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	base, err := command.SignatureBase()
	if err != nil {
		t.Fatal(err)
	}
	nodeKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x6a}, ed25519.SeedSize))
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(nodeKey, []byte(base)))
	encoded, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func mustCommand(t *testing.T, raw []byte) contract.Command {
	t.Helper()
	command, err := contract.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return command
}

func TestDeploymentCreateResumesReservationAfterReadBackFailure(t *testing.T) {
	broker, store, provider, raw := deploymentTestBroker(t)
	provider.readErr = NewError("provider_readback_unavailable", 503)
	failed := serve(t, broker, http.MethodPost, "/v2/commands", raw)
	assertHTTPError(t, failed, 503, "provider_readback_unavailable")
	if len(store.deployments) != 1 || len(store.records) != 0 || len(store.approvalUses) != 1 {
		t.Fatalf("failed attempt state deployments=%d records=%d approvals=%d", len(store.deployments), len(store.records), len(store.approvalUses))
	}
	provider.readErr = nil
	succeeded := serve(t, broker, http.MethodPost, "/v2/commands", raw)
	assertResultBinding(t, succeeded, "deployment_created", "committed", contract.ActionDeploymentCreate)
	if provider.ensureCalls != 2 || len(provider.tokens) != 2 || provider.tokens[0] != provider.tokens[1] {
		t.Fatalf("resume tokens/calls=%#v %d", provider.tokens, provider.ensureCalls)
	}
}

func TestDeploymentCreateRejectsTamperedStoredBootstrapSpecBeforeProvider(t *testing.T) {
	broker, store, provider, raw := deploymentTestBroker(t)
	provider.readErr = NewError("provider_readback_unavailable", 503)
	if response := serve(t, broker, http.MethodPost, "/v2/commands", raw); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("initial status=%d body=%s", response.Code, response.Body.String())
	}
	for key, reservation := range store.deployments {
		reservation.SpecJSON = bytes.Replace(reservation.SpecJSON, []byte(`"subnet_id":"subnet-0123456789abcdef0"`), []byte(`"subnet_id":"subnet-fffffffffffffffff"`), 1)
		store.deployments[key] = reservation
	}
	provider.readErr = nil
	response := serve(t, broker, http.MethodPost, "/v2/commands", raw)
	assertHTTPError(t, response, http.StatusConflict, "deployment_id_conflict")
	if provider.ensureCalls != 1 {
		t.Fatalf("tampered stored spec reached provider ensure=%d", provider.ensureCalls)
	}
}

func TestDeploymentCreateAuthenticatesBeforeApprovalStoreAndProvider(t *testing.T) {
	broker, store, provider, raw := deploymentTestBroker(t)
	var command map[string]any
	if err := json.Unmarshal(raw, &command); err != nil {
		t.Fatal(err)
	}
	command["signature_b64"] = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="
	tampered, _ := json.Marshal(command)
	response := serve(t, broker, http.MethodPost, "/v2/commands", tampered)
	assertHTTPError(t, response, 403, "invalid_node_signature")
	if provider.ensureCalls != 0 || len(store.deployments) != 0 || len(store.approvalUses) != 0 {
		t.Fatalf("unauthenticated mutation reached boundary")
	}
}

func TestDeploymentReplayNeverReturnsExpandedStoredReceipt(t *testing.T) {
	broker, store, _, raw := deploymentTestBroker(t)
	first := serve(t, broker, http.MethodPost, "/v2/commands", raw)
	if first.Code != 200 {
		t.Fatal(first.Body.String())
	}
	key := "connection-create-0001\x00command-deployment-0001"
	record := store.records[key]
	record.ResultJSON = []byte(strings.Replace(string(record.ResultJSON), `"instance_id":"i-0123456789abcdef0"`, `"instance_id":"i-0123456789abcdef0","secret_ref":"must-not-leak"`, 1))
	store.records[key] = record
	response := serve(t, broker, http.MethodPost, "/v2/commands", raw)
	assertHTTPError(t, response, 500, "receipt_store_invalid")
	if strings.Contains(response.Body.String(), "must-not-leak") {
		t.Fatalf("stored expansion leaked: %s", response.Body.String())
	}
}

func TestDeploymentCreateConcurrentExactRequestsShareReservationAndReceipt(t *testing.T) {
	broker, store, provider, raw := deploymentTestBroker(t)
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
			t.Fatalf("concurrent status=%d body=%s", response.Code, response.Body.String())
		}
		var result struct {
			Receipt struct {
				Disposition string `json:"disposition"`
			} `json:"receipt"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		switch result.Receipt.Disposition {
		case "committed":
			committed++
		case "idempotent":
			idempotent++
		default:
			t.Fatalf("unexpected response: %s", response.Body.String())
		}
	}
	if committed != 1 || idempotent != requests-1 || len(store.deployments) != 1 || len(store.records) != 1 || len(store.approvalUses) != 1 || len(store.challengeUses) != 1 {
		t.Fatalf("outcomes committed=%d idempotent=%d deployments=%d records=%d approvals=%d challenges=%d", committed, idempotent, len(store.deployments), len(store.records), len(store.approvalUses), len(store.challengeUses))
	}
	for _, token := range provider.tokens {
		if token != provider.tokens[0] {
			t.Fatalf("provider tokens differ: %#v", provider.tokens)
		}
	}
}

func TestDeploymentCreateCannotReuseApprovalForAnotherDeployment(t *testing.T) {
	broker, store, provider, raw := deploymentTestBroker(t)
	first := serve(t, broker, http.MethodPost, "/v2/commands", raw)
	if first.Code != http.StatusOK {
		t.Fatal(first.Body.String())
	}
	secondRaw := resignDeploymentCommand(t, raw, "command-deployment-0002", "deployment-create-0002", 10)
	second := serve(t, broker, http.MethodPost, "/v2/commands", secondRaw)
	assertHTTPError(t, second, http.StatusConflict, "approval_already_consumed")
	if len(store.deployments) != 1 || provider.ensureCalls != 1 {
		t.Fatalf("approval reuse mutated state deployments=%d ensure=%d", len(store.deployments), provider.ensureCalls)
	}
}

func TestDeploymentCreateRejectsPersistedQuoteDigestDriftBeforeReservation(t *testing.T) {
	broker, store, provider, raw := deploymentTestBroker(t)
	key := "connection-create-0001\x00quote-create-0001"
	issued := store.quotes[key]
	issued.QuoteJSON = bytes.Replace(issued.QuoteJSON, []byte(`"hourly_minor":10`), []byte(`"hourly_minor":11`), 1)
	store.quotes[key] = issued
	response := serve(t, broker, http.MethodPost, "/v2/commands", raw)
	assertHTTPError(t, response, http.StatusConflict, "issued_quote_mismatch")
	if provider.ensureCalls != 0 || len(store.deployments) != 0 || len(store.approvalUses) != 0 {
		t.Fatalf("quote drift reached mutation boundary")
	}
}

func resignDeploymentCommand(t *testing.T, raw []byte, commandID, deploymentID string, counter int64) []byte {
	t.Helper()
	command, err := contract.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	request, err := command.DeploymentRequest()
	if err != nil {
		t.Fatal(err)
	}
	request.DeploymentID = deploymentID
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	command.CommandID = commandID
	command.NodeCounter = counter
	command.PayloadB64 = base64.StdEncoding.EncodeToString(payload)
	command.PayloadSHA256 = fmt.Sprintf("%x", digest)
	base, err := command.SignatureBase()
	if err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x6a}, ed25519.SeedSize))
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(base)))
	encoded, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func deploymentTestBroker(t *testing.T) (Broker, *memoryCommandStore, *recordingDeploymentProvider, []byte) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "deployment-command-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.TrimSpace(raw)
	store := newMemoryCommandStore()
	quote := contract.Quote{Schema: contract.QuoteSchema, QuoteID: "quote-create-0001", ConnectionID: "connection-create-0001", CommandID: "command-quote-0001", RequestSHA256: string(bytes.Repeat([]byte{'a'}, 64)), QuoteRequestID: "quote-request-0001", PlanDigest: "sha256:" + string(bytes.Repeat([]byte{'f'}, 64)), Region: "us-east-1", Currency: "USD", QuotedAt: "2026-07-14T12:00:00.123Z", ValidUntil: "2026-07-14T12:10:00.123Z", Candidates: []contract.QuotedCandidate{{CandidateID: "candidate-create-0001", Tier: "recommended", InstanceType: "m7i.xlarge", PurchaseOption: "on_demand", EstimatedDiskGiB: 80, Architecture: "amd64", VCPU: 4, MemoryMiB: 16384, HourlyMinor: 10, ThirtyDayMinor: 7200, AvailabilityZones: []string{"us-east-1a"}}}, IncludedItems: []string{"ec2_linux_ondemand"}, UnincludedItems: []string{"cloudwatch_logs", "data_transfer", "ebs_gp3", "public_ipv4", "snapshots", "taxes"}}
	quoteJSON, _ := json.Marshal(quote)
	store.quotes["connection-create-0001\x00quote-create-0001"] = commandstore.IssuedQuote{ConnectionID: "connection-create-0001", QuoteID: "quote-create-0001", ValidUntil: quote.ValidUntil, QuoteJSON: quoteJSON}
	nodePublic := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x6a}, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	devicePublic := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x4f}, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	provider := &recordingDeploymentProvider{}
	broker := Broker{Resolver: StaticKeyResolver{ConnectionID: "connection-create-0001", NodeKeyID: "node-key-1", Generation: 2, PublicKey: nodePublic}, DeploymentEnabled: true, ApprovalResolver: StaticApprovalKeyResolver{ConnectionID: "connection-create-0001", SignerKeyID: "device-key-1", PublicKey: devicePublic}, DeploymentStore: store, DeploymentProvider: provider, DeploymentBoundary: DeploymentBoundary{WorkerArtifact: contract.WorkerArtifactReference{Kind: "fixed_ami", AMIID: "ami-0123456789abcdef0"}, WorkerNetwork: contract.WorkerNetworkReference{VPCID: "vpc-0123456789abcdef0", SubnetID: "subnet-0123456789abcdef0", AvailabilityZone: "us-east-1a"}, WorkerResourceManifestDigest: "sha256:" + string(bytes.Repeat([]byte{'c'}, 64)), WorkerSecurityGroupID: "sg-0123456789abcdef0", WorkerBootstrapEndpoint: "https://abcdefghij.execute-api.us-east-1.amazonaws.com/prod/v2/worker-sessions"}, Now: func() time.Time { return time.Date(2026, 7, 14, 12, 1, 0, 0, time.UTC) }}
	return broker, store, provider, raw
}

type recordingDeploymentProvider struct {
	mu                     sync.Mutex
	ensureCalls, readCalls int
	tokens                 []string
	readErr                error
}

func (p *recordingDeploymentProvider) EnsureCreated(_ context.Context, spec DeploymentSpec) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureCalls++
	p.tokens = append(p.tokens, spec.ClientToken)
	return "i-0123456789abcdef0", nil
}
func (p *recordingDeploymentProvider) ReadBack(_ context.Context, _ DeploymentSpec, instanceID string) (DeploymentEvidence, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readCalls++
	if p.readErr != nil {
		return DeploymentEvidence{}, p.readErr
	}
	return DeploymentEvidence{InstanceID: instanceID, VolumeIDs: []string{"vol-0123456789abcdef0"}, NetworkInterfaceIDs: []string{"eni-0123456789abcdef0"}}, nil
}
