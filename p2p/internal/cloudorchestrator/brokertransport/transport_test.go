package brokertransport

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestBuildQuoteCommandBindsOnlyThePrePriceRequest(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	transport, err := New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	request := testQuoteRequest(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	logical := runtime.QuoteCommand{
		CommandID: "command-quote-0001", ConnectionID: request.CloudConnectionID, NodeKeyID: "node-key-1",
		ExpectedGeneration: 1, NodeCounter: 7, RequestDigest: digest,
	}
	signed, err := transport.BuildQuoteCommand(logical, request)
	if err != nil {
		t.Fatal(err)
	}
	if signed.RequestSHA256 == signed.PayloadSHA256 || len(signed.RequestSHA256) != 64 || signed.IssuedAt != now || signed.ExpiresAt != now.Add(commandLifetime) {
		t.Fatalf("signed command identity = %#v", signed)
	}
	parsed, err := broker.ParseQuoteCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ConnectionID != logical.ConnectionID || parsed.CommandID != logical.CommandID || parsed.NodeKeyID != logical.NodeKeyID || parsed.ExpectedGeneration != logical.ExpectedGeneration || parsed.NodeCounter != logical.NodeCounter || parsed.RequestSHA256() != signed.RequestSHA256 || parsed.PayloadSHA256 != signed.PayloadSHA256 {
		t.Fatalf("parsed command does not bind logical identity: %#v", parsed)
	}
	decoded, err := parsed.QuoteRequest()
	if err != nil || !reflect.DeepEqual(decoded, brokerRequest(request, digest)) || signed.PayloadJSON == "" {
		t.Fatalf("decoded request=%#v err=%v", decoded, err)
	}
}

func TestRequestQuoteRejectsChangedPersistedEnvelopeBeforeNetwork(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := New(privateKey, func() time.Time { return time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	request := testQuoteRequest(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	logical := runtime.QuoteCommand{
		CommandID: "command-quote-0001", ConnectionID: request.CloudConnectionID, NodeKeyID: "node-key-1",
		ExpectedGeneration: 1, NodeCounter: 7, RequestDigest: digest,
	}
	signed, err := transport.BuildQuoteCommand(logical, request)
	if err != nil {
		t.Fatal(err)
	}
	logical.PayloadJSON = signed.PayloadJSON
	logical.PayloadSHA256 = signed.PayloadSHA256
	logical.RequestSHA256 = signed.RequestSHA256
	logical.SignedEnvelope = signed.EnvelopeJSON
	logical.IssuedAt = signed.IssuedAt
	logical.ExpiresAt = signed.ExpiresAt
	signed.PayloadJSON += " "
	if _, err := transport.RequestQuote(context.Background(), "https://broker.example/v2/commands", logical, signed, request); err == nil {
		t.Fatal("modified persisted payload must be rejected before any HTTP request")
	}
}

func TestRuntimeQuoteCarriesBrokerCapacityMetadata(t *testing.T) {
	result := broker.QuoteResult{Quote: broker.Quote{
		Schema: "dirextalk.aws.quote/v1", QuoteID: "quote-capacity-0001", ConnectionID: "connection-capacity-0001",
		CommandID: "command-capacity-0001", RequestSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		QuoteRequestID: "quote-request-capacity-0001", PlanDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Region: "us-east-1", Currency: "USD", QuotedAt: "2026-07-14T12:00:00.000Z", ValidUntil: "2026-07-14T12:15:00.000Z",
		Candidates: []broker.QuotedCandidate{{
			CandidateID: "recommended-capacity-0001", Tier: "recommended", InstanceType: "g5.xlarge", PurchaseOption: "on_demand",
			EstimatedDiskGiB: 80, Architecture: "amd64", VCPU: 4, MemoryMiB: 16384, GPUCount: 1, GPUMemoryMiB: 24576,
			HourlyMinor: 100, ThirtyDayMinor: 72000, StartupUpperMinor: 0, AvailabilityZones: []string{"us-east-1a"},
		}},
	}}
	quote, err := runtimeQuote(result)
	if err != nil {
		t.Fatalf("runtimeQuote: %v", err)
	}
	candidate := quote.Candidates[0]
	if candidate.Architecture != cloudcontracts.ArchitectureAMD64 || candidate.VCPU != 4 || candidate.MemoryMiB != 16384 || candidate.GPUCount != 1 || candidate.GPUMemoryMiB != 24576 {
		t.Fatalf("runtime capacity metadata = %#v", candidate)
	}
}

func TestBuildConnectionRegistrationCommandBindsOnlyPendingBootstrap(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	transport, err := New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	request := testConnectionRegistrationRequest(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	logical := runtime.ConnectionRegistrationCommand{
		CommandID: "command-registration-0001", BootstrapID: request.BootstrapID, ConnectionID: "connection-registration-0001",
		NodeKeyID: "node-key-1", ExpectedGeneration: 1, NodeCounter: 7, Attempt: 1, RequestDigest: digest,
	}
	signed, err := transport.BuildConnectionRegistrationCommand(logical, request)
	if err != nil {
		t.Fatal(err)
	}
	if signed.RequestSHA256 == signed.PayloadSHA256 || len(signed.RequestSHA256) != 64 || signed.IssuedAt != now || signed.ExpiresAt != now.Add(commandLifetime) {
		t.Fatalf("signed registration command identity = %#v", signed)
	}
	parsed, err := broker.ParseRegistrationCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ConnectionID != logical.ConnectionID || parsed.CommandID != logical.CommandID || parsed.NodeKeyID != logical.NodeKeyID ||
		parsed.ExpectedGeneration != logical.ExpectedGeneration || parsed.NodeCounter != logical.NodeCounter ||
		parsed.RequestSHA256() != signed.RequestSHA256 || parsed.PayloadSHA256 != signed.PayloadSHA256 {
		t.Fatalf("parsed registration command does not bind logical identity: %#v", parsed)
	}
	decoded, err := parsed.RegistrationRequest()
	if err != nil || decoded != registrationRequest(request) || signed.PayloadJSON == "" {
		t.Fatalf("decoded registration request=%#v err=%v", decoded, err)
	}
}

func TestRequestConnectionRegistrationRejectsChangedPersistedEnvelopeBeforeNetwork(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := New(privateKey, func() time.Time { return time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	request := testConnectionRegistrationRequest(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	logical := runtime.ConnectionRegistrationCommand{
		CommandID: "command-registration-0001", BootstrapID: request.BootstrapID, ConnectionID: "connection-registration-0001",
		NodeKeyID: "node-key-1", ExpectedGeneration: 1, NodeCounter: 7, Attempt: 1, RequestDigest: digest,
	}
	signed, err := transport.BuildConnectionRegistrationCommand(logical, request)
	if err != nil {
		t.Fatal(err)
	}
	logical.PayloadJSON = signed.PayloadJSON
	logical.PayloadSHA256 = signed.PayloadSHA256
	logical.RequestSHA256 = signed.RequestSHA256
	logical.SignedEnvelope = signed.EnvelopeJSON
	logical.IssuedAt = signed.IssuedAt
	logical.ExpiresAt = signed.ExpiresAt
	signed.PayloadJSON += " "
	if _, err := transport.RequestConnectionRegistration(context.Background(), "https://a1b2c3d4e5.execute-api.ap-northeast-1.amazonaws.com/prod/v2/commands", logical, signed, request); err == nil {
		t.Fatal("modified persisted registration payload must be rejected before any HTTP request")
	}
}

func TestBuildDeploymentCreateCommandBindsFixedRequestAndPrivateProof(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	transport, err := New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	request := testDeploymentCreateRequest(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	proofJSON, err := json.Marshal(testDeploymentApprovalProof(t, now))
	if err != nil {
		t.Fatal(err)
	}
	logical := runtime.DeploymentCreateCommand{
		CommandID: "command-deployment-0001", DeploymentID: request.DeploymentID, ConnectionID: "connection-create-0001", NodeKeyID: "node-key-1",
		ExpectedGeneration: request.ConnectionGeneration, NodeCounter: 7, Attempt: 1, RequestDigest: digest,
	}
	signed, err := transport.BuildDeploymentCreateCommand(logical, request, string(proofJSON), now.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if signed.RequestSHA256 == signed.PayloadSHA256 || len(signed.RequestSHA256) != 64 || signed.IssuedAt != now || signed.ExpiresAt != now.Add(commandLifetime) {
		t.Fatalf("signed deployment command identity = %#v", signed)
	}
	if strings.Contains(signed.PayloadJSON, "approval") {
		t.Fatalf("approval proof leaked into fixed deployment payload: %s", signed.PayloadJSON)
	}
	parsed, err := broker.ParseDeploymentCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ConnectionID != logical.ConnectionID || parsed.CommandID != logical.CommandID || parsed.NodeKeyID != logical.NodeKeyID ||
		parsed.ExpectedGeneration != logical.ExpectedGeneration || parsed.NodeCounter != logical.NodeCounter ||
		parsed.RequestSHA256() != signed.RequestSHA256 || parsed.PayloadSHA256 != signed.PayloadSHA256 || parsed.ApprovalProof.ApprovalID == "" {
		t.Fatalf("parsed deployment command does not bind logical identity: %#v", parsed)
	}
	decoded, err := parsed.DeploymentRequest()
	if err != nil || !reflect.DeepEqual(decoded, deploymentRequest(request)) {
		t.Fatalf("decoded deployment request=%#v err=%v", decoded, err)
	}
}

func TestBuildDeploymentCreateCommandClampsExpiryToQuoteAndApproval(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	transport, err := New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	request := testDeploymentCreateRequest(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	logical := runtime.DeploymentCreateCommand{
		CommandID: "command-deployment-0001", DeploymentID: request.DeploymentID, ConnectionID: "connection-create-0001", NodeKeyID: "node-key-1",
		ExpectedGeneration: request.ConnectionGeneration, NodeCounter: 7, Attempt: 1, RequestDigest: digest,
	}
	for _, test := range []struct {
		name           string
		approvalExpiry time.Time
		quoteExpiry    time.Time
		wantExpiry     time.Time
	}{
		{name: "quote", approvalExpiry: now.Add(2 * time.Minute), quoteExpiry: now.Add(2 * time.Minute), wantExpiry: now.Add(2 * time.Minute)},
		{name: "approval", approvalExpiry: now.Add(2 * time.Minute), quoteExpiry: now.Add(5 * time.Minute), wantExpiry: now.Add(2 * time.Minute)},
	} {
		t.Run(test.name, func(t *testing.T) {
			proofJSON, err := json.Marshal(testDeploymentApprovalProofWithValidity(t, now, test.approvalExpiry, test.quoteExpiry))
			if err != nil {
				t.Fatal(err)
			}
			signed, err := transport.BuildDeploymentCreateCommand(logical, request, string(proofJSON), test.quoteExpiry)
			if err != nil {
				t.Fatal(err)
			}
			if !signed.ExpiresAt.Equal(test.wantExpiry) {
				t.Fatalf("expires_at = %s, want %s", signed.ExpiresAt, test.wantExpiry)
			}
			parsed, err := broker.ParseDeploymentCommand([]byte(signed.EnvelopeJSON))
			if err != nil {
				t.Fatal(err)
			}
			if parsed.ExpiresAt != test.wantExpiry.Format("2006-01-02T15:04:05.000Z") {
				t.Fatalf("persisted envelope expires_at = %q, want %q", parsed.ExpiresAt, test.wantExpiry.Format("2006-01-02T15:04:05.000Z"))
			}
		})
	}
}

func TestClassifyDeploymentBrokerPlanExpiryAsKnownNoCreate(t *testing.T) {
	for _, test := range []struct {
		stackCode string
		wantCode  string
	}{
		{stackCode: "quote_expired", wantCode: runtime.DeploymentProvisionQuoteExpired},
		{stackCode: "approval_expired", wantCode: runtime.DeploymentProvisionApprovalExpired},
	} {
		t.Run(test.stackCode, func(t *testing.T) {
			got, ok := runtime.DeploymentProvisionPlanExpiryCode(classifyDeploymentBrokerError(&broker.Error{Code: test.stackCode, StatusCode: 409}))
			if !ok || got != test.wantCode {
				t.Fatalf("classification = code:%q known_no_create:%v, want code:%q", got, ok, test.wantCode)
			}
		})
	}
}

func TestRequestDeploymentCreateRejectsChangedPersistedEnvelopeBeforeNetwork(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	transport, err := New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	request := testDeploymentCreateRequest(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	proofJSON, err := json.Marshal(testDeploymentApprovalProof(t, now))
	if err != nil {
		t.Fatal(err)
	}
	logical := runtime.DeploymentCreateCommand{
		CommandID: "command-deployment-0001", DeploymentID: request.DeploymentID, ConnectionID: "connection-create-0001", NodeKeyID: "node-key-1",
		ExpectedGeneration: request.ConnectionGeneration, NodeCounter: 7, Attempt: 1, RequestDigest: digest,
	}
	signed, err := transport.BuildDeploymentCreateCommand(logical, request, string(proofJSON), now.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	logical.PayloadJSON = signed.PayloadJSON
	logical.PayloadSHA256 = signed.PayloadSHA256
	logical.RequestSHA256 = signed.RequestSHA256
	logical.SignedEnvelope = signed.EnvelopeJSON
	logical.IssuedAt = signed.IssuedAt
	logical.ExpiresAt = signed.ExpiresAt
	signed.PayloadJSON += " "
	if _, err := transport.RequestDeploymentCreate(context.Background(), "https://a1b2c3d4e5.execute-api.us-east-1.amazonaws.com/prod/v2/commands", logical, signed, request); err == nil {
		t.Fatal("modified persisted deployment payload must be rejected before any HTTP request")
	}
}

func testQuoteRequest(t *testing.T) cloudcontracts.QuoteRequestV1 {
	t.Helper()
	request := cloudcontracts.QuoteRequestV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1, QuoteRequestID: "quote-request-0001", PlanID: "plan-quote-0001", PlanRevision: 2,
		CloudConnectionID: "connection-quote-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Region: "ap-south-1",
		Candidates: []cloudcontracts.QuoteRequestCandidateV1{{
			CandidateID: "recommended-0001", Tier: cloudcontracts.QuoteTierRecommended, InstanceType: "m7i.xlarge",
			PurchaseOption: cloudcontracts.PurchaseOnDemand, EstimatedDiskGiB: 80,
		}},
	}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	return request
}

func testConnectionRegistrationRequest(t *testing.T) runtime.ConnectionRegistrationRequest {
	t.Helper()
	request := runtime.ConnectionRegistrationRequest{
		BootstrapID:     "bootstrap-registration-0001",
		RequestedRegion: "ap-northeast-1",
		StackARN:        "arn:aws:cloudformation:ap-northeast-1:123456789012:stack/dirextalk-registration/12345678-1234-1234-1234-123456789012",
	}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	return request
}

func testDeploymentCreateRequest(t *testing.T) runtime.DeploymentCreateRequest {
	t.Helper()
	request := runtime.DeploymentCreateRequest{
		Schema:                 runtime.DeploymentCreateSchema,
		DeploymentID:           "deployment-create-0001",
		ConnectionGeneration:   2,
		PlanHash:               transportNamedDigest('a'),
		PlanRevision:           4,
		QuoteID:                "quote-create-0001",
		QuoteDigest:            transportNamedDigest('b'),
		CandidateID:            "candidate-create-0001",
		ResourceManifestDigest: transportNamedDigest('c'),
		WorkerArtifact:         runtime.WorkerArtifactReferenceV1{Kind: "fixed_ami", AMIID: "ami-0123456789abcdef0"},
		Network:                runtime.DeploymentNetworkReference{VPCID: "vpc-0123456789abcdef0", SubnetID: "subnet-0123456789abcdef0", AvailabilityZone: "us-east-1a"},
	}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	return request
}

func testDeploymentApprovalProof(t *testing.T, now time.Time) cloudcontracts.ApprovalV1 {
	return testDeploymentApprovalProofWithValidity(t, now, now.Add(5*time.Minute), now.Add(10*time.Minute))
}

func testDeploymentApprovalProofWithValidity(t *testing.T, now, expiresAt, quoteValidUntil time.Time) cloudcontracts.ApprovalV1 {
	t.Helper()
	proof := cloudcontracts.ApprovalV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1, ApprovalID: "approval-create-0001", ChallengeID: "challenge-create-0001", SignerKeyID: "device-key-1",
		PlanID: "plan-create-0001", PlanHash: transportNamedDigest('a'), PlanRevision: 4, QuoteID: "quote-create-0001", QuoteDigest: transportNamedDigest('b'),
		QuoteValidUntil: quoteValidUntil, CloudConnectionID: "connection-create-0001", RecipeDigest: transportNamedDigest('d'),
		ResourceScope: cloudcontracts.ResourceScopeV1{
			Region: "us-east-1", AvailabilityZones: []string{"us-east-1a"}, InstanceType: "m7i.xlarge", Architecture: cloudcontracts.ArchitectureAMD64,
			VCPU: 4, MemoryMiB: 16384, DiskGiB: 80, PurchaseOption: cloudcontracts.PurchaseOnDemand,
		},
		NetworkScope: cloudcontracts.NetworkScopeV1{PublicIngress: false, EntryPoint: cloudcontracts.EntryPointNone, TLSRequired: false, AuthenticationRequired: false},
		ExpiresAt:    expiresAt,
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := proof.Sign(privateKey, now)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func transportNamedDigest(character rune) string {
	return "sha256:" + strings.Repeat(string(character), 64)
}
