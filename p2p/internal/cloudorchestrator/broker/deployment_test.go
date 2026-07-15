package broker

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestNewDeploymentCommandUsesFixedPayloadAndApprovalProofBinding(t *testing.T) {
	command := testDeploymentCommand(t)
	now := time.Date(2026, time.July, 14, 12, 0, 0, 123000000, time.UTC)
	wantPayload := `{"schema":"dirextalk.aws.deployment-create/v1","deployment_id":"deployment-create-0001","connection_generation":2,"plan_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","plan_revision":4,"quote_id":"quote-create-0001","quote_digest":"` + testDeploymentQuoteDigest(t, now) + `","candidate_id":"candidate-create-0001","resource_manifest_digest":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","worker_artifact":{"kind":"fixed_ami","ami_id":"ami-0123456789abcdef0"},"network":{"vpc_id":"vpc-0123456789abcdef0","subnet_id":"subnet-0123456789abcdef0","availability_zone":"us-east-1a"}}`
	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := string(payload); got != wantPayload {
		t.Fatalf("fixed deployment payload differs\n got: %s\nwant: %s", got, wantPayload)
	}
	if !strings.Contains(command.SignatureBase(), "approval_binding_sha256=\napproval_challenge_id=\napproval_signature_sha256=\napproval_proof_payload_sha256=") {
		t.Fatalf("deployment command signature base omitted fixed proof binding: %q", command.SignatureBase())
	}
	proofPayload, err := command.ApprovalProof.SigningPayload()
	if err != nil {
		t.Fatalf("ApprovalV1 SigningPayload: %v", err)
	}
	if want := "approval_proof_payload_sha256=" + sha256Hex(proofPayload) + "\n"; !strings.HasSuffix(command.SignatureBase(), want) {
		t.Fatalf("approval proof digest does not bind deterministic CBOR payload\n got: %s\nwant suffix: %s", command.SignatureBase(), want)
	}
	seed := bytes.Repeat([]byte{0x6a}, ed25519.SeedSize)
	if err := command.VerifySignature(ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)); err != nil {
		t.Fatalf("node signature does not verify: %v", err)
	}
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	if _, err := ParseDeploymentCommand(raw); err != nil {
		t.Fatalf("ParseDeploymentCommand canonical envelope: %v", err)
	}
	if _, err := ParseDeploymentCommand(bytes.Replace(raw, []byte(`"approval_proof"`), []byte(`"approval_binding"`), 1)); err == nil {
		t.Fatal("legacy approval_binding field must be rejected for deployment.create")
	}
}

func TestDeploymentCommandRejectsProofOrGenerationDrift(t *testing.T) {
	command := testDeploymentCommand(t)
	request, err := command.DeploymentRequest()
	if err != nil {
		t.Fatal(err)
	}
	issuedAt, err := time.Parse(canonicalInstantLayout, command.IssuedAt)
	if err != nil {
		t.Fatal(err)
	}
	expiresAt, err := time.Parse(canonicalInstantLayout, command.ExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	binding := DeploymentCommandBinding{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		IssuedAt: issuedAt, ExpiresAt: expiresAt, Request: request, ApprovalProof: command.ApprovalProof,
	}
	if err := command.ValidateBinding(binding); err != nil {
		t.Fatalf("valid deployment binding: %v", err)
	}
	binding.Request.ConnectionGeneration++
	if err := command.ValidateBinding(binding); err == nil {
		t.Fatal("connection generation drift must be rejected")
	}
	binding.Request = request
	binding.ApprovalProof.QuoteDigest = namedDigest('d')
	if err := command.ValidateBinding(binding); err == nil {
		t.Fatal("approval proof drift must be rejected")
	}
}

func TestDeploymentResultRejectsExpandedPrivateReceipt(t *testing.T) {
	command := testDeploymentCommand(t)
	result := validDeploymentResult(command)
	if err := ValidateDeploymentResult(command, result); err != nil {
		t.Fatalf("valid deployment result: %v", err)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	mutated := bytes.Replace(raw, []byte(`"network_interface_ids":["eni-0123456789abcdef0"]`), []byte(`"network_interface_ids":["eni-0123456789abcdef0"],"worker_token":"not-allowed"`), 1)
	if _, err := decodeDeploymentResultJSON(mutated); err == nil {
		t.Fatal("deployment result accepted a Worker secret field")
	}
}

func TestClientSubmitDeploymentAcceptsOnlyBoundPrivateReceipt(t *testing.T) {
	command := testDeploymentCommand(t)
	want := validDeploymentResult(command)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/prod/v2/commands" || request.TLS == nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		var received DeploymentCommand
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil || received.Validate() != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(writer).Encode(want)
	}))
	defer server.Close()
	client := newTestClient(t, server, DefaultMaxResponseBytes)
	got, err := client.SubmitDeployment(t.Context(), command)
	if err != nil {
		t.Fatalf("SubmitDeployment: %v", err)
	}
	if got.Status != "deployment_created" || got.Deployment.InstanceID != want.Deployment.InstanceID || got.Receipt.RequestSHA256 != command.RequestSHA256() {
		t.Fatalf("unexpected validated deployment result: %#v", got)
	}
}

func testDeploymentCommand(t *testing.T) DeploymentCommand {
	t.Helper()
	now := time.Date(2026, time.July, 14, 12, 0, 0, 123000000, time.UTC)
	proof := testDeploymentApprovalProof(t, now)
	seed := bytes.Repeat([]byte{0x6a}, ed25519.SeedSize)
	command, err := NewDeploymentCommand(DeploymentCommandInput{
		ConnectionID: "connection-create-0001", CommandID: "command-deployment-0001", NodeKeyID: "node-key-1",
		ExpectedGeneration: 2, NodeCounter: 9, IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute),
		Request: DeploymentRequest{
			Schema: DeploymentCreateSchema, DeploymentID: "deployment-create-0001", ConnectionGeneration: 2,
			PlanHash: namedDigest('a'), PlanRevision: 4, QuoteID: "quote-create-0001", QuoteDigest: testDeploymentQuoteDigest(t, now),
			CandidateID: "candidate-create-0001", ResourceManifestDigest: namedDigest('c'),
			WorkerArtifact: DeploymentWorkerArtifact{Kind: "fixed_ami", AMIID: "ami-0123456789abcdef0"},
			Network:        DeploymentNetwork{VPCID: "vpc-0123456789abcdef0", SubnetID: "subnet-0123456789abcdef0", AvailabilityZone: "us-east-1a"},
		},
		ApprovalProof: proof, PrivateKey: ed25519.NewKeyFromSeed(seed),
	})
	if err != nil {
		t.Fatalf("NewDeploymentCommand: %v", err)
	}
	return command
}

func testDeploymentApprovalProof(t *testing.T, now time.Time) cloudcontracts.ApprovalV1 {
	t.Helper()
	proof := cloudcontracts.ApprovalV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1, ApprovalID: "approval-create-0001", ChallengeID: "challenge-create-0001", SignerKeyID: "device-key-1",
		PlanID: "plan-create-0001", PlanHash: namedDigest('a'), PlanRevision: 4, QuoteID: "quote-create-0001", QuoteDigest: testDeploymentQuoteDigest(t, now),
		QuoteValidUntil: now.Add(10 * time.Minute), CloudConnectionID: "connection-create-0001", RecipeDigest: namedDigest('e'),
		ResourceScope: cloudcontracts.ResourceScopeV1{
			Region: "us-east-1", AvailabilityZones: []string{"us-east-1a"}, InstanceType: "m7i.xlarge", Architecture: cloudcontracts.ArchitectureAMD64,
			VCPU: 4, MemoryMiB: 16384, DiskGiB: 80, PurchaseOption: cloudcontracts.PurchaseOnDemand,
		},
		NetworkScope: cloudcontracts.NetworkScopeV1{PublicIngress: false, EntryPoint: cloudcontracts.EntryPointNone, TLSRequired: false, AuthenticationRequired: false},
		ExpiresAt:    now.Add(5 * time.Minute),
	}
	seed := bytes.Repeat([]byte{0x4f}, ed25519.SeedSize)
	signed, err := proof.Sign(ed25519.NewKeyFromSeed(seed), now)
	if err != nil {
		t.Fatalf("sign ApprovalV1: %v", err)
	}
	return signed
}

func testDeploymentQuoteDigest(t *testing.T, now time.Time) string {
	t.Helper()
	quote := cloudcontracts.QuoteV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1, QuoteID: "quote-create-0001", CloudConnectionID: "connection-create-0001",
		Region: "us-east-1", Currency: "USD", QuotedAt: now, ValidUntil: now.Add(10 * time.Minute),
		Candidates:    []cloudcontracts.QuoteCandidateV1{{CandidateID: "candidate-create-0001", Tier: cloudcontracts.QuoteTierRecommended, InstanceType: "m7i.xlarge", PurchaseOption: cloudcontracts.PurchaseOnDemand, Architecture: cloudcontracts.ArchitectureAMD64, VCPU: 4, MemoryMiB: 16384, HourlyMinor: 10, ThirtyDayMinor: 7200, EstimatedDiskGiB: 80, AvailabilityZones: []string{"us-east-1a"}}},
		IncludedItems: []string{"ec2_linux_ondemand"}, UnincludedItems: []string{"cloudwatch_logs", "data_transfer", "ebs_gp3", "public_ipv4", "snapshots", "taxes"},
	}
	digest, err := quote.Digest()
	if err != nil {
		t.Fatalf("QuoteV1.Digest: %v", err)
	}
	return digest
}

func validDeploymentResult(command DeploymentCommand) DeploymentResult {
	return DeploymentResult{
		Status: "deployment_created",
		Receipt: DeploymentCommandReceipt{
			Schema: ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID, ExpectedGeneration: command.ExpectedGeneration,
			NodeCounter: command.NodeCounter, CommandID: command.CommandID, RequestSHA256: command.RequestSHA256(), Action: DeploymentAction,
		},
		Deployment: DeploymentReceipt{
			Schema: DeploymentReceiptSchema, ConnectionID: command.ConnectionID, DeploymentID: "deployment-create-0001", RequestSHA256: command.RequestSHA256(),
			ResourceStatus: "provisioning", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, NetworkInterfaceIDs: []string{"eni-0123456789abcdef0"},
		},
	}
}

func namedDigest(character rune) string {
	return "sha256:" + strings.Repeat(string(character), 64)
}
