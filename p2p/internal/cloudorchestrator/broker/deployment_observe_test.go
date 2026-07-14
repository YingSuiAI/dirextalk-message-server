package broker

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewDeploymentObserveCommandUsesReadOnlyCanonicalEnvelope(t *testing.T) {
	command := testDeploymentObserveCommand(t)
	const wantPayload = `{"deployment_id":"deployment-create-0001"}`
	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := string(payload); got != wantPayload {
		t.Fatalf("observation payload differs\n got: %s\nwant: %s", got, wantPayload)
	}
	if got := command.SignatureBase(); !bytes.HasSuffix([]byte(got), []byte("approval_binding_sha256=\napproval_challenge_id=\napproval_signature_sha256=\napproval_proof_payload_sha256=\n")) {
		t.Fatalf("read-only command omitted required empty approval digest lines: %q", got)
	}
	seed := bytes.Repeat([]byte{0x7a}, ed25519.SeedSize)
	if err := command.VerifySignature(ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)); err != nil {
		t.Fatalf("verify command signature: %v", err)
	}
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseDeploymentObserveCommand(raw); err != nil {
		t.Fatalf("ParseDeploymentObserveCommand canonical envelope: %v", err)
	}
	if _, err := ParseDeploymentObserveCommand(bytes.Replace(raw, []byte(`"signature_b64"`), []byte(`"approval_proof"`), 1)); err == nil {
		t.Fatal("read-only deployment.observe accepted an approval proof field")
	}
}

func TestDeploymentObserveResultRequiresFreshDeSecretedActiveEvidence(t *testing.T) {
	command := testDeploymentObserveCommand(t)
	result := validDeploymentObserveResult(command)
	if err := ValidateDeploymentObserveResult(command, result); err != nil {
		t.Fatalf("valid observation: %v", err)
	}

	idempotent := result
	idempotent.Status = "idempotent"
	idempotent.Receipt.Disposition = "idempotent"
	if err := ValidateDeploymentObserveResult(command, idempotent); err != nil {
		t.Fatalf("fresh observation with idempotent command receipt: %v", err)
	}

	result.Observation.Worker.LeaseExpiresAt = stringPointer(command.IssuedAt)
	if err := ValidateDeploymentObserveResult(command, result); err == nil {
		t.Fatal("active Worker observation accepted an expired lease")
	}

	result = validDeploymentObserveResult(command)
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	mutated := bytes.Replace(raw, []byte(`"last_event_at":`), []byte(`"worker_token":"not-allowed","last_event_at":`), 1)
	if _, err := decodeDeploymentObserveResultJSON(mutated); err == nil {
		t.Fatal("observation response accepted a Worker secret field")
	}
}

func TestClientSubmitDeploymentObserveAcceptsOnlyBoundEvidence(t *testing.T) {
	command := testDeploymentObserveCommand(t)
	want := validDeploymentObserveResult(command)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/prod/v2/commands" || request.TLS == nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		var received DeploymentObserveCommand
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil || received.Validate() != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(writer).Encode(want)
	}))
	defer server.Close()

	got, err := newTestClient(t, server, DefaultMaxResponseBytes).SubmitDeploymentObserve(t.Context(), command)
	if err != nil {
		t.Fatalf("SubmitDeploymentObserve: %v", err)
	}
	request, err := command.DeploymentObserveRequest()
	if err != nil {
		t.Fatal(err)
	}
	if got.Observation.DeploymentID != request.DeploymentID || got.Observation.Worker.BootstrapSessionState != "active" || got.Receipt.RequestSHA256 != command.RequestSHA256() {
		t.Fatalf("unexpected validated observation: %#v", got)
	}
}

func testDeploymentObserveCommand(t *testing.T) DeploymentObserveCommand {
	t.Helper()
	now := time.Date(2026, time.July, 15, 8, 0, 0, 0, time.UTC)
	seed := bytes.Repeat([]byte{0x7a}, ed25519.SeedSize)
	command, err := NewDeploymentObserveCommand(DeploymentObserveCommandInput{
		ConnectionID: "connection-create-0001", CommandID: "command-observe-0001", NodeKeyID: "node-key-1",
		ExpectedGeneration: 2, NodeCounter: 10, IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute),
		Request: DeploymentObserveRequest{DeploymentID: "deployment-create-0001"}, PrivateKey: ed25519.NewKeyFromSeed(seed),
	})
	if err != nil {
		t.Fatalf("NewDeploymentObserveCommand: %v", err)
	}
	return command
}

func validDeploymentObserveResult(command DeploymentObserveCommand) DeploymentObserveResult {
	issuedAt, _ := parseCanonicalInstant(command.IssuedAt)
	observedAt := canonicalInstant(issuedAt.Add(15 * time.Second))
	leaseExpiresAt := canonicalInstant(issuedAt.Add(90 * time.Second))
	lastEventAt := canonicalInstant(issuedAt.Add(10 * time.Second))
	return DeploymentObserveResult{
		Status: "deployment_observed",
		Receipt: DeploymentObserveReceipt{
			Schema: ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID,
			ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, CommandID: command.CommandID,
			RequestSHA256: command.RequestSHA256(), Action: DeploymentObserveAction,
		},
		Observation: DeploymentObservation{
			Schema: DeploymentObservationSchema, DeploymentID: "deployment-create-0001",
			Resource: DeploymentObservationResource{Status: "provisioning", InstanceID: "i-0123456789abcdef0"},
			Worker: DeploymentObservationWorker{
				BootstrapSessionState: "active", LeaseEpoch: 1, LeaseExpiresAt: &leaseExpiresAt,
				LastSequence: 1, LastEventAt: &lastEventAt,
			},
			ObservedAt: observedAt,
		},
	}
}

func stringPointer(value string) *string { return &value }
