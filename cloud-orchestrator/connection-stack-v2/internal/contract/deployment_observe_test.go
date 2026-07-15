package contract

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestDeploymentObserveCommandMatchesOrchestratorGolden(t *testing.T) {
	command := deploymentObserveTestCommand(t)
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse(): %v", err)
	}
	request, err := parsed.DeploymentObserveRequest()
	if err != nil || request.DeploymentID != "deployment-create-0001" {
		t.Fatalf("DeploymentObserveRequest() = %#v, %v", request, err)
	}
	if err := parsed.VerifyNodeSignature(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x7a}, ed25519.SeedSize)).Public().(ed25519.PublicKey)); err != nil {
		t.Fatalf("VerifyNodeSignature(): %v", err)
	}

	const wantPayload = `{"deployment_id":"deployment-create-0001"}`
	payload, _ := base64.StdEncoding.DecodeString(parsed.PayloadB64)
	if string(payload) != wantPayload {
		t.Fatalf("payload = %s, want %s", payload, wantPayload)
	}
	const wantSignatureBase = "dirextalk.aws.command-signature/v2\n" +
		"schema=dirextalk.aws.command/v2\n" +
		"connection_id=connection-create-0001\n" +
		"command_id=command-observe-0001\n" +
		"node_key_id=node-key-1\n" +
		"issued_at=2026-07-15T08:00:00.000Z\n" +
		"expires_at=2026-07-15T08:04:00.000Z\n" +
		"expected_generation=2\n" +
		"node_counter=10\n" +
		"action=deployment.observe\n" +
		"payload_sha256=e9aef54c7fee353de6e1ea032a90056cd4dfa64f1c6859a17e054f7ff688cfee\n" +
		"approval_binding_sha256=\n" +
		"approval_challenge_id=\n" +
		"approval_signature_sha256=\n" +
		"approval_proof_payload_sha256=\n"
	if got, err := parsed.SignatureBase(); err != nil || got != wantSignatureBase {
		t.Fatalf("SignatureBase() = %q, %v; want %q", got, err, wantSignatureBase)
	}
}

func TestDeploymentObserveResultIsExactFreshAndDeSecreted(t *testing.T) {
	command := deploymentObserveTestCommand(t)
	observation := validDeploymentObservation()
	raw, err := MarshalDeploymentObserveResult(command, observation, false)
	if err != nil {
		t.Fatalf("MarshalDeploymentObserveResult(): %v", err)
	}
	result, err := DecodeDeploymentObserveResult(command, raw)
	if err != nil {
		t.Fatalf("DecodeDeploymentObserveResult(): %v", err)
	}
	if result.Status != "deployment_observed" || result.Receipt.Disposition != "committed" || result.Observation.Worker.BootstrapSessionState != "active" {
		t.Fatalf("unexpected result: %#v", result)
	}
	if strings.Contains(string(raw), "bootstrap_session_id") || strings.Contains(string(raw), "token") || strings.Contains(string(raw), "endpoint") {
		t.Fatalf("response leaked Worker-private state: %s", raw)
	}

	idempotent, err := MarshalDeploymentObserveResult(command, observation, true)
	if err != nil {
		t.Fatalf("idempotent MarshalDeploymentObserveResult(): %v", err)
	}
	result, err = DecodeDeploymentObserveResult(command, idempotent)
	if err != nil || result.Status != "idempotent" || result.Receipt.Disposition != "idempotent" {
		t.Fatalf("idempotent result = %#v, %v", result, err)
	}
}

func TestDeploymentObserveResultRejectsExpandedOrInvalidWorkerState(t *testing.T) {
	command := deploymentObserveTestCommand(t)
	raw, err := MarshalDeploymentObserveResult(command, validDeploymentObservation(), false)
	if err != nil {
		t.Fatal(err)
	}
	expanded := bytes.Replace(raw, []byte(`"last_event_at":`), []byte(`"worker_token":"must-not-leak","last_event_at":`), 1)
	if _, err := DecodeDeploymentObserveResult(command, expanded); Code(err) != "invalid_deployment_observation" {
		t.Fatalf("expanded response code = %q, want invalid_deployment_observation", Code(err))
	}

	observation := validDeploymentObservation()
	stale := "2026-07-15T08:00:10.000Z"
	observation.Worker.LeaseExpiresAt = &stale
	if _, err := MarshalDeploymentObserveResult(command, observation, false); Code(err) != "invalid_deployment_observation" {
		t.Fatalf("expired lease code = %q, want invalid_deployment_observation", Code(err))
	}

	observation = validDeploymentObservation()
	observation.Worker.LastSequence = 0
	if _, err := MarshalDeploymentObserveResult(command, observation, false); Code(err) != "invalid_deployment_observation" {
		t.Fatalf("event without sequence code = %q, want invalid_deployment_observation", Code(err))
	}
}

func deploymentObserveTestCommand(t *testing.T) Command {
	t.Helper()
	payload := []byte(`{"deployment_id":"deployment-create-0001"}`)
	payloadDigest := sha256.Sum256(payload)
	command := Command{
		Schema: CommandSchema, ConnectionID: "connection-create-0001", CommandID: "command-observe-0001",
		NodeKeyID: "node-key-1", IssuedAt: "2026-07-15T08:00:00.000Z", ExpiresAt: "2026-07-15T08:04:00.000Z",
		ExpectedGeneration: 2, NodeCounter: 10, Action: ActionDeploymentObserve,
		PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: hex.EncodeToString(payloadDigest[:]),
	}
	signatureBase, err := command.SignatureBase()
	if err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x7a}, ed25519.SeedSize))
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(signatureBase)))
	return command
}

func validDeploymentObservation() DeploymentObservation {
	leaseExpiresAt := "2026-07-15T08:01:30.000Z"
	lastEventAt := "2026-07-15T08:00:10.000Z"
	return DeploymentObservation{
		Schema: DeploymentObservationSchema, DeploymentID: "deployment-create-0001",
		Resource: DeploymentObservationResource{Status: "provisioning", InstanceID: "i-0123456789abcdef0"},
		Worker: DeploymentObservationWorker{
			BootstrapSessionState: "active", LeaseEpoch: 1, LeaseExpiresAt: &leaseExpiresAt,
			LastSequence: 1, LastEventAt: &lastEventAt,
		},
		ObservedAt: "2026-07-15T08:00:15.000Z",
	}
}
