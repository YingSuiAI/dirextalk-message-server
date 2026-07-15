package contract

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestServiceSecretObserveContractIsExactAndReplaySafe(t *testing.T) {
	now := time.Date(2026, 7, 15, 1, 2, 3, 0, time.UTC)
	request := ServiceSecretObserveRequest{SessionID: "secret-session-0001", DeploymentID: "deployment-0001", TaskID: "recipe-task-0001", ExecutionID: "execution-0001", ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SecretRef: "secret_ref:model-token", ContextDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	command := fixtureCommand(t, ActionServiceSecretObserve, "command-secret-observe-0001", "node-key-1", 2, 41, CanonicalInstant(now), CanonicalInstant(now.Add(5*time.Minute)), request)
	parsed, err := command.ServiceSecretObserveRequest()
	if err != nil || parsed != request {
		t.Fatalf("ServiceSecretObserveRequest() = %#v, %v", parsed, err)
	}
	if digest, _ := command.RequestSHA256(); digest != "ecfaf5c2e55875bd823f0fef27f37f08bbd389f0d94b9006162fe64612e13827" {
		t.Fatalf("observe request golden = %s", digest)
	}
	observation := ServiceSecretObservation{Schema: ServiceSecretObservationSchema, SessionID: request.SessionID, Status: "uploaded", ProviderVersion: "version-1", BindingDigest: request.ContextDigest, UpdatedMarker: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
	raw, err := MarshalServiceSecretObservation(command, observation)
	if err != nil || ValidateCommittedResult(command, raw) != nil {
		t.Fatalf("observation = %s, err=%v", raw, err)
	}
	replay, err := IdempotentResult(command, raw)
	if err != nil || string(replay) != string(raw) {
		t.Fatalf("replay = %s, %v", replay, err)
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	for _, forbidden := range []string{"token", "sealed_private_key", "sealed_upload_token", "envelope", "ciphertext", "secret_ref", "name", "arn"} {
		if _, ok := fields[forbidden]; ok {
			t.Fatalf("observation exposed %q", forbidden)
		}
	}
	pending := observation
	pending.Status, pending.ProviderVersion = "pending_upload", ""
	pendingRaw, err := MarshalServiceSecretObservation(command, pending)
	if err != nil || bytes.Contains(pendingRaw, []byte("provider_version")) {
		t.Fatalf("pending observation exposed provider version: %s, %v", pendingRaw, err)
	}
}

func TestServiceSecretObserveRejectsExpandedPayloadAndPrivateResult(t *testing.T) {
	now := time.Date(2026, 7, 15, 1, 2, 3, 0, time.UTC)
	request := ServiceSecretObserveRequest{SessionID: "secret-session-0001", DeploymentID: "deployment-0001", TaskID: "recipe-task-0001", ExecutionID: "execution-0001", ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SecretRef: "secret_ref:model-token", ContextDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	command := fixtureCommand(t, ActionServiceSecretObserve, "command-secret-observe-0001", "node-key-1", 2, 41, CanonicalInstant(now), CanonicalInstant(now.Add(5*time.Minute)), request)
	var expanded map[string]any
	payload, _ := command.actionPayload()
	_ = json.Unmarshal(payload, &expanded)
	expanded["provider_version"] = "agent-selected"
	command = fixtureCommand(t, ActionServiceSecretObserve, "command-secret-observe-0002", "node-key-1", 2, 42, CanonicalInstant(now), CanonicalInstant(now.Add(5*time.Minute)), expanded)
	if _, err := command.ServiceSecretObserveRequest(); err == nil {
		t.Fatal("expanded observe payload accepted")
	}
}
