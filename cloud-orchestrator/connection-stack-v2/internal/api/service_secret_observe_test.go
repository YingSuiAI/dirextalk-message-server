package api

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

func TestBrokerObservesServiceSecretWithoutExposingPrivateStateAndReplays(t *testing.T) {
	privateKey := testPrivateKey()
	now := time.Date(2026, 7, 15, 1, 2, 4, 0, time.UTC)
	receipts := newMemoryCommandStore()
	secrets := newMemoryServiceSecretStore()
	secrets.sessions["secret-session-0001"] = observedSecretSession("uploaded")
	broker := readOnlyTestBroker(privateKey, receipts, nil, nil, func() time.Time { return now })
	broker.ServiceSecretsEnabled, broker.ServiceSecretStore = true, secrets
	raw := signedServiceSecretObserveCommand(t, privateKey, "command-secret-observe-0001", 1, "secret-session-0001", "secret_ref:model-token")

	first := serve(t, broker, http.MethodPost, commandPath, raw)
	if first.Code != http.StatusOK {
		t.Fatalf("first observe status=%d body=%s", first.Code, first.Body.String())
	}
	replay := serve(t, broker, http.MethodPost, commandPath, raw)
	if replay.Code != http.StatusOK || replay.Body.String() != first.Body.String() {
		t.Fatalf("replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	forbidden := [][]byte{[]byte("token-canary"), []byte("sealed-canary"), []byte("envelope-canary"), []byte("secret_ref:model-token"), []byte("arn:aws:secretsmanager")}
	stored := receipts.records["connection-0001\x00command-secret-observe-0001"].ResultJSON
	for _, canary := range forbidden {
		if bytes.Contains(first.Body.Bytes(), canary) || bytes.Contains(stored, canary) {
			t.Fatalf("private canary escaped: %q", canary)
		}
	}
	var observation contract.ServiceSecretObservation
	if json.Unmarshal(first.Body.Bytes(), &observation) != nil || observation.Status != "uploaded" || observation.ProviderVersion != "version-1" || observation.BindingDigest == "" {
		t.Fatalf("observation=%#v", observation)
	}
}

func TestBrokerServiceSecretObserveGateAndBindingFailuresAreFailClosed(t *testing.T) {
	privateKey := testPrivateKey()
	now := time.Date(2026, 7, 15, 1, 2, 4, 0, time.UTC)
	secrets := newMemoryServiceSecretStore()
	secrets.sessions["secret-session-0001"] = observedSecretSession("pending_upload")
	raw := signedServiceSecretObserveCommand(t, privateKey, "command-secret-observe-0001", 1, "secret-session-0001", "secret_ref:wrong")
	broker := readOnlyTestBroker(privateKey, newMemoryCommandStore(), nil, nil, func() time.Time { return now })
	assertHTTPError(t, serve(t, broker, http.MethodPost, commandPath, raw), http.StatusNotImplemented, "operation_not_enabled")
	broker.ServiceSecretsEnabled, broker.ServiceSecretStore = true, secrets
	mismatch := serve(t, broker, http.MethodPost, commandPath, raw)
	missingRaw := signedServiceSecretObserveCommand(t, privateKey, "command-secret-observe-0002", 2, "secret-session-9999", "secret_ref:wrong")
	missing := serve(t, broker, http.MethodPost, commandPath, missingRaw)
	if mismatch.Code != missing.Code || mismatch.Body.String() != missing.Body.String() {
		t.Fatalf("binding mismatch enumerates session: mismatch=%d/%s missing=%d/%s", mismatch.Code, mismatch.Body.String(), missing.Code, missing.Body.String())
	}
}

func TestServiceSecretObservationExpiresOnlyUnfinishedBootstrap(t *testing.T) {
	now := time.Date(2026, 7, 15, 1, 7, 4, 0, time.UTC)
	uploaded := observedSecretSession(commandstore.ServiceSecretUploaded)
	observation, err := serviceSecretObservation(uploaded, now)
	if err != nil || observation.Status != "expired" || observation.ProviderVersion != "" {
		t.Fatalf("expired uploaded observation=%#v err=%v", observation, err)
	}
	completed := uploaded
	completed.State = commandstore.ServiceSecretCompleted
	observation, err = serviceSecretObservation(completed, now)
	if err != nil || observation.Status != commandstore.ServiceSecretCompleted || observation.ProviderVersion != completed.ProviderVersion {
		t.Fatalf("retained completed observation=%#v err=%v", observation, err)
	}
}

func signedServiceSecretObserveCommand(t *testing.T, privateKey ed25519.PrivateKey, commandID string, counter int64, sessionID, secretRef string) []byte {
	t.Helper()
	payload := []byte(`{"session_id":"` + sessionID + `","deployment_id":"deployment-0001","task_id":"recipe-task-0001","execution_id":"execution-0001","manifest_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","secret_ref":"` + secretRef + `","context_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`)
	return signedReadOnlyCommand(t, privateKey, commandID, counter, contract.ActionServiceSecretObserve, payload)
}

func observedSecretSession(state string) commandstore.ServiceSecretSession {
	return commandstore.ServiceSecretSession{SessionID: "secret-session-0001", ConnectionID: "connection-0001", DeploymentID: "deployment-0001", TaskID: "recipe-task-0001", ExecutionID: "execution-0001", ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SecretRef: "secret_ref:model-token", ContextDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", ExpiresAt: "2026-07-15T01:07:03.000Z", State: state, ProviderVersion: "version-1", TokenSHA256: "token-canary", SealedPrivateKey: "sealed-canary", SealedUploadToken: "sealed-canary", EnvelopeDigest: "envelope-canary"}
}
