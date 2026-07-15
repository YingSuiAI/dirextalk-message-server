package api

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

func TestDeploymentObserveDefersUntilActiveThenReplaysFreshDeSecretedState(t *testing.T) {
	broker, store, _, createRaw := deploymentTestBroker(t)
	if response := serve(t, broker, http.MethodPost, "/v2/commands", createRaw); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	observeRaw := signedDeploymentObserveCommand(t, 10)
	bound := serve(t, broker, http.MethodPost, "/v2/commands", observeRaw)
	assertHTTPError(t, bound, http.StatusConflict, "worker_bootstrap_unavailable")
	if len(store.records) != 1 {
		t.Fatalf("bound observation consumed counter/receipt records=%d", len(store.records))
	}

	session := onlyWorkerSession(t, store)
	broker.WorkerIdentity = &recordingWorkerIdentityVerifier{}
	broker.WorkerTokens = &sequenceWorkerTokens{values: []string{"observe-token-with-safe-length"}}
	if response := serveWorkerClaim(t, broker, session, validWorkerClaim(session)); response.Code != http.StatusOK {
		t.Fatalf("claim status=%d body=%s", response.Code, response.Body.String())
	}
	first := serve(t, broker, http.MethodPost, "/v2/commands", observeRaw)
	if first.Code != http.StatusOK {
		t.Fatalf("observe status=%d body=%s", first.Code, first.Body.String())
	}
	command, err := contract.Parse(observeRaw)
	if err != nil {
		t.Fatal(err)
	}
	firstResult, err := contract.DecodeDeploymentObserveResult(command, bytes.TrimSpace(first.Body.Bytes()))
	if err != nil || firstResult.Status != "deployment_observed" || firstResult.Observation.Worker.BootstrapSessionState != "active" {
		t.Fatalf("first result=%#v err=%v", firstResult, err)
	}
	for _, forbidden := range []string{"bootstrap_session_id", "token_sha256", "bootstrap_endpoint", "identity_document", "raw_event"} {
		if strings.Contains(first.Body.String(), forbidden) {
			t.Fatalf("observe leaked %q: %s", forbidden, first.Body.String())
		}
	}

	broker.Now = func() time.Time { return time.Date(2026, 7, 14, 12, 2, 0, 0, time.UTC) }
	replay := serve(t, broker, http.MethodPost, "/v2/commands", observeRaw)
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	replayResult, err := contract.DecodeDeploymentObserveResult(command, bytes.TrimSpace(replay.Body.Bytes()))
	if err != nil || replayResult.Status != "idempotent" || replayResult.Receipt.Disposition != "idempotent" || replayResult.Observation.ObservedAt == firstResult.Observation.ObservedAt {
		t.Fatalf("replay result=%#v err=%v", replayResult, err)
	}
	if len(store.records) != 2 {
		t.Fatalf("replay duplicated durable receipt records=%d", len(store.records))
	}
}

func TestDeploymentObserveRejectsExpiredLeaseAndStoredReceiptExpansion(t *testing.T) {
	broker, store, _, createRaw := deploymentTestBroker(t)
	if response := serve(t, broker, http.MethodPost, "/v2/commands", createRaw); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	session := onlyWorkerSession(t, store)
	store.mu.Lock()
	session.State = "active"
	session.LeaseEpoch = 1
	session.LeaseExpiresAt = "2026-07-14T12:00:59.000Z"
	session.TokenSHA256 = strings.Repeat("a", 64)
	store.workerSessions[session.BootstrapSessionID] = session
	store.mu.Unlock()
	response := serve(t, broker, http.MethodPost, "/v2/commands", signedDeploymentObserveCommand(t, 10))
	assertHTTPError(t, response, http.StatusConflict, "worker_session_expired")
}

func signedDeploymentObserveCommand(t *testing.T, counter int64) []byte {
	t.Helper()
	payload := []byte(`{"deployment_id":"deployment-create-0001"}`)
	sum := sha256.Sum256(payload)
	command := contract.Command{
		Schema: contract.CommandSchema, ConnectionID: "connection-create-0001", CommandID: "command-observe-0001",
		NodeKeyID: "node-key-1", IssuedAt: "2026-07-14T12:01:00.000Z", ExpiresAt: "2026-07-14T12:06:00.000Z",
		ExpectedGeneration: 2, NodeCounter: counter, Action: contract.ActionDeploymentObserve,
		PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: hex.EncodeToString(sum[:]),
	}
	base, err := command.SignatureBase()
	if err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x6a}, ed25519.SeedSize))
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(base)))
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
