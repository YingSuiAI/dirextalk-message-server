package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

func TestWorkerClaimBindsIIDAndStoresOnlyRotatedTokenDigest(t *testing.T) {
	broker, store, _, createRaw := deploymentTestBroker(t)
	created := serve(t, broker, http.MethodPost, "/v2/commands", createRaw)
	if created.Code != http.StatusOK {
		t.Fatal(created.Body.String())
	}
	session := onlyWorkerSession(t, store)
	verifier := &recordingWorkerIdentityVerifier{}
	tokens := &sequenceWorkerTokens{values: []string{"first-token-with-safe-length", "second-token-with-safe-length"}}
	broker.WorkerIdentity = verifier
	broker.WorkerTokens = tokens

	first := serveWorkerClaim(t, broker, session, validWorkerClaim(session))
	if first.Code != http.StatusOK {
		t.Fatalf("first claim status=%d body=%s", first.Code, first.Body.String())
	}
	var firstResult contract.WorkerSessionClaimResponse
	if err := json.Unmarshal(first.Body.Bytes(), &firstResult); err != nil || firstResult.LeaseEpoch != 1 || firstResult.AccessToken != tokens.values[0] {
		t.Fatalf("first result=%#v err=%v", firstResult, err)
	}
	stored := onlyWorkerSession(t, store)
	if stored.TokenSHA256 == "" || strings.Contains(stored.TokenSHA256, "token") || strings.Contains(first.Body.String(), stored.TokenSHA256) {
		t.Fatalf("token storage/response boundary violated stored=%q body=%s", stored.TokenSHA256, first.Body.String())
	}

	second := serveWorkerClaim(t, broker, stored, validWorkerClaim(stored))
	if second.Code != http.StatusOK {
		t.Fatalf("renew claim status=%d body=%s", second.Code, second.Body.String())
	}
	var secondResult contract.WorkerSessionClaimResponse
	if err := json.Unmarshal(second.Body.Bytes(), &secondResult); err != nil || secondResult.LeaseEpoch != 2 || secondResult.AccessToken != tokens.values[1] {
		t.Fatalf("second result=%#v err=%v", secondResult, err)
	}
	if verifier.calls != 2 || onlyWorkerSession(t, store).TokenSHA256 == stored.TokenSHA256 {
		t.Fatalf("renewal did not independently verify and rotate")
	}
}

func TestWorkerClaimRejectsBindingAndIdentityBeforeActivation(t *testing.T) {
	broker, store, _, createRaw := deploymentTestBroker(t)
	if result := serve(t, broker, http.MethodPost, "/v2/commands", createRaw); result.Code != http.StatusOK {
		t.Fatal(result.Body.String())
	}
	session := onlyWorkerSession(t, store)
	verifier := &recordingWorkerIdentityVerifier{err: NewError("worker_identity_rejected", http.StatusForbidden)}
	broker.WorkerIdentity = verifier
	broker.WorkerTokens = &sequenceWorkerTokens{values: []string{"unused-token-with-safe-length"}}

	badBinding := validWorkerClaim(session)
	badBinding.DeploymentID = "deployment-other-0001"
	response := serveWorkerClaim(t, broker, session, badBinding)
	assertHTTPError(t, response, http.StatusForbidden, "worker_session_not_found")
	if verifier.calls != 0 || onlyWorkerSession(t, store).State != "bound" {
		t.Fatal("binding failure reached verifier or activation")
	}

	response = serveWorkerClaim(t, broker, session, validWorkerClaim(session))
	assertHTTPError(t, response, http.StatusForbidden, "worker_identity_rejected")
	if verifier.calls != 1 || onlyWorkerSession(t, store).State != "bound" {
		t.Fatal("identity failure activated session")
	}
}

func TestWorkerClaimRejectsExpiredFirstClaimButAllowsBoundedActiveReconnect(t *testing.T) {
	broker, store, _, createRaw := deploymentTestBroker(t)
	if result := serve(t, broker, http.MethodPost, "/v2/commands", createRaw); result.Code != http.StatusOK {
		t.Fatal(result.Body.String())
	}
	session := onlyWorkerSession(t, store)
	broker.WorkerIdentity = &recordingWorkerIdentityVerifier{}
	broker.WorkerTokens = &sequenceWorkerTokens{values: []string{"reconnect-token-with-safe-length"}}
	broker.Now = func() time.Time { return time.Date(2026, 7, 14, 12, 12, 0, 0, time.UTC) }

	response := serveWorkerClaim(t, broker, session, validWorkerClaim(session))
	assertHTTPError(t, response, http.StatusConflict, "worker_session_expired")

	store.mu.Lock()
	session.State = "active"
	session.LeaseEpoch = 1
	session.LeaseExpiresAt = "2026-07-14T12:11:00.000Z"
	session.TokenSHA256 = strings.Repeat("a", 64)
	store.workerSessions[session.BootstrapSessionID] = session
	store.mu.Unlock()
	response = serveWorkerClaim(t, broker, session, validWorkerClaim(session))
	if response.Code != http.StatusOK {
		t.Fatalf("active reconnect status=%d body=%s", response.Code, response.Body.String())
	}
}

type recordingWorkerIdentityVerifier struct {
	calls int
	err   error
}

func (v *recordingWorkerIdentityVerifier) VerifyWorkerIdentity(_ context.Context, _ contract.WorkerSessionClaimRequest, _ commandstore.WorkerSession) error {
	v.calls++
	return v.err
}

type sequenceWorkerTokens struct {
	values []string
	next   int
}

func (g *sequenceWorkerTokens) GenerateWorkerToken() (string, error) {
	if g.next >= len(g.values) {
		return "", errors.New("no token")
	}
	value := g.values[g.next]
	g.next++
	return value, nil
}

func validWorkerClaim(session commandstore.WorkerSession) contract.WorkerSessionClaimRequest {
	return contract.WorkerSessionClaimRequest{
		Schema: contract.WorkerSessionClaimSchema, ConnectionID: session.ConnectionID, DeploymentID: session.DeploymentID,
		BootstrapSessionID: session.BootstrapSessionID, WorkerImageDigest: session.WorkerImageDigest,
		ArtifactManifestDigest:       session.ArtifactManifestDigest,
		InstanceIdentityDocumentB64:  base64.StdEncoding.EncodeToString([]byte(`{"accountId":"123456789012"}`)),
		InstanceIdentitySignatureB64: base64.StdEncoding.EncodeToString([]byte("signature")),
	}
}

func serveWorkerClaim(t *testing.T, broker Broker, session commandstore.WorkerSession, claim contract.WorkerSessionClaimRequest) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(claim)
	if err != nil {
		t.Fatal(err)
	}
	return serve(t, broker, http.MethodPost, "/v2/worker-sessions/"+session.BootstrapSessionID+"/claim", raw)
}

func onlyWorkerSession(t *testing.T, store *memoryCommandStore) commandstore.WorkerSession {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.workerSessions) != 1 {
		t.Fatalf("worker session count=%d", len(store.workerSessions))
	}
	for _, session := range store.workerSessions {
		return session
	}
	return commandstore.WorkerSession{}
}
