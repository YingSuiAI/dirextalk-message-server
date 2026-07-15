package api

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

func TestBrokerAuthenticatesBeforeFailClosedOperation(t *testing.T) {
	privateKey := testPrivateKey()
	raw := signedCommand(t, privateKey, contract.ActionArtifactPut)
	broker := Broker{
		Resolver: StaticKeyResolver{
			ConnectionID: "connection-0001",
			NodeKeyID:    "node-key-01",
			Generation:   1,
			PublicKey:    privateKey.Public().(ed25519.PublicKey),
		},
		Now: func() time.Time { return time.Date(2026, 7, 15, 1, 2, 4, 0, time.UTC) },
	}
	response := serve(t, broker, http.MethodPost, "/v2/commands", raw)
	if response.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d; body %s", response.Code, http.StatusNotImplemented, response.Body.String())
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	assertErrorCode(t, response, "operation_not_enabled")
}

func TestBrokerRejectsInvalidSignatureBeforeOperationGate(t *testing.T) {
	privateKey := testPrivateKey()
	raw := signedCommand(t, privateKey, contract.ActionQuoteRequest)
	otherSeed := sha256.Sum256([]byte("other-key"))
	other := ed25519.NewKeyFromSeed(otherSeed[:])
	broker := Broker{
		Resolver: StaticKeyResolver{
			ConnectionID: "connection-0001",
			NodeKeyID:    "node-key-01",
			Generation:   1,
			PublicKey:    other.Public().(ed25519.PublicKey),
		},
		Now: func() time.Time { return time.Date(2026, 7, 15, 1, 2, 4, 0, time.UTC) },
	}
	response := serve(t, broker, http.MethodPost, "/v2/commands", raw)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body %s", response.Code, http.StatusForbidden, response.Body.String())
	}
	assertErrorCode(t, response, "invalid_node_signature")
}

func TestBrokerDeploymentIsRejectedWithoutInvokingAnyProvider(t *testing.T) {
	privateKey := testPrivateKey()
	raw := signedCommand(t, privateKey, contract.ActionDeploymentCreate)
	broker := Broker{Resolver: StaticKeyResolver{
		ConnectionID: "connection-0001",
		NodeKeyID:    "node-key-01",
		Generation:   1,
		PublicKey:    privateKey.Public().(ed25519.PublicKey),
	}}
	response := serve(t, broker, http.MethodPost, "/v2/commands", raw)
	if response.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d; body %s", response.Code, http.StatusNotImplemented, response.Body.String())
	}
	assertErrorCode(t, response, "operation_not_enabled")
}

func TestBrokerFailsClosedWhenNotConfigured(t *testing.T) {
	response := serve(t, Broker{}, http.MethodPost, "/v2/commands", []byte(`{}`))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	assertErrorCode(t, response, "broker_not_configured")
}

func TestBrokerRejectsOversizedBody(t *testing.T) {
	response := serve(t, Broker{Resolver: StaticKeyResolver{}}, http.MethodPost, "/v2/commands", bytes.Repeat([]byte("x"), contract.MaxCommandBytes+1))
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
	assertErrorCode(t, response, "request_too_large")
}

func TestBrokerRestrictsMethodAndPath(t *testing.T) {
	broker := Broker{}
	method := serve(t, broker, http.MethodGet, "/v2/commands", nil)
	if method.Code != http.StatusMethodNotAllowed {
		t.Fatalf("method status = %d, want %d", method.Code, http.StatusMethodNotAllowed)
	}
	assertErrorCode(t, method, "method_not_allowed")
	path := serve(t, broker, http.MethodPost, "/v2/anything", nil)
	if path.Code != http.StatusNotFound {
		t.Fatalf("path status = %d, want %d", path.Code, http.StatusNotFound)
	}
	assertErrorCode(t, path, "not_found")
	query := serve(t, broker, http.MethodPost, "/v2/commands?unexpected=true", nil)
	if query.Code != http.StatusNotFound {
		t.Fatalf("query status = %d, want %d", query.Code, http.StatusNotFound)
	}
	assertErrorCode(t, query, "not_found")
}

func TestNewStaticKeyResolverRequiresPKIXSPKIEd25519Key(t *testing.T) {
	privateKey := testPrivateKey()
	encoded, err := x509.MarshalPKIXPublicKey(privateKey.Public())
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewStaticKeyResolver(
		"connection-0001",
		"node-key-01",
		base64.StdEncoding.EncodeToString(encoded),
		1,
	)
	if err != nil {
		t.Fatalf("NewStaticKeyResolver() error = %v", err)
	}
	if registration, found := resolver.Lookup(t.Context(), "connection-0001", "node-key-01"); !found || registration.Generation != 1 || !bytes.Equal(registration.PublicKey, privateKey.Public().(ed25519.PublicKey)) {
		t.Fatalf("Lookup() = (%v, %t), want registered public key", registration, found)
	}
	if _, err := NewStaticKeyResolver(
		"connection-0001",
		"node-key-01",
		base64.StdEncoding.EncodeToString(privateKey.Public().(ed25519.PublicKey)),
		1,
	); err == nil {
		t.Fatal("NewStaticKeyResolver() accepted a raw Ed25519 key instead of PKIX/SPKI")
	}
	if _, err := NewStaticKeyResolver(
		"short",
		"node-key-01",
		base64.StdEncoding.EncodeToString(encoded),
		1,
	); err == nil {
		t.Fatal("NewStaticKeyResolver() accepted an invalid Connection ID")
	}
}

func serve(t *testing.T, broker Broker, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	broker.ServeHTTP(response, request)
	return response
}

func assertErrorCode(t *testing.T, response *httptest.ResponseRecorder, want string) {
	t.Helper()
	var result struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.Error.Code != want {
		t.Fatalf("error code = %q, want %q", result.Error.Code, want)
	}
}

func signedCommand(t *testing.T, privateKey ed25519.PrivateKey, action string) []byte {
	t.Helper()
	payload := []byte(`{"quote_request_id":"quote-0001","plan_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","region":"ap-northeast-1","candidates":[]}`)
	sum := sha256.Sum256(payload)
	command := contract.Command{
		Schema:             contract.CommandSchema,
		ConnectionID:       "connection-0001",
		CommandID:          "command-0001",
		NodeKeyID:          "node-key-01",
		IssuedAt:           "2026-07-15T01:02:03.000Z",
		ExpiresAt:          "2026-07-15T01:07:03.000Z",
		ExpectedGeneration: 1,
		NodeCounter:        7,
		Action:             action,
		PayloadB64:         base64.StdEncoding.EncodeToString(payload),
		PayloadSHA256:      hex.EncodeToString(sum[:]),
	}
	if action == contract.ActionDeploymentCreate {
		command.ApprovalProof = json.RawMessage(`{"approval_id":"approval-0001"}`)
	}
	if action == contract.ActionDeploymentCreate {
		command.SignatureB64 = base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
	} else {
		signatureBase, err := command.SignatureBase()
		if err != nil {
			t.Fatal(err)
		}
		command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(signatureBase)))
	}
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func testPrivateKey() ed25519.PrivateKey {
	seed := sha256.Sum256([]byte("dirextalk-connection-stack-v2-api-test"))
	return ed25519.NewKeyFromSeed(seed[:])
}
