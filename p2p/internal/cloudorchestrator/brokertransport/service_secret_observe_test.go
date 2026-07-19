package brokertransport

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestServiceSecretObserveTransportBuildsAndReplaysExactSignedEnvelope(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Date(2026, 7, 15, 1, 2, 3, 0, time.UTC)
	transport, _ := New(key, func() time.Time { return now })
	request := transportSecretObserveRequest()
	command := runtime.ServiceSecretObserveCommand{CommandID: "command-secret-observe-0001", ConnectionID: "connection-0001", NodeKeyID: "node-key-1", ExpectedGeneration: 2, NodeCounter: 41, Attempt: 1, Action: runtime.ServiceSecretObserveAction}
	signed, err := transport.BuildServiceSecretObserveCommand(command, request, now)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := broker.ParseServiceSecretObserveCommand([]byte(signed.EnvelopeJSON))
	if err != nil || parsed.RequestSHA256() != signed.RequestSHA256 || parsed.PayloadSHA256 != signed.PayloadSHA256 {
		t.Fatalf("parsed=%#v err=%v", parsed, err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var got broker.ServiceSecretObserveCommand
		if json.NewDecoder(r.Body).Decode(&got) != nil || got.RequestSHA256() != signed.RequestSHA256 {
			t.Fatal("transport rebuilt persisted command")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"schema":"dirextalk.service-secret-observation/v1","session_id":"secret-session-0001","status":"completed","provider_version":"version-1","binding_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","updated_marker":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}`))
	}))
	defer server.Close()
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	transport.rootCAs = pool
	result, err := transport.RequestServiceSecretObserve(t.Context(), server.URL+"/v2/commands", command, signed, request)
	if err != nil || result.Status != "completed" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	tampered := signed
	tampered.PayloadJSON = strings.ReplaceAll(signed.PayloadJSON, "model-token", "other-token")
	if _, err := transport.RequestServiceSecretObserve(t.Context(), server.URL+"/v2/commands", command, tampered, request); err == nil {
		t.Fatal("tampered persisted payload accepted")
	}
}

func TestServiceSecretObserveTransportClassifiesPreCreate404AsUnavailable(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Date(2026, 7, 15, 1, 2, 3, 0, time.UTC)
	request := transportSecretObserveRequest()
	command := runtime.ServiceSecretObserveCommand{CommandID: "command-secret-observe-0001", ConnectionID: "connection-0001", NodeKeyID: "node-key-1", ExpectedGeneration: 2, NodeCounter: 41, Attempt: 1, Action: runtime.ServiceSecretObserveAction}
	transport, _ := New(key, func() time.Time { return now })
	signed, err := transport.BuildServiceSecretObserveCommand(command, request, now)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"service_secret_session_not_found"}`))
	}))
	defer server.Close()
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	transport.rootCAs = pool
	_, err = transport.RequestServiceSecretObserve(t.Context(), server.URL+"/v2/commands", command, signed, request)
	if err == nil || err.Error() != "service_secret_observe_unavailable" {
		t.Fatalf("404 err=%v", err)
	}
}

func transportSecretObserveRequest() runtime.ServiceSecretObserveRequest {
	return runtime.ServiceSecretObserveRequest{SessionID: "secret-session-0001", DeploymentID: "deployment-0001", TaskID: "recipe-task-0001", ExecutionID: "execution-0001", ManifestDigest: "sha256:" + strings.Repeat("a", 64), SecretRef: "secret_ref:model-token", ContextDigest: "sha256:" + strings.Repeat("b", 64)}
}
