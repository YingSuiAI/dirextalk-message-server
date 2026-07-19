package broker

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestServiceSecretObserveMatchesStackGoldenAndStrictResponse(t *testing.T) {
	request := ServiceSecretObserveRequest{SessionID: "secret-session-0001", DeploymentID: "deployment-0001", TaskID: "recipe-task-0001", ExecutionID: "execution-0001", ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SecretRef: "secret_ref:model-token", ContextDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, ed25519.SeedSize))
	issued := time.Date(2026, 7, 15, 1, 2, 3, 0, time.UTC)
	command, err := NewServiceSecretObserveCommand(ServiceSecretObserveCommandInput{ConnectionID: "connection-0001", CommandID: "command-secret-observe-0001", NodeKeyID: "node-key-1", ExpectedGeneration: 2, NodeCounter: 41, IssuedAt: issued, ExpiresAt: issued.Add(5 * time.Minute), Request: request, PrivateKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if command.RequestSHA256() != "ecfaf5c2e55875bd823f0fef27f37f08bbd389f0d94b9006162fe64612e13827" {
		t.Fatalf("request digest=%s", command.RequestSHA256())
	}
	raw, _ := json.Marshal(command)
	parsed, err := ParseServiceSecretObserveCommand(raw)
	if err != nil || parsed.RequestSHA256() != command.RequestSHA256() {
		t.Fatalf("parsed=%#v err=%v", parsed, err)
	}

	response := `{"schema":"dirextalk.service-secret-observation/v1","session_id":"secret-session-0001","status":"completed","provider_version":"version-1","binding_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","updated_marker":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}`
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(response))
	}))
	defer server.Close()
	client := newTestClient(t, server, DefaultMaxResponseBytes)
	result, err := client.SubmitServiceSecretObserve(t.Context(), command)
	if err != nil || result.Status != "completed" || result.ProviderVersion != "version-1" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestServiceSecretObserveRejectsPrivateOrUnboundResponse(t *testing.T) {
	request := ServiceSecretObserveRequest{SessionID: "secret-session-0001", DeploymentID: "deployment-0001", TaskID: "recipe-task-0001", ExecutionID: "execution-0001", ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SecretRef: "secret_ref:model-token", ContextDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, ed25519.SeedSize))
	now := time.Date(2026, 7, 15, 1, 2, 3, 0, time.UTC)
	command, _ := NewServiceSecretObserveCommand(ServiceSecretObserveCommandInput{ConnectionID: "connection-0001", CommandID: "command-secret-observe-0001", NodeKeyID: "node-key-1", ExpectedGeneration: 2, NodeCounter: 41, IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute), Request: request, PrivateKey: key})
	for _, body := range []string{`{"schema":"dirextalk.service-secret-observation/v1","session_id":"other-session-0001","status":"completed","provider_version":"version-1","binding_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","updated_marker":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}`, `{"schema":"dirextalk.service-secret-observation/v1","session_id":"secret-session-0001","status":"pending_upload","binding_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","updated_marker":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","sealed_private_key":"canary"}`} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
		}))
		client := newTestClient(t, server, DefaultMaxResponseBytes)
		if _, err := client.SubmitServiceSecretObserve(t.Context(), command); err == nil {
			t.Fatalf("accepted response %s", body)
		}
		server.Close()
	}
}
