package cloudworker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

func TestSecretMaterializerUsesOnlyTheBoundWorkerRouteAndAuthorization(t *testing.T) {
	var gotBody string
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v2/worker-sessions/worker-session-v2-01/service-secrets/materialize" || request.URL.RawQuery != "" {
			t.Fatalf("unexpected materialize URL %q", request.URL.String())
		}
		if request.Header.Get("Authorization") != "Bearer token" || request.Header.Get("X-Dirextalk-Worker-Lease-Epoch") != "7" {
			t.Fatalf("unexpected authorization headers")
		}
		body, _ := io.ReadAll(request.Body)
		gotBody = string(body)
		writer.Header().Set("Content-Type", "application/octet-stream")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("canary-value"))
	}))
	defer server.Close()
	client := activeMaterializeSession(t, server)
	materializer, err := client.NewSecretMaterializer()
	if err != nil {
		t.Fatal(err)
	}
	request := recipeexec.SecretMaterializeRequest{TaskID: "task-0001", ExecutionID: "execution-1", ManifestDigest: testDigest, ArtifactDigest: testDigest, SlotID: "model-token", SecretRef: "secret_ref:model-token"}
	value, err := materializer.Materialize(context.Background(), request)
	if err != nil || string(value) != "canary-value" {
		t.Fatalf("Materialize() = %q, %v", value, err)
	}
	want := `{"task_id":"task-0001","execution_id":"execution-1","manifest_digest":"` + testDigest + `","artifact_digest":"` + testDigest + `","slot_id":"model-token","secret_ref":"secret_ref:model-token"}`
	if gotBody != want {
		t.Fatalf("body = %s, want %s", gotBody, want)
	}
}

func TestSecretMaterializerRejectsRedirectContentTypeAndOversize(t *testing.T) {
	for _, test := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"redirect", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "https://example.invalid/steal", http.StatusTemporaryRedirect)
		}},
		{"content_type", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":"bad"}`))
		}},
		{"oversize", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write([]byte(strings.Repeat("x", maxMaterializedSecretSize+1)))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewTLSServer(test.handler)
			defer server.Close()
			materializer, _ := activeMaterializeSession(t, server).NewSecretMaterializer()
			_, err := materializer.Materialize(context.Background(), recipeexec.SecretMaterializeRequest{TaskID: "task-0001", ExecutionID: "execution-1", ManifestDigest: testDigest, ArtifactDigest: testDigest, SlotID: "slot-1", SecretRef: "secret_ref:x"})
			if err == nil {
				t.Fatal("Materialize() accepted invalid response")
			}
		})
	}
}

func TestSecretMaterializerMapsPendingUploadToRetryableSentinel(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooEarly)
		_, _ = w.Write([]byte(`{"code":"worker_service_secret_pending"}`))
	}))
	defer server.Close()
	materializer, _ := activeMaterializeSession(t, server).NewSecretMaterializer()
	_, err := materializer.Materialize(context.Background(), recipeexec.SecretMaterializeRequest{TaskID: "task-0001", ExecutionID: "execution-1", ManifestDigest: testDigest, ArtifactDigest: testDigest, SlotID: "slot-1", SecretRef: "secret_ref:dynamic-plan/x"})
	if !errors.Is(err, recipeexec.ErrSecretMaterializePending) {
		t.Fatalf("pending error=%v", err)
	}
}

func activeMaterializeSession(t *testing.T, server *httptest.Server) *SessionClient {
	t.Helper()
	endpoint := server.URL + "/v2/worker-sessions"
	client, err := NewSessionClient(validTestManifest(endpoint), SessionClientConfig{ExpectedConnectionID: "connection-v2-0001", ExpectedBootstrapEndpoint: endpoint, HTTPClient: server.Client(), Now: func() time.Time { return time.Date(2026, 7, 14, 7, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	client.state = SessionStateActive
	client.access = "token"
	client.epoch = 7
	return client
}
