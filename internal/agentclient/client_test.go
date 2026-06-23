package agentclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCallP2PActionPostsEnvelopeWithAgentToken(t *testing.T) {
	var gotAuth string
	var gotPath string
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	cfg, err := NewConfig(server.URL, "agent-token")
	if err != nil {
		t.Fatal(err)
	}
	client := New(cfg, server.Client())
	resp, err := client.CallP2PAction(context.Background(), "channels.list", map[string]any{"limit": 5}, P2PQuery)
	if err != nil {
		t.Fatal(err)
	}

	if gotPath != "/_p2p/query" {
		t.Fatalf("expected query path, got %q", gotPath)
	}
	if gotAuth != "Bearer agent-token" {
		t.Fatalf("expected bearer agent token, got %q", gotAuth)
	}
	if gotBody["action"] != "channels.list" {
		t.Fatalf("expected action body, got %#v", gotBody)
	}
	if resp["ok"] != true {
		t.Fatalf("expected ok response, got %#v", resp)
	}
}

func TestCallP2PActionReportsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "M_UNKNOWN_TOKEN"})
	}))
	defer server.Close()

	cfg, err := NewConfig(server.URL, "bad-token")
	if err != nil {
		t.Fatal(err)
	}
	client := New(cfg, server.Client())
	_, err = client.CallP2PAction(context.Background(), "channels.list", nil, P2PQuery)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "p2p channels.list failed with 401: M_UNKNOWN_TOKEN" {
		t.Fatalf("unexpected error: %v", err)
	}
}
