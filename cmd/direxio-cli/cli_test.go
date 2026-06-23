package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRootHelpMentionsCredentialsAndDomains(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected help exit 0, got %d stderr=%s", code, stderr.String())
	}
	text := stdout.String()
	for _, want := range []string{"DIREXIO_DOMAIN", "DIREXIO_AGENT_TOKEN", "contacts", "channels", "matrix", "p2p action"} {
		if !strings.Contains(text, want) {
			t.Fatalf("help missing %q:\n%s", want, text)
		}
	}
}

func TestUnknownCommandReturnsNonZero(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"missing"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("expected non-zero exit")
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout must stay empty on errors, got %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "unknown command") {
		t.Fatalf("expected unknown command error, got %q", stderr.String())
	}
}

func TestP2PActionRequiresParamsJSON(t *testing.T) {
	t.Setenv("DIREXIO_DOMAIN", "https://example.com")
	t.Setenv("DIREXIO_AGENT_TOKEN", "agent-token")
	var stdout, stderr bytes.Buffer
	code := run([]string{"p2p", "action", "channels.list", "--params", "not-json"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("expected invalid params to fail")
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout must be empty on failure, got %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "invalid params json") {
		t.Fatalf("unexpected stderr %q", stderr.String())
	}
}

func TestP2PActionCallsCommandEndpoint(t *testing.T) {
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
	t.Setenv("DIREXIO_DOMAIN", server.URL)
	t.Setenv("DIREXIO_AGENT_TOKEN", "agent-token")

	var stdout, stderr bytes.Buffer
	code := run([]string{"p2p", "action", "channels.list", "--params", `{"limit":5}`, "--raw"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected success, got %d stderr=%s", code, stderr.String())
	}
	if gotPath != "/_p2p/command" {
		t.Fatalf("expected command path, got %q", gotPath)
	}
	if gotAuth != "Bearer agent-token" {
		t.Fatalf("expected agent token auth, got %q", gotAuth)
	}
	if gotBody["action"] != "channels.list" {
		t.Fatalf("unexpected request body %#v", gotBody)
	}
	if stdout.String() != "{\"ok\":true}\n" {
		t.Fatalf("expected raw JSON stdout, got %q", stdout.String())
	}
}

func TestChannelsHelpIncludesExample(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"channels", "help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected help success, got %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "direxio channels list") {
		t.Fatalf("channels help missing list example:\n%s", stdout.String())
	}
}

func TestMatrixHelpIncludesMessageExamples(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"matrix", "help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected matrix help success, got %d stderr=%s", code, stderr.String())
	}
	help := stdout.String()
	for _, want := range []string{"matrix session init", "matrix messages send", "matrix messages list", "matrix sync", "matrix listen"} {
		if !strings.Contains(help, want) {
			t.Fatalf("matrix help missing %q:\n%s", want, help)
		}
	}
}

func TestMatrixMessagesSendRequiresRoomID(t *testing.T) {
	t.Setenv("DIREXIO_DOMAIN", "https://example.com")
	t.Setenv("DIREXIO_AGENT_TOKEN", "agent-token")
	var stdout, stderr bytes.Buffer
	code := run([]string{"matrix", "messages", "send", "--text", "hello"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("expected missing room-id to fail")
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout must be empty on failure, got %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "room-id is required") {
		t.Fatalf("unexpected stderr %q", stderr.String())
	}
}

func TestMatrixSessionInitHidesAccessToken(t *testing.T) {
	server := matrixTestServer(t)
	t.Setenv("DIREXIO_DOMAIN", server.URL)
	t.Setenv("DIREXIO_AGENT_TOKEN", "agent-token")

	var stdout, stderr bytes.Buffer
	code := run([]string{"matrix", "session", "init"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected success, got %d stderr=%s", code, stderr.String())
	}
	var got map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["status"] != "ok" || got["device_id"] != "DIREXIO_CLI" {
		t.Fatalf("unexpected session init output %#v", got)
	}
	if got["access_token"] != nil {
		t.Fatalf("session init must not print Matrix access token: %#v", got)
	}
}

func TestMatrixListenWritesNDJSONEvents(t *testing.T) {
	server := matrixTestServer(t)
	t.Setenv("DIREXIO_DOMAIN", server.URL)
	t.Setenv("DIREXIO_AGENT_TOKEN", "agent-token")

	var stdout, stderr bytes.Buffer
	code := run([]string{"matrix", "listen", "--timeout-ms", "1"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected success, got %d stderr=%s", code, stderr.String())
	}
	line := strings.TrimSpace(stdout.String())
	if !strings.Contains(line, `"room_id":"!room:example.com"`) || !strings.Contains(line, `"event_id":"$event"`) {
		t.Fatalf("unexpected ndjson line %q", line)
	}
}

func matrixTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_p2p/command":
			var got map[string]any
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Fatal(err)
			}
			if got["action"] != "agent.matrix_session.create" {
				t.Fatalf("unexpected p2p action %#v", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "matrix-token",
				"device_id":    "DIREXIO_CLI",
				"user_id":      "@owner:example.com",
				"homeserver":   "http://" + r.Host,
			})
		case "/_matrix/client/v3/sync":
			if r.Header.Get("Authorization") != "Bearer matrix-token" {
				t.Fatalf("missing matrix auth: %q", r.Header.Get("Authorization"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"next_batch": "s1",
				"rooms": map[string]any{
					"join": map[string]any{
						"!room:example.com": map[string]any{
							"timeline": map[string]any{
								"events": []any{
									map[string]any{"event_id": "$event", "type": "m.room.message"},
								},
							},
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(server.Close)
	return server
}
