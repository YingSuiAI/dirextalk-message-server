package agentclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCreateMatrixSessionUsesP2PAction(t *testing.T) {
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_p2p/command" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "matrix-token",
			"device_id":    "DIREXIO_CLI",
			"user_id":      "@owner:example.com",
			"homeserver":   serverURLHost(r),
		})
	}))
	defer server.Close()

	cfg, err := NewConfig(server.URL, "agent-token")
	if err != nil {
		t.Fatal(err)
	}
	client := New(cfg, server.Client())
	session, err := client.CreateMatrixSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if gotBody["action"] != "agent.matrix_session.create" {
		t.Fatalf("expected agent.matrix_session.create, got %#v", gotBody)
	}
	if session.AccessToken != "matrix-token" || session.DeviceID != "DIREXIO_CLI" {
		t.Fatalf("unexpected session: %#v", session)
	}
}

func TestSendTextMessageUsesMatrixEndpoint(t *testing.T) {
	var gotAuth string
	var gotMethod string
	var gotPath string
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"event_id": "$event"})
	}))
	defer server.Close()

	cfg, err := NewConfig(server.URL, "agent-token")
	if err != nil {
		t.Fatal(err)
	}
	client := New(cfg, server.Client())
	resp, err := client.SendTextMessage(context.Background(), MatrixSession{AccessToken: "matrix-token"}, "!room:example.com", "hello")
	if err != nil {
		t.Fatal(err)
	}

	if gotAuth != "Bearer matrix-token" {
		t.Fatalf("expected matrix bearer token, got %q", gotAuth)
	}
	if gotMethod != http.MethodPut {
		t.Fatalf("expected PUT, got %q", gotMethod)
	}
	if !strings.HasPrefix(gotPath, "/_matrix/client/v3/rooms/!room:example.com/send/m.room.message/direxio-cli-") {
		t.Fatalf("unexpected matrix send path %q", gotPath)
	}
	if gotBody["msgtype"] != "m.text" || gotBody["body"] != "hello" {
		t.Fatalf("unexpected message body %#v", gotBody)
	}
	if resp["event_id"] != "$event" {
		t.Fatalf("unexpected response %#v", resp)
	}
}

func TestExtractSyncTimelineEvents(t *testing.T) {
	events := ExtractSyncTimelineEvents(map[string]any{
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
	if len(events) != 1 {
		t.Fatalf("expected one event, got %#v", events)
	}
	if events[0]["room_id"] != "!room:example.com" || events[0]["event_id"] != "$event" {
		t.Fatalf("expected room id to be attached to event, got %#v", events[0])
	}
}

func serverURLHost(r *http.Request) string {
	return "http://" + r.Host
}
