package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestRealtimeWSTicketCreateIssuesSingleUseTicketForOwnerAndAgent(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	ownerTicket := mustCreateRealtimeWSTicket(t, router, service.AccessToken())
	if ownerTicket == "" {
		t.Fatal("expected owner ticket")
	}
	agentTicket := mustCreateRealtimeWSTicket(t, router, service.AgentToken())
	if agentTicket == "" {
		t.Fatal("expected agent ticket")
	}

	if err := service.consumeRealtimeWSTicket(ownerTicket); err != nil {
		t.Fatalf("expected owner ticket to be valid once: %v", err)
	}
	if err := service.consumeRealtimeWSTicket(ownerTicket); err == nil {
		t.Fatal("expected owner ticket to be single-use")
	}
	if err := service.consumeRealtimeWSTicket(agentTicket); err != nil {
		t.Fatalf("expected agent ticket to be valid once: %v", err)
	}
}

func TestRealtimeWSTicketCreateRejectsMissingOrInvalidBearer(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	for name, token := range map[string]string{
		"missing": "",
		"invalid": "not-valid",
	} {
		t.Run(name, func(t *testing.T) {
			req := jsonRequest(t, "/_p2p/command", map[string]any{
				"action": realtimeWSTicketAction,
				"params": map[string]any{},
			})
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401, got %d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestRealtimeWSAcceptsTicketAndReplaysEvents(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	if err := service.appendP2PEvent(context.Background(), p2pEvent{
		Seq:     1,
		Type:    "old.event",
		RoomID:  "!room:example.com",
		EventID: "$old",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.appendP2PEvent(context.Background(), p2pEvent{
		Seq:     2,
		Type:    "fresh.event",
		RoomID:  "!room:example.com",
		EventID: "$fresh",
		Payload: map[string]any{"ok": true},
	}); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(router)
	defer server.Close()
	conn := dialRealtimeWS(t, server.URL, mustCreateRealtimeWSTicket(t, router, service.AccessToken()))
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeRealtimeFrame(t, conn, map[string]any{
		"type":  "client.hello",
		"since": 1,
	})
	ready := readRealtimeFrame(t, conn)
	if ready["type"] != "server.ready" {
		t.Fatalf("expected server.ready, got %#v", ready)
	}
	event := readRealtimeFrame(t, conn)
	if event["type"] != "server.event" {
		t.Fatalf("expected server.event, got %#v", event)
	}
	payload, ok := event["event"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested event payload, got %#v", event)
	}
	if payload["type"] != "fresh.event" || int64(payload["seq"].(float64)) != 2 {
		t.Fatalf("expected replay of seq 2 fresh event, got %#v", payload)
	}
}

func TestRealtimeWSStreamsLiveEventsAndTracksClientState(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	server := httptest.NewServer(router)
	defer server.Close()
	conn := dialRealtimeWS(t, server.URL, mustCreateRealtimeWSTicket(t, router, service.AccessToken()))
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeRealtimeFrame(t, conn, map[string]any{"type": "client.hello"})
	if got := readRealtimeFrame(t, conn); got["type"] != "server.ready" {
		t.Fatalf("expected ready, got %#v", got)
	}
	writeRealtimeFrame(t, conn, map[string]any{"type": "client.lifecycle", "foreground": true})
	writeRealtimeFrame(t, conn, map[string]any{"type": "client.focus", "room_id": "!live:example.com"})
	writeRealtimeFrame(t, conn, map[string]any{"type": "client.ack", "seq": 7})

	waitForRealtimePushSuppressed(t, service, "!live:example.com")
	if service.shouldSuppressPushForRoom("!other:example.com") {
		t.Fatal("expected different room to keep normal push behavior")
	}

	if err := service.appendP2PEvent(context.Background(), p2pEvent{
		Type:    "live.event",
		RoomID:  "!live:example.com",
		EventID: "$live",
	}); err != nil {
		t.Fatal(err)
	}
	frame := readRealtimeFrame(t, conn)
	if frame["type"] != "server.event" {
		t.Fatalf("expected live server.event, got %#v", frame)
	}
}

func TestRealtimeWSClientRequestCallsOwnerProductActions(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	server := httptest.NewServer(router)
	defer server.Close()
	conn := dialRealtimeWS(t, server.URL, mustCreateRealtimeWSTicket(t, router, service.AccessToken()))
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeRealtimeFrame(t, conn, map[string]any{"type": "client.hello"})
	if got := readRealtimeFrame(t, conn); got["type"] != "server.ready" {
		t.Fatalf("expected ready, got %#v", got)
	}
	writeRealtimeFrame(t, conn, map[string]any{
		"type":   "client.request",
		"id":     "req-contacts-1",
		"action": "contacts.list",
		"params": map[string]any{},
	})
	contacts := readRealtimeResponse(t, conn, "req-contacts-1")
	if contacts["type"] != "server.response" || contacts["ok"] != true || contacts["action"] != "contacts.list" {
		t.Fatalf("expected contacts.list response, got %#v", contacts)
	}

	writeRealtimeFrame(t, conn, map[string]any{
		"type":   "client.request",
		"id":     "req-group-1",
		"action": "groups.create",
		"params": map[string]any{
			"name": "WS Group",
		},
	})
	groupResponse := readRealtimeResponse(t, conn, "req-group-1")
	if groupResponse["ok"] != true || groupResponse["action"] != "groups.create" {
		t.Fatalf("expected groups.create response, got %#v", groupResponse)
	}
	groupResult, ok := groupResponse["result"].(map[string]any)
	if !ok || groupResult["room_id"] == "" {
		t.Fatalf("expected created group result, got %#v", groupResponse)
	}

	writeRealtimeFrame(t, conn, map[string]any{
		"type":   "client.request",
		"id":     "cmd-read-1",
		"action": "sync.read_marker",
		"params": map[string]any{
			"room_id":          "!room:example.com",
			"event_id":         "$event",
			"origin_server_ts": int64(1710000000000),
		},
	})
	frame := readRealtimeResponse(t, conn, "cmd-read-1")
	if frame["type"] != "server.response" || frame["id"] != "cmd-read-1" || frame["ok"] != true {
		t.Fatalf("expected response for read marker, got %#v", frame)
	}
	result, ok := frame["result"].(map[string]any)
	if !ok || result["status"] != "ok" {
		t.Fatalf("expected ok command result, got %#v", frame)
	}
	service.mu.Lock()
	marker := service.readMarkers["!room:example.com"]
	service.mu.Unlock()
	if marker.EventID != "$event" || marker.OriginServerTS != 1710000000000 {
		t.Fatalf("expected read marker to update via WS command, got %#v", marker)
	}
}

func TestRealtimeWSClientCommandAliasUsesServerResponse(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	server := httptest.NewServer(router)
	defer server.Close()
	conn := dialRealtimeWS(t, server.URL, mustCreateRealtimeWSTicket(t, router, service.AccessToken()))
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeRealtimeFrame(t, conn, map[string]any{"type": "client.hello"})
	if got := readRealtimeFrame(t, conn); got["type"] != "server.ready" {
		t.Fatalf("expected ready, got %#v", got)
	}
	writeRealtimeFrame(t, conn, map[string]any{
		"type":   "client.command",
		"id":     "cmd-read-alias",
		"action": "channels.read_marker",
		"params": map[string]any{
			"room_id":  "!room:example.com",
			"event_id": "$event",
		},
	})
	frame := readRealtimeResponse(t, conn, "cmd-read-alias")
	if frame["type"] != "server.response" || frame["ok"] != true || frame["action"] != "channels.read_marker" {
		t.Fatalf("expected command alias to use server.response, got %#v", frame)
	}
}

func TestRealtimeWSAgentCanOnlyRequestMCPActions(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	server := httptest.NewServer(router)
	defer server.Close()
	conn := dialRealtimeWS(t, server.URL, mustCreateRealtimeWSTicket(t, router, service.AgentToken()))
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeRealtimeFrame(t, conn, map[string]any{"type": "client.hello"})
	if got := readRealtimeFrame(t, conn); got["type"] != "server.ready" {
		t.Fatalf("expected ready, got %#v", got)
	}
	writeRealtimeFrame(t, conn, map[string]any{
		"type":   "client.request",
		"id":     "req-agent-mcp",
		"action": "mcp.rooms.search",
		"params": map[string]any{"q": "none"},
	})
	mcp := readRealtimeResponse(t, conn, "req-agent-mcp")
	if mcp["type"] != "server.response" || mcp["ok"] != true || mcp["action"] != "mcp.rooms.search" {
		t.Fatalf("expected agent MCP request to succeed, got %#v", mcp)
	}

	writeRealtimeFrame(t, conn, map[string]any{
		"type":   "client.request",
		"id":     "req-agent-owner",
		"action": "contacts.list",
		"params": map[string]any{},
	})
	frame := readRealtimeResponse(t, conn, "req-agent-owner")
	if frame["type"] != "server.response" ||
		frame["id"] != "req-agent-owner" ||
		frame["ok"] != false ||
		int(frame["status"].(float64)) != http.StatusForbidden {
		t.Fatalf("expected agent owner action to be forbidden, got %#v", frame)
	}
}

func TestRealtimeWSClientRequestValidationErrors(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	server := httptest.NewServer(router)
	defer server.Close()
	conn := dialRealtimeWS(t, server.URL, mustCreateRealtimeWSTicket(t, router, service.AccessToken()))
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeRealtimeFrame(t, conn, map[string]any{"type": "client.hello"})
	if got := readRealtimeFrame(t, conn); got["type"] != "server.ready" {
		t.Fatalf("expected ready, got %#v", got)
	}

	writeRealtimeFrame(t, conn, map[string]any{
		"type":   "client.request",
		"id":     "req-unknown",
		"action": "does.not.exist",
		"params": map[string]any{},
	})
	unknown := readRealtimeResponse(t, conn, "req-unknown")
	if unknown["ok"] != false || int(unknown["status"].(float64)) != http.StatusBadRequest || unknown["error"] != "unknown action" {
		t.Fatalf("expected unknown action response, got %#v", unknown)
	}

	writeRealtimeFrame(t, conn, map[string]any{
		"type":   "client.request",
		"id":     "req-bad-params",
		"action": "contacts.list",
		"params": []any{},
	})
	badParams := readRealtimeResponse(t, conn, "req-bad-params")
	if badParams["ok"] != false || int(badParams["status"].(float64)) != http.StatusBadRequest {
		t.Fatalf("expected malformed params response, got %#v", badParams)
	}

	writeRealtimeFrame(t, conn, map[string]any{
		"type":   "client.request",
		"action": "contacts.list",
		"params": map[string]any{},
	})
	missingID := readRealtimeFrame(t, conn)
	if missingID["type"] != "server.response" ||
		missingID["ok"] != false ||
		int(missingID["status"].(float64)) != http.StatusBadRequest ||
		missingID["error"] != "id is required" {
		t.Fatalf("expected missing id response, got %#v", missingID)
	}

	writeRealtimeFrame(t, conn, map[string]any{
		"type":   "client.request",
		"id":     "req-handler-error",
		"action": "sync.read_marker",
		"params": map[string]any{"room_id": "!room:example.com"},
	})
	handlerErr := readRealtimeResponse(t, conn, "req-handler-error")
	if handlerErr["ok"] != false || int(handlerErr["status"].(float64)) != http.StatusBadRequest {
		t.Fatalf("expected handler error response, got %#v", handlerErr)
	}
}

func TestRealtimeWSAgentTicketOnlyStreamsAgentRoomMessages(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	if err := service.appendP2PEvent(context.Background(), p2pEvent{
		Seq:     1,
		Type:    "contact.requested",
		RoomID:  "!room:example.com",
		EventID: "$contact",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.appendP2PEvent(context.Background(), p2pEvent{
		Seq:     2,
		Type:    AgentRoomMessageEventType,
		RoomID:  "!agent:example.com",
		EventID: "$agent",
		Payload: map[string]any{"body": "hello"},
	}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	conn := dialRealtimeWS(t, server.URL, mustCreateRealtimeWSTicket(t, router, service.AgentToken()))
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeRealtimeFrame(t, conn, map[string]any{"type": "client.hello"})
	if got := readRealtimeFrame(t, conn); got["type"] != "server.ready" {
		t.Fatalf("expected ready, got %#v", got)
	}
	frame := readRealtimeFrame(t, conn)
	if frame["type"] != "server.event" {
		t.Fatalf("expected server.event, got %#v", frame)
	}
	event := frame["event"].(map[string]any)
	if event["type"] != AgentRoomMessageEventType || int64(event["seq"].(float64)) != 2 {
		t.Fatalf("expected only agent room message replay, got %#v", event)
	}
}

func TestRealtimeWSAgentStreamFanoutToOwner(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	service.mu.Lock()
	service.agentRoomID = "!agent-room:example.com"
	service.mu.Unlock()
	router := newP2PTestRouter(service)
	server := httptest.NewServer(router)
	defer server.Close()

	ownerConn := dialRealtimeWS(t, server.URL, mustCreateRealtimeWSTicket(t, router, service.AccessToken()))
	defer ownerConn.Close(websocket.StatusNormalClosure, "")
	writeRealtimeFrame(t, ownerConn, map[string]any{"type": "client.hello"})
	if got := readRealtimeFrame(t, ownerConn); got["type"] != "server.ready" {
		t.Fatalf("expected owner ready, got %#v", got)
	}

	agentConn := dialRealtimeWS(t, server.URL, mustCreateRealtimeWSTicket(t, router, service.AgentToken()))
	defer agentConn.Close(websocket.StatusNormalClosure, "")
	writeRealtimeFrame(t, agentConn, map[string]any{"type": "client.hello"})
	if got := readRealtimeFrame(t, agentConn); got["type"] != "server.ready" {
		t.Fatalf("expected agent ready, got %#v", got)
	}

	writeRealtimeFrame(t, agentConn, map[string]any{
		"type":      "client.agent_stream",
		"room_id":   "!agent-room:example.com",
		"stream_id": "turn-1",
		"seq":       1,
		"delta":     "Hello",
		"done":      false,
	})
	frame := readRealtimeFrame(t, ownerConn)
	if frame["type"] != "server.agent_stream" ||
		frame["room_id"] != "!agent-room:example.com" ||
		frame["stream_id"] != "turn-1" ||
		frame["delta"] != "Hello" ||
		frame["sender_mxid"] != "@agent:example.com" {
		t.Fatalf("expected owner to receive agent stream frame, got %#v", frame)
	}
}

func TestRealtimeWSSendsCursorResetForExpiredSince(t *testing.T) {
	service := NewService(Config{
		ServerName:                    "example.com",
		P2PEventRetentionMaxRows:      2,
		P2PEventRetentionPruneOnWrite: true,
	})
	router := newP2PTestRouter(service)
	for seq := int64(1); seq <= 4; seq++ {
		if err := service.appendP2PEvent(context.Background(), p2pEvent{
			Seq:     seq,
			Type:    "test.event",
			RoomID:  "!room:example.com",
			EventID: "$event",
		}); err != nil {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer(router)
	defer server.Close()
	conn := dialRealtimeWS(t, server.URL, mustCreateRealtimeWSTicket(t, router, service.AccessToken()))
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeRealtimeFrame(t, conn, map[string]any{"type": "client.hello", "since": 1})
	if got := readRealtimeFrame(t, conn); got["type"] != "server.ready" {
		t.Fatalf("expected ready, got %#v", got)
	}
	reset := readRealtimeFrame(t, conn)
	if reset["type"] != "server.cursor_reset" {
		t.Fatalf("expected cursor reset, got %#v", reset)
	}
	if reset["recovery"] != "bootstrap_required" || int64(reset["min_seq"].(float64)) != 3 {
		t.Fatalf("expected reset payload with retained bounds, got %#v", reset)
	}
}

func mustCreateRealtimeWSTicket(t *testing.T, router http.Handler, token string) string {
	t.Helper()
	req := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": realtimeWSTicketAction,
		"params": map[string]any{},
	})
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ticket create expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	ticket, _ := got["ticket"].(string)
	return ticket
}

func dialRealtimeWS(t *testing.T, serverURL, ticket string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(serverURL, "http") + "/_p2p/ws?ticket=" + ticket
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	return conn
}

func writeRealtimeFrame(t *testing.T, conn *websocket.Conn, frame map[string]any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := wsjson.Write(ctx, conn, frame); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func readRealtimeFrame(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var frame map[string]any
	if err := wsjson.Read(ctx, conn, &frame); err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return frame
}

func readRealtimeResponse(t *testing.T, conn *websocket.Conn, id string) map[string]any {
	t.Helper()
	for {
		frame := readRealtimeFrame(t, conn)
		if frame["type"] == "server.response" && frame["id"] == id {
			return frame
		}
	}
}

func waitForRealtimePushSuppressed(t *testing.T, service *Service, roomID string) {
	t.Helper()
	deadline := time.After(time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if service.shouldSuppressPushForRoom(roomID) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("expected focused foreground WS session to suppress push for %s", roomID)
		case <-tick.C:
		}
	}
}
