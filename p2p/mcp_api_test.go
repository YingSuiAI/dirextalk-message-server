package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/matrixhistory"
)

func mustInvokeMCP[T any](t *testing.T, service *Service, action string, params map[string]any) T {
	t.Helper()
	result, apiErr := service.invokeDirextalkMCP(context.Background(), action, params)
	if apiErr != nil {
		t.Fatalf("MCP action %s failed: %d %s", action, apiErr.Status, apiErr.Error)
	}
	typed, ok := result.(T)
	if !ok {
		t.Fatalf("MCP action %s returned %T, want requested type", action, result)
	}
	return typed
}

func TestMCPSearchRoomsReturnsConciseMixedRooms(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})

	mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:example.com",
		"display_name": "Alice",
	})
	mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Design Group",
	})
	mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "product",
		"room_id":      "!channel:example.com",
		"name":         "Product Channel",
		"channel_type": "post",
	})

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionRoomsSearch, map[string]any{
		"query": "product",
		"type":  "all",
		"limit": float64(10),
	})
	rooms := result["rooms"].([]mcpRoomSummary)
	if len(rooms) != 1 {
		t.Fatalf("expected one product room, got %#v", rooms)
	}
	if rooms[0].Type != "channel" || rooms[0].Name != "Product Channel" || rooms[0].RoomID != "!channel:example.com" {
		t.Fatalf("unexpected room summary: %#v", rooms[0])
	}
}

func TestMCPSearchRoomsEmptyQueryListsByType(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Design Group",
	})

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionRoomsSearch, map[string]any{
		"type": "group",
	})
	rooms := result["rooms"].([]mcpRoomSummary)
	if len(rooms) != 1 || rooms[0].Type != "group" || rooms[0].RoomID != "!group:example.com" {
		t.Fatalf("expected group list from empty query, got %#v", rooms)
	}
}

func TestMCPSearchRoomsUsesMatrixMemberCountWhenProductCountIsStale(t *testing.T) {
	transport := &recordingTransport{roomMembers: []memberRecord{
		{RoomID: "!group:example.com", UserID: "@owner:example.com", DisplayName: "Owner Name", Membership: "join", Role: "owner"},
		{RoomID: "!group:example.com", UserID: "@owner:t7.dirextalk.ai", DisplayName: "owner", Membership: "join", Role: "member"},
		{RoomID: "!group:example.com", UserID: "@owner:t8.dirextalk.ai", DisplayName: "owner", Membership: "join", Role: "member"},
	}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Design Group",
	})

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionRoomsSearch, map[string]any{
		"type":  "group",
		"query": "Design",
	})
	rooms := result["rooms"].([]mcpRoomSummary)
	if len(rooms) != 1 || rooms[0].Subtitle != "3 members" {
		t.Fatalf("expected Matrix-backed member count, got %#v", rooms)
	}
}

func TestMCPBlockedRoomsAreFilteredFromRoomSearch(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!visible:example.com",
		"name":    "Visible Group",
	})
	mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!blocked:example.com",
		"name":    "Blocked Group",
	})
	mustHandle[map[string]any](t, service, "agent.config.update", map[string]any{
		"mcp_blocked_room_ids": []any{"!blocked:example.com"},
	})

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionRoomsSearch, map[string]any{
		"type": "group",
	})
	rooms := result["rooms"].([]mcpRoomSummary)
	if len(rooms) != 1 || rooms[0].RoomID != "!visible:example.com" {
		t.Fatalf("expected blocked room to be filtered from MCP search, got %#v", rooms)
	}
}

func TestMCPContactsListReturnsAcceptedContacts(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	if err := service.saveContact(context.Background(), contactRecord{
		PeerMXID:    "@alice:example.com",
		DisplayName: "Alice",
		AvatarURL:   "mxc://example.com/alice",
		Domain:      "example.com",
		RoomID:      "!alice:example.com",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveContact(context.Background(), contactRecord{
		PeerMXID:    "@bob:example.com",
		DisplayName: "Bob",
		Domain:      "example.com",
		RoomID:      "!bob:example.com",
		Status:      "deleted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionContactsList, map[string]any{})
	contacts := result["contacts"].([]mcpContactSummary)
	if len(contacts) != 1 {
		t.Fatalf("expected only visible contacts, got %#v", contacts)
	}
	if contacts[0].PeerMXID != "@alice:example.com" || contacts[0].DisplayName != "Alice" || contacts[0].AvatarURL != "mxc://example.com/alice" {
		t.Fatalf("unexpected contact summary: %#v", contacts[0])
	}
}

func TestMCPContactsSearchMatchesNamePeerAndDomain(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	if err := service.saveContact(context.Background(), contactRecord{
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice Product",
		Domain:      "remote.example",
		RoomID:      "!alice:remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveContact(context.Background(), contactRecord{
		PeerMXID:    "@bob:example.com",
		DisplayName: "Bob",
		Domain:      "example.com",
		RoomID:      "!bob:example.com",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionContactsSearch, map[string]any{
		"query": "remote",
		"limit": float64(10),
	})
	contacts := result["contacts"].([]mcpContactSummary)
	if len(contacts) != 1 || contacts[0].PeerMXID != "@alice:remote.example" {
		t.Fatalf("expected remote Alice contact, got %#v", contacts)
	}

	result = mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionContactsSearch, map[string]any{
		"query": "product",
	})
	contacts = result["contacts"].([]mcpContactSummary)
	if len(contacts) != 1 || contacts[0].DisplayName != "Alice Product" {
		t.Fatalf("expected display-name match, got %#v", contacts)
	}
}

func TestMCPMessagesSendUsesTransportAndReturnsConciseResult(t *testing.T) {
	transport := &recordingTransport{eventID: "$mcp:event", ts: 1710000000000}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.profile.DisplayName = "Owner"

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionMessagesSend, map[string]any{
		"room_id": "!room:example.com",
		"msg":     "hello",
	})
	if result["ok"] != true || result["room_id"] != "!room:example.com" || result["event_id"] != "$mcp:event" {
		t.Fatalf("unexpected send result: %#v", result)
	}
	if len(transport.messages) != 1 {
		t.Fatalf("expected one Matrix message, got %#v", transport.messages)
	}
	if transport.messages[0].SenderMXID != "@owner:example.com" {
		t.Fatalf("expected ordinary MCP send to proxy owner, got %#v", transport.messages[0])
	}
	content := transport.messages[0].Content
	if content["body"] != "hello" || content["msgtype"] != "m.text" {
		t.Fatalf("expected text Matrix message content, got %#v", content)
	}
	if _, ok := content["p2p_kind"]; ok {
		t.Fatalf("ordinary MCP message must not create channel content: %#v", content)
	}
}

func TestMCPMessagesSendMarksGatewayReplies(t *testing.T) {
	transport := &recordingTransport{eventID: "$mcp:event", ts: 1710000000000}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.agentRoomID = "!agents:example.com"

	mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionMessagesSend, map[string]any{
		"room_id":        "!agents:example.com",
		"msg":            "agent reply",
		"agent_gateway":  true,
		"gateway_source": "codex-cli",
	})
	if len(transport.messages) != 1 {
		t.Fatalf("expected one Matrix message, got %#v", transport.messages)
	}
	if transport.messages[0].SenderMXID != "@agent:example.com" {
		t.Fatalf("expected gateway reply to be sent by agent, got %#v", transport.messages[0])
	}
	content := transport.messages[0].Content
	if content[AgentGatewayContentKey] != true || content[AgentGatewaySourceContentKey] != "codex-cli" {
		t.Fatalf("expected gateway marker content, got %#v", content)
	}
}

func TestMCPMessagesSendRejectsOwnerSendToAgentRoom(t *testing.T) {
	transport := &recordingTransport{eventID: "$mcp:event", ts: 1710000000000}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.agentRoomID = "!agents:example.com"

	_, apiErr := service.invokeDirextalkMCP(context.Background(), dirextalkmcp.ActionMessagesSend, map[string]any{
		"room_id": "!agents:example.com",
		"msg":     "hello agent room",
	})
	if apiErr == nil || apiErr.Status != 400 || !strings.Contains(apiErr.Error, "agent room") {
		t.Fatalf("expected agent room send to fail with bad request, got %#v", apiErr)
	}
	if len(transport.messages) != 0 {
		t.Fatalf("owner MCP send to agent room must not write Matrix messages, got %#v", transport.messages)
	}
}

func TestMCPMessagesRejectBlockedRooms(t *testing.T) {
	transport := &recordingTransport{eventID: "$mcp:event", ts: 1710000000000}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.SetMatrixMessageReader(&fakeMCPMessageReader{messages: []mcpMessageSummary{
		{EventID: "$secret", OriginServerTS: 1710000000000, CreatedAt: "2024-03-09T16:00:00Z", Sender: "Alice", Msg: "secret", SenderMXID: "@alice:example.com"},
	}})
	mustHandle[map[string]any](t, service, "agent.config.update", map[string]any{
		"mcp_blocked_room_ids": []any{"!blocked:example.com"},
	})

	_, apiErr := service.invokeDirextalkMCP(context.Background(), dirextalkmcp.ActionMessagesList, map[string]any{
		"room_id": "!blocked:example.com",
	})
	if apiErr == nil || apiErr.Status != 403 || !strings.Contains(apiErr.Error, "blocked") {
		t.Fatalf("expected blocked room list to fail, got %#v", apiErr)
	}
	_, apiErr = service.invokeDirextalkMCP(context.Background(), dirextalkmcp.ActionMessagesSend, map[string]any{
		"room_id": "!blocked:example.com",
		"msg":     "secret",
	})
	if apiErr == nil || apiErr.Status != 403 || !strings.Contains(apiErr.Error, "blocked") {
		t.Fatalf("expected blocked room send to fail, got %#v", apiErr)
	}
	if len(transport.messages) != 0 {
		t.Fatalf("blocked MCP send must not write Matrix messages, got %#v", transport.messages)
	}
}

type fakeMCPMessageReader struct {
	messages []mcpMessageSummary
	err      error
}

func (r *fakeMCPMessageReader) ListOrdinaryMessages(ctx context.Context, roomID string, page mcpMessagePage) (mcpMessagePageResult, error) {
	if r.err != nil {
		return mcpMessagePageResult{}, r.err
	}
	out := make([]mcpMessageSummary, 0, len(r.messages))
	for _, msg := range r.messages {
		if mcpPageIncludes(msg.OriginServerTS, msg.EventID, page) {
			out = append(out, msg)
		}
	}
	matrixhistory.SortMessageSummaries(out)
	hasMore := false
	if len(out) > page.Limit {
		hasMore = true
		out = out[:page.Limit]
	}
	return mcpMessagePageResult{Messages: out, HasMore: hasMore}, nil
}

type fakeMatrixProfileResolver struct {
	profiles map[string]matrixUserProfile
}

func (r *fakeMatrixProfileResolver) ResolveMatrixProfile(ctx context.Context, userID string) (matrixUserProfile, error) {
	return r.profiles[strings.TrimSpace(userID)], nil
}

func TestMCPMessagesListUsesReaderAndReturnsConciseMessages(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	service.SetMatrixMessageReader(&fakeMCPMessageReader{messages: []mcpMessageSummary{
		{EventID: "$old", OriginServerTS: 1710000000000, CreatedAt: "2024-03-09T16:00:00Z", Sender: "Alice", Msg: "old", SenderMXID: "@alice:remote.example"},
		{EventID: "$inside", OriginServerTS: 1710000100000, CreatedAt: "2024-03-09T16:01:40Z", Sender: "Alice", Msg: "inside", SenderMXID: "@alice:remote.example", SenderDomain: "remote.example", SenderLocalpart: "alice"},
	}})
	if err := service.saveConversation(context.Background(), conversationRecord{
		MatrixRoomID: "!room:example.com",
		Kind:         conversationKindDirect,
		Lifecycle:    conversationLifecycleActive,
		Title:        "Alice",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionMessagesList, map[string]any{
		"room_id":   "!room:example.com",
		"from_time": "2024-03-09T16:00:50Z",
		"limit":     float64(20),
	})
	if result["room_id"] != "!room:example.com" || result["name"] != "Alice" {
		t.Fatalf("unexpected message list envelope: %#v", result)
	}
	messages := result["messages"].([]mcpMessageSummary)
	if len(messages) != 1 || messages[0].Msg != "inside" {
		t.Fatalf("unexpected message summaries: %#v", messages)
	}
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Messages []struct {
			Sender          string `json:"sender"`
			SenderMXID      string `json:"sender_mxid"`
			SenderDomain    string `json:"sender_domain"`
			SenderLocalpart string `json:"sender_localpart"`
			CreatedAt       string `json:"created_at"`
			TS              int64  `json:"ts,omitempty"`
			Msg             string `json:"msg"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Messages) != 1 ||
		decoded.Messages[0].Sender != "Alice" ||
		decoded.Messages[0].SenderMXID != "@alice:remote.example" ||
		decoded.Messages[0].SenderDomain != "remote.example" ||
		decoded.Messages[0].SenderLocalpart != "alice" ||
		decoded.Messages[0].CreatedAt != "2024-03-09T16:01:40Z" ||
		decoded.Messages[0].TS != 0 {
		t.Fatalf("expected message JSON to expose sender identity, got %s", string(payload))
	}
}

func TestMCPMessagesPaginationUsesStableSnapshot(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	reader := &fakeMCPMessageReader{}
	service.SetMatrixMessageReader(reader)
	base := time.Date(2026, 7, 5, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 12; i++ {
		id := "msg_" + string(rune('a'+i))
		reader.messages = append(reader.messages, mcpMessageSummary{
			EventID:        "$" + id,
			OriginServerTS: base.Add(time.Duration(i) * time.Minute).UnixMilli(),
			CreatedAt:      base.Add(time.Duration(i) * time.Minute).Format(time.RFC3339),
			Sender:         "Owner",
			SenderMXID:     "@owner:example.com",
			Msg:            id,
		})
	}

	first := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionMessagesList, map[string]any{
		"room_id": "!channel:example.com",
		"to_time": "2026-07-05T10:12:00Z",
		"limit":   float64(5),
	})
	firstMessages := first["messages"].([]mcpMessageSummary)
	if len(firstMessages) != 5 || firstMessages[0].Msg != "msg_l" || firstMessages[4].Msg != "msg_h" {
		t.Fatalf("expected newest five ordinary messages, got %#v", firstMessages)
	}
	cursor, _ := first["next_cursor"].(string)
	if cursor == "" {
		t.Fatalf("expected next_cursor in first page: %#v", first)
	}

	for i := 0; i < 10; i++ {
		id := "new_msg_" + string(rune('a'+i))
		reader.messages = append(reader.messages, mcpMessageSummary{
			EventID:        "$" + id,
			OriginServerTS: base.Add(time.Hour + time.Duration(i)*time.Minute).UnixMilli(),
			CreatedAt:      base.Add(time.Hour + time.Duration(i)*time.Minute).Format(time.RFC3339),
			Sender:         "Owner",
			SenderMXID:     "@owner:example.com",
			Msg:            id,
		})
	}

	second := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionMessagesList, map[string]any{
		"room_id": "!channel:example.com",
		"cursor":  cursor,
		"limit":   float64(5),
	})
	secondMessages := second["messages"].([]mcpMessageSummary)
	if len(secondMessages) != 5 || secondMessages[0].Msg != "msg_g" || secondMessages[4].Msg != "msg_c" {
		t.Fatalf("expected cursor to continue original message snapshot, got %#v", secondMessages)
	}
	for _, msg := range secondMessages {
		if strings.HasPrefix(msg.Msg, "new_msg_") {
			t.Fatalf("cursor page must not include newer inserts: %#v", secondMessages)
		}
	}
}

func TestMCPRejectsLegacyTimestampParams(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	service.SetMatrixMessageReader(&fakeMCPMessageReader{})

	for _, tc := range []struct {
		action string
		params map[string]any
	}{
		{dirextalkmcp.ActionMessagesList, map[string]any{"room_id": "!room:example.com", "from_ts": float64(1710000000000)}},
		{dirextalkmcp.ActionChannelPostsList, map[string]any{"room_id": "!channel:example.com", "to_ts": float64(1710000000000)}},
		{dirextalkmcp.ActionChannelCommentsList, map[string]any{"post_id": "post_1", "from_ts": float64(1710000000000)}},
	} {
		_, apiErr := service.invokeDirextalkMCP(context.Background(), tc.action, tc.params)
		if apiErr == nil || apiErr.Status != http.StatusBadRequest || !strings.Contains(apiErr.Error, "from_time") {
			t.Fatalf("expected %s to reject legacy timestamp params, got %#v", tc.action, apiErr)
		}
	}
}

func TestMCPRejectsNonUTCTimeParams(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	service.SetMatrixMessageReader(&fakeMCPMessageReader{})

	_, apiErr := service.invokeDirextalkMCP(context.Background(), dirextalkmcp.ActionMessagesList, map[string]any{
		"room_id":   "!room:example.com",
		"from_time": "2024-03-10T00:00:00+08:00",
	})
	if apiErr == nil || apiErr.Status != http.StatusBadRequest || !strings.Contains(apiErr.Error, "UTC") {
		t.Fatalf("expected non-UTC time to be rejected, got %#v", apiErr)
	}
}

func TestMCPMessagesListPropagatesMatrixAccessErrors(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	service.SetMatrixMessageReader(&fakeMCPMessageReader{
		err: matrixhistory.StatusError{StatusCode: http.StatusForbidden, Message: "matrix messages failed with status 403"},
	})

	_, apiErr := service.invokeDirextalkMCP(context.Background(), dirextalkmcp.ActionMessagesList, map[string]any{
		"room_id": "!room:example.com",
	})
	if apiErr == nil || apiErr.Status != http.StatusForbidden || !strings.Contains(apiErr.Error, "not allowed") {
		t.Fatalf("expected Matrix access failure to be exposed as 403, got %#v", apiErr)
	}
}

func TestMCPMessagesListEnrichesSenderDisplayNamesFromRoomMembers(t *testing.T) {
	transport := &recordingTransport{roomMembers: []memberRecord{
		{RoomID: "!group:example.com", UserID: "@owner:example.com", DisplayName: "Owner Name", Membership: "join", Role: "owner"},
		{RoomID: "!group:example.com", UserID: "@alice:remote.example", DisplayName: "Alice Remote", Membership: "join", Role: "member"},
	}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.SetMatrixMessageReader(&fakeMCPMessageReader{messages: []mcpMessageSummary{
		{
			EventID:         "$alice",
			OriginServerTS:  1710000100000,
			CreatedAt:       "2024-03-09T16:01:40Z",
			Sender:          "alice",
			SenderMXID:      "@alice:remote.example",
			SenderDomain:    "remote.example",
			SenderLocalpart: "alice",
			Msg:             "hello from alice",
		},
	}})
	mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Design Group",
	})

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionMessagesList, map[string]any{
		"room_id": "!group:example.com",
	})
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Messages []struct {
			Sender            string `json:"sender"`
			SenderMXID        string `json:"sender_mxid"`
			SenderDisplayName string `json:"sender_display_name"`
			Msg               string `json:"msg"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Messages) != 1 ||
		decoded.Messages[0].Sender != "Alice Remote" ||
		decoded.Messages[0].SenderDisplayName != "Alice Remote" ||
		decoded.Messages[0].SenderMXID != "@alice:remote.example" {
		t.Fatalf("expected message JSON to expose readable sender display name, got %s", string(payload))
	}
}

func TestMCPMessagesListEnrichesFallbackSenderNameFromMatrixProfile(t *testing.T) {
	transport := &recordingTransport{roomMembers: []memberRecord{
		{RoomID: "!group:example.com", UserID: "@owner:example.com", DisplayName: "Owner Name", Membership: "join", Role: "owner"},
		{RoomID: "!group:example.com", UserID: "@owner:t8.dirextalk.ai", DisplayName: "owner", Membership: "join", Role: "member"},
	}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.SetMatrixProfileResolver(&fakeMatrixProfileResolver{profiles: map[string]matrixUserProfile{
		"@owner:t8.dirextalk.ai": {DisplayName: "liyanan8", AvatarURL: "mxc://t8/avatar"},
	}})
	service.SetMatrixMessageReader(&fakeMCPMessageReader{messages: []mcpMessageSummary{
		{
			EventID:         "$t8",
			OriginServerTS:  1710000100000,
			CreatedAt:       "2024-03-09T16:01:40Z",
			Sender:          "owner",
			SenderMXID:      "@owner:t8.dirextalk.ai",
			SenderDomain:    "t8.dirextalk.ai",
			SenderLocalpart: "owner",
			Msg:             "hello from t8",
		},
	}})
	mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Design Group",
	})

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionMessagesList, map[string]any{
		"room_id": "!group:example.com",
	})
	messages := result["messages"].([]mcpMessageSummary)
	if len(messages) != 1 ||
		messages[0].Sender != "liyanan8" ||
		messages[0].SenderDisplayName != "liyanan8" {
		t.Fatalf("expected profile display name for fallback sender, got %#v", messages)
	}
}

func TestMCPRoomMembersListReturnsMemberIdentities(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Design Group",
	})
	mustHandle[map[string]any](t, service, "groups.join", map[string]any{
		"room_id":      group.RoomID,
		"user_mxid":    "@alice:remote.example",
		"display_name": "Alice Remote",
	})

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionRoomMembersList, map[string]any{
		"room_id": group.RoomID,
	})
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		RoomID  string `json:"room_id"`
		Name    string `json:"name"`
		Members []struct {
			UserID      string `json:"user_id"`
			UserMXID    string `json:"user_mxid"`
			Localpart   string `json:"localpart"`
			Domain      string `json:"domain"`
			DisplayName string `json:"display_name"`
			Role        string `json:"role"`
			Membership  string `json:"membership"`
		} `json:"members"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.RoomID != group.RoomID || decoded.Name != "Design Group" {
		t.Fatalf("unexpected room member envelope: %s", string(payload))
	}
	if len(decoded.Members) != 2 {
		t.Fatalf("expected owner plus alice, got %s", string(payload))
	}
	if decoded.Members[1].UserID != "@alice:remote.example" ||
		decoded.Members[1].UserMXID != "@alice:remote.example" ||
		decoded.Members[1].Localpart != "alice" ||
		decoded.Members[1].Domain != "remote.example" ||
		decoded.Members[1].DisplayName != "Alice Remote" ||
		decoded.Members[1].Membership != "join" {
		t.Fatalf("expected member JSON to expose Matrix identity, got %s", string(payload))
	}
}

func TestMCPRoomMembersListMergesMatrixRoomStateMembers(t *testing.T) {
	transport := &recordingTransport{roomMembers: []memberRecord{
		{RoomID: "!group:example.com", UserID: "@owner:example.com", DisplayName: "Owner Name", Membership: "join", Role: "owner"},
		{RoomID: "!group:example.com", UserID: "@alice:remote.example", DisplayName: "Alice Remote", Membership: "join", Role: "member"},
	}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Design Group",
	})

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionRoomMembersList, map[string]any{
		"room_id": group.RoomID,
	})
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Members []struct {
			UserMXID    string `json:"user_mxid"`
			DisplayName string `json:"display_name"`
			Membership  string `json:"membership"`
		} `json:"members"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Members) != 2 {
		t.Fatalf("expected owner plus Matrix room member, got %s", string(payload))
	}
	if decoded.Members[1].UserMXID != "@alice:remote.example" ||
		decoded.Members[1].DisplayName != "Alice Remote" ||
		decoded.Members[1].Membership != "join" {
		t.Fatalf("expected Matrix room state member identity, got %s", string(payload))
	}
}

func TestMCPRoomMembersListRejectsUnknownRoomsBeforeMatrixLookup(t *testing.T) {
	transport := &recordingTransport{roomMembers: []memberRecord{
		{RoomID: "!secret:example.com", UserID: "@alice:remote.example", DisplayName: "Alice Remote", Membership: "join", Role: "member"},
	}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)

	_, apiErr := service.invokeDirextalkMCP(context.Background(), dirextalkmcp.ActionRoomMembersList, map[string]any{
		"room_id": "!secret:example.com",
	})
	if apiErr == nil || apiErr.Status != 404 || !strings.Contains(apiErr.Error, "room not found") {
		t.Fatalf("expected unknown room to be rejected, got %#v", apiErr)
	}
}

func TestMCPRoomMembersListFiltersMergedMatrixMembers(t *testing.T) {
	transport := &recordingTransport{roomMembers: []memberRecord{
		{RoomID: "!group:example.com", UserID: "@owner:example.com", DisplayName: "Owner Name", Membership: "join", Role: "owner"},
		{RoomID: "!group:example.com", UserID: "@alice:remote.example", DisplayName: "Alice Remote", Membership: "join", Role: "member"},
	}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Design Group",
	})

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionRoomMembersList, map[string]any{
		"room_id": group.RoomID,
		"role":    "member",
	})
	members := result["members"].([]mcpMemberSummary)
	if len(members) != 1 || members[0].UserMXID != "@alice:remote.example" || members[0].Role != "member" {
		t.Fatalf("expected role filter to apply after Matrix member merge, got %#v", members)
	}
}

func TestMCPRoomMembersListEnrichesFallbackNamesFromMatrixProfiles(t *testing.T) {
	transport := &recordingTransport{roomMembers: []memberRecord{
		{RoomID: "!group:example.com", UserID: "@owner:example.com", DisplayName: "Owner Name", Membership: "join", Role: "owner"},
		{RoomID: "!group:example.com", UserID: "@owner:t7.dirextalk.ai", DisplayName: "owner", Membership: "join", Role: "member"},
		{RoomID: "!group:example.com", UserID: "@owner:t8.dirextalk.ai", DisplayName: "owner", Membership: "join", Role: "member"},
	}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.SetMatrixProfileResolver(&fakeMatrixProfileResolver{profiles: map[string]matrixUserProfile{
		"@owner:t7.dirextalk.ai": {DisplayName: "liyanan7", AvatarURL: "mxc://t7/avatar"},
		"@owner:t8.dirextalk.ai": {DisplayName: "liyanan8", AvatarURL: "mxc://t8/avatar"},
	}})
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Design Group",
	})

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionRoomMembersList, map[string]any{
		"room_id": group.RoomID,
	})
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Members []struct {
			UserMXID    string `json:"user_mxid"`
			DisplayName string `json:"display_name"`
			AvatarURL   string `json:"avatar_url"`
		} `json:"members"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	names := map[string]string{}
	avatars := map[string]string{}
	for _, member := range decoded.Members {
		names[member.UserMXID] = member.DisplayName
		avatars[member.UserMXID] = member.AvatarURL
	}
	if names["@owner:t7.dirextalk.ai"] != "liyanan7" ||
		names["@owner:t8.dirextalk.ai"] != "liyanan8" ||
		avatars["@owner:t7.dirextalk.ai"] != "mxc://t7/avatar" {
		t.Fatalf("expected profile-enriched member identities, got %s", string(payload))
	}
}

func TestMCPRoomMembersRejectsBlockedRooms(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!blocked:example.com",
		"name":    "Blocked Group",
	})
	mustHandle[map[string]any](t, service, "agent.config.update", map[string]any{
		"mcp_blocked_room_ids": []any{group.RoomID},
	})

	_, apiErr := service.invokeDirextalkMCP(context.Background(), dirextalkmcp.ActionRoomMembersList, map[string]any{
		"room_id": group.RoomID,
	})
	if apiErr == nil || apiErr.Status != 403 || !strings.Contains(apiErr.Error, "blocked") {
		t.Fatalf("expected blocked room member list to fail, got %#v", apiErr)
	}
}

func TestMCPMessagesListUsesAgentRoomNameAndDisplayName(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	service.agentRoomID = "!agents:example.com"
	service.agentConfig.DisplayName = "Codex"
	service.SetMatrixMessageReader(&fakeMCPMessageReader{messages: []mcpMessageSummary{
		{EventID: "$question", OriginServerTS: 1710000000000, CreatedAt: "2024-03-09T16:00:00Z", Sender: "owner", Msg: "question", SenderMXID: "@owner:example.com"},
		{EventID: "$answer", OriginServerTS: 1710000100000, CreatedAt: "2024-03-09T16:01:40Z", Sender: "agent", Msg: "answer", SenderMXID: "@agent:example.com"},
	}})

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionMessagesList, map[string]any{
		"room_id": "!agents:example.com",
	})
	if result["room_id"] != "!agents:example.com" || result["name"] != agentRoomName {
		t.Fatalf("unexpected agent room envelope: %#v", result)
	}
	messages := result["messages"].([]mcpMessageSummary)
	if len(messages) != 2 || messages[0].Sender != "Codex" {
		t.Fatalf("expected agent sender display name, got %#v", messages)
	}
}

func TestMCPChannelPostsAndCommentsReturnConciseJSON(t *testing.T) {
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, &recordingTransport{
		eventID: "$post:event",
		ts:      1710000000000,
	})
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":       "product",
		"room_id":          "!channel:example.com",
		"name":             "Product Channel",
		"channel_type":     "post",
		"comments_enabled": true,
	})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"body":       "post body",
	})
	commentResult := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionChannelCommentsCreate, map[string]any{
		"post_id": post.PostID,
		"msg":     "comment body",
	})
	if commentResult["ok"] != true || commentResult["post_id"] != post.PostID {
		t.Fatalf("unexpected comment create result: %#v", commentResult)
	}

	posts := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionChannelPostsList, map[string]any{
		"room_id": ch.RoomID,
	})
	gotPosts := posts["posts"].([]mcpPostSummary)
	if len(gotPosts) != 1 || gotPosts[0].PostID != post.PostID || gotPosts[0].Msg != "post body" {
		t.Fatalf("unexpected post summaries: %#v", gotPosts)
	}

	comments := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionChannelCommentsList, map[string]any{
		"post_id": post.PostID,
	})
	gotComments := comments["comments"].([]mcpCommentSummary)
	if len(gotComments) != 1 || gotComments[0].Msg != "comment body" {
		t.Fatalf("unexpected comment summaries: %#v", gotComments)
	}
}

func TestMCPChannelPostsPaginationUsesStableSnapshotAndReadableCounts(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":       "product",
		"room_id":          "!channel:example.com",
		"name":             "Product Channel",
		"comments_enabled": true,
	})
	base := time.Date(2026, 7, 5, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 12; i++ {
		service.posts = append(service.posts, channelPostRecord{
			PostID:         "post_" + string(rune('a'+i)),
			ChannelID:      ch.ChannelID,
			RoomID:         ch.RoomID,
			EventID:        "$post_" + string(rune('a'+i)),
			AuthorMXID:     "@owner:example.com",
			AuthorName:     "Owner",
			Body:           "post body",
			OriginServerTS: base.Add(time.Duration(i) * time.Minute).UnixMilli(),
		})
	}
	service.comments = append(service.comments, channelCommentRecord{
		CommentID:      "comment_1",
		PostID:         "post_l",
		ChannelID:      ch.ChannelID,
		OriginServerTS: base.Add(13 * time.Minute).UnixMilli(),
	})
	service.reactions[reactionKey("post", "post_l", "like", "@owner:example.com")] = reactionRecord{
		TargetType: "post",
		TargetID:   "post_l",
		Reaction:   "like",
		UserID:     "@owner:example.com",
		Active:     true,
	}
	service.favorites[1] = favoriteRecord{
		ID:             1,
		EventID:        "$post_l",
		RoomID:         ch.RoomID,
		MessageType:    "channel_post",
		OriginServerTS: base.Add(11 * time.Minute).UnixMilli(),
	}

	first := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionChannelPostsList, map[string]any{
		"room_id": ch.RoomID,
		"limit":   float64(5),
	})
	firstPosts := first["posts"].([]mcpPostSummary)
	if len(firstPosts) != 5 || firstPosts[0].PostID != "post_l" || firstPosts[4].PostID != "post_h" {
		t.Fatalf("expected newest five posts, got %#v", firstPosts)
	}
	if firstPosts[0].CreatedAt != "2026-07-05T10:11:00Z" ||
		firstPosts[0].CommentCount != 1 ||
		firstPosts[0].LikeCount != 1 ||
		firstPosts[0].FavoriteCount != 1 ||
		!firstPosts[0].FavoritedByMe {
		t.Fatalf("expected readable post counts and time, got %#v", firstPosts[0])
	}
	cursor, _ := first["next_cursor"].(string)
	if cursor == "" {
		t.Fatalf("expected next_cursor in first page: %#v", first)
	}

	for i := 0; i < 10; i++ {
		service.posts = append(service.posts, channelPostRecord{
			PostID:         "new_post_" + string(rune('a'+i)),
			ChannelID:      ch.ChannelID,
			RoomID:         ch.RoomID,
			EventID:        "$new_post_" + string(rune('a'+i)),
			AuthorMXID:     "@owner:example.com",
			AuthorName:     "Owner",
			Body:           "newer post",
			OriginServerTS: time.Now().UTC().Add(time.Hour + time.Duration(i)*time.Minute).UnixMilli(),
		})
	}

	second := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionChannelPostsList, map[string]any{
		"room_id": ch.RoomID,
		"cursor":  cursor,
		"limit":   float64(5),
	})
	secondPosts := second["posts"].([]mcpPostSummary)
	if len(secondPosts) != 5 || secondPosts[0].PostID != "post_g" || secondPosts[4].PostID != "post_c" {
		t.Fatalf("expected cursor to continue original snapshot, got %#v", secondPosts)
	}
	for _, post := range secondPosts {
		if strings.HasPrefix(post.PostID, "new_post_") {
			t.Fatalf("cursor page must not include newer inserts: %#v", secondPosts)
		}
	}
}

func TestMCPChannelCommentsPaginationUsesStableSnapshot(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":       "product",
		"room_id":          "!channel:example.com",
		"name":             "Product Channel",
		"comments_enabled": true,
	})
	service.posts = append(service.posts, channelPostRecord{
		PostID:         "post",
		ChannelID:      ch.ChannelID,
		RoomID:         ch.RoomID,
		EventID:        "$post",
		OriginServerTS: time.Date(2026, 7, 5, 9, 0, 0, 0, time.UTC).UnixMilli(),
	})
	base := time.Date(2026, 7, 5, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 12; i++ {
		service.comments = append(service.comments, channelCommentRecord{
			CommentID:      "comment_" + string(rune('a'+i)),
			PostID:         "post",
			ChannelID:      ch.ChannelID,
			AuthorMXID:     "@owner:example.com",
			AuthorName:     "Owner",
			Body:           "comment body",
			OriginServerTS: base.Add(time.Duration(i) * time.Minute).UnixMilli(),
		})
	}

	first := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionChannelCommentsList, map[string]any{
		"post_id": "post",
		"limit":   float64(5),
	})
	firstComments := first["comments"].([]mcpCommentSummary)
	if len(firstComments) != 5 || firstComments[0].CommentID != "comment_l" || firstComments[4].CommentID != "comment_h" {
		t.Fatalf("expected newest five comments, got %#v", firstComments)
	}
	if firstComments[0].CreatedAt != "2026-07-05T10:11:00Z" {
		t.Fatalf("expected readable comment time, got %#v", firstComments[0])
	}
	cursor, _ := first["next_cursor"].(string)
	if cursor == "" {
		t.Fatalf("expected next_cursor in first page: %#v", first)
	}

	for i := 0; i < 10; i++ {
		service.comments = append(service.comments, channelCommentRecord{
			CommentID:      "new_comment_" + string(rune('a'+i)),
			PostID:         "post",
			ChannelID:      ch.ChannelID,
			AuthorMXID:     "@owner:example.com",
			AuthorName:     "Owner",
			Body:           "newer comment",
			OriginServerTS: time.Now().UTC().Add(time.Hour + time.Duration(i)*time.Minute).UnixMilli(),
		})
	}

	second := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionChannelCommentsList, map[string]any{
		"post_id": "post",
		"cursor":  cursor,
		"limit":   float64(5),
	})
	secondComments := second["comments"].([]mcpCommentSummary)
	if len(secondComments) != 5 || secondComments[0].CommentID != "comment_g" || secondComments[4].CommentID != "comment_c" {
		t.Fatalf("expected cursor to continue original snapshot, got %#v", secondComments)
	}
	for _, comment := range secondComments {
		if strings.HasPrefix(comment.CommentID, "new_comment_") {
			t.Fatalf("cursor page must not include newer inserts: %#v", secondComments)
		}
	}
}

func TestMCPChannelContentRejectsBlockedRooms(t *testing.T) {
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, &recordingTransport{
		eventID: "$post:event",
		ts:      1710000000000,
	})
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":       "product",
		"room_id":          "!blocked-channel:example.com",
		"name":             "Product Channel",
		"channel_type":     "post",
		"comments_enabled": true,
	})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"body":       "post body",
	})
	mustHandle[map[string]any](t, service, "agent.config.update", map[string]any{
		"mcp_blocked_room_ids": []any{ch.RoomID},
	})

	for _, tc := range []struct {
		action string
		params map[string]any
	}{
		{dirextalkmcp.ActionChannelPostsList, map[string]any{"room_id": ch.RoomID}},
		{dirextalkmcp.ActionChannelCommentsList, map[string]any{"post_id": post.PostID}},
		{dirextalkmcp.ActionChannelCommentsCreate, map[string]any{"post_id": post.PostID, "msg": "blocked"}},
	} {
		_, apiErr := service.invokeDirextalkMCP(context.Background(), tc.action, tc.params)
		if apiErr == nil || apiErr.Status != 403 || !strings.Contains(apiErr.Error, "blocked") {
			t.Fatalf("expected %s to reject blocked room, got %#v", tc.action, apiErr)
		}
	}
}
