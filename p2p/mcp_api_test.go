package p2p

import (
	"context"
	"testing"
)

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

	result := mustHandle[map[string]any](t, service, "mcp.rooms.search", map[string]any{
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

	result := mustHandle[map[string]any](t, service, "mcp.rooms.search", map[string]any{
		"type": "group",
	})
	rooms := result["rooms"].([]mcpRoomSummary)
	if len(rooms) != 1 || rooms[0].Type != "group" || rooms[0].RoomID != "!group:example.com" {
		t.Fatalf("expected group list from empty query, got %#v", rooms)
	}
}

func TestMCPMessagesSendUsesTransportAndReturnsConciseResult(t *testing.T) {
	transport := &recordingTransport{eventID: "$mcp:event", ts: 1710000000000}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.profile.DisplayName = "Owner"

	result := mustHandle[map[string]any](t, service, "mcp.messages.send", map[string]any{
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

	mustHandle[map[string]any](t, service, "mcp.messages.send", map[string]any{
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

type fakeMCPMessageReader struct {
	messages []mcpMessageSummary
}

func (r *fakeMCPMessageReader) ListOrdinaryMessages(ctx context.Context, roomID string, fromTS, toTS int64, limit int) ([]mcpMessageSummary, error) {
	out := make([]mcpMessageSummary, 0, len(r.messages))
	for _, msg := range r.messages {
		if inMCPTimeRange(msg.TS, fromTS, toTS) {
			out = append(out, msg)
		}
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func TestMCPMessagesListUsesReaderAndReturnsConciseMessages(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	service.SetMatrixMessageReader(&fakeMCPMessageReader{messages: []mcpMessageSummary{
		{TS: 1710000000000, Sender: "Alice", Msg: "old"},
		{TS: 1710000100000, Sender: "Alice", Msg: "inside"},
	}})
	if err := service.saveConversation(context.Background(), conversationRecord{
		MatrixRoomID: "!room:example.com",
		Kind:         conversationKindDirect,
		Lifecycle:    conversationLifecycleActive,
		Title:        "Alice",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "mcp.messages.list", map[string]any{
		"room_id": "!room:example.com",
		"from_ts": float64(1710000050000),
		"limit":   float64(20),
	})
	if result["room_id"] != "!room:example.com" || result["name"] != "Alice" {
		t.Fatalf("unexpected message list envelope: %#v", result)
	}
	messages := result["messages"].([]mcpMessageSummary)
	if len(messages) != 1 || messages[0].Msg != "inside" {
		t.Fatalf("unexpected message summaries: %#v", messages)
	}
}

func TestMCPMessagesListUsesAgentRoomNameAndDisplayName(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	service.agentRoomID = "!agents:example.com"
	service.agentConfig.DisplayName = "Codex"
	service.SetMatrixMessageReader(&fakeMCPMessageReader{messages: []mcpMessageSummary{
		{TS: 1710000000000, Sender: "owner", Msg: "question", SenderMXID: "@owner:example.com"},
		{TS: 1710000100000, Sender: "agent", Msg: "answer", SenderMXID: "@agent:example.com"},
	}})

	result := mustHandle[map[string]any](t, service, "mcp.messages.list", map[string]any{
		"room_id": "!agents:example.com",
	})
	if result["room_id"] != "!agents:example.com" || result["name"] != agentRoomName {
		t.Fatalf("unexpected agent room envelope: %#v", result)
	}
	messages := result["messages"].([]mcpMessageSummary)
	if len(messages) != 2 || messages[1].Sender != "Codex" {
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
	commentResult := mustHandle[map[string]any](t, service, "mcp.channel_comments.create", map[string]any{
		"post_id": post.PostID,
		"msg":     "comment body",
	})
	if commentResult["ok"] != true || commentResult["post_id"] != post.PostID {
		t.Fatalf("unexpected comment create result: %#v", commentResult)
	}

	posts := mustHandle[map[string]any](t, service, "mcp.channel_posts.list", map[string]any{
		"room_id": ch.RoomID,
	})
	gotPosts := posts["posts"].([]mcpPostSummary)
	if len(gotPosts) != 1 || gotPosts[0].PostID != post.PostID || gotPosts[0].Msg != "post body" {
		t.Fatalf("unexpected post summaries: %#v", gotPosts)
	}

	comments := mustHandle[map[string]any](t, service, "mcp.channel_comments.list", map[string]any{
		"post_id": post.PostID,
	})
	gotComments := comments["comments"].([]mcpCommentSummary)
	if len(gotComments) != 1 || gotComments[0].Msg != "comment body" {
		t.Fatalf("unexpected comment summaries: %#v", gotComments)
	}
}
