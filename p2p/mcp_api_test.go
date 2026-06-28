package p2p

import (
	"context"
	"encoding/json"
	"strings"
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

func TestMCPSearchRoomsUsesMatrixMemberCountWhenProductCountIsStale(t *testing.T) {
	transport := &recordingTransport{roomMembers: []memberRecord{
		{RoomID: "!group:example.com", UserID: "@owner:example.com", DisplayName: "Owner Name", Membership: "join", Role: "owner"},
		{RoomID: "!group:example.com", UserID: "@owner:t7.direxio.ai", DisplayName: "owner", Membership: "join", Role: "member"},
		{RoomID: "!group:example.com", UserID: "@owner:t8.direxio.ai", DisplayName: "owner", Membership: "join", Role: "member"},
	}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Design Group",
	})

	result := mustHandle[map[string]any](t, service, "mcp.rooms.search", map[string]any{
		"type":  "group",
		"query": "Design",
	})
	rooms := result["rooms"].([]mcpRoomSummary)
	if len(rooms) != 1 || rooms[0].Subtitle != "3 members" {
		t.Fatalf("expected Matrix-backed member count, got %#v", rooms)
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
	service.agentRoomID = "!agents:example.com"

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

func TestMCPMessagesSendRejectsOwnerSendToAgentRoom(t *testing.T) {
	transport := &recordingTransport{eventID: "$mcp:event", ts: 1710000000000}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.agentRoomID = "!agents:example.com"

	_, apiErr := service.Handle(context.Background(), "mcp.messages.send", map[string]any{
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

type fakeMatrixProfileResolver struct {
	profiles map[string]matrixUserProfile
}

func (r *fakeMatrixProfileResolver) ResolveMatrixProfile(ctx context.Context, userID string) (matrixUserProfile, error) {
	return r.profiles[strings.TrimSpace(userID)], nil
}

func TestMCPMessagesListUsesReaderAndReturnsConciseMessages(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	service.SetMatrixMessageReader(&fakeMCPMessageReader{messages: []mcpMessageSummary{
		{TS: 1710000000000, Sender: "Alice", Msg: "old", SenderMXID: "@alice:remote.example"},
		{TS: 1710000100000, Sender: "Alice", Msg: "inside", SenderMXID: "@alice:remote.example", SenderDomain: "remote.example", SenderLocalpart: "alice"},
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
		decoded.Messages[0].SenderLocalpart != "alice" {
		t.Fatalf("expected message JSON to expose sender identity, got %s", string(payload))
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
			TS:              1710000100000,
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

	result := mustHandle[map[string]any](t, service, "mcp.messages.list", map[string]any{
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
		{RoomID: "!group:example.com", UserID: "@owner:t8.direxio.ai", DisplayName: "owner", Membership: "join", Role: "member"},
	}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.SetMatrixProfileResolver(&fakeMatrixProfileResolver{profiles: map[string]matrixUserProfile{
		"@owner:t8.direxio.ai": {DisplayName: "liyanan8", AvatarURL: "mxc://t8/avatar"},
	}})
	service.SetMatrixMessageReader(&fakeMCPMessageReader{messages: []mcpMessageSummary{
		{
			TS:              1710000100000,
			Sender:          "owner",
			SenderMXID:      "@owner:t8.direxio.ai",
			SenderDomain:    "t8.direxio.ai",
			SenderLocalpart: "owner",
			Msg:             "hello from t8",
		},
	}})
	mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Design Group",
	})

	result := mustHandle[map[string]any](t, service, "mcp.messages.list", map[string]any{
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

	result := mustHandle[map[string]any](t, service, "mcp.room_members.list", map[string]any{
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

	result := mustHandle[map[string]any](t, service, "mcp.room_members.list", map[string]any{
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

func TestMCPRoomMembersListEnrichesFallbackNamesFromMatrixProfiles(t *testing.T) {
	transport := &recordingTransport{roomMembers: []memberRecord{
		{RoomID: "!group:example.com", UserID: "@owner:example.com", DisplayName: "Owner Name", Membership: "join", Role: "owner"},
		{RoomID: "!group:example.com", UserID: "@owner:t7.direxio.ai", DisplayName: "owner", Membership: "join", Role: "member"},
		{RoomID: "!group:example.com", UserID: "@owner:t8.direxio.ai", DisplayName: "owner", Membership: "join", Role: "member"},
	}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.SetMatrixProfileResolver(&fakeMatrixProfileResolver{profiles: map[string]matrixUserProfile{
		"@owner:t7.direxio.ai": {DisplayName: "liyanan7", AvatarURL: "mxc://t7/avatar"},
		"@owner:t8.direxio.ai": {DisplayName: "liyanan8", AvatarURL: "mxc://t8/avatar"},
	}})
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Design Group",
	})

	result := mustHandle[map[string]any](t, service, "mcp.room_members.list", map[string]any{
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
	if names["@owner:t7.direxio.ai"] != "liyanan7" ||
		names["@owner:t8.direxio.ai"] != "liyanan8" ||
		avatars["@owner:t7.direxio.ai"] != "mxc://t7/avatar" {
		t.Fatalf("expected profile-enriched member identities, got %s", string(payload))
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
