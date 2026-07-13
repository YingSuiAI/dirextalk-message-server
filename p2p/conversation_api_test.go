package p2p

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestContactRequestReturnsConversationOperation(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})

	got, apiErr := service.Handle(ctx, "contacts.request", map[string]any{
		"mxid":         "@alice:example.com",
		"display_name": "Alice",
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	contact := got.(contactRecord)
	if !strings.HasPrefix(contact.RoomID, "!dm-") || !strings.HasSuffix(contact.RoomID, ":example.com") {
		t.Fatalf("transportless contact request room ID = %q", contact.RoomID)
	}
	if contact.Operation["action"] != "contacts.request" || contact.Operation["status"] != "pending_outbound" || contact.Operation["room_id"] != contact.RoomID {
		t.Fatalf("unexpected contact request operation: %#v", contact.Operation)
	}
	if contact.Conversation == nil {
		t.Fatalf("expected contact request conversation, got %#v", contact)
	}
	assertConversationFacts(t, *contact.Conversation, map[string]any{
		"matrix_room_id":      contact.RoomID,
		"kind":                "direct",
		"peer_mxid":           "@alice:example.com",
		"relationship_status": "pending_outbound",
		"membership":          "pending",
		"hydration_state":     "ready",
		"projection_state":    "ready",
	})
	assertConversationCapabilities(t, *contact.Conversation, map[string]bool{
		"open": false,
		"send": false,
	})

	duplicate := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:example.com",
		"display_name": "Alice",
	})
	if duplicate.RoomID != contact.RoomID ||
		duplicate.Operation["action"] != "contacts.request" ||
		duplicate.Operation["status"] != "pending_outbound" ||
		duplicate.Conversation == nil {
		t.Fatalf("expected duplicate contact request to keep operation and conversation on existing contact, got %#v", duplicate)
	}
}

func TestContactAcceptAndDeleteReturnConversationOperation(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	request := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:example.com",
		"display_name": "Alice",
	})

	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":      request.RoomID,
		"peer_mxid":    request.PeerMXID,
		"display_name": request.DisplayName,
		"domain":       request.Domain,
	})
	if accepted.Operation["action"] != "contacts.requests.accept" || accepted.Operation["status"] != "accepted" || accepted.Operation["room_id"] != request.RoomID {
		t.Fatalf("unexpected contact accept operation: %#v", accepted.Operation)
	}
	if accepted.Conversation == nil {
		t.Fatalf("expected accepted contact conversation, got %#v", accepted)
	}
	assertConversationCapabilities(t, *accepted.Conversation, map[string]bool{
		"open":       true,
		"send":       true,
		"send_media": true,
		"call":       true,
	})

	deleted := mustHandle[map[string]any](t, service, "contacts.delete", map[string]any{
		"room_id": request.RoomID,
	})
	operation := deleted["operation"].(map[string]any)
	if operation["action"] != "contacts.delete" || operation["status"] != "deleted" || operation["room_id"] != request.RoomID {
		t.Fatalf("unexpected contact delete operation: %#v", operation)
	}
	conversation := deleted["conversation"].(conversationView)
	assertConversationFacts(t, conversation, map[string]any{
		"matrix_room_id":      request.RoomID,
		"kind":                "direct",
		"peer_mxid":           "@alice:example.com",
		"relationship_status": "deleted",
		"hydration_state":     "ready",
		"projection_state":    "ready",
	})
	assertConversationCapabilities(t, conversation, map[string]bool{
		"open":       false,
		"send":       false,
		"send_media": false,
		"call":       false,
	})
}

func TestChannelPostCommentAndReactionReturnConversationOperation(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})
	if _, apiErr := service.Handle(ctx, "channels.create", map[string]any{
		"channel_id":       "channel",
		"room_id":          "!channel:example.com",
		"name":             "Product Channel",
		"channel_type":     "post",
		"comments_enabled": true,
	}); apiErr != nil {
		t.Fatal(apiErr)
	}

	post, apiErr := service.Handle(ctx, "channels.posts.create", map[string]any{
		"channel_id": "channel",
		"body":       "post body",
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	assertChannelMutationOperation(t, post.(channelPostRecord), "channels.posts.create", "!channel:example.com")

	comment, apiErr := service.Handle(ctx, "channels.comments.create", map[string]any{
		"channel_id": "channel",
		"post_id":    post.(channelPostRecord).PostID,
		"body":       "comment body",
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	assertChannelMutationOperation(t, comment.(channelCommentRecord), "channels.comments.create", "!channel:example.com")

	reaction, apiErr := service.Handle(ctx, "channels.post_reaction.toggle", map[string]any{
		"channel_id": "channel",
		"post_id":    post.(channelPostRecord).PostID,
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	result := reaction.(map[string]any)
	operation := result["operation"].(map[string]any)
	if operation["action"] != "channels.post_reaction.toggle" || operation["status"] != "ok" || operation["room_id"] != "!channel:example.com" {
		t.Fatalf("unexpected reaction operation: %#v", operation)
	}
	conversation := result["conversation"].(conversationView)
	assertConversationFacts(t, conversation, map[string]any{
		"matrix_room_id":  "!channel:example.com",
		"kind":            "channel",
		"hydration_state": "ready",
	})
}

func TestGroupActionsReturnConversationOperations(t *testing.T) {
	tests := []struct {
		name         string
		action       string
		status       string
		roomID       string
		prepare      func(*testing.T, *Service) map[string]any
		facts        map[string]any
		capabilities map[string]bool
	}{
		{
			name:   "create",
			action: "groups.create",
			status: "ok",
			roomID: "!created-group:example.com",
			prepare: func(_ *testing.T, _ *Service) map[string]any {
				return map[string]any{"room_id": "!created-group:example.com", "name": "Created Group"}
			},
			facts: map[string]any{
				"matrix_room_id": "!created-group:example.com", "kind": "group", "title": "Created Group",
				"membership": "join", "role": "owner",
			},
			capabilities: map[string]bool{
				"open": true, "send": true, "send_media": true, "call": true, "invite": true,
				"manage_members": true, "rename": true, "remove_members": true, "leave": true, "delete": true,
			},
		},
		{
			name:   "join",
			action: "groups.join",
			status: "ok",
			roomID: "!remote-group:example.com",
			prepare: func(_ *testing.T, _ *Service) map[string]any {
				return map[string]any{"room_id": "!remote-group:example.com", "group_name": "Remote Group"}
			},
			facts: map[string]any{
				"matrix_room_id": "!remote-group:example.com", "kind": "group", "title": "Remote Group",
				"membership": "join", "role": "member", "hydration_state": "ready", "projection_state": "ready",
			},
			capabilities: map[string]bool{
				"open": true, "send": true, "invite": false, "manage_members": false,
				"rename": false, "remove_members": false, "leave": true, "delete": false,
			},
		},
		{
			name:   "invite",
			action: "groups.invite",
			status: "ok",
			roomID: "!invite-group:example.com",
			prepare: func(t *testing.T, service *Service) map[string]any {
				group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
					"room_id": "!invite-group:example.com", "name": "Invite Group",
				})
				return map[string]any{"room_id": group.RoomID, "user_id": "@alice:example.com"}
			},
			facts: map[string]any{
				"matrix_room_id": "!invite-group:example.com", "kind": "group", "title": "Invite Group",
				"membership": "join", "role": "owner", "hydration_state": "ready", "projection_state": "ready",
			},
		},
		{
			name:   "invite reject",
			action: "groups.invite.reject",
			status: "rejected",
			roomID: "!remote-invite:remote.example",
			prepare: func(t *testing.T, service *Service) map[string]any {
				ctx := context.Background()
				group := groupRecord{RoomID: "!remote-invite:remote.example", Name: "Remote Invite", InvitePolicy: "member"}
				if err := service.saveGroup(ctx, group); err != nil {
					t.Fatal(err)
				}
				if err := service.saveMember(ctx, memberRecord{
					RoomID: group.RoomID, UserID: "@owner:example.com", Domain: "example.com", Membership: "invite", Role: "member",
				}); err != nil {
					t.Fatal(err)
				}
				return map[string]any{"room_id": group.RoomID}
			},
			facts: map[string]any{
				"matrix_room_id": "!remote-invite:remote.example", "kind": "group", "title": "Remote Invite",
				"hydration_state": "pending", "hydration_reason": "owner_membership_missing", "projection_state": "ready",
			},
			capabilities: map[string]bool{
				"open": false, "send": false, "send_media": false, "call": false, "invite": false,
				"manage_members": false, "rename": false, "remove_members": false, "leave": false, "delete": false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := NewService(Config{ServerName: "example.com"})
			got, apiErr := service.Handle(context.Background(), tt.action, tt.prepare(t, service))
			if apiErr != nil {
				t.Fatal(apiErr)
			}
			result := structPayload(t, got)
			operation, ok := result["operation"].(map[string]any)
			if !ok || operation["action"] != tt.action || operation["status"] != tt.status || operation["room_id"] != tt.roomID {
				t.Fatalf("unexpected %s operation: %#v", tt.action, operation)
			}
			conversation := conversationViewPayload(t, result["conversation"])
			assertConversationFacts(t, conversation, tt.facts)
			if tt.capabilities != nil {
				assertConversationCapabilities(t, conversation, tt.capabilities)
			}
		})
	}
}

func TestChannelJoinReturnsConversationOperation(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})
	if _, apiErr := service.Handle(ctx, "channels.create", map[string]any{
		"channel_id": "channel",
		"room_id":    "!channel:example.com",
		"name":       "Product Channel",
	}); apiErr != nil {
		t.Fatal(apiErr)
	}

	got, apiErr := service.Handle(ctx, "channels.join", map[string]any{
		"room_id": "!channel:example.com",
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	result := got.(map[string]any)
	operation := result["operation"].(map[string]any)
	if operation["action"] != "channels.join" || operation["status"] != "ok" || operation["room_id"] != "!channel:example.com" {
		t.Fatalf("unexpected channel join operation: %#v", operation)
	}
	conversation := result["conversation"].(conversationView)
	assertConversationFacts(t, conversation, map[string]any{
		"matrix_room_id":   "!channel:example.com",
		"kind":             "channel",
		"title":            "Product Channel",
		"membership":       "join",
		"role":             "owner",
		"hydration_state":  "ready",
		"projection_state": "ready",
	})
	assertConversationCapabilities(t, conversation, map[string]bool{
		"open":           true,
		"send":           true,
		"send_media":     true,
		"call":           false,
		"invite":         true,
		"manage_members": true,
		"rename":         true,
		"remove_members": true,
		"leave":          true,
		"delete":         true,
	})
}

func TestProductRecordsCreateConversations(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})
	if err := service.saveContact(ctx, contactRecord{
		RoomID:      "!dm:example.com",
		PeerMXID:    "@alice:example.com",
		DisplayName: "Alice",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveGroup(ctx, groupRecord{
		RoomID: "!group:example.com",
		Name:   "Group",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveChannel(ctx, channel{
		ChannelID: "channel",
		RoomID:    "!channel:example.com",
		Name:      "Channel",
	}); err != nil {
		t.Fatal(err)
	}

	list, err := service.listConversations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byKind := map[conversationKind]conversationRecord{}
	for _, item := range list {
		byKind[item.Kind] = item
	}
	if byKind[conversationKindDirect].Title != "Alice" ||
		byKind[conversationKindGroup].Title != "Group" ||
		byKind[conversationKindChannel].Title != "Channel" {
		t.Fatalf("expected product records to create conversations, got %#v", list)
	}

	if err = service.saveConversation(ctx, conversationRecord{
		MatrixRoomID:   "!dm:example.com",
		Kind:           conversationKindDirect,
		LastEventID:    "$message",
		LastActivityAt: 42,
	}); err != nil {
		t.Fatal(err)
	}
	if err = service.saveContact(ctx, contactRecord{
		RoomID:      "!dm:example.com",
		PeerMXID:    "@alice:example.com",
		DisplayName: "Alice Renamed",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := service.getConversation(ctx, "", "!dm:example.com")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got.Title != "Alice Renamed" || got.LastEventID != "$message" || got.LastActivityAt != 42 {
		t.Fatalf("expected product update to preserve activity, got %#v ok=%v", got, ok)
	}
}

func assertConversationFacts(t *testing.T, view conversationView, want map[string]any) {
	t.Helper()
	payload := conversationPayload(t, view)
	for field, expected := range want {
		if got := payload[field]; got != expected {
			t.Fatalf("expected %s=%#v, got %#v in %#v", field, expected, got, payload)
		}
	}
}

func assertConversationCapabilities(t *testing.T, view conversationView, want map[string]bool) {
	t.Helper()
	payload := conversationPayload(t, view)
	capabilities, ok := payload["capabilities"].(map[string]any)
	if !ok {
		t.Fatalf("expected capabilities object, got %#v in %#v", payload["capabilities"], payload)
	}
	for field, expected := range want {
		if got := capabilities[field]; got != expected {
			t.Fatalf("expected capabilities.%s=%v, got %#v in %#v", field, expected, got, capabilities)
		}
	}
}

func assertChannelMutationOperation(t *testing.T, record any, action, roomID string) {
	t.Helper()
	payload := structPayload(t, record)
	operation, ok := payload["operation"].(map[string]any)
	if !ok {
		t.Fatalf("expected operation object, got %#v in %#v", payload["operation"], payload)
	}
	if operation["action"] != action || operation["status"] != "ok" || operation["room_id"] != roomID {
		t.Fatalf("unexpected operation: %#v", operation)
	}
	conversation, ok := payload["conversation"].(map[string]any)
	if !ok {
		t.Fatalf("expected conversation object, got %#v in %#v", payload["conversation"], payload)
	}
	if conversation["matrix_room_id"] != roomID || conversation["kind"] != "channel" || conversation["hydration_state"] != "ready" {
		t.Fatalf("unexpected conversation: %#v", conversation)
	}
}

func conversationPayload(t *testing.T, view conversationView) map[string]any {
	t.Helper()
	return structPayload(t, view)
}

func conversationViewPayload(t *testing.T, value any) conversationView {
	t.Helper()
	var view conversationView
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatal(err)
	}
	return view
}

func structPayload(t *testing.T, value any) map[string]any {
	t.Helper()
	var payload map[string]any
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}
