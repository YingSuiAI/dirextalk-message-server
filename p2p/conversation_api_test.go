package p2p

import (
	"context"
	"encoding/json"
	"testing"
)

func TestConversationsListAndGet(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})
	if err := service.saveConversation(ctx, conversationRecord{
		ConversationID: "conv_group",
		MatrixRoomID:   "!group:example.com",
		Kind:           conversationKindGroup,
		Lifecycle:      conversationLifecycleActive,
		Title:          "Product Group",
		UpdatedAt:      100,
	}); err != nil {
		t.Fatal(err)
	}

	list, apiErr := service.Handle(ctx, "conversations.list", nil)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	conversations := list.(map[string]any)["conversations"].([]conversationView)
	if len(conversations) != 1 {
		t.Fatalf("expected one conversation, got %#v", conversations)
	}
	if conversations[0].Kind != conversationKindGroup || conversations[0].Title != "Product Group" {
		t.Fatalf("unexpected conversation view: %#v", conversations[0])
	}

	got, apiErr := service.Handle(ctx, "conversations.get", map[string]any{"conversation_id": "conv_group"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	view := got.(conversationView)
	if view.MatrixRoomID != "!group:example.com" || view.Title != "Product Group" {
		t.Fatalf("unexpected conversation get view: %#v", view)
	}
}

func TestConversationsGetAcceptsRoomID(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})
	if err := service.saveConversation(ctx, conversationRecord{
		MatrixRoomID: "!dm:example.com",
		Kind:         conversationKindDirect,
		Lifecycle:    conversationLifecycleActive,
		Title:        "Direct",
	}); err != nil {
		t.Fatal(err)
	}

	got, apiErr := service.Handle(ctx, "conversations.get", map[string]any{"room_id": "!dm:example.com"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if got.(conversationView).Kind != conversationKindDirect {
		t.Fatalf("unexpected conversation by room: %#v", got)
	}
}

func TestDirectProductConversationIncludesPeerMXID(t *testing.T) {
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

	list, apiErr := service.Handle(ctx, "conversations.list", nil)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	conversations := list.(map[string]any)["conversations"].([]conversationView)
	if len(conversations) != 1 {
		t.Fatalf("expected one direct conversation, got %#v", conversations)
	}
	if conversations[0].PeerMXID != "@alice:example.com" {
		t.Fatalf("expected peer mxid in conversation view, got %#v", conversations[0])
	}
}

func TestDirectConversationViewIncludesRelationshipFacts(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})
	if err := service.saveContact(ctx, contactRecord{
		RoomID:      "!dm:example.com",
		PeerMXID:    "@alice:example.com",
		DisplayName: "Alice",
		AvatarURL:   "mxc://example.com/alice",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	record := service.conversations[conversationIDForRoomID("!dm:example.com")]
	record.AvatarURL = ""
	service.conversations[record.ConversationID] = record
	service.mu.Unlock()

	list, apiErr := service.Handle(ctx, "conversations.list", nil)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	conversations := list.(map[string]any)["conversations"].([]conversationView)
	assertConversationFacts(t, conversations[0], map[string]any{
		"relationship_status": "accepted",
		"member_count":        float64(2),
		"membership":          "join",
		"role":                "member",
		"hydration_state":     "ready",
	})
	if conversations[0].AvatarURL != "mxc://example.com/alice" {
		t.Fatalf("expected direct conversation avatar from contact fallback, got %#v", conversations[0])
	}
	assertConversationCapabilities(t, conversations[0], map[string]bool{
		"open":           true,
		"send":           true,
		"send_media":     true,
		"call":           true,
		"invite":         false,
		"manage_members": false,
		"rename":         false,
		"remove_members": false,
		"leave":          false,
		"delete":         true,
	})

	got, apiErr := service.Handle(ctx, "conversations.get", map[string]any{"room_id": "!dm:example.com"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	assertConversationFacts(t, got.(conversationView), map[string]any{
		"relationship_status": "accepted",
		"member_count":        float64(2),
		"membership":          "join",
		"role":                "member",
		"hydration_state":     "ready",
	})
	if got.(conversationView).AvatarURL != "mxc://example.com/alice" {
		t.Fatalf("expected direct conversation get avatar from contact fallback, got %#v", got)
	}
	assertConversationCapabilities(t, got.(conversationView), map[string]bool{
		"open":           true,
		"send":           true,
		"send_media":     true,
		"call":           true,
		"invite":         false,
		"manage_members": false,
		"rename":         false,
		"remove_members": false,
		"leave":          false,
		"delete":         true,
	})
}

func TestMergeConversationUpdatePreservesProfileOnActivityOnlyUpdate(t *testing.T) {
	merged := mergeConversationUpdate(
		conversationRecord{
			MatrixRoomID:   "!dm:example.com",
			Kind:           conversationKindDirect,
			CreatedByMXID:  "@owner:example.com",
			PeerMXID:       "@alice:example.com",
			Title:          "Alice",
			AvatarURL:      "mxc://example.com/alice",
			LastEventID:    "$old",
			LastMessage:    "old",
			LastActivityAt: 10,
		},
		conversationRecord{
			MatrixRoomID:    "!dm:example.com",
			Kind:            conversationKindDirect,
			LastEventID:     "$new",
			LastMessage:     "new",
			LastActivityAt:  20,
			ProjectionState: conversationProjectionReady,
		},
	)
	if merged.CreatedByMXID != "@owner:example.com" ||
		merged.PeerMXID != "@alice:example.com" ||
		merged.Title != "Alice" ||
		merged.AvatarURL != "mxc://example.com/alice" {
		t.Fatalf("expected activity-only merge to preserve identity fields, got %#v", merged)
	}
}

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

func TestGroupConversationViewIncludesOwnerMembershipFacts(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})
	if err := service.saveGroup(ctx, groupRecord{
		RoomID: "!group:example.com",
		Name:   "Product Group",
	}); err != nil {
		t.Fatal(err)
	}
	for _, member := range []memberRecord{
		{RoomID: "!group:example.com", UserID: "@owner:example.com", DisplayName: "Owner", Membership: "join", Role: "owner", JoinedAt: 1},
		{RoomID: "!group:example.com", UserID: "@alice:example.com", DisplayName: "Alice", Membership: "join", Role: "member", JoinedAt: 2},
		{RoomID: "!group:example.com", UserID: "@bob:example.com", DisplayName: "Bob", Membership: "join", Role: "member", JoinedAt: 3},
	} {
		if err := service.saveMember(ctx, member); err != nil {
			t.Fatal(err)
		}
	}

	list, apiErr := service.Handle(ctx, "conversations.list", nil)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	conversations := list.(map[string]any)["conversations"].([]conversationView)
	assertConversationFacts(t, conversations[0], map[string]any{
		"member_count":    float64(3),
		"membership":      "join",
		"role":            "owner",
		"hydration_state": "ready",
	})
	assertConversationCapabilities(t, conversations[0], map[string]bool{
		"open":           true,
		"send":           true,
		"send_media":     true,
		"call":           true,
		"invite":         true,
		"manage_members": true,
		"rename":         true,
		"remove_members": true,
		"leave":          true,
		"delete":         true,
	})

	got, apiErr := service.Handle(ctx, "conversations.get", map[string]any{"room_id": "!group:example.com"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	assertConversationFacts(t, got.(conversationView), map[string]any{
		"member_count":    float64(3),
		"membership":      "join",
		"role":            "owner",
		"hydration_state": "ready",
	})
	assertConversationCapabilities(t, got.(conversationView), map[string]bool{
		"open":           true,
		"send":           true,
		"send_media":     true,
		"call":           true,
		"invite":         true,
		"manage_members": true,
		"rename":         true,
		"remove_members": true,
		"leave":          true,
		"delete":         true,
	})
}

func TestChannelConversationViewIncludesOwnerMembershipFacts(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})
	if err := service.saveChannel(ctx, channel{
		ChannelID:       "channel",
		RoomID:          "!channel:example.com",
		Name:            "Product Channel",
		ChannelType:     "post",
		CommentsEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	for _, member := range []memberRecord{
		{RoomID: "!channel:example.com", ChannelID: "channel", UserID: "@owner:example.com", DisplayName: "Owner", Membership: "join", Role: "owner", JoinedAt: 1},
		{RoomID: "!channel:example.com", ChannelID: "channel", UserID: "@alice:example.com", DisplayName: "Alice", Membership: "join", Role: "member", JoinedAt: 2},
	} {
		if err := service.saveMember(ctx, member); err != nil {
			t.Fatal(err)
		}
	}

	list, apiErr := service.Handle(ctx, "conversations.list", nil)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	conversations := list.(map[string]any)["conversations"].([]conversationView)
	assertConversationFacts(t, conversations[0], map[string]any{
		"member_count":    float64(2),
		"membership":      "join",
		"role":            "owner",
		"hydration_state": "ready",
	})
	assertConversationCapabilities(t, conversations[0], map[string]bool{
		"open":             true,
		"send":             true,
		"send_media":       true,
		"call":             false,
		"invite":           true,
		"manage_members":   true,
		"rename":           true,
		"remove_members":   true,
		"leave":            true,
		"delete":           true,
		"post_create":      true,
		"comment_create":   true,
		"reaction_toggle":  true,
		"post_recall":      true,
		"comment_recall":   true,
		"comments_enabled": true,
	})

	got, apiErr := service.Handle(ctx, "conversations.get", map[string]any{"room_id": "!channel:example.com"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	assertConversationFacts(t, got.(conversationView), map[string]any{
		"member_count":    float64(2),
		"membership":      "join",
		"role":            "owner",
		"hydration_state": "ready",
	})
	assertConversationCapabilities(t, got.(conversationView), map[string]bool{
		"open":             true,
		"send":             true,
		"send_media":       true,
		"call":             false,
		"invite":           true,
		"manage_members":   true,
		"rename":           true,
		"remove_members":   true,
		"leave":            true,
		"delete":           true,
		"post_create":      true,
		"comment_create":   true,
		"reaction_toggle":  true,
		"post_recall":      true,
		"comment_recall":   true,
		"comments_enabled": true,
	})
}

func TestChannelConversationViewDisablesCommentCreateWhenChannelCommentsOff(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})
	if err := service.saveChannel(ctx, channel{
		ChannelID:       "channel",
		RoomID:          "!channel:example.com",
		Name:            "Product Channel",
		ChannelType:     "post",
		CommentsEnabled: false,
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(ctx, memberRecord{
		RoomID:      "!channel:example.com",
		ChannelID:   "channel",
		UserID:      "@owner:example.com",
		DisplayName: "Owner",
		Membership:  "join",
		Role:        "owner",
		JoinedAt:    1,
	}); err != nil {
		t.Fatal(err)
	}

	got, apiErr := service.Handle(ctx, "conversations.get", map[string]any{"room_id": "!channel:example.com"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	assertConversationCapabilities(t, got.(conversationView), map[string]bool{
		"open":             true,
		"post_create":      true,
		"comment_create":   false,
		"reaction_toggle":  true,
		"post_recall":      true,
		"comment_recall":   true,
		"comments_enabled": false,
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

func TestGroupJoinReturnsConversationOperation(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})

	got, apiErr := service.Handle(ctx, "groups.join", map[string]any{
		"room_id":    "!remote-group:example.com",
		"group_name": "Remote Group",
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	result := got.(map[string]any)
	operation := result["operation"].(map[string]any)
	if operation["action"] != "groups.join" || operation["status"] != "ok" || operation["room_id"] != "!remote-group:example.com" {
		t.Fatalf("unexpected group join operation: %#v", operation)
	}
	conversation := result["conversation"].(conversationView)
	assertConversationFacts(t, conversation, map[string]any{
		"matrix_room_id":   "!remote-group:example.com",
		"kind":             "group",
		"title":            "Remote Group",
		"membership":       "join",
		"role":             "member",
		"hydration_state":  "ready",
		"projection_state": "ready",
	})
	assertConversationCapabilities(t, conversation, map[string]bool{
		"open":           true,
		"send":           true,
		"invite":         false,
		"manage_members": false,
		"rename":         false,
		"remove_members": false,
		"leave":          true,
		"delete":         false,
	})
}

func TestGroupCreateReturnsConversationOperation(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})

	got, apiErr := service.Handle(ctx, "groups.create", map[string]any{
		"room_id": "!created-group:example.com",
		"name":    "Created Group",
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	operation, ok := result["operation"].(map[string]any)
	if !ok {
		t.Fatalf("missing operation in groups.create response: %#v", result)
	}
	if operation["action"] != "groups.create" || operation["status"] != "ok" || operation["room_id"] != "!created-group:example.com" {
		t.Fatalf("unexpected group create operation: %#v", operation)
	}
	conversation, ok := result["conversation"].(map[string]any)
	if !ok {
		t.Fatalf("missing conversation in groups.create response: %#v", result)
	}
	if conversation["matrix_room_id"] != "!created-group:example.com" ||
		conversation["kind"] != "group" ||
		conversation["title"] != "Created Group" ||
		conversation["membership"] != "join" ||
		conversation["role"] != "owner" {
		t.Fatalf("unexpected group create conversation: %#v", conversation)
	}
	capabilities, ok := conversation["capabilities"].(map[string]any)
	if !ok ||
		capabilities["open"] != true ||
		capabilities["send"] != true ||
		capabilities["send_media"] != true ||
		capabilities["call"] != true ||
		capabilities["invite"] != true ||
		capabilities["manage_members"] != true ||
		capabilities["rename"] != true ||
		capabilities["remove_members"] != true ||
		capabilities["leave"] != true ||
		capabilities["delete"] != true {
		t.Fatalf("unexpected group create capabilities: %#v", conversation["capabilities"])
	}
}

func TestGroupInviteReturnsConversationOperation(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!invite-group:example.com",
		"name":    "Invite Group",
	})

	got, apiErr := service.Handle(ctx, "groups.invite", map[string]any{
		"room_id": group.RoomID,
		"user_id": "@alice:example.com",
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	result := got.(map[string]any)
	operation := result["operation"].(map[string]any)
	if operation["action"] != "groups.invite" || operation["status"] != "ok" || operation["room_id"] != group.RoomID {
		t.Fatalf("unexpected group invite operation: %#v", operation)
	}
	conversation := result["conversation"].(conversationView)
	assertConversationFacts(t, conversation, map[string]any{
		"matrix_room_id":   group.RoomID,
		"kind":             "group",
		"title":            "Invite Group",
		"membership":       "join",
		"role":             "owner",
		"hydration_state":  "ready",
		"projection_state": "ready",
	})
}

func TestGroupInviteRejectReturnsConversationOperation(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})
	group := groupRecord{
		RoomID:       "!remote-invite:remote.example",
		Name:         "Remote Invite",
		InvitePolicy: "member",
	}
	if err := service.saveGroup(ctx, group); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(ctx, memberRecord{
		RoomID:     group.RoomID,
		UserID:     "@owner:example.com",
		Domain:     "example.com",
		Membership: "invite",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	got, apiErr := service.Handle(ctx, "groups.invite.reject", map[string]any{"room_id": group.RoomID})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	result := got.(map[string]any)
	operation := result["operation"].(map[string]any)
	if operation["action"] != "groups.invite.reject" || operation["status"] != "rejected" || operation["room_id"] != group.RoomID {
		t.Fatalf("unexpected group invite reject operation: %#v", operation)
	}
	conversation := result["conversation"].(conversationView)
	assertConversationFacts(t, conversation, map[string]any{
		"matrix_room_id":   group.RoomID,
		"kind":             "group",
		"title":            "Remote Invite",
		"hydration_state":  "pending",
		"hydration_reason": "owner_membership_missing",
		"projection_state": "ready",
	})
	assertConversationCapabilities(t, conversation, map[string]bool{
		"open":           false,
		"send":           false,
		"send_media":     false,
		"call":           false,
		"invite":         false,
		"manage_members": false,
		"rename":         false,
		"remove_members": false,
		"leave":          false,
		"delete":         false,
	})
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
