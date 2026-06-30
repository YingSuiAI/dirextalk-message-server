package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YingSuiAI/direxio-message-server/internal/productpolicy"
	"github.com/YingSuiAI/direxio-message-server/internal/pushrules"
	"github.com/YingSuiAI/direxio-message-server/p2p/matrixhistory"
	roomserverAPI "github.com/YingSuiAI/direxio-message-server/roomserver/api"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func TestGroupAndTextChannelCreateUseJoinedHistoryVisibility(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	mustHandle[groupRecord](t, service, "groups.create", map[string]any{"name": "Team"})
	transport.roomID = "!chat:example.com"
	mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "chat",
		"name":         "Chat",
		"channel_type": "chat",
	})
	transport.roomID = "!text:example.com"
	mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "text",
		"name":         "Text",
		"channel_type": "text",
	})

	if len(transport.createRooms) != 3 {
		t.Fatalf("expected group, chat channel, and text channel rooms to be created, got %#v", transport.createRooms)
	}
	for _, req := range transport.createRooms {
		got, ok := initialHistoryVisibility(req)
		if !ok || got != string(gomatrixserverlib.HistoryVisibilityJoined) {
			t.Fatalf("expected joined history visibility for %s room create, got %q ok=%v in %#v", req.RoomType, got, ok, req.InitialState)
		}
	}
}

func TestPostChannelCreateUsesSharedHistoryVisibility(t *testing.T) {
	transport := &recordingTransport{roomID: "!posts:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "posts",
		"name":         "Posts",
		"channel_type": "post",
	})

	if len(transport.createRooms) != 1 {
		t.Fatalf("expected post channel room to be created, got %#v", transport.createRooms)
	}
	if got, ok := initialHistoryVisibility(transport.createRooms[0]); !ok || got != string(gomatrixserverlib.HistoryVisibilityShared) {
		t.Fatalf("post channels must use shared history visibility for existing posts/comments, got %q ok=%v in %#v", got, ok, transport.createRooms[0].InitialState)
	}
}

func TestPostChannelCreateWithExistingRoomPublishesSharedHistoryVisibility(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "posts",
		"room_id":      "!existing:example.com",
		"name":         "Posts",
		"channel_type": "post",
	})

	if len(transport.createRooms) != 0 {
		t.Fatalf("existing room channel must not create a new room, got %#v", transport.createRooms)
	}
	if len(transport.stateEvents) != 1 {
		t.Fatalf("expected existing post room to publish shared history visibility, got %#v", transport.stateEvents)
	}
	if got, ok := updateStateHistoryVisibility(transport.stateEvents[0]); !ok || got != string(gomatrixserverlib.HistoryVisibilityShared) {
		t.Fatalf("expected existing post room to publish shared history visibility, got %q ok=%v in %#v", got, ok, transport.stateEvents[0])
	}
}

func TestServiceUsesTransportForRoomsAndRemovesP2PMessageActions(t *testing.T) {
	transport := &recordingTransport{
		roomID:  "!matrix-room:example.com",
		eventID: "$matrix-event:example.com",
		ts:      1770000000123,
	}
	service := NewServiceWithTransport(Config{
		ServerName:                      "example.com",
		RemoteNodeAllowPrivateBaseURLs:  true,
		RemoteNodeInsecureSkipTLSVerify: true,
	}, transport)
	bootstrapService(t, service)

	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"name": "Team"})
	if group.RoomID != transport.roomID {
		t.Fatalf("expected transport room_id, got %#v", group)
	}
	if len(transport.createRooms) != 1 || transport.createRooms[0].Name != "Team" {
		t.Fatalf("expected group room creation through transport, got %#v", transport.createRooms)
	}

	for _, action := range []string{"rooms.send", "rooms.send_media", "search", "sync.messages", "sync.unread", "rooms.messages.recall"} {
		if _, apiErr := service.Handle(context.Background(), action, map[string]any{"room_id": group.RoomID, "content": "hello", "event_id": transport.eventID}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
			t.Fatalf("expected removed %s to be unknown, got %#v", action, apiErr)
		}
	}
	if len(transport.messages) != 0 {
		t.Fatalf("removed P2P message actions must not send through transport, got %#v", transport.messages)
	}
}

func initialHistoryVisibility(req CreateRoomRequest) (string, bool) {
	for _, state := range req.InitialState {
		if state.Type != spec.MRoomHistoryVisibility || state.StateKey != "" {
			continue
		}
		value, _ := state.Content["history_visibility"].(string)
		return value, true
	}
	return "", false
}

func updateStateHistoryVisibility(req SendStateEventRequest) (string, bool) {
	if req.Event.Type != spec.MRoomHistoryVisibility || req.Event.StateKey != "" {
		return "", false
	}
	value, _ := req.Event.Content["history_visibility"].(string)
	return value, true
}

func agentStatusOnlineState(state RoomStateEvent, agentMXID string) (bool, bool) {
	if state.Type != DirexioAgentStatusEventType || state.StateKey != agentMXID {
		return false, false
	}
	online, ok := state.Content["online"].(bool)
	return online, ok
}

func agentStatusOnlineUpdate(req SendStateEventRequest, roomID, senderMXID, agentMXID string) (bool, bool) {
	if req.RoomID != roomID || req.SenderMXID != senderMXID {
		return false, false
	}
	return agentStatusOnlineState(req.Event, agentMXID)
}

func initialPowerLevelForUser(states []RoomStateEvent, userMXID string) (int, bool) {
	state, ok := initialStateOfType(states, spec.MRoomPowerLevels)
	if !ok {
		return 0, false
	}
	users, ok := state.Content["users"].(map[string]any)
	if !ok {
		return 0, false
	}
	switch level := users[userMXID].(type) {
	case int:
		return level, true
	case int64:
		return int(level), true
	case float64:
		return int(level), true
	default:
		return 0, false
	}
}

func initialStateOfType(states []RoomStateEvent, eventType string) (RoomStateEvent, bool) {
	for _, state := range states {
		if state.Type == eventType {
			return state, true
		}
	}
	return RoomStateEvent{}, false
}

func TestEnsureAgentRoomCreatesRealRoomForLegacyID(t *testing.T) {
	transport := &recordingTransport{roomID: "!agents-real:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)

	changed, err := service.ensureAgentRoom(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatalf("expected legacy agent room id to be replaced")
	}
	if service.agentRoomID != "!agents-real:example.com" {
		t.Fatalf("expected real agent room id, got %q", service.agentRoomID)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("expected one agent room create, got %#v", transport.createRooms)
	}
	req := transport.createRooms[0]
	if req.CreatorMXID != "@owner:example.com" || req.Name != "Agents" || req.Visibility != "private" || req.RoomType != "" {
		t.Fatalf("unexpected agent room create request: %#v", req)
	}
	if len(req.InviteMXIDs) != 1 || req.InviteMXIDs[0] != "@agent:example.com" {
		t.Fatalf("expected agent invite on room create, got %#v", req.InviteMXIDs)
	}
	if level, ok := initialPowerLevelForUser(req.InitialState, "@agent:example.com"); !ok || level < 50 {
		t.Fatalf("expected created agent room to grant @agent state power, level=%d ok=%v state=%#v", level, ok, req.InitialState)
	}
	if statusState, ok := initialStateOfType(req.InitialState, DirexioAgentStatusEventType); ok {
		t.Fatalf("agent status state must be sent after join, not as owner-created initial state: %#v", statusState)
	}
	if len(transport.joinRequests) != 1 || transport.joinRequests[0].UserMXID != "@agent:example.com" || transport.joinRequests[0].DisplayName != "Agent" {
		t.Fatalf("expected agent to join created room, got %#v", transport.joinRequests)
	}
	if len(transport.stateEvents) != 1 {
		t.Fatalf("expected agent to publish status after joining created room, got %#v", transport.stateEvents)
	}
	if online, ok := agentStatusOnlineUpdate(transport.stateEvents[0], "!agents-real:example.com", "@agent:example.com", "@agent:example.com"); !ok || online {
		t.Fatalf("expected agent room repair to publish offline status until gateway connects, got %#v", transport.stateEvents[0])
	}
}

func TestEnsureAgentRoomMutesOwnerPushRuleByDefault(t *testing.T) {
	transport := &recordingTransport{roomID: "!agents-real:example.com"}
	pushRules := &recordingPushRuleManager{
		ruleSets: pushrules.DefaultAccountRuleSets(ownerLocalpart, "example.com"),
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.SetPushRuleManager(pushRules)

	if _, err := service.ensureAgentRoom(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pushRules.putUserID != "@owner:example.com" {
		t.Fatalf("expected owner push rules to be updated, got %q", pushRules.putUserID)
	}
	if pushRules.putRuleSets == nil {
		t.Fatal("expected push rules to be saved")
	}
	for _, rule := range pushRules.putRuleSets.Global.Room {
		if rule.RuleID == "!agents-real:example.com" && rule.Enabled && !rule.Default && len(rule.Actions) == 0 {
			return
		}
	}
	t.Fatalf("expected agent room to default to a room-level dont_notify push rule, got %#v", pushRules.putRuleSets.Global.Room)
}

func TestEnsureAgentRoomJoinsAgentAndOwnerForExistingRealRoom(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.agentRoomID = "!agents-real:example.com"
	service.agentConfig.DisplayName = "Codex"

	changed, err := service.ensureAgentRoom(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatalf("expected existing real agent room id to remain unchanged")
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("expected no room create for existing agent room, got %#v", transport.createRooms)
	}
	if len(transport.joinRequests) != 2 {
		t.Fatalf("expected agent and owner to join existing room, got %#v", transport.joinRequests)
	}
	if transport.joinRequests[0].UserMXID != "@agent:example.com" || transport.joinRequests[0].DisplayName != "Codex" {
		t.Fatalf("expected agent to join existing room first, got %#v", transport.joinRequests)
	}
	if transport.joinRequests[1].UserMXID != "@owner:example.com" {
		t.Fatalf("expected owner to join existing room, got %#v", transport.joinRequests)
	}
	if len(transport.stateEvents) != 2 {
		t.Fatalf("expected existing agent room to repair power levels and publish status state, got %#v", transport.stateEvents)
	}
	if transport.stateEvents[0].RoomID != "!agents-real:example.com" ||
		transport.stateEvents[0].SenderMXID != "@owner:example.com" ||
		transport.stateEvents[0].Event.Type != spec.MRoomPowerLevels {
		t.Fatalf("expected owner to repair agent room power levels first, got %#v", transport.stateEvents[0])
	}
	if level, ok := initialPowerLevelForUser([]RoomStateEvent{transport.stateEvents[0].Event}, "@agent:example.com"); !ok || level < 50 {
		t.Fatalf("expected repaired power levels to grant @agent state power, level=%d ok=%v state=%#v", level, ok, transport.stateEvents[0])
	}
	if online, ok := agentStatusOnlineUpdate(transport.stateEvents[1], "!agents-real:example.com", "@agent:example.com", "@agent:example.com"); !ok || online {
		t.Fatalf("expected existing agent room status to remain offline until gateway connects, got %#v", transport.stateEvents[1])
	}
}

func TestEnsureAgentRoomInvitesOwnerFromAgentWhenOwnerJoinRequiresInvite(t *testing.T) {
	transport := &ownerJoinRequiresInviteTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.agentRoomID = "!agents-real:example.com"

	changed, err := service.ensureAgentRoom(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatalf("expected existing real agent room id to remain unchanged")
	}
	if len(transport.joinRequests) != 3 {
		t.Fatalf("expected agent join and owner join retry, got %#v", transport.joinRequests)
	}
	if len(transport.inviteRequests) != 1 ||
		transport.inviteRequests[0].InviterMXID != "@agent:example.com" ||
		transport.inviteRequests[0].InviteeMXID != "@owner:example.com" {
		t.Fatalf("expected agent to invite owner back into agents room, got %#v", transport.inviteRequests)
	}
	if len(transport.stateEvents) != 2 {
		t.Fatalf("expected repaired agent room to repair power levels and publish status state, got %#v", transport.stateEvents)
	}
}

func TestSyncBootstrapIncludesAgentRoomID(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	service.agentRoomID = "!agents-real:example.com"
	bootstrapService(t, service)

	bootstrap := mustHandle[map[string]any](t, service, "sync.bootstrap", nil)
	if bootstrap["agent_room_id"] != "!agents-real:example.com" {
		t.Fatalf("expected sync.bootstrap agent_room_id, got %#v", bootstrap)
	}
}

func TestAgentConfigUpdatePublishesAgentRoomStatusState(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.agentRoomID = "!agents-real:example.com"
	bootstrapService(t, service)

	result := mustHandle[map[string]any](t, service, "agent.config.update", map[string]any{
		"enabled": false,
	})

	if result["enabled"] != false {
		t.Fatalf("expected disabled agent config response, got %#v", result)
	}
	if len(transport.stateEvents) != 1 {
		t.Fatalf("expected one agent status state update, got %#v", transport.stateEvents)
	}
	if online, ok := agentStatusOnlineUpdate(transport.stateEvents[0], "!agents-real:example.com", "@agent:example.com", "@agent:example.com"); !ok || online {
		t.Fatalf("expected disabled agent status state, got %#v", transport.stateEvents[0])
	}
}

func TestAgentConfigEnableDoesNotPublishOnlineState(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	service.agentRoomID = "!agents-real:example.com"
	service.agentConfig.Enabled = false
	bootstrapService(t, service)

	result := mustHandle[map[string]any](t, service, "agent.config.update", map[string]any{
		"enabled": true,
	})

	if result["enabled"] != true {
		t.Fatalf("expected enabled agent config response, got %#v", result)
	}
	if len(transport.stateEvents) != 0 {
		t.Fatalf("enabling config alone must not publish online status, got %#v", transport.stateEvents)
	}
}

func TestRoomSendPreservesChannelSharePayload(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	if _, apiErr := service.Handle(context.Background(), "rooms.send", map[string]any{
		"room_id":      "!room:example.com",
		"content":      "频道分享\n产品频道",
		"message_type": "channel_share",
		"channel_share": map[string]any{
			"channel_id":  "product",
			"room_id":     "!product:example.com",
			"name":        "产品频道",
			"join_policy": "open",
		},
	}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected removed rooms.send to return unknown action, got %#v", apiErr)
	}
	if len(transport.messages) != 0 {
		t.Fatalf("removed rooms.send must not write Matrix message, got %#v", transport.messages)
	}
}

func TestRoomSendPreservesGroupInvitePayload(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)

	if _, apiErr := service.Handle(context.Background(), "rooms.send", map[string]any{
		"room_id":      "!dm:example.com",
		"content":      "邀请加入群聊\n产品群",
		"message_type": "group_invite",
		"group_invite": map[string]any{
			"msgtype":              "p2p.group.invite.v1",
			"group_room_id":        "!group:example.com",
			"group_name":           "产品群",
			"inviter_mxid":         "@owner:example.com",
			"inviter_display_name": "Owner",
			"direct_room_id":       "!dm:example.com",
		},
	}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected removed rooms.send to return unknown action, got %#v", apiErr)
	}
	if len(transport.messages) != 0 {
		t.Fatalf("removed rooms.send must not write Matrix message, got %#v", transport.messages)
	}
}

func TestContactRequestCreatesDirectInviteRoomThroughTransport(t *testing.T) {
	transport := &recordingTransport{roomID: "!dm:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Requester Nick",
		"avatar_url":   "mxc://example.com/requester",
	})

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice",
	})

	if contact.RoomID != "!dm:example.com" || contact.Status != "pending_outbound" {
		t.Fatalf("expected pending outbound contact with transport room, got %#v", contact)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("expected one transport room create, got %#v", transport.createRooms)
	}
	room := transport.createRooms[0]
	if room.CreatorMXID != "@owner:example.com" || !room.IsDirect || room.Visibility != "private" {
		t.Fatalf("expected private direct invite room, got %#v", room)
	}
	if room.CreatorDisplayName != "Requester Nick" {
		t.Fatalf("expected direct invite room to carry requester nickname, got %#v", room)
	}
	if room.CreatorAvatarURL != "mxc://example.com/requester" {
		t.Fatalf("expected direct invite room to carry requester avatar, got %#v", room)
	}
	var directProfile map[string]any
	for _, state := range room.InitialState {
		if state.Type == DirexioRoomProfileEventType && state.Content["room_type"] == DirexioRoomTypeDirect {
			directProfile = state.Content
		}
		if strings.HasPrefix(state.Type, "p2p.") {
			t.Fatalf("new direct rooms must not write legacy P2P product state, got %#v", room.InitialState)
		}
	}
	if directProfile == nil {
		t.Fatalf("expected native direct profile in direct invite room, got %#v", room.InitialState)
	}
	if directProfile["requester_mxid"] != "@owner:example.com" || directProfile["target_mxid"] != "@alice:remote.example" || directProfile["display_name"] != "Requester Nick" || directProfile["avatar_url"] != "mxc://example.com/requester" || directProfile["domain"] != "example.com" {
		t.Fatalf("expected requester profile in native direct profile state, got %#v", directProfile)
	}
	if len(room.InviteMXIDs) != 1 || room.InviteMXIDs[0] != "@alice:remote.example" {
		t.Fatalf("expected contact request to invite target user, got %#v", room.InviteMXIDs)
	}
}

func TestContactRequestRejectsSelfContact(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	if _, apiErr := service.Handle(context.Background(), "contacts.request", map[string]any{
		"mxid":         "@owner:example.com",
		"display_name": "Me",
	}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected self contact request to be rejected, got %#v", apiErr)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("self contact request must not create a direct room, got %#v", transport.createRooms)
	}
}

func TestContactRequestIsIdempotentForExistingDirectContact(t *testing.T) {
	transport := &recordingTransport{roomID: "!dm:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	first := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice",
	})
	second := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice Duplicate",
	})

	if second.RoomID != first.RoomID || second.Status != "pending_outbound" {
		t.Fatalf("expected duplicate contact request to reuse pending outbound room, got first=%#v second=%#v", first, second)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("duplicate contact request must not create another direct room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 1 {
		t.Fatalf("duplicate pending contact request must resend direct invite, got %#v", transport.inviteRequests)
	}
	if invite := transport.inviteRequests[0]; invite.RoomID != first.RoomID || invite.InviterMXID != "@owner:example.com" || invite.InviteeMXID != "@alice:remote.example" || !invite.IsDirect {
		t.Fatalf("unexpected repeated direct invite request: %#v", invite)
	}

	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":      first.RoomID,
		"peer_mxid":    first.PeerMXID,
		"display_name": first.DisplayName,
		"domain":       first.Domain,
	})
	afterAccept := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice Again",
	})

	if afterAccept.RoomID != first.RoomID || afterAccept.Status != accepted.Status {
		t.Fatalf("expected duplicate request after accept to return accepted contact, got %#v", afterAccept)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("duplicate accepted contact request must not create another direct room, got %#v", transport.createRooms)
	}
}

func TestPendingOutboundContactRequestKeepsOldRoomWhenSenderLeft(t *testing.T) {
	transport := &failingInviteTransport{
		err: productpolicy.Forbidden("sender is not joined to the direxio room"),
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_outbound",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice Again",
		"domain":       "remote.example",
	})

	if contact.Status != "pending_outbound" || contact.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected pending outbound retry to keep old room, got %#v", contact)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("pending outbound retry must not create a replacement direct room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 1 || transport.inviteRequests[0].RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected retry to attempt old-room invite once, got %#v", transport.inviteRequests)
	}
}

func TestAcceptedContactRequestCreatesPendingInviteWhenPeerNoLongerRetainsOldRoom(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "retained contact not found"})
	}))
	defer remote.Close()

	transport := &recordingTransport{roomID: "!fresh-dm:example.com"}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice Again",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "pending_outbound" || contact.RoomID != "!fresh-dm:example.com" {
		t.Fatalf("expected peer-deleted re-request to create a replacement direct room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation probe, got %#v", remoteActions)
	}
	if len(transport.joinRequests) != 0 {
		t.Fatalf("peer-deleted re-request must not join the old room before approval, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 1 ||
		transport.createRooms[0].RoomType != DirexioRoomTypeDirect ||
		len(transport.createRooms[0].InviteMXIDs) != 1 ||
		transport.createRooms[0].InviteMXIDs[0] != "@alice:remote.example" {
		t.Fatalf("peer-deleted re-request must create a replacement direct invite room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 0 {
		t.Fatalf("replacement room creation carries the invite, must not send old-room invite, got %#v", transport.inviteRequests)
	}
}

func TestAcceptedContactRequestCreatesReplacementWhenOldRoomInviteSenderLeft(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "retained contact not found"})
	}))
	defer remote.Close()

	transport := &failingInviteTransport{
		err: productpolicy.Forbidden("sender is not joined to the direxio room"),
	}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice Again",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "pending_outbound" || contact.RoomID != "!recorded:example.com" {
		t.Fatalf("expected left-sender re-request to create a replacement direct room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation probe, got %#v", remoteActions)
	}
	if len(transport.createRooms) != 1 ||
		transport.createRooms[0].RoomType != DirexioRoomTypeDirect ||
		len(transport.createRooms[0].InviteMXIDs) != 1 ||
		transport.createRooms[0].InviteMXIDs[0] != "@alice:remote.example" {
		t.Fatalf("left-sender re-request must create a replacement direct invite room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 0 {
		t.Fatalf("replacement room creation carries the invite, must not send old-room invite, got %#v", transport.inviteRequests)
	}
}

func TestContactRequestAcceptsPendingInboundDirectInvite(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice",
	})

	if contact.Status != "accepted" || contact.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected outbound request to accept pending inbound invite, got %#v", contact)
	}
	if len(transport.joinRequests) != 1 || transport.joinRequests[0].RoomIDOrAlias != "!old-dm:remote.example" {
		t.Fatalf("expected pending inbound invite to be joined, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("accepting pending inbound invite must not create a replacement direct room, got %#v", transport.createRooms)
	}
}

func TestContactRequestReactivatesStalePendingInboundDirectInvite(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "invited",
			"room_id": "!old-dm:remote.example",
		})
	}))
	defer remote.Close()

	transport := &failOnceJoinTransport{
		err: productpolicy.Forbidden("direct room join requires invite"),
	}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "accepted" || contact.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected stale inbound invite to reactivate old direct room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation call, got %#v", remoteActions)
	}
	if len(transport.joinRequests) != 2 {
		t.Fatalf("expected join retry after peer reactivation, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("stale inbound invite reactivation must not create a replacement direct room, got %#v", transport.createRooms)
	}
}

func TestContactRequestKeepsOldRoomWhenPendingInboundInviteIsGone(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "retained contact not found"})
	}))
	defer remote.Close()

	transport := &failOnceJoinTransport{
		recordingTransport: recordingTransport{roomID: "!fresh-dm:example.com"},
		err:                productpolicy.Forbidden("direct room join requires invite"),
		failures:           100,
	}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice Again",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "pending_outbound" || contact.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected stale pending inbound to wait for approval in the old room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation probe, got %#v", remoteActions)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("stale pending inbound retry must preserve the old direct room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 1 ||
		transport.inviteRequests[0].RoomID != "!old-dm:remote.example" ||
		transport.inviteRequests[0].InviteeMXID != "@alice:remote.example" {
		t.Fatalf("expected pending invite in old direct room, got %#v", transport.inviteRequests)
	}
}

func TestContactRequestKeepsOldRoomWhenPeerRecordsPendingFromStaleInboundInvite(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "pending_inbound",
			"room_id": "!old-dm:remote.example",
		})
	}))
	defer remote.Close()

	transport := &failOnceJoinTransport{
		recordingTransport: recordingTransport{roomID: "!fresh-dm:example.com"},
		err:                productpolicy.Forbidden("direct room join requires invite"),
		failures:           100,
	}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice Again",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "pending_outbound" || contact.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected stale inbound request to become pending outbound in old room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation call, got %#v", remoteActions)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("stale inbound retry must preserve the old direct room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 0 {
		t.Fatalf("peer-recorded pending request must not invite from a left sender, got %#v", transport.inviteRequests)
	}
}

func TestContactAcceptJoinsDirectRoomThroughTransport(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Owner B",
		"avatar_url":   "mxc://example.com/owner-b",
	})
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":      "!dm:remote.example",
		"peer_mxid":    "@alice:remote.example",
		"display_name": "Wrong Param",
		"domain":       "remote.example",
	})

	if accepted.Status != "accepted" {
		t.Fatalf("expected accepted contact, got %#v", accepted)
	}
	if accepted.DisplayName != "Alice" {
		t.Fatalf("expected accept to preserve stored requester nickname, got %#v", accepted)
	}
	if len(transport.joins) != 1 || transport.joins[0] != "@owner:example.com in !dm:remote.example" {
		t.Fatalf("expected accept to join direct room through transport, got %#v", transport.joins)
	}
	if transport.joinRequests[0].DisplayName != "Owner B" || transport.joinRequests[0].AvatarURL != "mxc://example.com/owner-b" {
		t.Fatalf("expected accept join to carry accepting owner profile, got %#v", transport.joinRequests[0])
	}
	if !transport.joinRequests[0].DirectContactReactivation {
		t.Fatalf("expected accept join to be marked as direct contact reactivation, got %#v", transport.joinRequests[0])
	}
}

func TestContactAcceptUsesDirectReactivationJoinWhenInviteIsGone(t *testing.T) {
	transport := &directReactivationJoinTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":      "!old-dm:remote.example",
		"peer_mxid":    "@alice:remote.example",
		"server_names": []string{"remote.example"},
	})

	if accepted.Status != "accepted" || accepted.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected accept to restore pending inbound contact in old room, got %#v", accepted)
	}
	if len(transport.joinRequests) != 1 || !transport.joinRequests[0].DirectContactReactivation {
		t.Fatalf("expected accept to use direct reactivation join, got %#v", transport.joinRequests)
	}
	if transport.joinRequests[0].ServerNames[0] != "remote.example" {
		t.Fatalf("expected accept to preserve remote server names, got %#v", transport.joinRequests[0])
	}
}

func TestContactAcceptCreatesReplacementDirectRoomWhenOldRoomCannotBeRejoined(t *testing.T) {
	transport := &failOnceJoinTransport{
		recordingTransport: recordingTransport{roomID: "!replacement-dm:example.com"},
		err:                productpolicy.Forbidden("direct room join requires invite"),
		failures:           100,
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Owner B",
		"avatar_url":   "mxc://example.com/owner-b",
	})
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":   "!old-dm:remote.example",
		"peer_mxid": "@alice:remote.example",
	})

	if accepted.Status != "accepted" || accepted.RoomID != "!replacement-dm:example.com" {
		t.Fatalf("expected accept to create replacement direct room when old room cannot be rejoined, got %#v", accepted)
	}
	if len(transport.joinRequests) != 1 || !transport.joinRequests[0].DirectContactReactivation {
		t.Fatalf("expected accept to try old-room direct reactivation first, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("expected one replacement direct room, got %#v", transport.createRooms)
	}
	room := transport.createRooms[0]
	if room.CreatorMXID != "@owner:example.com" || len(room.InviteMXIDs) != 1 || room.InviteMXIDs[0] != "@alice:remote.example" || !room.IsDirect || room.RoomType != DirexioRoomTypeDirect {
		t.Fatalf("unexpected replacement direct room request: %#v", room)
	}
}

func TestContactAcceptAlreadyAcceptedDoesNotJoinDirectRoomThroughTransport(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":      "!dm:remote.example",
		"peer_mxid":    "@alice:remote.example",
		"display_name": "Wrong Param",
		"domain":       "remote.example",
	})

	if accepted.Status != "accepted" || accepted.RoomID != "!dm:remote.example" {
		t.Fatalf("expected existing accepted contact, got %#v", accepted)
	}
	if len(transport.joins) != 0 {
		t.Fatalf("accepted contact accept must not join direct room again, got %#v", transport.joins)
	}
}

func TestContactDeleteLeavesDirectRoomThroughTransport(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "contacts.delete", map[string]any{
		"room_id": "!dm:remote.example",
	})

	if result["status"] != "ok" {
		t.Fatalf("expected delete status ok, got %#v", result)
	}
	if len(transport.leaves) != 1 || transport.leaves[0] != "@owner:example.com from !dm:remote.example" {
		t.Fatalf("expected contact delete to leave direct room through transport, got %#v", transport.leaves)
	}
}

func TestContactDeleteMarksDeletedWhenMatrixMembershipAlreadyLeft(t *testing.T) {
	transport := &failingLeaveTransport{err: errors.New(`user "@owner:example.com" is not joined to the room (membership is "leave"`)}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "contacts.delete", map[string]any{
		"room_id": "!dm:remote.example",
	})

	if result["status"] != "ok" {
		t.Fatalf("expected delete ok when Matrix membership is already leave, got %#v", result)
	}
	contact, ok, err := service.lookupContactByRoom(context.Background(), "!dm:remote.example")
	if err != nil || !ok || contact.Status != "deleted" {
		t.Fatalf("expected contact to be marked deleted, ok=%v contact=%#v err=%v", ok, contact, err)
	}
}

func TestContactRequestRestoresDeletedDirectRoomThroughTransport(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "deleted",
	}); err != nil {
		t.Fatal(err)
	}

	restored := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice Updated",
		"domain":       "remote.example",
	})

	if restored.Status != "accepted" || restored.RoomID != "!dm:remote.example" {
		t.Fatalf("expected deleted contact request to restore original room, got %#v", restored)
	}
	if restored.DisplayName != "Alice Updated" {
		t.Fatalf("expected re-request to refresh contact display name, got %#v", restored)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("deleted contact request must not create a new direct room, got %#v", transport.createRooms)
	}
	if len(transport.joins) != 1 || transport.joins[0] != "@owner:example.com in !dm:remote.example" {
		t.Fatalf("expected deleted contact request to rejoin original direct room, got %#v", transport.joins)
	}
}

func TestDeletedContactRequestReactivatesOldDirectRoomThroughPeerNode(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		if trimString(req.Params["room_id"]) != "!old-dm:remote.example" ||
			trimString(req.Params["requester_mxid"]) != "@owner:example.com" {
			t.Fatalf("unexpected reactivation params %#v", req.Params)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "invited",
			"room_id": "!old-dm:remote.example",
		})
	}))
	defer remote.Close()

	transport := &recordingTransport{roomID: "!new-dm:example.com"}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "deleted",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice New",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "accepted" || contact.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected re-add to restore the old direct room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation call, got %#v", remoteActions)
	}
	if len(transport.joinRequests) != 1 ||
		transport.joinRequests[0].RoomIDOrAlias != "!old-dm:remote.example" {
		t.Fatalf("expected rejoin of original room after peer reactivation, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("re-adding a retained peer must not create a replacement direct room, got %#v", transport.createRooms)
	}
}

func TestDeletedContactRequestWaitsForFederatedReactivationInvite(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "invited",
			"room_id": "!old-dm:remote.example",
		})
	}))
	defer remote.Close()

	transport := &failOnceJoinTransport{
		recordingTransport: recordingTransport{roomID: "!new-dm:example.com"},
		err:                productpolicy.Forbidden("direct room join requires invite"),
		failures:           3,
	}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "deleted",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "accepted" || contact.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected delayed invite reactivation to restore old direct room, got %#v", contact)
	}
	if len(transport.joinRequests) != 4 {
		t.Fatalf("expected join retries until invite is visible, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("reactivation retry must not create a replacement direct room, got %#v", transport.createRooms)
	}
}

func TestDeletedContactRequestCreatesFreshRequestWhenPeerNoLongerRetainsOldRoom(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "retained contact not found"})
	}))
	defer remote.Close()

	transport := &failOnceJoinTransport{
		recordingTransport: recordingTransport{roomID: "!fresh-dm:example.com"},
		err:                productpolicy.Forbidden("direct room join requires invite"),
		failures:           100,
	}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "deleted",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice Again",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "pending_outbound" || contact.RoomID != "!fresh-dm:example.com" {
		t.Fatalf("expected both-deleted re-add to create a replacement direct room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation probe, got %#v", remoteActions)
	}
	if len(transport.joinRequests) != 0 {
		t.Fatalf("expected retained-contact probe to avoid old-room join before pending invite, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 1 ||
		transport.createRooms[0].RoomType != DirexioRoomTypeDirect ||
		len(transport.createRooms[0].InviteMXIDs) != 1 ||
		transport.createRooms[0].InviteMXIDs[0] != "@alice:remote.example" {
		t.Fatalf("both-deleted re-add must create a replacement direct invite room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 0 {
		t.Fatalf("replacement room creation carries the invite, must not send old-room invite, got %#v", transport.inviteRequests)
	}
}

func TestDeletedContactRequestCreatesReplacementRoomWhenPeerRecordsInboundRequest(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "pending_inbound",
			"room_id": "!old-dm:remote.example",
		})
	}))
	defer remote.Close()

	transport := &failOnceJoinTransport{
		recordingTransport: recordingTransport{roomID: "!fresh-dm:example.com"},
		err:                productpolicy.Forbidden("direct room join requires invite"),
		failures:           100,
	}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "deleted",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice Again",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "pending_outbound" || contact.RoomID != "!fresh-dm:example.com" {
		t.Fatalf("expected peer-recorded re-request to create a replacement direct room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation call, got %#v", remoteActions)
	}
	if len(transport.joinRequests) != 0 {
		t.Fatalf("peer-recorded pending request must not join old direct room, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 1 ||
		transport.createRooms[0].RoomType != DirexioRoomTypeDirect ||
		len(transport.createRooms[0].InviteMXIDs) != 1 ||
		transport.createRooms[0].InviteMXIDs[0] != "@alice:remote.example" {
		t.Fatalf("peer-recorded pending request must create a replacement direct invite room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 0 {
		t.Fatalf("replacement room creation carries the invite, must not send old-room invite, got %#v", transport.inviteRequests)
	}
}

func TestContactReactivateInvitesOnlyRetainedAcceptedPeer(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "remote.example"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:example.com",
		PeerMXID:    "@owner:example.com",
		DisplayName: "Owner",
		Domain:      "example.com",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "contacts.reactivate", map[string]any{
		"room_id":        "!old-dm:example.com",
		"requester_mxid": "@owner:example.com",
	})

	if result["status"] != "invited" || result["room_id"] != "!old-dm:example.com" {
		t.Fatalf("expected retained contact reactivation invite, got %#v", result)
	}
	if len(transport.invites) != 1 ||
		transport.invites[0] != "@owner:remote.example -> @owner:example.com in !old-dm:example.com" {
		t.Fatalf("expected peer node to invite requester back to old direct room, got %#v", transport.invites)
	}

	if _, apiErr := service.Handle(context.Background(), "contacts.reactivate", map[string]any{
		"room_id":        "!other:example.com",
		"requester_mxid": "@owner:example.com",
	}); apiErr == nil || apiErr.Status != http.StatusNotFound {
		t.Fatalf("expected mismatched room reactivation to be rejected, got %#v", apiErr)
	}
}

func TestContactReactivateRecordsPendingInboundForDeletedPeerRequest(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "remote.example"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:example.com",
		PeerMXID:    "@owner:example.com",
		DisplayName: "Owner",
		Domain:      "example.com",
		Status:      "deleted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "contacts.reactivate", map[string]any{
		"room_id":        "!old-dm:example.com",
		"requester_mxid": "@owner:example.com",
	})

	if result["status"] != "pending_inbound" || result["room_id"] != "!old-dm:example.com" {
		t.Fatalf("expected deleted retained room to become pending inbound, got %#v", result)
	}
	contact, ok, err := service.lookupContactByPeer(context.Background(), "@owner:example.com")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || contact.Status != "pending_inbound" || contact.RoomID != "!old-dm:example.com" {
		t.Fatalf("expected pending inbound contact in old room, got ok=%v contact=%#v", ok, contact)
	}
	if len(transport.inviteRequests) != 0 {
		t.Fatalf("deleted peer request must not invite from a left sender, got %#v", transport.inviteRequests)
	}
}

func TestContactReactivateDoesNotTrustCallerProfileFields(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "remote.example"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:   "!old-dm:example.com",
		PeerMXID: "@owner:example.com",
		Status:   "deleted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "contacts.reactivate", map[string]any{
		"room_id":        "!old-dm:example.com",
		"requester_mxid": "@owner:example.com",
		"display_name":   "Spoofed Owner",
		"avatar_url":     "mxc://evil/avatar",
		"domain":         "evil.example",
	})

	if result["status"] != "pending_inbound" {
		t.Fatalf("expected pending inbound result, got %#v", result)
	}
	contact, ok, err := service.lookupContactByPeer(context.Background(), "@owner:example.com")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("expected retained contact")
	}
	if contact.DisplayName != "owner" || contact.AvatarURL != "" || contact.Domain != "example.com" {
		t.Fatalf("contacts.reactivate must not trust caller-supplied profile fields, got %#v", contact)
	}
}

func TestContactReactivateRejectsSelfInvite(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "remote.example"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!self-dm:remote.example",
		PeerMXID:    "@owner:remote.example",
		DisplayName: "Self",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	if _, apiErr := service.Handle(context.Background(), "contacts.reactivate", map[string]any{
		"room_id":        "!self-dm:remote.example",
		"requester_mxid": "@owner:remote.example",
	}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected self reactivation to be rejected, got %#v", apiErr)
	}
	if len(transport.invites) != 0 {
		t.Fatalf("self reactivation must not create an invite, got %#v", transport.invites)
	}
}

func TestAcceptedContactRequestMutationsDoNotBypassDeleteLeave(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	rejected := mustHandle[contactRecord](t, service, "contacts.requests.reject", map[string]any{
		"room_id": "!dm:remote.example",
	})
	if rejected.Status != "accepted" {
		t.Fatalf("request reject must not downgrade accepted contact, got %#v", rejected)
	}
	mustHandle[map[string]any](t, service, "contacts.requests.delete", map[string]any{
		"room_id": "!dm:remote.example",
	})
	contact, ok, err := service.lookupContactByRoom(context.Background(), "!dm:remote.example")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || contact.Status != "accepted" {
		t.Fatalf("request delete must not delete accepted contact, got %#v ok=%v", contact, ok)
	}

	result := mustHandle[map[string]any](t, service, "contacts.delete", map[string]any{
		"room_id": "!dm:remote.example",
	})

	if result["status"] != "ok" {
		t.Fatalf("expected delete status ok, got %#v", result)
	}
	if len(transport.leaves) != 1 || transport.leaves[0] != "@owner:example.com from !dm:remote.example" {
		t.Fatalf("expected contact delete to leave direct room after request mutations, got %#v", transport.leaves)
	}
}

func TestServiceCreatesChannelRoomStateThroughTransport(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "ch_post",
		"name":         "Posts",
		"description":  "Announcements",
		"avatar_url":   "mxc://example.com/ch",
		"visibility":   "private",
		"join_policy":  "approval",
		"channel_type": "post",
	})
	if ch.RoomID != transport.roomID {
		t.Fatalf("expected transport room_id, got %#v", ch)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("expected one transport room create, got %#v", transport.createRooms)
	}
	state := transport.createRooms[0].InitialState
	profileState, ok := initialStateOfType(state, DirexioRoomProfileEventType)
	if len(state) != 2 || !ok || profileState.Content["room_type"] != DirexioRoomTypeChannel || profileState.Content["channel_type"] != "post" {
		t.Fatalf("expected Direxio channel profile state, got %#v", state)
	}
	if got, ok := initialHistoryVisibility(transport.createRooms[0]); !ok || got != string(gomatrixserverlib.HistoryVisibilityShared) {
		t.Fatalf("expected shared post channel history visibility, got %q ok=%v in %#v", got, ok, state)
	}
	content := profileState.Content
	for key, want := range map[string]any{
		"channel_id":       "ch_post",
		"name":             "Posts",
		"description":      "Announcements",
		"avatar_url":       "mxc://example.com/ch",
		"visibility":       "private",
		"join_policy":      "approval",
		"channel_type":     "post",
		"comments_enabled": true,
		"dissolved":        false,
	} {
		if content[key] != want {
			t.Fatalf("expected channel state %s=%#v, got %#v", key, want, content)
		}
	}
}

func TestChannelUpdateAndDissolvePublishRoomStateThroughTransport(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "ch_lifecycle",
		"name":        "Before",
		"visibility":  "public",
		"join_policy": "open",
	})

	updated := mustHandle[channel](t, service, "channels.update", map[string]any{
		"channel_id":  ch.ChannelID,
		"name":        "After",
		"visibility":  "private",
		"join_policy": "approval",
	})
	if updated.Name != "After" || updated.Visibility != "private" || updated.JoinPolicy != "approval" {
		t.Fatalf("expected updated channel response, got %#v", updated)
	}
	if len(transport.stateEvents) != 1 {
		t.Fatalf("expected channel update to publish one state event, got %#v", transport.stateEvents)
	}
	updateState := transport.stateEvents[0]
	if updateState.RoomID != ch.RoomID || updateState.Event.Type != DirexioRoomProfileEventType || updateState.Event.Content["room_type"] != DirexioRoomTypeChannel || updateState.Event.Content["name"] != "After" || updateState.Event.Content["join_policy"] != "approval" {
		t.Fatalf("expected updated channel metadata state, got %#v", updateState)
	}

	mustHandle[map[string]any](t, service, "channels.dissolve", map[string]any{"channel_id": ch.ChannelID})
	if len(transport.stateEvents) != 2 {
		t.Fatalf("expected channel dissolve to publish second state event, got %#v", transport.stateEvents)
	}
	dissolveState := transport.stateEvents[1]
	if dissolveState.RoomID != ch.RoomID || dissolveState.Event.Content["dissolved"] != true {
		t.Fatalf("expected dissolved channel state, got %#v", dissolveState)
	}
}

func TestChannelUpdateIgnoresChannelTypeChanges(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "ch_to_post",
		"name":         "Before",
		"channel_type": "chat",
	})

	updated := mustHandle[channel](t, service, "channels.update", map[string]any{
		"channel_id":   ch.ChannelID,
		"name":         "Still Chat",
		"channel_type": "post",
	})
	if updated.ChannelType != "chat" || updated.Name != "Still Chat" {
		t.Fatalf("expected channel_type update to be ignored while mutable fields apply, got %#v", updated)
	}
	if len(transport.stateEvents) != 1 {
		t.Fatalf("expected ignored channel_type update to publish only metadata state, got %#v", transport.stateEvents)
	}
	if transport.stateEvents[0].Event.Content["channel_type"] != "chat" {
		t.Fatalf("expected published profile to preserve original channel_type, got %#v", transport.stateEvents[0])
	}
}

func TestGroupCreateUpdateAndDissolvePublishRoomStateThroughTransport(t *testing.T) {
	transport := &recordingTransport{roomID: "!group:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"name":          "Before",
		"topic":         "Original",
		"avatar_url":    "mxc://example.com/group",
		"invite_policy": "member",
	})
	if len(transport.createRooms) != 1 {
		t.Fatalf("expected group create to publish initial state, got %#v", transport.createRooms)
	}
	createState, ok := initialStateOfType(transport.createRooms[0].InitialState, DirexioRoomProfileEventType)
	if !ok || createState.Content["room_type"] != DirexioRoomTypeGroup || createState.Content["name"] != "Before" || createState.Content["invite_policy"] != "member" {
		t.Fatalf("expected group metadata initial state, got %#v", transport.createRooms[0].InitialState)
	}

	updated := mustHandle[groupRecord](t, service, "groups.update", map[string]any{
		"room_id":       group.RoomID,
		"name":          "After",
		"invite_policy": "owner",
	})
	if updated.Name != "After" || updated.InvitePolicy != "owner" {
		t.Fatalf("expected updated group response, got %#v", updated)
	}
	if len(transport.stateEvents) != 1 {
		t.Fatalf("expected group update to publish one state event, got %#v", transport.stateEvents)
	}
	updateState := transport.stateEvents[0]
	if updateState.RoomID != group.RoomID || updateState.Event.Type != DirexioRoomProfileEventType || updateState.Event.Content["room_type"] != DirexioRoomTypeGroup || updateState.Event.Content["name"] != "After" || updateState.Event.Content["invite_policy"] != "owner" {
		t.Fatalf("expected updated group metadata state, got %#v", updateState)
	}

	mustHandle[map[string]any](t, service, "groups.dissolve", map[string]any{"room_id": group.RoomID})
	if len(transport.stateEvents) != 2 {
		t.Fatalf("expected group dissolve to publish second state event, got %#v", transport.stateEvents)
	}
	dissolveState := transport.stateEvents[1]
	if dissolveState.RoomID != group.RoomID || dissolveState.Event.Content["dissolved"] != true {
		t.Fatalf("expected dissolved group state, got %#v", dissolveState)
	}
}

func TestChannelPostAndCommentUseChannelRoomAndMediaThroughTransport(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "ch_media",
		"name":         "Media Posts",
		"channel_type": "post",
	})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id":   ch.ChannelID,
		"message_type": "m.image",
		"body":         "photo.jpg",
		"media_json":   `{"url":"mxc://example.com/photo","info":{"mimetype":"image/jpeg"}}`,
	})
	comment := mustHandle[channelCommentRecord](t, service, "channels.comments.create", map[string]any{
		"channel_id":   ch.ChannelID,
		"post_id":      post.PostID,
		"message_type": "m.image",
		"body":         "reply.jpg",
		"media_json":   `{"url":"mxc://example.com/reply","info":{"mimetype":"image/jpeg"}}`,
	})

	if post.RoomID != ch.RoomID || comment.ChannelID != ch.ChannelID {
		t.Fatalf("expected post/comment to use channel identity, post=%#v comment=%#v channel=%#v", post, comment, ch)
	}
	if len(transport.messages) != 2 {
		t.Fatalf("expected post and comment to be sent through Matrix transport, got %#v", transport.messages)
	}
	postContent := transport.messages[0].Content
	if transport.messages[0].RoomID != ch.RoomID || postContent["msgtype"] != "m.image" || postContent["url"] != "mxc://example.com/photo" {
		t.Fatalf("expected image post Matrix content with channel room, got %#v", transport.messages[0])
	}
	commentContent := transport.messages[1].Content
	if transport.messages[1].RoomID != ch.RoomID || commentContent["msgtype"] != "m.image" || commentContent["url"] != "mxc://example.com/reply" {
		t.Fatalf("expected image comment Matrix content with channel room, got %#v", transport.messages[1])
	}
}

func TestChannelReactionDoesNotSaveProjectionWhenMatrixSendFails(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "ch_reaction",
		"room_id":      "!channel:example.com",
		"name":         "Reaction Channel",
		"channel_type": "post",
	})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": ch.ChannelID,
		"body":       "post",
	})
	service.transport = &failingSendTransport{err: productpolicy.Forbidden("channel comments are disabled")}

	_, apiErr := service.Handle(context.Background(), "channels.post_reaction.toggle", map[string]any{
		"channel_id": ch.ChannelID,
		"post_id":    post.PostID,
		"reaction":   "like",
	})

	if apiErr == nil || apiErr.Status != http.StatusForbidden {
		t.Fatalf("expected ProductPolicy failure to return 403, got %#v", apiErr)
	}
	service.mu.Lock()
	ownerMXID := service.ownerMXID
	service.mu.Unlock()
	if reaction, ok, err := service.getReaction(context.Background(), "post", post.PostID, "like", ownerMXID); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("reaction projection should not be saved when Matrix send fails, got %#v", reaction)
	}
}

type fakeChannelBackfillReader struct {
	events []matrixhistory.Event
	calls  int
}

func (r *fakeChannelBackfillReader) ListOrdinaryMessages(ctx context.Context, roomID string, fromTS, toTS int64, limit int) ([]mcpMessageSummary, error) {
	return nil, nil
}

func (r *fakeChannelBackfillReader) ListChannelContent(ctx context.Context, roomID string, limit int) ([]matrixhistory.Event, error) {
	r.calls++
	if limit > 0 && len(r.events) > limit {
		return r.events[:limit], nil
	}
	return r.events, nil
}

func TestChannelJoinBackfillsHistoricalPostsCommentsAndReactions(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":       "post_channel",
		"name":             "Post Channel",
		"channel_type":     "post",
		"comments_enabled": true,
	})
	service.SetMatrixMessageReader(&fakeChannelBackfillReader{events: []matrixhistory.Event{
		{
			Type:           "m.reaction",
			EventID:        "$reaction-post:example.com",
			Sender:         "@alice:example.com",
			OriginServerTS: 3000,
			Content: map[string]any{
				"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": "$post-one:example.com", "key": "like"},
			},
		},
		{
			Type:           "m.room.message",
			EventID:        "$comment-one:example.com",
			Sender:         "@owner:example.com",
			OriginServerTS: 2000,
			Content: map[string]any{
				"p2p_kind":   "channel_comment",
				"channel_id": ch.ChannelID,
				"post_id":    "post_one",
				"comment_id": "comment_one",
				"body":       "historical comment",
				"msgtype":    "m.text",
			},
		},
		{
			Type:           "m.reaction",
			EventID:        "$reaction-comment:example.com",
			Sender:         "@alice:example.com",
			OriginServerTS: 4000,
			Content: map[string]any{
				"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": "$comment-one:example.com", "key": "like"},
			},
		},
		{
			Type:           "m.room.message",
			EventID:        "$post-one:example.com",
			Sender:         "@owner:example.com",
			OriginServerTS: 1000,
			Content: map[string]any{
				"p2p_kind":   "channel_post",
				"channel_id": ch.ChannelID,
				"post_id":    "post_one",
				"body":       "historical post",
				"msgtype":    "m.text",
			},
		},
		{
			Type:           "m.room.message",
			EventID:        "$post-two:example.com",
			Sender:         "@owner:example.com",
			OriginServerTS: 1100,
			Content: map[string]any{
				"p2p_kind":   "channel_post",
				"channel_id": ch.ChannelID,
				"post_id":    "post_two",
				"body":       "unliked post",
				"msgtype":    "m.text",
			},
		},
		{
			Type:           "m.reaction",
			EventID:        "$reaction-post-two-on:example.com",
			Sender:         "@alice:example.com",
			OriginServerTS: 1200,
			Content: map[string]any{
				"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": "$post-two:example.com", "key": "like"},
				"active":       true,
			},
		},
		{
			Type:           "m.reaction",
			EventID:        "$reaction-post-two-off:example.com",
			Sender:         "@alice:example.com",
			OriginServerTS: 1300,
			Content: map[string]any{
				"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": "$post-two:example.com", "key": "like"},
				"active":       false,
			},
		},
	}})

	joined := mustHandle[map[string]any](t, service, "channels.join", map[string]any{
		"room_id":    ch.RoomID,
		"channel_id": ch.ChannelID,
		"user_id":    "@alice:example.com",
	})
	if joined["status"] != "ok" {
		t.Fatalf("expected channels.join ok, got %#v", joined)
	}

	posts := mustHandle[map[string]any](t, service, "channels.posts.list", map[string]any{
		"channel_id": ch.ChannelID,
	})["posts"].([]channelPostRecord)
	if len(posts) != 2 || posts[0].PostID != "post_one" || posts[0].Body != "historical post" || posts[0].CommentCount != 1 || posts[0].ReactionCount != 1 {
		t.Fatalf("expected backfilled post with comment/reaction counts, got %#v", posts)
	}
	if posts[1].PostID != "post_two" || posts[1].ReactionCount != 0 {
		t.Fatalf("expected active=false reaction event to clear backfilled reaction count, got %#v", posts)
	}
	comments := mustHandle[map[string]any](t, service, "channels.comments.list", map[string]any{
		"post_id": "post_one",
	})["comments"].([]channelCommentRecord)
	if len(comments) != 1 || comments[0].CommentID != "comment_one" || comments[0].Body != "historical comment" || comments[0].ReactionCount != 1 {
		t.Fatalf("expected backfilled comment with reaction count, got %#v", comments)
	}
}

func TestChatChannelJoinDoesNotBackfillHistoricalContent(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "chat_channel",
		"name":         "Chat Channel",
		"channel_type": "chat",
	})
	reader := &fakeChannelBackfillReader{events: []matrixhistory.Event{
		{
			Type:           "m.room.message",
			EventID:        "$post-one:example.com",
			Sender:         "@owner:example.com",
			OriginServerTS: 1000,
			Content: map[string]any{
				"p2p_kind": "channel_post",
				"post_id":  "post_one",
				"body":     "should not sync",
				"msgtype":  "m.text",
			},
		},
	}}
	service.SetMatrixMessageReader(reader)

	joined := mustHandle[map[string]any](t, service, "channels.join", map[string]any{
		"room_id":    ch.RoomID,
		"channel_id": ch.ChannelID,
		"user_id":    "@alice:example.com",
	})
	if joined["status"] != "ok" {
		t.Fatalf("expected channels.join ok, got %#v", joined)
	}
	if reader.calls != 0 {
		t.Fatalf("chat channel join should not backfill historical channel content, called reader %d times", reader.calls)
	}
	posts := mustHandle[map[string]any](t, service, "channels.posts.list", map[string]any{
		"channel_id": ch.ChannelID,
	})["posts"].([]channelPostRecord)
	if len(posts) != 0 {
		t.Fatalf("chat channel join should not project historical post content, got %#v", posts)
	}
}

func TestRoomSendMapsProductPolicyTransportErrorToForbidden(t *testing.T) {
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, &failingSendTransport{
		err: productpolicy.Forbidden("sender is muted in the direxio room"),
	})
	bootstrapService(t, service)

	_, apiErr := service.Handle(context.Background(), "rooms.send", map[string]any{
		"room_id": "!room:example.com",
		"content": "blocked",
	})

	if apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected removed P2P rooms.send to be unknown before transport, got %#v", apiErr)
	}
}

func TestRoomSendChannelCommentIncludesP2PKindForTransport(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	if _, apiErr := service.Handle(context.Background(), "rooms.send", map[string]any{
		"room_id":              "!channel:example.com",
		"content":              "comment through compatibility facade",
		"message_type":         "channel_comment",
		"channel_id":           "ch",
		"post_id":              "post",
		"comment_id":           "comment",
		"reply_to_comment_id":  "parent",
		"reply_to_author_mxid": "@parent:example.com",
		"mentions":             []any{"@alice:example.com"},
		"mentions_json":        `["@alice:example.com"]`,
		"media_json":           `{"url":"mxc://example.com/comment"}`,
	}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected removed rooms.send to return unknown action, got %#v", apiErr)
	}
	if len(transport.messages) != 0 {
		t.Fatalf("removed rooms.send must not write Matrix message, got %#v", transport.messages)
	}
}

func TestRoomSendMediaChannelCommentKeepsProductKindAndMatrixMsgtype(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	if _, apiErr := service.Handle(context.Background(), "rooms.send_media", map[string]any{
		"room_id":      "!channel:example.com",
		"content":      "image comment",
		"message_type": "channel_comment",
		"msgtype":      "m.image",
		"channel_id":   "ch",
		"post_id":      "post",
		"comment_id":   "comment",
		"media_json":   `{"url":"mxc://example.com/image"}`,
	}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected removed rooms.send_media to return unknown action, got %#v", apiErr)
	}
	if len(transport.messages) != 0 {
		t.Fatalf("removed rooms.send_media must not write Matrix message, got %#v", transport.messages)
	}
}

func TestChannelPostRecallDoesNotDeleteProjectionWhenMatrixRedactionFails(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch_recall",
		"room_id":    "!channel:example.com",
		"name":       "Recall Channel",
	})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": ch.ChannelID,
		"body":       "post",
	})
	service.transport = &failingRedactTransport{err: productpolicy.Forbidden("sender cannot redact another sender in direxio room")}

	_, apiErr := service.Handle(context.Background(), "channels.posts.recall", map[string]any{
		"channel_id": ch.ChannelID,
		"post_id":    post.PostID,
	})

	if apiErr == nil || apiErr.Status != http.StatusForbidden {
		t.Fatalf("expected ProductPolicy redaction failure to return 403, got %#v", apiErr)
	}
	posts := mustHandle[map[string]any](t, service, "channels.posts.list", map[string]any{"channel_id": ch.ChannelID})["posts"].([]channelPostRecord)
	if len(posts) != 1 || posts[0].PostID != post.PostID {
		t.Fatalf("post projection should remain when Matrix redaction fails, got %#v", posts)
	}
}

func TestChannelCommentRecallDoesNotDeleteProjectionWhenMatrixRedactionFails(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch_comment_recall",
		"room_id":    "!channel:example.com",
		"name":       "Comment Recall Channel",
	})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": ch.ChannelID,
		"body":       "post",
	})
	comment := mustHandle[channelCommentRecord](t, service, "channels.comments.create", map[string]any{
		"channel_id": ch.ChannelID,
		"post_id":    post.PostID,
		"body":       "comment",
	})
	service.transport = &failingRedactTransport{err: productpolicy.Forbidden("sender cannot redact another sender in direxio room")}

	_, apiErr := service.Handle(context.Background(), "channels.comments.recall", map[string]any{
		"channel_id":  ch.ChannelID,
		"post_id":     post.PostID,
		"comment_id":  comment.CommentID,
		"target_type": "comment",
	})

	if apiErr == nil || apiErr.Status != http.StatusForbidden {
		t.Fatalf("expected ProductPolicy redaction failure to return 403, got %#v", apiErr)
	}
	comments := mustHandle[map[string]any](t, service, "channels.comments.list", map[string]any{"post_id": post.PostID})["comments"].([]channelCommentRecord)
	if len(comments) != 1 || comments[0].CommentID != comment.CommentID {
		t.Fatalf("comment projection should remain when Matrix redaction fails, got %#v", comments)
	}
}

func TestChannelPostRecallWithTransportDoesNotRequireLocalOwnerProjection(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch_stale_owner",
		"name":       "Stale Owner Projection",
	})
	service.mu.Lock()
	delete(service.members, ch.RoomID+"|"+service.ownerMXID)
	service.posts = append(service.posts, channelPostRecord{
		PostID:     "post_remote",
		ChannelID:  ch.ChannelID,
		RoomID:     ch.RoomID,
		EventID:    "$remote-post:example.com",
		AuthorMXID: "@remote:example.com",
		Body:       "remote post",
	})
	service.mu.Unlock()

	_, apiErr := service.Handle(context.Background(), "channels.posts.recall", map[string]any{
		"channel_id": ch.ChannelID,
		"post_id":    "post_remote",
	})

	if apiErr != nil {
		t.Fatalf("expected transport ProductPolicy to be authoritative for recall, got %#v", apiErr)
	}
	if len(transport.redactions) != 1 || !strings.Contains(transport.redactions[0], "$remote-post:example.com") {
		t.Fatalf("expected recall to send Matrix redaction, got %#v", transport.redactions)
	}
}

func TestServiceUsesTransportForMemberLifecycle(t *testing.T) {
	transport := &recordingTransport{roomID: "!group:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"name": "Team"})
	mustHandle[map[string]any](t, service, "groups.invite", map[string]any{"room_id": group.RoomID, "user_id": "@alice:example.com"})
	mustHandle[map[string]any](t, service, "groups.join", map[string]any{"room_id": group.RoomID, "user_id": "@alice:example.com"})
	mustHandle[map[string]any](t, service, "groups.member.remove", map[string]any{"room_id": group.RoomID, "user_id": "@alice:example.com"})
	if _, apiErr := service.Handle(context.Background(), "groups.leave", map[string]any{"room_id": group.RoomID}); apiErr == nil || apiErr.Status != 409 {
		t.Fatalf("expected owner group leave to return 409, got %#v", apiErr)
	}

	if len(transport.invites) != 1 || transport.invites[0] != "@owner:example.com -> @alice:example.com in !group:example.com" {
		t.Fatalf("expected invite through transport, got %#v", transport.invites)
	}
	if len(transport.inviteRequests) != 1 || len(transport.inviteRequests[0].InviteRoomState) != 1 {
		t.Fatalf("expected native group invite metadata through transport, got %#v", transport.inviteRequests)
	}
	var nativeProfile bool
	for _, inviteState := range transport.inviteRequests[0].InviteRoomState {
		if inviteState.Type == DirexioRoomProfileEventType && inviteState.Content["room_type"] == DirexioRoomTypeGroup && inviteState.Content["name"] == group.Name {
			nativeProfile = true
		}
		if strings.HasPrefix(inviteState.Type, "p2p.") {
			t.Fatalf("group invite must not carry legacy P2P product state, got %#v", transport.inviteRequests[0].InviteRoomState)
		}
	}
	if !nativeProfile {
		t.Fatalf("expected native group invite state, got %#v", transport.inviteRequests[0].InviteRoomState)
	}
	if len(transport.joins) != 1 || transport.joins[0] != "@alice:example.com in !group:example.com" {
		t.Fatalf("expected join through transport, got %#v", transport.joins)
	}
	if len(transport.kicks) != 1 || transport.kicks[0] != "@owner:example.com kicks @alice:example.com from !group:example.com" {
		t.Fatalf("expected kick through transport, got %#v", transport.kicks)
	}
	if len(transport.leaves) != 0 {
		t.Fatalf("expected owner leave rejection to avoid transport leave, got %#v", transport.leaves)
	}
}

func TestGroupJoinUsesOwnerProfileForMemberAndMatrixJoin(t *testing.T) {
	transport := &recordingTransport{roomID: "!group:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Alice Device",
		"avatar_url":   "mxc://example.com/alice",
	})
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"name": "Team"})

	joined := mustHandle[map[string]any](t, service, "groups.join", map[string]any{
		"room_id": group.RoomID,
	})

	member := joined["member"].(memberRecord)
	if member.UserID != "@owner:example.com" || member.DisplayName != "Alice Device" || member.AvatarURL != "mxc://example.com/alice" {
		t.Fatalf("expected local owner profile on joined member, got %#v", member)
	}
	if len(transport.joinRequests) != 1 {
		t.Fatalf("expected one join request, got %#v", transport.joinRequests)
	}
	req := transport.joinRequests[0]
	if req.DisplayName != "Alice Device" || req.AvatarURL != "mxc://example.com/alice" {
		t.Fatalf("expected Matrix join to carry owner profile, got %#v", req)
	}
}

func TestChannelPublicJoinRequestPublishesApprovalStateWithoutInvite(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":       "public",
		"name":             "Public Channel",
		"visibility":       "public",
		"join_policy":      "open",
		"channel_type":     "post",
		"comments_enabled": true,
	})
	result := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"room_id":  ch.RoomID,
		"user_id":  "@alice:remote.example",
		"reason":   "let me in",
		"nickname": "Alice",
	})
	if result["status"] != "approved" {
		t.Fatalf("expected remote open channel request without callback to approve, got %#v", result)
	}
	if len(transport.inviteRequests) != 0 || len(transport.invites) != 0 {
		t.Fatalf("public join request must not expose Matrix invite flow, got invites=%#v requests=%#v", transport.invites, transport.inviteRequests)
	}
	var approvedState bool
	for _, state := range transport.stateEvents {
		if state.Event.Type == DirexioJoinRequestEventType &&
			state.Event.StateKey == productpolicy.UserStateKey("@alice:remote.example") &&
			state.Event.Content["status"] == "approved" {
			approvedState = true
		}
	}
	if !approvedState {
		t.Fatalf("expected approved join request state, got %#v", transport.stateEvents)
	}
}

func TestJoinRefreshesCurrentRoomMembersFromTransport(t *testing.T) {
	transport := &recordingTransport{
		roomID:      "!channel:example.com",
		roomChannel: channel{ChannelID: "remote_ch", RoomID: "!channel:example.com", Name: "Remote Channel", ChannelType: "chat"},
		roomMembers: []memberRecord{
			{RoomID: "!channel:example.com", UserID: "@owner:example.com", DisplayName: "Owner", Membership: "join", Role: "owner"},
			{RoomID: "!channel:example.com", UserID: "@alice:remote.example", DisplayName: "Alice", AvatarURL: "mxc://remote/avatar", Membership: "join", Role: "member"},
		},
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{"channel_id": "ch", "name": "Channel"})

	mustHandle[map[string]any](t, service, "channels.join", map[string]any{"room_id": ch.RoomID, "user_id": "@alice:remote.example"})

	members := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": "remote_ch"})["members"].([]memberRecord)
	owner := findMember(members, "@owner:example.com")
	alice := findMember(members, "@alice:remote.example")
	if owner.Membership != "join" || owner.ChannelID != "remote_ch" {
		t.Fatalf("expected owner backfilled with channel id, got %#v", owner)
	}
	if alice.DisplayName != "Alice" || alice.AvatarURL != "mxc://remote/avatar" || alice.ChannelID != "remote_ch" {
		t.Fatalf("expected joined member profile backfilled, got %#v", alice)
	}
	channels := mustHandle[map[string]any](t, service, "channels.list", nil)["channels"].([]channel)
	if findChannel(channels, "remote_ch").RoomID != ch.RoomID {
		t.Fatalf("expected remote channel state backfilled, got %#v", channels)
	}
}

func TestGroupJoinDoesNotBackfillRoomAsChannel(t *testing.T) {
	transport := &recordingTransport{
		roomID:      "!group:example.com",
		roomChannel: channel{ChannelID: "ghost_channel", RoomID: "!group:example.com", Name: "Group with A, B", ChannelType: "chat"},
		roomMembers: []memberRecord{
			{RoomID: "!group:example.com", UserID: "@owner:example.com", DisplayName: "Owner", Membership: "join", Role: "owner"},
			{RoomID: "!group:example.com", UserID: "@alice:remote.example", DisplayName: "Alice", Membership: "join", Role: "member"},
		},
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Group with A, B",
	})

	mustHandle[map[string]any](t, service, "groups.join", map[string]any{
		"room_id": group.RoomID,
		"user_id": "@alice:remote.example",
	})

	channels, err := service.listChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(channels) != 0 {
		t.Fatalf("expected group join not to create channel records, got %#v", channels)
	}
	groups := mustHandle[map[string]any](t, service, "groups.list", nil)["groups"].([]groupRecord)
	if len(groups) != 1 || groups[0].RoomID != group.RoomID {
		t.Fatalf("expected group list to keep the joined group only, got %#v", groups)
	}
}

func TestGroupJoinDoesNotCopyStaleChannelIDToMembers(t *testing.T) {
	transport := &recordingTransport{
		roomID: "!group:example.com",
		roomMembers: []memberRecord{
			{RoomID: "!group:example.com", UserID: "@owner:example.com", DisplayName: "Owner", Membership: "join", Role: "owner"},
			{RoomID: "!group:example.com", UserID: "@alice:remote.example", DisplayName: "Alice", Membership: "join", Role: "member"},
		},
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Group with A, B",
	})
	service.mu.Lock()
	service.channels["ghost_channel"] = channel{
		ChannelID: "ghost_channel",
		RoomID:    group.RoomID,
		Name:      "Stale channel projection",
	}
	service.mu.Unlock()

	mustHandle[map[string]any](t, service, "groups.join", map[string]any{
		"room_id": group.RoomID,
		"user_id": "@alice:remote.example",
	})

	members := mustHandle[map[string]any](t, service, "groups.members", map[string]any{"room_id": group.RoomID})["members"].([]memberRecord)
	alice := findMember(members, "@alice:remote.example")
	if alice.UserID == "" {
		t.Fatalf("expected joined group member, got %#v", members)
	}
	if alice.ChannelID != "" {
		t.Fatalf("expected group member channel_id to stay empty, got %#v", alice)
	}
}

func TestJoinRefreshPreservesExistingSparseRoomStateFields(t *testing.T) {
	transport := &recordingTransport{
		roomID:      "!channel:example.com",
		roomChannel: channel{ChannelID: "remote_ch", RoomID: "!channel:example.com", Name: "Remote Channel"},
		roomMembers: []memberRecord{
			{RoomID: "!channel:example.com", UserID: "@alice:remote.example", Membership: "join", Role: "member"},
		},
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveChannel(context.Background(), channel{
		ChannelID:       "remote_ch",
		RoomID:          "!channel:example.com",
		Name:            "Known Remote Channel",
		AvatarURL:       "mxc://remote/channel",
		Visibility:      "public",
		JoinPolicy:      "approval",
		ChannelType:     "chat",
		CommentsEnabled: true,
		Muted:           true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:      "!channel:example.com",
		ChannelID:   "remote_ch",
		UserID:      "@alice:remote.example",
		DisplayName: "Alice",
		AvatarURL:   "mxc://remote/alice",
		Domain:      "remote.example",
		Membership:  "pending",
		Role:        "member",
		Muted:       true,
	}); err != nil {
		t.Fatal(err)
	}

	mustHandle[map[string]any](t, service, "channels.join", map[string]any{"room_id": "!channel:example.com", "user_id": "@alice:remote.example"})

	channels, err := service.listChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ch := findChannel(channels, "remote_ch")
	if ch.AvatarURL != "mxc://remote/channel" || !ch.CommentsEnabled || !ch.Muted {
		t.Fatalf("expected sparse room channel refresh to preserve local fields, got %#v", ch)
	}
	members, err := service.membersForProduct(context.Background(), "", "remote_ch")
	if err != nil {
		t.Fatal(err)
	}
	alice := findMember(members, "@alice:remote.example")
	if alice.DisplayName != "Alice" || alice.AvatarURL != "mxc://remote/alice" || !alice.Muted {
		t.Fatalf("expected sparse member refresh to preserve local fields, got %#v", alice)
	}
}

func TestPublicChannelGetBackfillsRoomStateFromTransport(t *testing.T) {
	transport := &recordingTransport{
		roomChannel: channel{
			ChannelID:   "remote_ch",
			RoomID:      "!remote:example.com",
			Name:        "Remote Public",
			Visibility:  "public",
			JoinPolicy:  "open",
			ChannelType: "chat",
		},
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	got := mustHandle[channel](t, service, "channels.public.get", map[string]any{
		"room_id": "!remote:example.com",
	})
	if got.ChannelID != "remote_ch" || got.RoomID != "!remote:example.com" {
		t.Fatalf("expected public channel fetched from transport, got %#v", got)
	}
	channels := mustHandle[map[string]any](t, service, "channels.public.search", map[string]any{"q": "remote"})["channels"].([]channel)
	if len(channels) != 1 || channels[0].ChannelID != "remote_ch" {
		t.Fatalf("expected fetched channel cached for public search, got %#v", channels)
	}
}

func TestRemotePublicChannelGetUnavailableOwnerNodeReturnsBadGateway(t *testing.T) {
	service := NewServiceWithTransport(Config{
		ServerName:                     "dendrite-b:8448",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, &recordingTransport{})
	bootstrapService(t, service)

	_, apiErr := service.Handle(context.Background(), "channels.public.get", map[string]any{
		"room_id":              "!remote:dendrite-a:8448",
		"remote_node_base_url": "http://127.0.0.1:9/_p2p",
	})
	if apiErr == nil || apiErr.Status != 502 {
		t.Fatalf("expected unavailable remote owner node to return 502, got %#v", apiErr)
	}
}

func TestRemotePublicChannelGetFetchesOwnerNodeByRoomID(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_p2p/query" {
			t.Fatalf("expected remote public query path, got %s", r.URL.Path)
		}
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode remote request: %v", err)
		}
		if req.Action != "channels.public.get" || trimString(req.Params["room_id"]) != "!remote:remote.example" {
			t.Fatalf("unexpected remote request %#v", req)
		}
		_ = json.NewEncoder(w).Encode(channel{
			ChannelID:   "remote_ch",
			RoomID:      "!remote:remote.example",
			Name:        "Remote Public",
			Visibility:  "public",
			JoinPolicy:  "approval",
			ChannelType: "chat",
		})
	}))
	defer remote.Close()

	service := NewService(Config{
		ServerName:                     "local.example",
		RemoteNodeAllowPrivateBaseURLs: true,
	})
	bootstrapService(t, service)

	got := mustHandle[channel](t, service, "channels.public.get", map[string]any{
		"room_id":              "!remote:remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})
	if got.ChannelID != "remote_ch" || got.JoinPolicy != "approval" {
		t.Fatalf("expected remote public channel, got %#v", got)
	}

	search := mustHandle[map[string]any](t, service, "channels.public.search", map[string]any{
		"q":                    "!remote:remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})
	channels := search["channels"].([]channel)
	if len(channels) != 1 || channels[0].ChannelID != "remote_ch" {
		t.Fatalf("expected Matrix room id search to use remote public get, got %#v", search)
	}
}

func TestRemotePublicChannelGetUsesClientProvidedOwnerNodeBaseURL(t *testing.T) {
	calls := 0
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/_p2p/query" {
			t.Fatalf("expected remote public query path, got %s", r.URL.Path)
		}
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode remote request: %v", err)
		}
		if req.Action != "channels.public.get" || trimString(req.Params["remote_node_base_url"]) != "" {
			t.Fatalf("unexpected remote request %#v", req)
		}
		_ = json.NewEncoder(w).Encode(channel{
			ChannelID:   "remote_ch",
			RoomID:      "!remote:remote.example",
			Name:        "Remote Public",
			Visibility:  "public",
			JoinPolicy:  "open",
			ChannelType: "chat",
		})
	}))
	defer remote.Close()

	service := NewService(Config{
		ServerName:                     "local.example",
		RemoteNodeAllowPrivateBaseURLs: true,
	})
	bootstrapService(t, service)

	got := mustHandle[channel](t, service, "channels.public.get", map[string]any{
		"room_id":              "!remote:remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})
	if got.ChannelID != "remote_ch" {
		t.Fatalf("expected remote public channel via client-provided owner node, got %#v", got)
	}
	if calls != 1 {
		t.Fatalf("expected one remote owner node call, got %d", calls)
	}
}

func TestUserPublicChannelsForwardsToOwnerNodeBaseURL(t *testing.T) {
	calls := 0
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/_p2p/query" {
			t.Fatalf("expected remote public query path, got %s", r.URL.Path)
		}
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode remote request: %v", err)
		}
		if req.Action != "users.public_channels" ||
			trimString(req.Params["user_id"]) != "@owner:remote.example" ||
			trimString(req.Params["remote_node_base_url"]) != "" {
			t.Fatalf("unexpected remote request %#v", req)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"user_id": "@owner:remote.example",
			"channels": []channel{{
				ChannelID:   "remote_owned",
				RoomID:      "!remote-owned:remote.example",
				Name:        "Remote Owned",
				Visibility:  "public",
				JoinPolicy:  "open",
				ChannelType: "chat",
			}},
		})
	}))
	defer remote.Close()

	service := NewService(Config{
		ServerName:                     "local.example",
		RemoteNodeAllowPrivateBaseURLs: true,
	})
	bootstrapService(t, service)

	result := mustHandle[map[string]any](t, service, "users.public_channels", map[string]any{
		"user_id":              "@owner:remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})
	channels := result["channels"].([]channel)
	if len(channels) != 1 || channels[0].ChannelID != "remote_owned" {
		t.Fatalf("expected remote owner public channels, got %#v", result)
	}
	if calls != 1 {
		t.Fatalf("expected one remote owner node call, got %d", calls)
	}
}

func TestUserPublicChannelsRemoteLookupRequiresValidUserID(t *testing.T) {
	service := NewService(Config{
		ServerName:                     "local.example",
		RemoteNodeAllowPrivateBaseURLs: true,
	})
	bootstrapService(t, service)

	if _, apiErr := service.Handle(context.Background(), "users.public_channels", map[string]any{
		"user_id":              "owner",
		"remote_node_base_url": "https://remote.example/_p2p",
	}); apiErr == nil || apiErr.Status != http.StatusBadRequest || apiErr.Error != "valid user_id is required" {
		t.Fatalf("expected invalid remote user id to return targeted 400, got %#v", apiErr)
	}
}

func TestRemotePublicChannelJoinRequestForwardsToOwnerNode(t *testing.T) {
	requests := 0
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode remote request: %v", err)
		}
		requests++
		switch req.Action {
		case "channels.public.get":
			_ = json.NewEncoder(w).Encode(channel{
				ChannelID:   "remote_ch",
				RoomID:      "!remote:remote.example",
				Name:        "Remote Public",
				Visibility:  "public",
				JoinPolicy:  "approval",
				ChannelType: "chat",
			})
		case "channels.public.join_request":
			if trimString(req.Params["user_id"]) != "@owner:local.example" {
				t.Fatalf("unexpected forwarded join params %#v", req.Params)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "pending",
				"member": memberRecord{
					RoomID:      "!remote:remote.example",
					ChannelID:   "remote_ch",
					UserID:      "@owner:local.example",
					Membership:  "pending",
					Role:        "member",
					DisplayName: "Local Owner",
				},
			})
		default:
			t.Fatalf("unexpected remote action %s", req.Action)
		}
	}))
	defer remote.Close()

	service := NewService(Config{
		ServerName:                     "local.example",
		RemoteNodeAllowPrivateBaseURLs: true,
	})
	bootstrapService(t, service)

	res := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"room_id":              "!remote:remote.example",
		"user_id":              "@owner:local.example",
		"display_name":         "Local Owner",
		"remote_node_base_url": remote.URL + "/_p2p",
	})
	if res["status"] != "pending" {
		t.Fatalf("expected pending remote join request, got %#v", res)
	}
	members := mustHandle[map[string]any](t, service, "channels.members", map[string]any{
		"room_id": "!remote:remote.example",
	})["members"].([]memberRecord)
	if len(members) != 1 || members[0].Membership != "pending" || members[0].ChannelID != "remote_ch" {
		t.Fatalf("expected local pending member cache, got %#v", members)
	}
	if requests < 2 {
		t.Fatalf("expected remote detail and join request calls, got %d", requests)
	}
}

func TestRemotePublicChannelApprovalCallsRequesterNodeFromStoredJoinRequest(t *testing.T) {
	requesterTransport := &recordingTransport{roomID: "!remote:c.example"}
	requesterService := NewServiceWithTransport(Config{
		ServerName:                     "b.example",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, requesterTransport)
	bootstrapService(t, requesterService)
	requesterServer := httptest.NewServer(newP2PTestRouter(requesterService))
	defer requesterServer.Close()
	requesterService.homeserver = requesterServer.URL

	ownerService := NewService(Config{
		ServerName:                     "c.example",
		RemoteNodeAllowPrivateBaseURLs: true,
	})
	bootstrapService(t, ownerService)
	ownerServer := httptest.NewServer(newP2PTestRouter(ownerService))
	defer ownerServer.Close()

	ch := mustHandle[channel](t, ownerService, "channels.create", map[string]any{
		"channel_id":       "remote_ch",
		"room_id":          "!remote:c.example",
		"name":             "Remote Public",
		"visibility":       "public",
		"join_policy":      "approval",
		"channel_type":     "chat",
		"comments_enabled": true,
	})

	pending := mustHandle[map[string]any](t, requesterService, "channels.public.join_request", map[string]any{
		"room_id":              ch.RoomID,
		"user_id":              "@owner:b.example",
		"display_name":         "Requester",
		"remote_node_base_url": ownerServer.URL + "/_p2p",
	})
	if pending["status"] != "pending" {
		t.Fatalf("expected forwarded public join request to stay pending, got %#v", pending)
	}
	storedOwnerMember, ok, err := ownerService.lookupMember(context.Background(), ch.RoomID, "@owner:b.example")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || storedOwnerMember.RequesterNodeBaseURL != requesterServer.URL+"/_p2p" {
		t.Fatalf("expected owner node to store requester callback URL, got ok=%v member=%#v", ok, storedOwnerMember)
	}

	approved := mustHandle[map[string]any](t, ownerService, "channels.join_request.approve", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_id":    "@owner:b.example",
	})
	if approved["status"] != "joined" {
		t.Fatalf("expected approval to call requester node and join, got %#v", approved)
	}
	if len(requesterTransport.joins) != 1 || requesterTransport.joins[0] != "@owner:b.example in !remote:c.example" {
		t.Fatalf("expected requester node Matrix join, got %#v", requesterTransport.joins)
	}
	if len(requesterTransport.joinRequests) != 1 || len(requesterTransport.joinRequests[0].ServerNames) != 1 || requesterTransport.joinRequests[0].ServerNames[0] != "c.example" {
		t.Fatalf("expected requester node Matrix join to carry owner room server name, got %#v", requesterTransport.joinRequests)
	}
	requesterMembers := mustHandle[map[string]any](t, requesterService, "channels.members", map[string]any{
		"room_id": ch.RoomID,
	})["members"].([]memberRecord)
	requesterMember := findMember(requesterMembers, "@owner:b.example")
	if requesterMember.Membership != "join" {
		t.Fatalf("expected requester member to become joined, got %#v", requesterMembers)
	}
}

func TestChannelPublicJoinResultApprovedJoinsRequesterNode(t *testing.T) {
	transport := &recordingTransport{roomID: "!remote:remote.example"}
	service := NewServiceWithTransport(Config{ServerName: "local.example"}, transport)
	bootstrapService(t, service)
	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Local Owner",
		"avatar_url":   "mxc://local.example/owner",
	})
	ch := channel{
		ChannelID:  "remote_ch",
		RoomID:     "!remote:remote.example",
		Name:       "Remote Public",
		Visibility: "public",
		JoinPolicy: "approval",
	}
	if err := service.saveChannel(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@owner:local.example",
		Domain:     "local.example",
		Membership: "pending",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "channels.public.join_result", map[string]any{
		"room_id":      ch.RoomID,
		"channel_id":   ch.ChannelID,
		"user_id":      "@owner:local.example",
		"status":       "approved",
		"server_names": []string{"remote.example"},
		"request_id":   "req-1",
	})
	if result["status"] != "joined" {
		t.Fatalf("expected approved join result to join requester node, got %#v", result)
	}
	if len(transport.joins) != 1 || transport.joins[0] != "@owner:local.example in !remote:remote.example" {
		t.Fatalf("expected requester node Matrix join, got %#v", transport.joins)
	}
	if len(transport.joinRequests) != 1 || len(transport.joinRequests[0].ServerNames) != 1 || transport.joinRequests[0].ServerNames[0] != "remote.example" {
		t.Fatalf("expected join to carry remote server_names, got %#v", transport.joinRequests)
	}
	if transport.joinRequests[0].DisplayName != "Local Owner" || transport.joinRequests[0].AvatarURL != "mxc://local.example/owner" {
		t.Fatalf("expected channel join result to carry local owner profile, got %#v", transport.joinRequests[0])
	}
	members := mustHandle[map[string]any](t, service, "channels.members", map[string]any{
		"channel_id": ch.ChannelID,
	})["members"].([]memberRecord)
	owner := findMember(members, "@owner:local.example")
	if owner.Membership != "join" {
		t.Fatalf("expected local projection to become joined, got %#v", members)
	}
	if owner.DisplayName != "Local Owner" || owner.AvatarURL != "mxc://local.example/owner" {
		t.Fatalf("expected local member to keep owner profile after join result, got %#v", owner)
	}
}

func TestChannelPublicJoinResultApprovedFallsBackToRoomServerName(t *testing.T) {
	transport := &recordingTransport{roomID: "!remote:remote.example"}
	service := NewServiceWithTransport(Config{ServerName: "local.example"}, transport)
	bootstrapService(t, service)
	ch := channel{
		ChannelID:  "remote_ch",
		RoomID:     "!remote:remote.example",
		Name:       "Remote Public",
		Visibility: "public",
		JoinPolicy: "approval",
	}
	if err := service.saveChannel(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@owner:local.example",
		Domain:     "local.example",
		Membership: "pending",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "channels.public.join_result", map[string]any{
		"room_id":    ch.RoomID,
		"channel_id": ch.ChannelID,
		"user_id":    "@owner:local.example",
		"status":     "approved",
	})
	if result["status"] != "joined" {
		t.Fatalf("expected approved join result to join requester node, got %#v", result)
	}
	if len(transport.joinRequests) != 1 || len(transport.joinRequests[0].ServerNames) != 1 || transport.joinRequests[0].ServerNames[0] != "remote.example" {
		t.Fatalf("expected join result to fall back to room server name, got %#v", transport.joinRequests)
	}
}

func TestChannelPublicJoinResultRejectedUpdatesRequesterNode(t *testing.T) {
	service := NewService(Config{ServerName: "local.example"})
	bootstrapService(t, service)
	ch := channel{
		ChannelID:  "remote_ch",
		RoomID:     "!remote:remote.example",
		Name:       "Remote Public",
		Visibility: "public",
		JoinPolicy: "approval",
	}
	if err := service.saveChannel(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@owner:local.example",
		Domain:     "local.example",
		Membership: "pending",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "channels.public.join_result", map[string]any{
		"room_id":    ch.RoomID,
		"channel_id": ch.ChannelID,
		"user_id":    "@owner:local.example",
		"status":     "rejected",
		"reason":     "not now",
	})
	if result["status"] != "rejected" {
		t.Fatalf("expected rejected join result, got %#v", result)
	}
	member := result["member"].(memberRecord)
	if member.Membership != "reject" {
		t.Fatalf("expected local projection to become rejected, got %#v", member)
	}
	service.mu.Lock()
	events := append([]p2pEvent(nil), service.events...)
	service.mu.Unlock()
	if len(events) != 1 || events[0].Type != "channel.join_request.changed" {
		t.Fatalf("expected P2P event for rejected join request, got %#v", events)
	}
}

func TestChannelJoinRequestApprovalJoinsLocalRequesterThroughTransport(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "ch",
		"name":        "Channel",
		"join_policy": "approval",
	})
	mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@alice:example.com",
	})
	mustHandle[map[string]any](t, service, "channels.join_request.approve", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@alice:example.com",
	})
	member := mustHandle[map[string]any](t, service, "channels.members", map[string]any{
		"channel_id": ch.ChannelID,
	})["members"].([]memberRecord)
	alice := findMember(member, "@alice:example.com")
	if alice.Membership != "join" {
		t.Fatalf("expected approved local join request to join through Matrix, got %#v", member)
	}

	if len(transport.invites) != 0 {
		t.Fatalf("expected approved join request not to invite through transport, got %#v", transport.invites)
	}
	if len(transport.joins) != 1 || transport.joins[0] != "@alice:example.com in !channel:example.com" {
		t.Fatalf("expected approved join request to join through transport, got %#v", transport.joins)
	}
	joinRequestStates := recordedStatesOfType(transport.stateEvents, DirexioJoinRequestEventType)
	if len(joinRequestStates) != 2 {
		t.Fatalf("expected pending and approved join request state events, got %#v", joinRequestStates)
	}
	if joinRequestStates[0].Event.StateKey != productpolicy.UserStateKey("@alice:example.com") || joinRequestStates[0].Event.Content["status"] != "pending" {
		t.Fatalf("expected pending join request state, got %#v", joinRequestStates[0])
	}
	if joinRequestStates[1].Event.StateKey != productpolicy.UserStateKey("@alice:example.com") || joinRequestStates[1].Event.Content["status"] != "approved" {
		t.Fatalf("expected approved join request state, got %#v", joinRequestStates[1])
	}
}

func TestChannelInviteGrantInvitesJoinedShareRoomMembers(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	createdChannel := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "private",
		"room_id":     "!private:example.com",
		"name":        "Private",
		"visibility":  "private",
		"join_policy": "invite",
	})
	shareRoomID := "!share:example.com"
	if err := service.saveGroup(context.Background(), groupRecord{
		RoomID: shareRoomID,
		Name:   "Share Room",
	}); err != nil {
		t.Fatal(err)
	}
	for _, member := range []memberRecord{
		{RoomID: shareRoomID, UserID: "@owner:example.com", Domain: "example.com", Membership: "join", Role: "owner"},
		{RoomID: shareRoomID, UserID: "@alice:remote.example", Domain: "remote.example", Membership: "join", Role: "member"},
		{RoomID: shareRoomID, UserID: "@bob:remote.example", Domain: "remote.example", Membership: "invite", Role: "member"},
	} {
		if err := service.saveMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
	}

	result := mustHandle[map[string]any](t, service, "channels.invite_grant.create", map[string]any{
		"room_id":       createdChannel.RoomID,
		"channel_id":    createdChannel.ChannelID,
		"share_room_id": shareRoomID,
	})

	if result["share_room_id"] != shareRoomID || result["room_id"] != createdChannel.RoomID {
		t.Fatalf("expected grant to echo channel and share room, got %#v", result)
	}
	if len(transport.invites) != 1 || transport.invites[0] != "@owner:example.com -> @alice:remote.example in !private:example.com" {
		t.Fatalf("expected grant to invite only joined non-owner share-room member, got %#v", transport.invites)
	}
}

func TestChannelInviteGrantUsesMatrixShareRoomMembersWhenProjectionMissing(t *testing.T) {
	shareRoomID := "!share:example.com"
	transport := &recordingTransport{
		roomMembers: []memberRecord{
			{RoomID: shareRoomID, UserID: "@owner:example.com", Domain: "example.com", Membership: "join", Role: "owner"},
			{RoomID: shareRoomID, UserID: "@alice:remote.example", Domain: "remote.example", Membership: "join", Role: "member"},
		},
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	createdChannel := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "private",
		"room_id":     "!private:example.com",
		"name":        "Private",
		"visibility":  "private",
		"join_policy": "invite",
	})
	if err := service.saveGroup(context.Background(), groupRecord{
		RoomID: shareRoomID,
		Name:   "Share Room",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     shareRoomID,
		UserID:     "@owner:example.com",
		Domain:     "example.com",
		Membership: "join",
		Role:       "owner",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "channels.invite_grant.create", map[string]any{
		"room_id":       createdChannel.RoomID,
		"channel_id":    createdChannel.ChannelID,
		"share_room_id": shareRoomID,
	})

	members := result["members"].([]memberRecord)
	if len(members) != 1 || members[0].UserID != "@alice:remote.example" || members[0].Membership != "invite" {
		t.Fatalf("expected Matrix share-room member to be invited, got %#v", result)
	}
	if len(transport.invites) != 1 || transport.invites[0] != "@owner:example.com -> @alice:remote.example in !private:example.com" {
		t.Fatalf("expected grant to invite Matrix share-room member, got %#v", transport.invites)
	}
}

func TestMemberMutePublishesMemberPolicyState(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch",
		"name":       "Channel",
	})
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@alice:example.com",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	mustHandle[map[string]any](t, service, "channels.member.mute", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_id":    "@alice:example.com",
	})

	memberPolicyStates := recordedStatesOfType(transport.stateEvents, DirexioMemberPolicyEventType)
	if len(memberPolicyStates) != 1 {
		t.Fatalf("expected member policy state event, got %#v", memberPolicyStates)
	}
	state := memberPolicyStates[0]
	if state.Event.StateKey != productpolicy.UserStateKey("@alice:example.com") || state.Event.Content["role"] != "member" || state.Event.Content["muted"] != true {
		t.Fatalf("expected muted member policy state, got %#v", state)
	}
}

func TestChannelMutePublishesMemberPolicyStateForAffectedMembers(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch",
		"name":       "Channel",
	})
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@alice:example.com",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@bob:example.com",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	mustHandle[map[string]any](t, service, "channels.mute", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
	})

	memberPolicyStates := recordedStatesOfType(transport.stateEvents, DirexioMemberPolicyEventType)
	if len(memberPolicyStates) != 2 {
		t.Fatalf("expected member policy state events for all non-owner members, got %#v", memberPolicyStates)
	}
	mutedByUser := map[string]RoomStateEvent{}
	for _, state := range memberPolicyStates {
		mutedByUser[state.Event.StateKey] = state.Event
	}
	for _, userID := range []string{"@alice:example.com", "@bob:example.com"} {
		state, ok := mutedByUser[productpolicy.UserStateKey(userID)]
		if !ok || state.Content["role"] != "member" || state.Content["muted"] != true {
			t.Fatalf("expected muted member policy state for %s as regular member, got %#v", userID, state)
		}
	}
}

func TestRoomSendAfterChannelUnmuteUsesMatrixPolicyInsteadOfStaleProjection(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch",
		"name":       "Channel",
	})

	mustHandle[map[string]any](t, service, "channels.mute", map[string]any{
		"channel_id": ch.ChannelID,
	})
	mustHandle[map[string]any](t, service, "channels.unmute", map[string]any{
		"channel_id": ch.ChannelID,
	})
	_, apiErr := service.Handle(context.Background(), "rooms.send", map[string]any{
		"room_id":      ch.RoomID,
		"content":      "after unmute",
		"message_type": "text",
	})

	if apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected removed rooms.send to return unknown action, got %#v", apiErr)
	}
}

func TestRoomSendWithTransportDoesNotUseChannelProjectionMuteAsControlSource(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch",
		"name":       "Channel",
	})

	mustHandle[map[string]any](t, service, "channels.mute", map[string]any{
		"channel_id": ch.ChannelID,
	})
	_, apiErr := service.Handle(context.Background(), "rooms.send", map[string]any{
		"room_id":      ch.RoomID,
		"content":      "owner can still send",
		"message_type": "text",
	})

	if apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected removed rooms.send to return unknown action, got %#v", apiErr)
	}
}

func TestGroupMutePublishesMemberPolicyStateForAffectedMembers(t *testing.T) {
	transport := &recordingTransport{roomID: "!group:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"name": "Group",
	})
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     group.RoomID,
		UserID:     "@alice:example.com",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     group.RoomID,
		UserID:     "@bob:example.com",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	mustHandle[map[string]any](t, service, "groups.mute", map[string]any{
		"room_id": group.RoomID,
	})

	memberPolicyStates := recordedStatesOfType(transport.stateEvents, DirexioMemberPolicyEventType)
	if len(memberPolicyStates) != 2 {
		t.Fatalf("expected member policy state events for all non-owner members, got %#v", memberPolicyStates)
	}
	mutedByUser := map[string]RoomStateEvent{}
	for _, state := range memberPolicyStates {
		mutedByUser[state.Event.StateKey] = state.Event
	}
	for _, userID := range []string{"@alice:example.com", "@bob:example.com"} {
		state, ok := mutedByUser[productpolicy.UserStateKey(userID)]
		if !ok || state.Content["role"] != "member" || state.Content["muted"] != true {
			t.Fatalf("expected muted member policy state for %s as regular member, got %#v", userID, state)
		}
	}
}

func TestProfileUpdatePublishesMemberProfileState(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{"channel_id": "ch", "name": "Channel"})

	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Owner New",
		"avatar_url":   "mxc://example.com/avatar",
	})

	if len(transport.profiles) != 1 || transport.profiles[0] != "@owner:example.com in "+ch.RoomID+" as Owner New mxc://example.com/avatar" {
		t.Fatalf("expected profile member state through transport, got %#v", transport.profiles)
	}
	members := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	owner := findMember(members, "@owner:example.com")
	if owner.DisplayName != "Owner New" || owner.AvatarURL != "mxc://example.com/avatar" {
		t.Fatalf("expected owner member profile refreshed, got %#v", owner)
	}
}

func TestProfileUpdateIgnoresSingleRoomMemberProfileFailure(t *testing.T) {
	transport := &recordingTransport{
		roomID:        "!first:example.com",
		profileErrors: map[string]error{"!first:example.com": errors.New("stale room")},
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	first := mustHandle[channel](t, service, "channels.create", map[string]any{"channel_id": "first", "name": "First"})
	transport.roomID = "!second:example.com"
	second := mustHandle[channel](t, service, "channels.create", map[string]any{"channel_id": "second", "name": "Second"})

	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Owner Best Effort",
		"avatar_url":   "mxc://example.com/best-effort",
	})

	if len(transport.profiles) != 2 {
		t.Fatalf("expected both room profile updates attempted, got %#v", transport.profiles)
	}
	members := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": first.ChannelID})["members"].([]memberRecord)
	if owner := findMember(members, "@owner:example.com"); owner.DisplayName != "Owner Best Effort" || owner.AvatarURL != "mxc://example.com/best-effort" {
		t.Fatalf("expected local member profile refreshed for failed transport room, got %#v", owner)
	}
	members = mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": second.ChannelID})["members"].([]memberRecord)
	if owner := findMember(members, "@owner:example.com"); owner.DisplayName != "Owner Best Effort" || owner.AvatarURL != "mxc://example.com/best-effort" {
		t.Fatalf("expected local member profile refreshed for successful transport room, got %#v", owner)
	}
}

func TestFillMissingRoomVersionPrefersLookupBeforeDefault(t *testing.T) {
	ctx := context.Background()
	queryRes := roomserverAPI.QueryLatestEventsAndStateResponse{RoomExists: true}
	fillMissingRoomVersion(
		ctx,
		"!room:example.com",
		&queryRes,
		gomatrixserverlib.RoomVersionV10,
		func(context.Context, string) (gomatrixserverlib.RoomVersion, error) {
			return gomatrixserverlib.RoomVersionV11, nil
		},
	)
	if queryRes.RoomVersion != gomatrixserverlib.RoomVersionV11 {
		t.Fatalf("expected lookup room version, got %q", queryRes.RoomVersion)
	}

	queryRes = roomserverAPI.QueryLatestEventsAndStateResponse{RoomExists: true}
	fillMissingRoomVersion(
		ctx,
		"!room:example.com",
		&queryRes,
		gomatrixserverlib.RoomVersionV10,
		func(context.Context, string) (gomatrixserverlib.RoomVersion, error) {
			return "", errors.New("missing room version")
		},
	)
	if queryRes.RoomVersion != gomatrixserverlib.RoomVersionV10 {
		t.Fatalf("expected default room version fallback, got %q", queryRes.RoomVersion)
	}
}

func TestServiceUsesTransportForRedactions(t *testing.T) {
	transport := &recordingTransport{eventID: "$event:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	if _, apiErr := service.Handle(context.Background(), "rooms.messages.recall", map[string]any{"room_id": "!room:example.com", "event_id": "$event:example.com", "reason": "mistake"}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected removed rooms.messages.recall to return unknown action, got %#v", apiErr)
	}

	if len(transport.redactions) != 0 {
		t.Fatalf("removed recall action must not redact through P2P transport, got %#v", transport.redactions)
	}
}

func TestLocalMessageDeleteDoesNotUseMatrixRedaction(t *testing.T) {
	transport := &recordingTransport{eventID: "$event:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	if _, apiErr := service.Handle(context.Background(), "rooms.messages.delete", map[string]any{"room_id": "!room:example.com", "event_id": "$event:example.com"}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected removed rooms.messages.delete to return unknown action, got %#v", apiErr)
	}

	if len(transport.redactions) != 0 {
		t.Fatalf("local message delete must not redact Matrix event, got %#v", transport.redactions)
	}
}

type recordingTransport struct {
	roomID           string
	eventID          string
	ts               int64
	createRooms      []CreateRoomRequest
	messages         []SendMessageRequest
	invites          []string
	joins            []string
	leaves           []string
	kicks            []string
	roomChannel      channel
	roomChannelError error
	roomMembers      []memberRecord
	profiles         []string
	profileErrors    map[string]error
	redactions       []string
	inviteRequests   []InviteUserRequest
	joinRequests     []JoinRoomRequest
	stateEvents      []SendStateEventRequest
}

type recordingPushRuleManager struct {
	ruleSets    *pushrules.AccountRuleSets
	putUserID   string
	putRuleSets *pushrules.AccountRuleSets
}

func (m *recordingPushRuleManager) QueryPushRules(ctx context.Context, userID string) (*pushrules.AccountRuleSets, error) {
	if m.ruleSets == nil {
		m.ruleSets = pushrules.DefaultAccountRuleSets(ownerLocalpart, "example.com")
	}
	return m.ruleSets, nil
}

func (m *recordingPushRuleManager) PerformPushRulesPut(ctx context.Context, userID string, ruleSets *pushrules.AccountRuleSets) error {
	m.putUserID = userID
	m.putRuleSets = ruleSets
	return nil
}

type failingSendTransport struct {
	recordingTransport
	err error
}

func (t *failingSendTransport) SendMessage(ctx context.Context, req SendMessageRequest) (SendMessageResult, error) {
	t.messages = append(t.messages, req)
	return SendMessageResult{}, t.err
}

type failingInviteTransport struct {
	recordingTransport
	err error
}

func (t *failingInviteTransport) InviteUser(ctx context.Context, req InviteUserRequest) error {
	t.invites = append(t.invites, req.InviterMXID+" -> "+req.InviteeMXID+" in "+req.RoomID)
	t.inviteRequests = append(t.inviteRequests, req)
	return t.err
}

type failingRedactTransport struct {
	recordingTransport
	err error
}

func (t *failingRedactTransport) RedactEvent(ctx context.Context, req RedactEventRequest) (RedactEventResult, error) {
	t.redactions = append(t.redactions, req.EventID)
	return RedactEventResult{}, t.err
}

type failingLeaveTransport struct {
	recordingTransport
	err error
}

func (t *failingLeaveTransport) LeaveRoom(ctx context.Context, req LeaveRoomRequest) error {
	t.leaves = append(t.leaves, req.UserMXID+" from "+req.RoomID)
	return t.err
}

type failOnceJoinTransport struct {
	recordingTransport
	err      error
	attempts int
	failures int
}

func (t *failOnceJoinTransport) JoinRoom(ctx context.Context, req JoinRoomRequest) (JoinRoomResult, error) {
	t.joins = append(t.joins, req.UserMXID+" in "+req.RoomIDOrAlias)
	t.joinRequests = append(t.joinRequests, req)
	t.attempts++
	failures := t.failures
	if failures <= 0 {
		failures = 1
	}
	if t.attempts <= failures {
		return JoinRoomResult{}, t.err
	}
	return JoinRoomResult{RoomID: req.RoomIDOrAlias}, nil
}

type directReactivationJoinTransport struct {
	recordingTransport
}

func (t *directReactivationJoinTransport) JoinRoom(ctx context.Context, req JoinRoomRequest) (JoinRoomResult, error) {
	t.joins = append(t.joins, req.UserMXID+" in "+req.RoomIDOrAlias)
	t.joinRequests = append(t.joinRequests, req)
	if !req.DirectContactReactivation {
		return JoinRoomResult{}, productpolicy.Forbidden("direct room join requires invite")
	}
	return JoinRoomResult{RoomID: req.RoomIDOrAlias}, nil
}

type ownerJoinRequiresInviteTransport struct {
	recordingTransport
	ownerInvited bool
}

func (t *ownerJoinRequiresInviteTransport) JoinRoom(ctx context.Context, req JoinRoomRequest) (JoinRoomResult, error) {
	t.joins = append(t.joins, req.UserMXID+" in "+req.RoomIDOrAlias)
	t.joinRequests = append(t.joinRequests, req)
	if req.UserMXID == "@owner:example.com" && !t.ownerInvited {
		return JoinRoomResult{}, errors.New("owner join requires invite")
	}
	return JoinRoomResult{RoomID: req.RoomIDOrAlias}, nil
}

func (t *ownerJoinRequiresInviteTransport) InviteUser(ctx context.Context, req InviteUserRequest) error {
	if req.InviterMXID == "@agent:example.com" && req.InviteeMXID == "@owner:example.com" {
		t.ownerInvited = true
	}
	return t.recordingTransport.InviteUser(ctx, req)
}

func (t *recordingTransport) CreateRoom(ctx context.Context, req CreateRoomRequest) (CreateRoomResult, error) {
	t.createRooms = append(t.createRooms, req)
	if t.roomID == "" {
		t.roomID = "!recorded:example.com"
	}
	return CreateRoomResult{RoomID: t.roomID}, nil
}

func (t *recordingTransport) SendMessage(ctx context.Context, req SendMessageRequest) (SendMessageResult, error) {
	t.messages = append(t.messages, req)
	if t.eventID == "" {
		t.eventID = "$recorded:example.com"
	}
	if t.ts == 0 {
		t.ts = 1770000000000
	}
	return SendMessageResult{EventID: t.eventID, OriginServerTS: t.ts}, nil
}

func (t *recordingTransport) SendStateEvent(ctx context.Context, req SendStateEventRequest) error {
	t.stateEvents = append(t.stateEvents, req)
	return nil
}

func (t *recordingTransport) InviteUser(ctx context.Context, req InviteUserRequest) error {
	t.invites = append(t.invites, req.InviterMXID+" -> "+req.InviteeMXID+" in "+req.RoomID)
	t.inviteRequests = append(t.inviteRequests, req)
	return nil
}

func (t *recordingTransport) JoinRoom(ctx context.Context, req JoinRoomRequest) (JoinRoomResult, error) {
	t.joins = append(t.joins, req.UserMXID+" in "+req.RoomIDOrAlias)
	t.joinRequests = append(t.joinRequests, req)
	return JoinRoomResult{RoomID: req.RoomIDOrAlias}, nil
}

func (t *recordingTransport) LeaveRoom(ctx context.Context, req LeaveRoomRequest) error {
	t.leaves = append(t.leaves, req.UserMXID+" from "+req.RoomID)
	return nil
}

func (t *recordingTransport) KickUser(ctx context.Context, req KickUserRequest) error {
	t.kicks = append(t.kicks, req.SenderMXID+" kicks "+req.TargetMXID+" from "+req.RoomID)
	return nil
}

func (t *recordingTransport) GetRoomChannel(ctx context.Context, roomID string) (channel, bool, error) {
	if t.roomChannelError != nil {
		return channel{}, false, t.roomChannelError
	}
	if t.roomChannel.ChannelID == "" {
		return channel{}, false, nil
	}
	ch := t.roomChannel
	if ch.RoomID == "" {
		ch.RoomID = roomID
	}
	if ch.RoomID != roomID {
		return channel{}, false, nil
	}
	return ch, true, nil
}

func (t *recordingTransport) ListRoomMembers(ctx context.Context, roomID string) ([]memberRecord, error) {
	members := make([]memberRecord, 0, len(t.roomMembers))
	for _, member := range t.roomMembers {
		if member.RoomID == "" {
			member.RoomID = roomID
		}
		if member.RoomID == roomID {
			members = append(members, member)
		}
	}
	return members, nil
}

func (t *recordingTransport) UpdateMemberProfile(ctx context.Context, req UpdateMemberProfileRequest) error {
	t.profiles = append(t.profiles, req.UserMXID+" in "+req.RoomID+" as "+req.DisplayName+" "+req.AvatarURL)
	if t.profileErrors != nil {
		return t.profileErrors[req.RoomID]
	}
	return nil
}

func (t *recordingTransport) RedactEvent(ctx context.Context, req RedactEventRequest) (RedactEventResult, error) {
	t.redactions = append(t.redactions, req.SenderMXID+" redacts "+req.EventID+" in "+req.RoomID)
	return RedactEventResult{EventID: "$redaction:example.com"}, nil
}

func recordedStatesOfType(states []SendStateEventRequest, eventType string) []SendStateEventRequest {
	filtered := make([]SendStateEventRequest, 0, len(states))
	for _, state := range states {
		if state.Event.Type == eventType {
			filtered = append(filtered, state)
		}
	}
	return filtered
}

func findChannel(channels []channel, channelID string) channel {
	for _, ch := range channels {
		if ch.ChannelID == channelID {
			return ch
		}
	}
	return channel{}
}
