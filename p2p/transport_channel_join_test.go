package p2p

import (
	"context"
	"encoding/json"
	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	"net/http"
	"net/http/httptest"
	"testing"
)

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
		if state.Event.Type == DirextalkJoinRequestEventType &&
			state.Event.StateKey == productpolicy.UserStateKey("@alice:remote.example") &&
			state.Event.Content["status"] == "approved" {
			approvedState = true
		}
	}
	if !approvedState {
		t.Fatalf("expected approved join request state, got %#v", transport.stateEvents)
	}
}

func TestPublicChannelJoinRequestReinvitesRebuiltAlreadyJoinedRemoteMember(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Action != "channels.public.join_result" ||
			trimString(req.Params["user_id"]) != "@alice:remote.example" ||
			trimString(req.Params["status"]) != "approved" {
			t.Fatalf("unexpected public join callback %#v", req)
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "joined", "room_id": trimString(req.Params["room_id"])})
	}))
	defer remote.Close()

	transport := &alreadyJoinedOnceInviteTransport{recordingTransport: recordingTransport{roomID: "!channel:example.com"}}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "public",
		"name":        "Public Channel",
		"visibility":  "public",
		"join_policy": "open",
	})
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@alice:remote.example",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"room_id":                 ch.RoomID,
		"channel_id":              ch.ChannelID,
		"user_id":                 "@alice:remote.example",
		"requester_node_base_url": remote.URL + "/_p2p",
	})

	if result["status"] != "joined" {
		t.Fatalf("expected rebuilt public channel member to join after re-invite, got %#v", result)
	}
	if len(transport.kicks) != 1 || len(transport.inviteRequests) != 2 {
		t.Fatalf("expected stale public membership kick and fresh invite, kicks=%#v invites=%#v", transport.kicks, transport.inviteRequests)
	}
}

func TestPublicChannelJoinRequestRetriesRemoteJoinResultFailure(t *testing.T) {
	var attempts int
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Action != "channels.public.join_result" ||
			trimString(req.Params["user_id"]) != "@alice:remote.example" ||
			trimString(req.Params["status"]) != "approved" {
			t.Fatalf("unexpected public join callback %#v", req)
		}
		attempts++
		if attempts == 1 {
			writeJSON(w, http.StatusOK, map[string]any{"status": "join_failed", "error": "invite not received yet"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "joined", "room_id": trimString(req.Params["room_id"])})
	}))
	defer remote.Close()

	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "public",
		"name":        "Public Channel",
		"visibility":  "public",
		"join_policy": "open",
	})

	result := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"room_id":                 ch.RoomID,
		"channel_id":              ch.ChannelID,
		"user_id":                 "@alice:remote.example",
		"requester_node_base_url": remote.URL + "/_p2p",
	})

	if result["status"] != "joined" || attempts != 2 {
		t.Fatalf("expected public join callback retry to joined, attempts=%d result=%#v", attempts, result)
	}
	member, ok, err := service.lookupMember(context.Background(), ch.RoomID, "@alice:remote.example")
	if err != nil || !ok || member.Membership != "join" {
		t.Fatalf("expected owner projection to become joined after remote retry, ok=%v member=%#v err=%v", ok, member, err)
	}
}

func TestRemotePublicChannelJoinRequestFallsBackToLocalJoinAfterRemoteJoinFailed(t *testing.T) {
	remoteChannel := channel{
		ChannelID:       "remote_public",
		RoomID:          "!remote:remote.example",
		Name:            "Remote Public",
		Visibility:      "public",
		JoinPolicy:      "open",
		ChannelType:     "post",
		CommentsEnabled: true,
	}
	var joinRequests int
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		switch req.Action {
		case "channels.public.get":
			writeJSON(w, http.StatusOK, remoteChannel)
		case "channels.public.join_request":
			joinRequests++
			writeJSON(w, http.StatusOK, map[string]any{
				"status": "join_failed",
				"member": memberRecord{
					RoomID:     remoteChannel.RoomID,
					ChannelID:  remoteChannel.ChannelID,
					UserID:     "@owner:local.example",
					Membership: "join_failed",
					Role:       "member",
				},
				"channel": remoteChannel,
				"error":   "requester callback was not ready",
			})
		default:
			t.Fatalf("unexpected remote action %#v", req)
		}
	}))
	defer remote.Close()

	transport := &recordingTransport{roomID: remoteChannel.RoomID}
	service := NewServiceWithTransport(Config{
		ServerName:                     "local.example",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)

	result := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"room_id":              remoteChannel.RoomID,
		"channel_id":           remoteChannel.ChannelID,
		"remote_node_base_url": remote.URL + "/_p2p",
		"server_names":         []string{"remote.example"},
	})

	if result["status"] != "joined" || joinRequests != 1 {
		t.Fatalf("expected local requester fallback join, joinRequests=%d result=%#v", joinRequests, result)
	}
	if len(transport.joins) != 1 || transport.joins[0] != "@owner:local.example in !remote:remote.example" {
		t.Fatalf("expected requester node Matrix join, got %#v", transport.joins)
	}
	member, ok, err := service.lookupMember(context.Background(), remoteChannel.RoomID, "@owner:local.example")
	if err != nil || !ok || member.Membership != "join" {
		t.Fatalf("expected local member to become joined, ok=%v member=%#v err=%v", ok, member, err)
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
