package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
		if inviteState.Type == DirextalkRoomProfileEventType && inviteState.Content["room_type"] == DirextalkRoomTypeGroup && inviteState.Content["name"] == group.Name {
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

func TestGroupInviteReactivatesRemoteNodeWhenMatrixAlreadyJoined(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "rooms.reactivate" ||
			trimString(req.Params["room_type"]) != "group" ||
			trimString(req.Params["room_id"]) == "" ||
			trimString(req.Params["user_id"]) != "@alice:remote.example" {
			t.Fatalf("unexpected room reactivation request %#v", req)
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "invite", "room_id": trimString(req.Params["room_id"])})
	}))
	defer remote.Close()

	transport := &alreadyJoinedOnceInviteTransport{}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"name": "Team"})

	result := mustHandle[map[string]any](t, service, "groups.invite", map[string]any{
		"room_id":              group.RoomID,
		"user_id":              "@alice:remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if result["status"] != "ok" || len(remoteActions) != 1 {
		t.Fatalf("expected invite to reactivate remote node, result=%#v actions=%#v", result, remoteActions)
	}
	member, ok, err := service.lookupMember(context.Background(), group.RoomID, "@alice:remote.example")
	if err != nil || !ok || member.Membership != "invite" {
		t.Fatalf("expected retained remote member to be re-invited, ok=%v member=%#v err=%v", ok, member, err)
	}
	if len(transport.kicks) != 1 || len(transport.inviteRequests) != 2 {
		t.Fatalf("expected kick then replacement invite for already-joined member, kicks=%#v invites=%#v", transport.kicks, transport.inviteRequests)
	}
}

func TestRoomReactivateRecordsRetainedGroupInvite(t *testing.T) {
	transport := &recordingTransport{roomID: "!group:remote.example"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	result := mustHandle[map[string]any](t, service, "rooms.reactivate", map[string]any{
		"room_id":      "!group:remote.example",
		"room_type":    "group",
		"user_id":      "@owner:example.com",
		"name":         "Remote Team",
		"server_names": []any{"remote.example"},
	})

	if result["status"] != "invite" || result["room_id"] != "!group:remote.example" {
		t.Fatalf("expected retained group reactivation to record invite, got %#v", result)
	}
	group, ok, err := service.groupByRoom(context.Background(), "!group:remote.example")
	if err != nil || !ok || group.Name != "Remote Team" {
		t.Fatalf("expected group projection after reactivation, ok=%v group=%#v err=%v", ok, group, err)
	}
	member, ok, err := service.lookupMember(context.Background(), "!group:remote.example", "@owner:example.com")
	if err != nil || !ok || member.Membership != "invite" {
		t.Fatalf("expected invited member after reactivation, ok=%v member=%#v err=%v", ok, member, err)
	}
	if len(transport.joinRequests) != 0 {
		t.Fatalf("reactivation invite must not silently join retained group, got %#v", transport.joinRequests)
	}
}

func TestPrivateChannelInviteReactivatesRemoteNodeWhenMatrixAlreadyJoined(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "rooms.reactivate" ||
			trimString(req.Params["room_type"]) != "channel" ||
			trimString(req.Params["channel_id"]) != "private" ||
			trimString(req.Params["user_id"]) != "@alice:remote.example" {
			t.Fatalf("unexpected channel reactivation request %#v", req)
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "invite", "room_id": "!channel:example.com", "channel_id": "private"})
	}))
	defer remote.Close()

	transport := &alreadyJoinedOnceInviteTransport{recordingTransport: recordingTransport{roomID: "!channel:example.com"}}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "private",
		"name":        "Private",
		"visibility":  "private",
		"join_policy": "invite",
	})

	result := mustHandle[map[string]any](t, service, "channels.invite", map[string]any{
		"room_id":              ch.RoomID,
		"channel_id":           ch.ChannelID,
		"user_id":              "@alice:remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if result["status"] != "ok" || len(remoteActions) != 1 {
		t.Fatalf("expected private channel invite to reactivate remote node, result=%#v actions=%#v", result, remoteActions)
	}
	member, ok, err := service.lookupMember(context.Background(), ch.RoomID, "@alice:remote.example")
	if err != nil || !ok || member.Membership != "invite" || member.ChannelID != ch.ChannelID {
		t.Fatalf("expected retained remote channel member to be re-invited, ok=%v member=%#v err=%v", ok, member, err)
	}
	if len(transport.kicks) != 1 || len(transport.inviteRequests) != 2 {
		t.Fatalf("expected kick then replacement invite for already-joined channel member, kicks=%#v invites=%#v", transport.kicks, transport.inviteRequests)
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
