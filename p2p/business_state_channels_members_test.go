package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPublicChannelSearchFindsPublicChannelsOnly(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	public := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "ch_public",
		"room_id":     "!public:example.com",
		"name":        "Public Garden",
		"description": "open discussion",
		"visibility":  "public",
	})
	mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch_private",
		"room_id":    "!private:example.com",
		"name":       "Private Garden",
		"visibility": "private",
	})

	search := mustHandle[map[string]any](t, service, "channels.public.search", map[string]any{"q": "garden"})
	results, ok := search["channels"].([]channel)
	if !ok || len(results) != 1 || results[0].ChannelID != public.ChannelID {
		t.Fatalf("expected only public matching channel, got %#v", search)
	}
}

func TestChannelJoinRequestPersistsPendingMemberAndResolves(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "approval",
		"room_id":     "!approval:example.com",
		"name":        "Approval",
		"visibility":  "public",
		"join_policy": "approval",
	})

	request := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id":   ch.ChannelID,
		"room_id":      ch.RoomID,
		"user_mxid":    "@alice:remote.example",
		"display_name": "Alice",
	})
	if request["status"] != "pending" {
		t.Fatalf("expected pending join request, got %#v", request)
	}
	members := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	alice := findMember(members, "@alice:remote.example")
	if alice.Membership != "pending" || alice.DisplayName != "Alice" || alice.ChannelID != ch.ChannelID {
		t.Fatalf("expected persisted pending member, got %#v in %#v", alice, members)
	}

	approved := mustHandle[map[string]any](t, service, "channels.join_request.approve", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@alice:remote.example",
	})
	approvedMember := approved["member"].(memberRecord)
	approvedChannel := approved["channel"].(channel)
	if approved["status"] != "approved" || approvedMember.Membership != "approved" ||
		approvedChannel.ChannelID != ch.ChannelID || approvedChannel.PendingJoinCount != 0 {
		t.Fatalf("expected remote approved request without callback to remain approved until requester node joins, got %#v", approved)
	}

	mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@bob:remote.example",
	})
	rejected := mustHandle[map[string]any](t, service, "channels.join_request.reject", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@bob:remote.example",
	})
	rejectedMember := rejected["member"].(memberRecord)
	if rejectedMember.Membership != "reject" {
		t.Fatalf("expected rejected request to become rejected member, got %#v", rejected)
	}
	members = mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	if findMember(members, "@bob:remote.example").UserID != "" {
		t.Fatalf("expected rejected request hidden from visible members, got %#v", members)
	}
}

func TestChannelJoinRequestApproveCanRetryJoinFailedRemoteRequest(t *testing.T) {
	callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"temporary requester node failure"}`, http.StatusBadGateway)
	}))
	defer callback.Close()

	service := NewService(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "approval-retry",
		"room_id":     "!approval-retry:example.com",
		"name":        "Approval Retry",
		"visibility":  "public",
		"join_policy": "approval",
	})

	mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id":              ch.ChannelID,
		"room_id":                 ch.RoomID,
		"user_mxid":               "@alice:remote.example",
		"requester_node_base_url": callback.URL + "/_p2p",
	})
	first := mustHandle[map[string]any](t, service, "channels.join_request.approve", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@alice:remote.example",
	})
	if first["status"] != "join_failed" {
		t.Fatalf("expected first approval to record join_failed, got %#v", first)
	}

	second := mustHandle[map[string]any](t, service, "channels.join_request.approve", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@alice:remote.example",
	})
	if second["status"] != "join_failed" {
		t.Fatalf("expected retry approval to run instead of 404, got %#v", second)
	}
	member := second["member"].(memberRecord)
	if member.Membership != "join_failed" || member.UserID != "@alice:remote.example" {
		t.Fatalf("expected retry to preserve join_failed member, got %#v", member)
	}
}

func TestChannelJoinAcceptsRoomScopedInviteGrant(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "private",
		"room_id":     "!private:example.com",
		"name":        "Private",
		"visibility":  "private",
		"join_policy": "invite",
	})
	shareRoomID := "!share:example.com"
	if err := service.saveGroup(context.Background(), groupRecord{RoomID: shareRoomID, Name: "Share Room"}); err != nil {
		t.Fatal(err)
	}
	for _, member := range []memberRecord{
		{RoomID: shareRoomID, UserID: "@owner:example.com", Domain: "example.com", Membership: "join", Role: "owner"},
		{RoomID: shareRoomID, UserID: "@alice:remote.example", Domain: "remote.example", Membership: "join", Role: "member"},
	} {
		if err := service.saveMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
	}
	grant := mustHandle[map[string]any](t, service, "channels.invite_grant.create", map[string]any{
		"room_id":       ch.RoomID,
		"channel_id":    ch.ChannelID,
		"share_room_id": shareRoomID,
	})

	joined := mustHandle[map[string]any](t, service, "channels.join", map[string]any{
		"grant_id":      grant["grant_id"],
		"share_room_id": shareRoomID,
		"user_id":       "@alice:remote.example",
	})
	if joined["room_id"] != ch.RoomID {
		t.Fatalf("expected grant join to return channel room id, got %#v", joined)
	}
	member := joined["member"].(memberRecord)
	if member.UserID != "@alice:remote.example" || member.Membership != "join" || member.ChannelID != ch.ChannelID {
		t.Fatalf("expected grant user to join channel, got %#v", member)
	}
	if _, apiErr := service.Handle(context.Background(), "channels.join", map[string]any{
		"grant_id":      grant["grant_id"],
		"share_room_id": shareRoomID,
		"user_id":       "@eve:remote.example",
	}); apiErr == nil || apiErr.Status != http.StatusForbidden {
		t.Fatalf("expected non share-room member to be rejected, got %#v", apiErr)
	}
}

func TestChannelJoinWithMatrixInviteDoesNotRequireLocalInviteGrant(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "private",
		"room_id":     "!private:example.com",
		"name":        "Private",
		"visibility":  "private",
		"join_policy": "invite",
	})
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:      ch.RoomID,
		ChannelID:   ch.ChannelID,
		UserID:      "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Membership:  "invite",
		Role:        "member",
	}); err != nil {
		t.Fatal(err)
	}

	joined := mustHandle[map[string]any](t, service, "channels.join", map[string]any{
		"room_id":       ch.RoomID,
		"grant_id":      "grant-from-owner-node",
		"share_room_id": "!share-on-owner-node:example.com",
		"user_id":       "@alice:remote.example",
	})

	member := joined["member"].(memberRecord)
	if joined["room_id"] != ch.RoomID || member.Membership != "join" {
		t.Fatalf("expected existing Matrix invite to permit join without local grant, got %#v", joined)
	}
}

func TestPrivateChannelRejectsPublicJoinRequest(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "private",
		"room_id":     "!private:example.com",
		"name":        "Private",
		"visibility":  "private",
		"join_policy": "invite",
	})

	if _, apiErr := service.Handle(context.Background(), "channels.public.join_request", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@alice:remote.example",
	}); apiErr == nil || apiErr.Status != 403 {
		t.Fatalf("expected private channel public join request to return 403, got %#v", apiErr)
	}
	members := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	if findMember(members, "@alice:remote.example").UserID != "" {
		t.Fatalf("expected rejected private public request to avoid creating member, got %#v", members)
	}
}

func TestKickedChannelMemberRequiresFreshInviteGrant(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "moderated",
		"room_id":     "!moderated:example.com",
		"name":        "Moderated",
		"visibility":  "public",
		"join_policy": "approval",
	})

	mustHandle[map[string]any](t, service, "channels.invite", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@kicked:remote.example",
	})
	mustHandle[map[string]any](t, service, "channels.member.remove", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@kicked:remote.example",
	})
	rejected := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@kicked:remote.example",
	})
	if rejected["status"] != "rejected" {
		t.Fatalf("expected kicked member join request to be auto rejected, got %#v", rejected)
	}
	members := mustHandle[map[string]any](t, service, "channels.members", map[string]any{
		"channel_id": ch.ChannelID,
	})["members"].([]memberRecord)
	if findMember(members, "@kicked:remote.example").UserID != "" {
		t.Fatalf("expected kicked member to stay hidden, got %#v", members)
	}

	shareRoomID := "!share:example.com"
	if err := service.saveGroup(context.Background(), groupRecord{RoomID: shareRoomID, Name: "Share Room"}); err != nil {
		t.Fatal(err)
	}
	for _, member := range []memberRecord{
		{RoomID: shareRoomID, UserID: "@owner:example.com", Domain: "example.com", Membership: "join", Role: "owner"},
		{RoomID: shareRoomID, UserID: "@kicked:remote.example", Domain: "remote.example", Membership: "join", Role: "member"},
	} {
		if err := service.saveMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
	}
	grant := mustHandle[map[string]any](t, service, "channels.invite_grant.create", map[string]any{
		"room_id":       ch.RoomID,
		"channel_id":    ch.ChannelID,
		"share_room_id": shareRoomID,
	})
	joined := mustHandle[map[string]any](t, service, "channels.join", map[string]any{
		"grant_id":      grant["grant_id"],
		"share_room_id": shareRoomID,
		"user_mxid":     "@kicked:remote.example",
	})
	member := joined["member"].(memberRecord)
	if member.UserID != "@kicked:remote.example" || member.Membership != "join" || member.ChannelID != ch.ChannelID {
		t.Fatalf("expected fresh channel invite grant to let kicked member rejoin, got %#v", joined)
	}

}

func TestChannelMemberLeaveActionCanReapply(t *testing.T) {
	ch := channel{
		ChannelID:  "moderated",
		RoomID:     "!moderated:example.com",
		Name:       "Moderated",
		Visibility: "public",
		JoinPolicy: "approval",
	}
	remoteActions := []string{}
	remoteOwner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_p2p/query" {
			http.NotFound(w, r)
			return
		}
		var env envelope
		if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		remoteActions = append(remoteActions, env.Action)
		w.Header().Set("Content-Type", "application/json")
		switch env.Action {
		case "channels.public.get":
			_ = json.NewEncoder(w).Encode(ch)
		case "channels.public.join_request":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "pending",
				"member": memberRecord{
					RoomID:     ch.RoomID,
					ChannelID:  ch.ChannelID,
					UserID:     "@owner:remote.example",
					Domain:     "remote.example",
					Membership: "pending",
					Role:       "member",
				},
				"channel": ch,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer remoteOwner.Close()

	service := NewService(Config{
		ServerName:                     "remote.example",
		RemoteNodeAllowPrivateBaseURLs: true,
	})
	bootstrapService(t, service)
	if err := service.saveChannel(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@owner:remote.example",
		Domain:     "remote.example",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	left := mustHandle[map[string]any](t, service, "channels.leave", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
	})
	member := left["member"].(memberRecord)
	if member.UserID != "@owner:remote.example" || member.Membership != "leave" || member.Role != "member" {
		t.Fatalf("expected current member to leave through real action, got %#v", left)
	}
	pending := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id":           ch.ChannelID,
		"room_id":              ch.RoomID,
		"user_mxid":            "@owner:remote.example",
		"remote_node_base_url": remoteOwner.URL,
	})
	if pending["status"] != "pending" {
		t.Fatalf("expected action-left channel member to be able to reapply, got %#v", pending)
	}
	if strings.Join(remoteActions, ",") != "channels.public.get,channels.public.join_request" {
		t.Fatalf("expected reapply to query and post to remote owner node, got %#v", remoteActions)
	}
}

func TestRemotePublicLookupRejectsMalformedAndUnconfiguredRoomTargets(t *testing.T) {
	service := NewService(Config{ServerName: "local.example"})
	bootstrapService(t, service)

	if _, apiErr := service.Handle(context.Background(), "channels.public.get", map[string]any{
		"room_id": "!room:https://127.0.0.1:8448",
	}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected URL-shaped room server to be rejected before outbound lookup, got %#v", apiErr)
	}

	if _, apiErr := service.Handle(context.Background(), "channels.public.get", map[string]any{
		"room_id": "!room:remote.example",
	}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected missing remote node base URL to return 400, got %#v", apiErr)
	}
}

func TestOpenPublicJoinRequestUsesMatrixJoinForLocalRequester(t *testing.T) {
	transport := &recordingTransport{roomID: "!open:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "open",
		"name":        "Open",
		"visibility":  "public",
		"join_policy": "open",
	})

	result := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@owner:example.com",
	})
	if result["status"] != "joined" {
		t.Fatalf("expected open public join request to auto join local requester, got %#v", result)
	}
	member := result["member"].(memberRecord)
	if member.Membership != "join" {
		t.Fatalf("expected product member to join after Matrix join, got %#v", member)
	}
	if len(transport.invites) != 0 {
		t.Fatalf("expected open public join request not to create Matrix invite, got %#v", transport.invites)
	}
	if len(transport.joins) != 1 || transport.joins[0] != "@owner:example.com in !open:example.com" {
		t.Fatalf("expected open public join request to join through Matrix, got %#v", transport.joins)
	}
}

func TestUserPublicChannelsReturnsOwnedPublicChannelsOnly(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	publicChannel := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "public_owned",
		"room_id":     "!public-owned:example.com",
		"name":        "Public Owned",
		"avatar_url":  "mxc://example.com/public-owned",
		"visibility":  "public",
		"description": "visible",
	})
	privateChannel := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "private_owned",
		"room_id":    "!private-owned:example.com",
		"name":       "Private Owned",
		"visibility": "private",
	})
	memberOnly := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "member_only",
		"room_id":    "!member-only:example.com",
		"name":       "Member Only",
		"visibility": "public",
	})
	legacyAdmin := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "legacy_admin",
		"room_id":    "!legacy-admin:example.com",
		"name":       "Legacy Admin",
		"visibility": "public",
	})
	mustHandle[map[string]any](t, service, "channels.join", map[string]any{
		"channel_id": memberOnly.ChannelID,
		"user_mxid":  "@alice:example.com",
	})
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:      publicChannel.RoomID,
		ChannelID:   publicChannel.ChannelID,
		UserID:      "@alice:example.com",
		DisplayName: "Alice",
		Domain:      "example.com",
		Membership:  "join",
		Role:        "owner",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:      privateChannel.RoomID,
		ChannelID:   privateChannel.ChannelID,
		UserID:      "@alice:example.com",
		DisplayName: "Alice",
		Domain:      "example.com",
		Membership:  "join",
		Role:        "owner",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:      legacyAdmin.RoomID,
		ChannelID:   legacyAdmin.ChannelID,
		UserID:      "@alice:example.com",
		DisplayName: "Alice",
		Domain:      "example.com",
		Membership:  "join",
		Role:        "admin",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "users.public_channels", map[string]any{"user_mxid": "@alice:example.com"})
	channels := result["channels"].([]channel)
	if len(channels) != 1 {
		t.Fatalf("expected alice owned public channels only, got %#v", result)
	}
	got := map[string]channel{}
	for _, ch := range channels {
		got[ch.ChannelID] = ch
	}
	if ch := got[publicChannel.ChannelID]; ch.RoomID != publicChannel.RoomID || ch.AvatarURL != "mxc://example.com/public-owned" {
		t.Fatalf("expected alice owned public channel with display fields, got %#v", result)
	}
	if _, ok := got[memberOnly.ChannelID]; ok {
		t.Fatalf("did not expect alice member-only public channel, got %#v", result)
	}
	if _, ok := got[legacyAdmin.ChannelID]; ok {
		t.Fatalf("did not expect deprecated admin role to count as owned channel, got %#v", result)
	}
	if _, ok := got[privateChannel.ChannelID]; ok {
		t.Fatalf("expected private channel to be hidden, got %#v", result)
	}
}
