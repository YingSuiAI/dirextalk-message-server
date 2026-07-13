package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestServiceUsesTransportForMemberLifecycle(t *testing.T) {
	transport := &recordingTransport{roomID: "!group:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"name": "Team"})
	mustHandle[map[string]any](t, service, "groups.invite", map[string]any{"room_id": group.RoomID, "user_id": "@alice:example.com"})
	mustHandle[map[string]any](t, service, "groups.join", map[string]any{"room_id": group.RoomID, "user_id": "@alice:example.com"})
	mustHandle[map[string]any](t, service, "groups.member.remove", map[string]any{"room_id": group.RoomID, "user_id": "@alice:example.com"})

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
}

func TestRepeatedMemberInviteKeepsJoinedMemberAndNeverKicks(t *testing.T) {
	tests := []struct {
		name   string
		action string
		roomID string
		create func(*testing.T, *Service) map[string]any
	}{
		{
			name:   "group",
			action: "groups.invite",
			roomID: "!group:example.com",
			create: func(t *testing.T, service *Service) map[string]any {
				group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"name": "Team"})
				return map[string]any{"room_id": group.RoomID}
			},
		},
		{
			name:   "channel",
			action: "channels.invite",
			roomID: "!channel:example.com",
			create: func(t *testing.T, service *Service) map[string]any {
				created := mustHandle[channel](t, service, "channels.create", map[string]any{
					"channel_id": "private", "name": "Private", "visibility": "private", "join_policy": "invite",
				})
				return map[string]any{"room_id": created.RoomID, "channel_id": created.ChannelID}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			remoteCalls := 0
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				remoteCalls++
				writeJSON(w, http.StatusOK, map[string]any{"status": "invite"})
			}))
			defer remote.Close()

			transport := &alreadyJoinedOnceInviteTransport{recordingTransport: recordingTransport{
				roomID: tt.roomID,
				roomMembers: []memberRecord{{
					RoomID: tt.roomID, UserID: "@alice:remote.example", Membership: "join", Role: "member",
				}},
			}}
			service := NewServiceWithTransport(Config{
				ServerName: "example.com", RemoteNodeAllowPrivateBaseURLs: true,
			}, transport)
			bootstrapService(t, service)
			params := tt.create(t, service)
			params["user_id"] = "@alice:remote.example"
			params["remote_node_base_url"] = remote.URL + "/_p2p"

			result := mustHandle[map[string]any](t, service, tt.action, params)
			members := result["members"].([]memberRecord)
			if result["status"] != "ok" || len(members) != 1 || members[0].Membership != "join" {
				t.Fatalf("repeated %s result = %#v, want current joined member", tt.action, result)
			}
			if len(transport.kicks) != 0 || len(transport.inviteRequests) != 1 || remoteCalls != 0 {
				t.Fatalf("repeated %s performed recovery side effects: kicks=%#v invites=%#v remote_calls=%d", tt.action, transport.kicks, transport.inviteRequests, remoteCalls)
			}
		})
	}
}

func TestGroupInviteReactivatesRemoteNodeWhenMatrixAlreadyJoined(t *testing.T) {
	const rebuildGeneration = "rebuild_group_1"
	remoteActions := []string{}
	transport := &alreadyJoinedOnceInviteTransport{recordingTransport: recordingTransport{
		roomMembers: []memberRecord{{
			UserID: "@alice:remote.example", Membership: "join", Role: "member",
		}},
	}}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "rooms.reactivate" ||
			trimString(req.Params["room_type"]) != "group" ||
			trimString(req.Params["room_id"]) == "" ||
			trimString(req.Params["user_id"]) != "@alice:remote.example" ||
			trimString(req.Params["rebuild_generation"]) != rebuildGeneration {
			t.Fatalf("unexpected room reactivation request %#v", req)
		}
		if len(transport.kicks) != 0 {
			t.Fatalf("owner kicked before target confirmed rebuild: %#v", transport.kicks)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "invite", "room_id": trimString(req.Params["room_id"]),
			"needs_rebuild": true, "rebuild_generation": rebuildGeneration,
		})
	}))
	defer remote.Close()

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
		"rebuild_generation":   rebuildGeneration,
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

func TestRoomReactivateReturnsJoinedWithoutOpeningRebuildWhenMatrixIsReady(t *testing.T) {
	const rebuildGeneration = "rebuild_group_joined_1"
	transport := &recordingTransport{
		roomID: "!group:remote.example",
		roomMembers: []memberRecord{{
			RoomID: "!group:remote.example", UserID: "@owner:example.com", Membership: "join", Role: "member",
		}},
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID: "!group:remote.example", UserID: "@owner:example.com", Membership: "join", Role: "member", RequestID: "$matrix-invite",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "rooms.reactivate", map[string]any{
		"room_id": "!group:remote.example", "room_type": "group", "user_id": "@owner:example.com",
		"name": "Remote Team", "rebuild_generation": rebuildGeneration,
	})

	if result["status"] != "joined" || result["needs_rebuild"] != false || result["rebuild_generation"] != rebuildGeneration {
		t.Fatalf("joined room reactivation decision = %#v", result)
	}
	member, ok, err := service.lookupMember(context.Background(), "!group:remote.example", "@owner:example.com")
	if err != nil || !ok || member.Membership != "join" || member.RequestID != "$matrix-invite" {
		t.Fatalf("joined room projection changed recovery generation: member=%#v found=%v err=%v", member, ok, err)
	}
}

func TestRoomReactivateSafelySupersedesStaleRebuildGeneration(t *testing.T) {
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, &recordingTransport{})
	bootstrapService(t, service)
	params := map[string]any{
		"room_id": "!group:remote.example", "room_type": "group", "user_id": "@owner:example.com",
		"name": "Remote Team", "rebuild_generation": "rebuild_group_a",
	}
	first := mustHandle[map[string]any](t, service, "rooms.reactivate", params)
	second := mustHandle[map[string]any](t, service, "rooms.reactivate", params)
	if first["status"] != "invite" || first["needs_rebuild"] != true || second["rebuild_generation"] != "rebuild_group_a" {
		t.Fatalf("same rebuild generation was not idempotent: first=%#v second=%#v", first, second)
	}

	replacement := cloneParams(params)
	replacement["rebuild_generation"] = "rebuild_group_b"
	replaced := mustHandle[map[string]any](t, service, "rooms.reactivate", replacement)
	if replaced["status"] != "invite" || replaced["needs_rebuild"] != true || replaced["rebuild_generation"] != "rebuild_group_b" {
		t.Fatalf("stale rebuild generation was not safely superseded: %#v", replaced)
	}
	member, ok, err := service.lookupMember(context.Background(), "!group:remote.example", "@owner:example.com")
	if err != nil || !ok || member.Membership != "invite" || member.RequestID != "rebuild_group_b" {
		t.Fatalf("replacement rebuild generation was not persisted: member=%#v found=%v err=%v", member, ok, err)
	}

	member.Membership = "pending"
	if err := service.saveMember(context.Background(), member); err != nil {
		t.Fatal(err)
	}
	active := cloneParams(params)
	active["rebuild_generation"] = "rebuild_group_c"
	result, apiErr := service.Handle(context.Background(), "rooms.reactivate", active)
	if result != nil || apiErr == nil || apiErr.Status != http.StatusConflict {
		t.Fatalf("active join generation = (%#v, %#v), want 409", result, apiErr)
	}
}

func TestExplicitRoomRebuildRejectsMismatchedConfirmationWithoutKick(t *testing.T) {
	transport := &alreadyJoinedOnceInviteTransport{recordingTransport: recordingTransport{
		roomID: "!group:example.com",
		roomMembers: []memberRecord{{
			RoomID: "!group:example.com", UserID: "@alice:remote.example", Membership: "join", Role: "member",
		}},
	}}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "invite", "needs_rebuild": true, "rebuild_generation": "different_generation",
		})
	}))
	defer remote.Close()
	service := NewServiceWithTransport(Config{
		ServerName: "example.com", RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"name": "Team"})

	result, apiErr := service.Handle(context.Background(), "groups.invite", map[string]any{
		"room_id": group.RoomID, "user_id": "@alice:remote.example",
		"remote_node_base_url": remote.URL + "/_p2p", "rebuild_generation": "rebuild_group_2",
	})
	if result != nil || apiErr == nil || apiErr.Status != http.StatusBadGateway {
		t.Fatalf("mismatched rebuild confirmation = (%#v, %#v), want 502", result, apiErr)
	}
	if len(transport.kicks) != 0 || len(transport.inviteRequests) != 1 {
		t.Fatalf("mismatched rebuild confirmation caused Matrix mutation: kicks=%#v invites=%#v", transport.kicks, transport.inviteRequests)
	}
}

func TestPrivateChannelInviteReactivatesRemoteNodeWhenMatrixAlreadyJoined(t *testing.T) {
	const rebuildGeneration = "rebuild_channel_1"
	remoteActions := []string{}
	transport := &alreadyJoinedOnceInviteTransport{recordingTransport: recordingTransport{
		roomID: "!channel:example.com",
		roomMembers: []memberRecord{{
			RoomID: "!channel:example.com", UserID: "@alice:remote.example", Membership: "join", Role: "member",
		}},
	}}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "rooms.reactivate" ||
			trimString(req.Params["room_type"]) != "channel" ||
			trimString(req.Params["channel_id"]) != "private" ||
			trimString(req.Params["user_id"]) != "@alice:remote.example" ||
			trimString(req.Params["rebuild_generation"]) != rebuildGeneration {
			t.Fatalf("unexpected channel reactivation request %#v", req)
		}
		if len(transport.kicks) != 0 {
			t.Fatalf("owner kicked before target confirmed rebuild: %#v", transport.kicks)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "invite", "room_id": "!channel:example.com", "channel_id": "private",
			"needs_rebuild": true, "rebuild_generation": rebuildGeneration,
		})
	}))
	defer remote.Close()

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
		"rebuild_generation":   rebuildGeneration,
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

type cancelAfterReplacementInviteTransport struct {
	alreadyJoinedOnceInviteTransport
	cancel context.CancelFunc
}

func (t *cancelAfterReplacementInviteTransport) InviteUser(ctx context.Context, req InviteUserRequest) error {
	err := t.alreadyJoinedOnceInviteTransport.InviteUser(ctx, req)
	if t.attempts == 2 && t.cancel != nil {
		t.roomMembers = []memberRecord{{
			RoomID: req.RoomID, UserID: req.InviteeMXID, Membership: "invite", Role: "member",
		}}
		t.cancel()
	}
	return err
}

func TestExplicitRetainedRoomRebuildSettlesAndReplaysAcrossServices(t *testing.T) {
	const rebuildGeneration = "rebuild_group_durable_1"
	requestCtx, cancel := context.WithCancel(context.Background())
	store := &cancelAwareMemberStore{MemoryStore: p2pstorage.NewMemoryStore()}
	transport := &cancelAfterReplacementInviteTransport{
		alreadyJoinedOnceInviteTransport: alreadyJoinedOnceInviteTransport{recordingTransport: recordingTransport{
			roomID: "!group:example.com",
			roomMembers: []memberRecord{{
				RoomID: "!group:example.com", UserID: "@alice:remote.example", Membership: "join", Role: "member",
			}},
		}},
		cancel: cancel,
	}
	remoteCalls := 0
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls++
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "invite", "room_id": trimString(req.Params["room_id"]),
			"needs_rebuild": true, "rebuild_generation": rebuildGeneration,
		})
	}))
	defer remote.Close()

	cfg := Config{ServerName: "example.com", RemoteNodeAllowPrivateBaseURLs: true}
	firstService := newService(cfg, store, transport, portalState{}, false)
	group := mustHandle[groupRecord](t, firstService, "groups.create", map[string]any{
		"room_id": "!group:example.com", "name": "Team",
	})
	params := map[string]any{
		"room_id": group.RoomID, "user_id": "@alice:remote.example",
		"remote_node_base_url": remote.URL + "/_p2p", "rebuild_generation": rebuildGeneration,
		"operation_id": "caller_rebuild_one",
	}
	first, apiErr := firstService.Handle(requestCtx, "groups.invite", cloneParams(params))
	if apiErr != nil {
		t.Fatalf("rebuild did not finish after request cancellation: %#v", apiErr)
	}
	firstResult := first.(map[string]any)
	if firstResult["status"] != "ok" || trimString(firstResult["operation_id"]) == "" {
		t.Fatalf("unexpected first rebuild result: %#v", firstResult)
	}

	secondService := newService(cfg, store, transport, portalState{}, false)
	replayParams := cloneParams(params)
	replayParams["operation_id"] = "caller_rebuild_two"
	second := mustHandle[map[string]any](t, secondService, "groups.invite", replayParams)
	if second["status"] != "ok" || second["operation_id"] != firstResult["operation_id"] {
		t.Fatalf("cross-service rebuild replay changed result: first=%#v second=%#v", firstResult, second)
	}
	if remoteCalls != 1 || len(transport.kicks) != 1 || len(transport.inviteRequests) != 2 {
		t.Fatalf("replayed rebuild repeated side effects: remote=%d kicks=%#v invites=%#v", remoteCalls, transport.kicks, transport.inviteRequests)
	}
	member, found, err := secondService.lookupMember(context.Background(), group.RoomID, "@alice:remote.example")
	if err != nil || !found || member.Membership != "invite" || member.RequestID != rebuildGeneration {
		t.Fatalf("settled rebuild projection = %#v found=%v err=%v", member, found, err)
	}
}

func TestExplicitRetainedRoomRebuildRejectsMultipleInvitees(t *testing.T) {
	transport := &recordingTransport{roomID: "!group:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"name": "Team"})

	result, apiErr := service.Handle(context.Background(), "groups.invite", map[string]any{
		"room_id": group.RoomID, "user_ids": []string{"@alice:remote.example", "@bob:remote.example"},
		"rebuild_generation": "rebuild_group_multi",
	})
	if result != nil || apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("multi-user explicit rebuild = (%#v, %#v), want 400", result, apiErr)
	}
	if len(transport.inviteRequests) != 0 || len(transport.kicks) != 0 {
		t.Fatalf("invalid rebuild performed side effects: invites=%#v kicks=%#v", transport.inviteRequests, transport.kicks)
	}
}

func TestCompletedRetainedRoomRebuildGenerationNeverKicksJoinedMember(t *testing.T) {
	const rebuildGeneration = "rebuild_group_completed"
	transport := &alreadyJoinedOnceInviteTransport{recordingTransport: recordingTransport{
		roomID: "!group:example.com",
		roomMembers: []memberRecord{{
			RoomID: "!group:example.com", UserID: "@alice:remote.example", Membership: "join", Role: "member",
		}},
	}}
	remoteCalls := 0
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		remoteCalls++
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "must not be called"})
	}))
	defer remote.Close()
	service := NewServiceWithTransport(Config{
		ServerName: "example.com", RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com", "name": "Team",
	})
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID: group.RoomID, UserID: "@alice:remote.example", Membership: "invite", Role: "member",
		RequestID: rebuildGeneration,
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "groups.invite", map[string]any{
		"room_id": group.RoomID, "user_id": "@alice:remote.example",
		"remote_node_base_url": remote.URL + "/_p2p", "rebuild_generation": rebuildGeneration,
	})
	members := result["members"].([]memberRecord)
	if result["status"] != "ok" || len(members) != 1 || members[0].Membership != "join" {
		t.Fatalf("completed rebuild did not return current joined member: %#v", result)
	}
	if remoteCalls != 0 || len(transport.kicks) != 0 || len(transport.inviteRequests) != 1 {
		t.Fatalf("completed rebuild repeated destructive side effects: remote=%d kicks=%#v invites=%#v", remoteCalls, transport.kicks, transport.inviteRequests)
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
