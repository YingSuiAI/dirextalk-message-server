package p2p

import (
	"context"
	"encoding/json"
	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	"net/http"
	"net/http/httptest"
	"testing"
)

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

func TestChannelPublicJoinResultApprovedJoinsRequesterAfterInviteProjection(t *testing.T) {
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
		Membership: "invite",
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
	})
	if result["status"] != "joined" {
		t.Fatalf("expected approved invite projection to join requester node, got %#v", result)
	}
	if len(transport.joins) != 1 || transport.joins[0] != "@owner:local.example in !remote:remote.example" {
		t.Fatalf("expected requester node Matrix join, got %#v", transport.joins)
	}
	member, ok, err := service.lookupMember(context.Background(), ch.RoomID, "@owner:local.example")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || member.Membership != "join" {
		t.Fatalf("expected requester member to become joined, ok=%v member=%#v", ok, member)
	}
}

func TestChannelPublicJoinResultCreatesConversationFromRefreshedChannel(t *testing.T) {
	transport := &recordingTransport{
		roomID: "!remote:remote.example",
		roomChannel: channel{
			ChannelID:       "remote_ch",
			RoomID:          "!remote:remote.example",
			Name:            "Remote Public",
			Visibility:      "public",
			JoinPolicy:      "approval",
			ChannelType:     "chat",
			CommentsEnabled: true,
		},
	}
	service := NewServiceWithTransport(Config{ServerName: "local.example"}, transport)
	bootstrapService(t, service)
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     "!remote:remote.example",
		ChannelID:  "remote_ch",
		UserID:     "@owner:local.example",
		Domain:     "local.example",
		Membership: "pending",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "channels.public.join_result", map[string]any{
		"room_id":      "!remote:remote.example",
		"channel_id":   "remote_ch",
		"user_id":      "@owner:local.example",
		"status":       "approved",
		"server_names": []string{"remote.example"},
	})
	if result["status"] != "joined" {
		t.Fatalf("expected approved join result to join requester node, got %#v", result)
	}

	list := mustHandle[map[string]any](t, service, "conversations.list", nil)
	conversations := list["conversations"].([]conversationView)
	if len(conversations) != 1 || conversations[0].MatrixRoomID != "!remote:remote.example" || conversations[0].Kind != conversationKindChannel {
		t.Fatalf("expected refreshed joined channel to create channel conversation, got %#v", conversations)
	}
	if conversations[0].Membership != "join" || conversations[0].Title != "Remote Public" {
		t.Fatalf("expected joined channel conversation facts, got %#v", conversations[0])
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
	joinRequestStates := recordedStatesOfType(transport.stateEvents, DirextalkJoinRequestEventType)
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
