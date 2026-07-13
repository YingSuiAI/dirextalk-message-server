package p2p

import (
	"context"
	"encoding/json"
	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

type generationSwapOnMemberFactTransport struct {
	*statefulJoinTransport
	service     *Service
	replacement memberRecord
	armed       atomic.Bool
}

func (t *generationSwapOnMemberFactTransport) ListRoomMembers(ctx context.Context, roomID string) ([]memberRecord, error) {
	if t.armed.CompareAndSwap(true, false) {
		if err := t.service.saveMember(ctx, t.replacement); err != nil {
			return nil, err
		}
	}
	return t.statefulJoinTransport.ListRoomMembers(ctx, roomID)
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
	member := result["member"].(memberRecord)
	if member.UserID != "@alice:remote.example" || member.ChannelID != ch.ChannelID || member.Membership != "approved" {
		t.Fatalf("expected approved member snapshot, got %#v", member)
	}
	if len(transport.inviteRequests) != 0 || len(transport.invites) != 0 {
		t.Fatalf("public join request must not expose Matrix invite flow, got invites=%#v requests=%#v", transport.invites, transport.inviteRequests)
	}
	var approvedState bool
	for _, state := range transport.stateEvents {
		if state.Event.Type == DirextalkJoinRequestEventType &&
			state.Event.StateKey == productpolicy.UserStateKey("@alice:remote.example") &&
			state.Event.Content["status"] == "approved" &&
			state.Event.Content["request_id"] == member.RequestID {
			approvedState = true
		}
	}
	if !approvedState {
		t.Fatalf("expected approved join request state, got %#v", transport.stateEvents)
	}
}

func TestChannelPublicJoinRequestReplayDoesNotDowngradeRecoveryState(t *testing.T) {
	for _, status := range []string{"approved", "joining", "join_failed"} {
		t.Run(status, func(t *testing.T) {
			service := NewService(Config{ServerName: "example.com"})
			bootstrapService(t, service)
			ch := mustHandle[channel](t, service, "channels.create", map[string]any{
				"channel_id": "public", "name": "Public Channel", "visibility": "public", "join_policy": "approval",
			})
			if err := service.saveMember(context.Background(), memberRecord{
				RoomID: ch.RoomID, ChannelID: ch.ChannelID, UserID: "@alice:remote.example",
				Membership: status, Role: "member", RequestID: "request-a",
			}); err != nil {
				t.Fatal(err)
			}

			result := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
				"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": "@alice:remote.example", "request_id": "request-a",
			})
			if result["status"] != status {
				t.Fatalf("replay downgraded %s: %#v", status, result)
			}
			member, ok, err := service.lookupMember(context.Background(), ch.RoomID, "@alice:remote.example")
			if err != nil || !ok || member.Membership != status || member.RequestID != "request-a" {
				t.Fatalf("persisted member changed: ok=%v member=%#v err=%v", ok, member, err)
			}
		})
	}
}

func TestChannelPublicJoinGenerationRejectThenDelayedCallbackCannotOverwriteNewRequest(t *testing.T) {
	service := NewService(Config{ServerName: "local.example"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "public", "name": "Public Channel", "visibility": "public", "join_policy": "approval",
	})
	userID := "@owner:local.example"
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID: ch.RoomID, ChannelID: ch.ChannelID, UserID: userID,
		Membership: "rejected", Role: "member", RequestID: "request-a",
	}); err != nil {
		t.Fatal(err)
	}

	created := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": userID, "request_id": "request-b",
	})
	if created["status"] != "pending" {
		t.Fatalf("new generation was not created: %#v", created)
	}
	requestB := created["member"].(memberRecord).RequestID
	if requestB == "" || requestB == "request-a" {
		t.Fatalf("new generation was not server-owned: %#v", created)
	}
	delayed := mustHandle[map[string]any](t, service, "channels.public.join_result", map[string]any{
		"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": userID,
		"request_id": "request-a", "status": "rejected",
	})
	if delayed["status"] != "pending" {
		t.Fatalf("delayed generation A callback did not report generation B: %#v", delayed)
	}
	member, ok, err := service.lookupMember(context.Background(), ch.RoomID, userID)
	if err != nil || !ok || member.Membership != "pending" || member.RequestID != requestB {
		t.Fatalf("delayed callback overwrote generation B: ok=%v member=%#v err=%v", ok, member, err)
	}

	current := mustHandle[map[string]any](t, service, "channels.public.join_result", map[string]any{
		"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": userID,
		"request_id": requestB, "status": "rejected",
	})
	if current["status"] != "rejected" {
		t.Fatalf("delayed generation poisoned the current callback operation: %#v", current)
	}
	member, ok, err = service.lookupMember(context.Background(), ch.RoomID, userID)
	if err != nil || !ok || member.Membership != "reject" || member.RequestID != requestB {
		t.Fatalf("current callback did not settle generation B: ok=%v member=%#v err=%v", ok, member, err)
	}
}

func TestChannelPublicJoinRejectedLegacyRetryStartsStableServerGeneration(t *testing.T) {
	for _, previousRequestID := range []string{"request-a", ""} {
		t.Run(fallbackString(previousRequestID, "empty_previous_request_id"), func(t *testing.T) {
			service := NewService(Config{ServerName: "local.example"})
			bootstrapService(t, service)
			ch := mustHandle[channel](t, service, "channels.create", map[string]any{
				"channel_id": "public", "name": "Public Channel", "visibility": "public", "join_policy": "approval",
			})
			userID := "@owner:local.example"
			if err := service.saveMember(context.Background(), memberRecord{
				RoomID: ch.RoomID, ChannelID: ch.ChannelID, UserID: userID,
				Membership: "rejected", Role: "member", RequestID: previousRequestID,
			}); err != nil {
				t.Fatal(err)
			}

			params := map[string]any{"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": userID}
			first := mustHandle[map[string]any](t, service, "channels.public.join_request", cloneParams(params))
			if first["status"] != "pending" {
				t.Fatalf("legacy retry did not start a fresh pending generation: %#v", first)
			}
			firstMember := first["member"].(memberRecord)
			if firstMember.RequestID == "" || firstMember.RequestID == previousRequestID {
				t.Fatalf("server-owned request generation was not renewed: %#v", firstMember)
			}

			second := mustHandle[map[string]any](t, service, "channels.public.join_request", cloneParams(params))
			secondMember := second["member"].(memberRecord)
			if second["status"] != "pending" || secondMember.RequestID != firstMember.RequestID {
				t.Fatalf("pending replay did not reuse generation %q: %#v", firstMember.RequestID, second)
			}
		})
	}
}

func TestChannelPublicJoinRejectedCallbackKeepsAuthoritativeMatrixJoin(t *testing.T) {
	const roomID = "!remote:remote.example"
	const userID = "@owner:local.example"
	transport := &recordingTransport{
		roomID:      roomID,
		roomChannel: channel{ChannelID: "remote_ch", RoomID: roomID, Name: "Remote Public", Visibility: "public", JoinPolicy: "approval"},
		roomMembers: []memberRecord{{RoomID: roomID, ChannelID: "remote_ch", UserID: userID, Membership: "join", Role: "member"}},
	}
	service := NewServiceWithTransport(Config{ServerName: "local.example"}, transport)
	bootstrapService(t, service)
	if err := service.saveChannel(context.Background(), transport.roomChannel); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID: roomID, ChannelID: "remote_ch", UserID: userID,
		Membership: "joining", Role: "member", RequestID: "request-a",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "channels.public.join_result", map[string]any{
		"room_id": roomID, "channel_id": "remote_ch", "user_id": userID,
		"request_id": "request-a", "status": "rejected",
	})
	if result["status"] != "joined" {
		t.Fatalf("Matrix join was overwritten by rejected callback: %#v", result)
	}
	member, ok, err := service.lookupMember(context.Background(), roomID, userID)
	if err != nil || !ok || member.Membership != "join" {
		t.Fatalf("joined projection was not repaired: ok=%v member=%#v err=%v", ok, member, err)
	}
}

func TestChannelPublicJoinResultGenerationSwapStopsBeforeMatrixJoin(t *testing.T) {
	const (
		roomID    = "!remote:remote.example"
		channelID = "remote_ch"
		userID    = "@owner:local.example"
	)
	transport := &generationSwapOnMemberFactTransport{
		statefulJoinTransport: &statefulJoinTransport{recordingTransport: recordingTransport{
			roomID: roomID,
			roomChannel: channel{
				ChannelID: channelID, RoomID: roomID, Name: "Remote Public",
				Visibility: "public", JoinPolicy: "approval",
			},
		}},
	}
	service := NewServiceWithTransport(Config{ServerName: "local.example"}, transport)
	transport.service = service
	bootstrapService(t, service)
	if err := service.saveChannel(context.Background(), transport.roomChannel); err != nil {
		t.Fatal(err)
	}
	memberA := memberRecord{
		RoomID: roomID, ChannelID: channelID, UserID: userID,
		Membership: "pending", Role: "member", RequestID: "request-a",
	}
	if err := service.saveMember(context.Background(), memberA); err != nil {
		t.Fatal(err)
	}
	transport.replacement = memberRecord{
		RoomID: roomID, ChannelID: channelID, UserID: userID,
		Membership: "pending", Role: "member", RequestID: "request-b",
	}
	joinsBefore := len(transport.joinRequests)
	invitesBefore := len(transport.inviteRequests)
	transport.armed.Store(true)

	result := mustHandle[map[string]any](t, service, "channels.public.join_result", map[string]any{
		"room_id": roomID, "channel_id": channelID, "user_id": userID,
		"request_id": "request-a", "status": "approved",
	})
	current := result["member"].(memberRecord)
	if result["status"] != "pending" || current.RequestID != "request-b" || current.Membership != "pending" {
		t.Fatalf("generation A callback did not return generation B: %#v", result)
	}
	persisted, found, err := service.lookupMember(context.Background(), roomID, userID)
	if err != nil || !found || persisted.RequestID != "request-b" || persisted.Membership != "pending" {
		t.Fatalf("generation A callback overwrote generation B: found=%v member=%#v err=%v", found, persisted, err)
	}
	if len(transport.joinRequests) != joinsBefore || len(transport.inviteRequests) != invitesBefore {
		t.Fatalf("stale generation performed Matrix writes: joins=%#v invites=%#v", transport.joinRequests, transport.inviteRequests)
	}
}

func TestChannelJoinDecisionUsesPersistedGenerationCallbackURL(t *testing.T) {
	tests := []struct {
		name           string
		action         string
		callbackStatus string
		wantStatus     string
	}{
		{name: "approve", action: "channels.join_request.approve", callbackStatus: "joined", wantStatus: "joined"},
		{name: "reject", action: "channels.join_request.reject", callbackStatus: "rejected", wantStatus: "rejected"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var persistedCalls atomic.Int32
			persistedCallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				persistedCalls.Add(1)
				writeJSON(w, http.StatusOK, map[string]any{"status": tt.callbackStatus})
			}))
			defer persistedCallback.Close()

			var requestCalls atomic.Int32
			requestCallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requestCalls.Add(1)
				writeJSON(w, http.StatusOK, map[string]any{"status": tt.callbackStatus})
			}))
			defer requestCallback.Close()

			service := NewService(Config{
				ServerName:                     "owner.example",
				RemoteNodeAllowPrivateBaseURLs: true,
			})
			bootstrapService(t, service)
			ch := mustHandle[channel](t, service, "channels.create", map[string]any{
				"channel_id": "callback_generation", "room_id": "!callback:owner.example",
				"name": "Callback Generation", "visibility": "public", "join_policy": "approval",
			})
			if err := service.saveMember(context.Background(), memberRecord{
				RoomID: ch.RoomID, ChannelID: ch.ChannelID, UserID: "@alice:requester.example",
				Membership: "pending", Role: "member", RequestID: "generation-a",
				RequesterNodeBaseURL: persistedCallback.URL + "/_p2p",
			}); err != nil {
				t.Fatal(err)
			}

			result := mustHandle[map[string]any](t, service, tt.action, map[string]any{
				"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": "@alice:requester.example",
				"request_id": "generation-a", "requester_node_base_url": requestCallback.URL + "/_p2p",
			})
			if result["status"] != tt.wantStatus {
				t.Fatalf("unexpected decision result: %#v", result)
			}
			if persistedCalls.Load() != 1 || requestCalls.Load() != 0 {
				t.Fatalf("decision callback did not use the persisted generation URL: persisted=%d request=%d", persistedCalls.Load(), requestCalls.Load())
			}
		})
	}
}

func TestChannelJoinDecisionFallsBackToRequestCallbackForLegacyMember(t *testing.T) {
	tests := []struct {
		name           string
		action         string
		callbackStatus string
		wantStatus     string
	}{
		{name: "approve", action: "channels.join_request.approve", callbackStatus: "joined", wantStatus: "joined"},
		{name: "reject", action: "channels.join_request.reject", callbackStatus: "rejected", wantStatus: "rejected"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var callbackCalls atomic.Int32
			callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				callbackCalls.Add(1)
				writeJSON(w, http.StatusOK, map[string]any{"status": tt.callbackStatus})
			}))
			defer callback.Close()

			service := NewService(Config{
				ServerName:                     "owner.example",
				RemoteNodeAllowPrivateBaseURLs: true,
			})
			bootstrapService(t, service)
			ch := mustHandle[channel](t, service, "channels.create", map[string]any{
				"channel_id": "legacy_callback", "room_id": "!legacy-callback:owner.example",
				"name": "Legacy Callback", "visibility": "public", "join_policy": "approval",
			})
			if err := service.saveMember(context.Background(), memberRecord{
				RoomID: ch.RoomID, ChannelID: ch.ChannelID, UserID: "@alice:requester.example",
				Membership: "pending", Role: "member", RequestID: "legacy-generation",
			}); err != nil {
				t.Fatal(err)
			}

			result := mustHandle[map[string]any](t, service, tt.action, map[string]any{
				"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": "@alice:requester.example",
				"request_id": "legacy-generation", "requester_node_base_url": callback.URL + "/_p2p",
			})
			if result["status"] != tt.wantStatus || callbackCalls.Load() != 1 {
				t.Fatalf("legacy callback URL fallback failed: calls=%d result=%#v", callbackCalls.Load(), result)
			}
		})
	}
}

func TestPublicChannelJoinRequestDoesNotTrustAlreadyJoinedTextOrKickMember(t *testing.T) {
	callbackCalls := 0
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackCalls++
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

	if result["status"] != "joining" || result["error_code"] != "join_result_unconfirmed" {
		t.Fatalf("unconfirmed already-joined text was treated as success: %#v", result)
	}
	if len(transport.kicks) != 0 || len(transport.inviteRequests) != 1 {
		t.Fatalf("ordinary approval retry must not kick or re-invite a joined member, kicks=%#v invites=%#v", transport.kicks, transport.inviteRequests)
	}
	if callbackCalls != 0 {
		t.Fatalf("unconfirmed Matrix membership triggered callback: calls=%d", callbackCalls)
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

func TestRemotePublicChannelJoinedResponseStillImportsRoomLocally(t *testing.T) {
	remoteChannel := channel{
		ChannelID:       "remote_public",
		RoomID:          "!remote:remote.example",
		Name:            "Remote Public",
		Visibility:      "public",
		JoinPolicy:      "open",
		ChannelType:     "post",
		CommentsEnabled: true,
	}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		switch req.Action {
		case "channels.public.get":
			writeJSON(w, http.StatusOK, remoteChannel)
		case "channels.public.join_request":
			writeJSON(w, http.StatusOK, map[string]any{
				"status": "joined",
				"member": memberRecord{
					RoomID:     remoteChannel.RoomID,
					ChannelID:  remoteChannel.ChannelID,
					UserID:     "@owner:local.example",
					Membership: "join",
					Role:       "member",
				},
				"channel": remoteChannel,
			})
		default:
			t.Fatalf("unexpected remote action %#v", req)
		}
	}))
	defer remote.Close()

	transport := &statefulJoinTransport{recordingTransport: recordingTransport{
		roomID:      remoteChannel.RoomID,
		roomChannel: remoteChannel,
	}}
	service := NewServiceWithTransport(Config{
		ServerName:                     "local.example",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if len(transport.roomMembers) != 0 {
		t.Fatalf("requester unexpectedly started with Matrix membership: %#v", transport.roomMembers)
	}

	result := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"room_id":              remoteChannel.RoomID,
		"channel_id":           remoteChannel.ChannelID,
		"remote_node_base_url": remote.URL + "/_p2p",
		"server_names":         []string{"remote.example"},
	})

	if result["status"] != "joined" {
		t.Fatalf("expected joined only after local Matrix import, got %#v", result)
	}
	if len(transport.joinRequests) != 1 || transport.joinRequests[0].RoomIDOrAlias != remoteChannel.RoomID {
		t.Fatalf("remote joined response bypassed requester JoinRoom: %#v", transport.joinRequests)
	}
	member, ok, err := service.lookupMember(context.Background(), remoteChannel.RoomID, "@owner:local.example")
	if err != nil || !ok || member.Membership != "join" {
		t.Fatalf("local joined projection was not imported: ok=%v member=%#v err=%v", ok, member, err)
	}
}

func TestRemotePublicChannelJoinedResponseDoesNotPersistFalseJoin(t *testing.T) {
	remoteChannel := channel{
		ChannelID:  "remote_public",
		RoomID:     "!remote:remote.example",
		Name:       "Remote Public",
		Visibility: "public",
		JoinPolicy: "open",
	}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		switch req.Action {
		case "channels.public.get":
			writeJSON(w, http.StatusOK, remoteChannel)
		case "channels.public.join_request":
			writeJSON(w, http.StatusOK, map[string]any{
				"status": "joined",
				"member": memberRecord{
					RoomID:     remoteChannel.RoomID,
					ChannelID:  remoteChannel.ChannelID,
					UserID:     "@owner:local.example",
					Membership: "join",
					Role:       "member",
				},
				"channel": remoteChannel,
			})
		default:
			t.Fatalf("unexpected remote action %#v", req)
		}
	}))
	defer remote.Close()

	transport := &failOnceJoinTransport{
		recordingTransport: recordingTransport{roomID: remoteChannel.RoomID, roomChannel: remoteChannel},
		err:                productpolicy.Forbidden("injected local join rejection"),
		failures:           100,
	}
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

	if result["status"] != "join_failed" || result["error_code"] != actionbase.MatrixJoinFailedCode {
		t.Fatalf("failed requester import reported a false join: %#v", result)
	}
	member, ok, err := service.lookupMember(context.Background(), remoteChannel.RoomID, "@owner:local.example")
	if err != nil || !ok || member.Membership != "join_failed" {
		t.Fatalf("failed requester import persisted false joined state: ok=%v member=%#v err=%v", ok, member, err)
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

func TestRemotePublicChannelJoinRequestPreservesStructuredTerminalError(t *testing.T) {
	remoteChannel := channel{
		ChannelID: "remote_public", RoomID: "!remote:remote.example",
		Name: "Remote Public", Visibility: "public", JoinPolicy: "approval",
	}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		switch req.Action {
		case "channels.public.get":
			writeJSON(w, http.StatusOK, remoteChannel)
		case "channels.public.join_request":
			writeJSON(w, http.StatusGone, map[string]any{
				"error": "join request expired", "code": actionbase.RequestExpiredCode,
				"error_code": actionbase.RequestExpiredCode, "operation_id": "remote-op",
				"current_room_id": remoteChannel.RoomID,
			})
		default:
			t.Fatalf("unexpected remote action %q", req.Action)
		}
	}))
	defer remote.Close()

	service := NewService(Config{ServerName: "local.example", RemoteNodeAllowPrivateBaseURLs: true})
	bootstrapService(t, service)
	params := map[string]any{
		"room_id": remoteChannel.RoomID, "channel_id": remoteChannel.ChannelID,
		"operation_id": "remote-op", "remote_node_base_url": remote.URL + "/_p2p",
	}
	operation, operationErr := service.operationRecordFor(
		context.Background(), "channels.public.join_request", cloneParams(params),
	)
	if operationErr != nil {
		t.Fatalf("resolve canonical operation: %#v", operationErr)
	}
	result, apiErr := service.Handle(context.Background(), "channels.public.join_request", params)
	if result != nil || apiErr == nil || apiErr.Status != http.StatusGone ||
		apiErr.Code != actionbase.RequestExpiredCode || apiErr.OperationID != operation.OperationID ||
		apiErr.CurrentRoomID != remoteChannel.RoomID {
		t.Fatalf("structured remote join error was not preserved: result=%#v err=%#v", result, apiErr)
	}
}

func TestRemotePublicChannelResponseLossReplaysSameServerGeneration(t *testing.T) {
	remoteChannel := channel{
		ChannelID: "remote_public", RoomID: "!remote:remote.example",
		Name: "Remote Public", Visibility: "public", JoinPolicy: "approval",
	}
	requestIDs := make([]string, 0, 2)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		switch req.Action {
		case "channels.public.get":
			writeJSON(w, http.StatusOK, remoteChannel)
		case "channels.public.join_request":
			requestID := trimString(req.Params["request_id"])
			requestIDs = append(requestIDs, requestID)
			if len(requestIDs) == 1 {
				connection, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Fatal(err)
				}
				_ = connection.Close()
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"status": "pending",
				"member": memberRecord{
					RoomID: remoteChannel.RoomID, ChannelID: remoteChannel.ChannelID,
					UserID: "@owner:local.example", Membership: "pending", Role: "member", RequestID: requestID,
				},
				"channel": remoteChannel,
			})
		default:
			t.Fatalf("unexpected remote action %q", req.Action)
		}
	}))
	defer remote.Close()

	service := NewService(Config{ServerName: "local.example", RemoteNodeAllowPrivateBaseURLs: true})
	bootstrapService(t, service)
	params := map[string]any{
		"room_id": remoteChannel.RoomID, "remote_node_base_url": remote.URL + "/_p2p",
	}
	first, firstErr := service.Handle(context.Background(), "channels.public.join_request", cloneParams(params))
	if first != nil || firstErr == nil {
		t.Fatalf("first lost response was not ambiguous: result=%#v err=%#v", first, firstErr)
	}
	second := mustHandle[map[string]any](t, service, "channels.public.join_request", cloneParams(params))
	if second["status"] != "pending" || len(requestIDs) != 2 || requestIDs[0] == "" || requestIDs[0] != requestIDs[1] {
		t.Fatalf("response-loss replay changed generation: request_ids=%#v result=%#v", requestIDs, second)
	}
	if trimString(second["operation_id"]) == "" || trimString(second["operation_id"]) != firstErr.OperationID {
		t.Fatalf("response-loss replay changed operation identity: first=%#v second=%#v", firstErr, second)
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

func TestChannelPublicJoinRejectedCallbackGenerationSwapPreservesNewRequest(t *testing.T) {
	const (
		roomID    = "!remote:remote.example"
		channelID = "remote_ch"
		userID    = "@owner:local.example"
	)
	transport := &generationSwapOnMemberFactTransport{
		statefulJoinTransport: &statefulJoinTransport{recordingTransport: recordingTransport{
			roomID: roomID,
			roomChannel: channel{
				ChannelID: channelID, RoomID: roomID, Name: "Remote Public",
				Visibility: "public", JoinPolicy: "approval",
			},
		}},
	}
	service := NewServiceWithTransport(Config{ServerName: "local.example"}, transport)
	transport.service = service
	bootstrapService(t, service)
	if err := service.saveChannel(context.Background(), transport.roomChannel); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID: roomID, ChannelID: channelID, UserID: userID,
		Membership: "pending", Role: "member", RequestID: "request-a",
	}); err != nil {
		t.Fatal(err)
	}
	transport.replacement = memberRecord{
		RoomID: roomID, ChannelID: channelID, UserID: userID,
		Membership: "pending", Role: "member", RequestID: "request-b",
	}
	transport.armed.Store(true)

	result := mustHandle[map[string]any](t, service, "channels.public.join_result", map[string]any{
		"room_id": roomID, "channel_id": channelID, "user_id": userID,
		"request_id": "request-a", "status": "rejected",
	})
	current := result["member"].(memberRecord)
	if result["status"] != "pending" || current.RequestID != "request-b" || current.Membership != "pending" {
		t.Fatalf("generation A rejection did not return generation B: %#v", result)
	}
	persisted, found, err := service.lookupMember(context.Background(), roomID, userID)
	if err != nil || !found || persisted.RequestID != "request-b" || persisted.Membership != "pending" {
		t.Fatalf("generation A rejection overwrote generation B: found=%v member=%#v err=%v", found, persisted, err)
	}
}

func TestRefreshRoomMembersPreservesOtherActivePublicJoinWorkflow(t *testing.T) {
	const (
		roomID    = "!channel:example.com"
		channelID = "channel_1"
		targetID  = "@alice:example.com"
		otherID   = "@bob:remote.example"
	)
	transport := &recordingTransport{
		roomID: roomID,
		roomMembers: []memberRecord{
			{RoomID: roomID, ChannelID: channelID, UserID: targetID, Membership: "join", Role: "member"},
			// Matrix still exposes the approval invite, while Bob's ProductCore
			// workflow is already resolving the same public join generation.
			{RoomID: roomID, ChannelID: channelID, UserID: otherID, Membership: "invite", Role: "member"},
		},
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	for _, member := range []memberRecord{
		{RoomID: roomID, ChannelID: channelID, UserID: targetID, Membership: "join", Role: "member", RequestID: "request-a"},
		{RoomID: roomID, ChannelID: channelID, UserID: otherID, Membership: "joining", Role: "member", RequestID: "request-b"},
	} {
		if err := service.saveMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
	}

	_, stale, err := service.refreshRoomMembersForGeneration(context.Background(), roomID, channelID, memberRecord{
		RoomID: roomID, ChannelID: channelID, UserID: targetID, Membership: "join", Role: "member", RequestID: "request-a",
	})
	if err != nil || stale {
		t.Fatalf("refresh result = stale:%v err:%v", stale, err)
	}
	other, found, err := service.lookupMember(context.Background(), roomID, otherID)
	if err != nil || !found || other.Membership != "joining" || other.RequestID != "request-b" {
		t.Fatalf("room refresh downgraded another active public join: found=%v member=%#v err=%v", found, other, err)
	}
}
