package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
	dendritetest "github.com/YingSuiAI/dirextalk-message-server/test"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
)

func TestDendriteTransportSendMessageAppliesProductPolicy(t *testing.T) {
	owner := dendritetest.NewUser(t)
	member := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	room.CreateAndInsert(t, member, spec.MRoomMember, map[string]any{"membership": spec.Join}, dendritetest.WithStateKey(member.ID))
	room.CreateAndInsert(t, owner, DirextalkRoomProfileEventType, map[string]any{
		"room_type":        DirextalkRoomTypeChannel,
		"comments_enabled": false,
	}, dendritetest.WithStateKey(""))

	rsAPI := &policyTransportRoomserver{
		roomID: room.ID,
		state:  room.CurrentState(),
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

	_, err := transport.SendMessage(context.Background(), SendMessageRequest{
		SenderMXID: member.ID,
		RoomID:     room.ID,
		EventType:  "m.room.message",
		Content:    map[string]any{"msgtype": "m.text", "body": "blocked", "p2p_kind": "channel_comment"},
	})

	if err == nil || !strings.Contains(err.Error(), "channel comments are disabled") {
		t.Fatalf("expected product policy error, got %v", err)
	}
	if rsAPI.signingIdentityCalled {
		t.Fatalf("expected product policy to reject before building and signing the message event")
	}
}

func TestDendriteTransportSendMessageRejectsBlockedDirectMessageBeforeWrite(t *testing.T) {
	owner := dendritetest.NewUser(t)
	member := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	room.CreateAndInsert(t, member, spec.MRoomMember, map[string]any{"membership": spec.Join}, dendritetest.WithStateKey(member.ID))
	room.CreateAndInsert(t, owner, DirextalkRoomProfileEventType, map[string]any{
		"room_type":      DirextalkRoomTypeDirect,
		"requester_mxid": owner.ID,
		"target_mxid":    member.ID,
	}, dendritetest.WithStateKey(""))

	rsAPI := &policyTransportRoomserver{
		roomID: room.ID,
		state:  room.CurrentState(),
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)
	transport.SetBlockedDirectMessageChecker(func(_ context.Context, roomID, senderMXID string) (bool, error) {
		if roomID != room.ID || senderMXID != member.ID {
			t.Fatalf("block checker received room=%q sender=%q", roomID, senderMXID)
		}
		return true, nil
	})

	_, err := transport.SendMessage(context.Background(), SendMessageRequest{
		SenderMXID: owner.ID,
		RoomID:     room.ID,
		EventType:  "m.room.message",
		Content:    map[string]any{"msgtype": "m.text", "body": "blocked"},
	})

	if err == nil || !strings.Contains(err.Error(), "sender is blocked in this dirextalk direct room") {
		t.Fatalf("expected blocked direct message error, got %v", err)
	}
	if rsAPI.signingIdentityCalled {
		t.Fatalf("expected blocked direct message to reject before signing")
	}
	if rsAPI.inputRoomEventsCalled {
		t.Fatalf("expected blocked direct message to reject before roomserver write")
	}
}

func TestDendriteTransportRedactEventAppliesProductPolicy(t *testing.T) {
	owner := dendritetest.NewUser(t)
	member := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	room.CreateAndInsert(t, member, spec.MRoomMember, map[string]any{"membership": spec.Join}, dendritetest.WithStateKey(member.ID))
	room.CreateAndInsert(t, owner, DirextalkRoomProfileEventType, map[string]any{
		"room_type":        DirextalkRoomTypeChannel,
		"comments_enabled": true,
	}, dendritetest.WithStateKey(""))
	target := room.CreateAndInsert(t, owner, "m.room.message", map[string]any{"msgtype": "m.text", "body": "owned"})

	rsAPI := &policyTransportRoomserver{
		roomID: room.ID,
		state:  room.CurrentState(),
		events: map[string]*types.HeaderedEvent{
			target.EventID(): target,
		},
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

	_, err := transport.RedactEvent(context.Background(), RedactEventRequest{
		SenderMXID: member.ID,
		RoomID:     room.ID,
		EventID:    target.EventID(),
	})

	if err == nil || !strings.Contains(err.Error(), "redact another sender") {
		t.Fatalf("expected product policy redaction error, got %v", err)
	}
	if rsAPI.signingIdentityCalled {
		t.Fatalf("expected product policy to reject before building and signing the redaction event")
	}
}

func TestDendriteTransportInviteUserAppliesProductPolicy(t *testing.T) {
	owner := dendritetest.NewUser(t)
	member := dendritetest.NewUser(t)
	invitee := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	room.CreateAndInsert(t, member, spec.MRoomMember, map[string]any{"membership": spec.Join}, dendritetest.WithStateKey(member.ID))
	room.CreateAndInsert(t, owner, DirextalkRoomProfileEventType, map[string]any{
		"room_type":     DirextalkRoomTypeGroup,
		"invite_policy": "owner",
	}, dendritetest.WithStateKey(""))
	rsAPI := &policyTransportRoomserver{
		roomID: room.ID,
		state:  room.CurrentState(),
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

	err := transport.InviteUser(context.Background(), InviteUserRequest{
		RoomID:      room.ID,
		InviterMXID: member.ID,
		InviteeMXID: invitee.ID,
	})

	if err == nil || !strings.Contains(err.Error(), "invite members") {
		t.Fatalf("expected product policy invite error, got %v", err)
	}
	if rsAPI.inviteCalled {
		t.Fatalf("expected product policy to reject before PerformInvite")
	}
}

func TestDendriteTransportJoinRoomAppliesProductPolicy(t *testing.T) {
	owner := dendritetest.NewUser(t)
	requester := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	room.CreateAndInsert(t, owner, DirextalkRoomProfileEventType, map[string]any{
		"room_type":   DirextalkRoomTypeChannel,
		"join_policy": "approval",
	}, dendritetest.WithStateKey(""))
	rsAPI := &policyTransportRoomserver{
		roomID: room.ID,
		state:  room.CurrentState(),
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

	_, err := transport.JoinRoom(context.Background(), JoinRoomRequest{
		RoomIDOrAlias: room.ID,
		UserMXID:      requester.ID,
	})

	if err == nil || !strings.Contains(err.Error(), "approved join request") {
		t.Fatalf("expected product policy join error, got %v", err)
	}
	if rsAPI.joinCalled {
		t.Fatalf("expected product policy to reject before PerformJoin")
	}
}

func TestDendriteTransportJoinRoomLetsRoomserverAcceptPendingDirectInvite(t *testing.T) {
	owner := dendritetest.NewUser(t)
	requester := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	room.CreateAndInsert(t, owner, DirextalkRoomProfileEventType, map[string]any{
		"room_type":      DirextalkRoomTypeDirect,
		"requester_mxid": requester.ID,
		"target_mxid":    owner.ID,
	}, dendritetest.WithStateKey(""))
	rsAPI := &policyTransportRoomserver{
		roomID:    room.ID,
		state:     room.CurrentState(),
		allowJoin: true,
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

	joined, err := transport.JoinRoom(context.Background(), JoinRoomRequest{
		RoomIDOrAlias: room.ID,
		UserMXID:      requester.ID,
	})

	if err != nil {
		t.Fatalf("expected roomserver pending direct invite to decide join, got %v", err)
	}
	if joined.RoomID != room.ID || !rsAPI.joinCalled {
		t.Fatalf("expected PerformJoin to accept the pending direct invite, got joined=%#v joinCalled=%v", joined, rsAPI.joinCalled)
	}
}

func TestDendriteTransportJoinRoomCarriesProfileContent(t *testing.T) {
	owner := dendritetest.NewUser(t)
	requester := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	rsAPI := &policyTransportRoomserver{
		roomID:    room.ID,
		state:     room.CurrentState(),
		allowJoin: true,
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

	_, err := transport.JoinRoom(context.Background(), JoinRoomRequest{
		RoomIDOrAlias: room.ID,
		UserMXID:      requester.ID,
		DisplayName:   "Alice Device",
		AvatarURL:     "mxc://example.com/alice",
	})

	if err != nil {
		t.Fatalf("expected join to succeed, got %v", err)
	}
	if rsAPI.joinContent["displayname"] != "Alice Device" || rsAPI.joinContent["avatar_url"] != "mxc://example.com/alice" {
		t.Fatalf("expected profile content on PerformJoin, got %#v", rsAPI.joinContent)
	}
}

func TestDendriteTransportJoinRoomLeavesIncompleteFederatedImportForRecovery(t *testing.T) {
	owner := dendritetest.NewUser(t)
	requester := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	rsAPI := &policyTransportRoomserver{
		roomID:    room.ID,
		state:     room.CurrentState(),
		allowJoin: true,
		memberships: map[string]map[string]string{
			room.ID: {requester.ID: string(spec.Join)},
		},
		latestStateResponses: []roomserverAPI.QueryLatestEventsAndStateResponse{{RoomExists: false}},
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)
	request := JoinRoomRequest{
		RoomIDOrAlias: room.ID,
		UserMXID:      requester.ID,
		DisplayName:   "Requester",
		AvatarURL:     "mxc://test/requester",
		ServerNames:   []string{"remote.test"},
	}

	joined, err := transport.JoinRoom(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "federated join in progress") {
		t.Fatalf("incomplete room result = %#v, err=%v", joined, err)
	}
	if len(rsAPI.purgedRooms) != 0 {
		t.Fatalf("incomplete import was destructively purged: %#v", rsAPI.purgedRooms)
	}
	if len(rsAPI.joinRequests) != 1 {
		t.Fatalf("PerformJoin calls = %d, want 1", len(rsAPI.joinRequests))
	}
	if rsAPI.latestStateQueries != 1 {
		t.Fatalf("readiness queries = %d, want 1", rsAPI.latestStateQueries)
	}
}

func TestDendriteTransportCreateRoomCarriesCreatorProfile(t *testing.T) {
	rsAPI := &policyTransportRoomserver{}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

	for _, roomType := range []string{DirextalkRoomTypeDirect, DirextalkRoomTypeGroup, DirextalkRoomTypeChannel} {
		t.Run(roomType, func(t *testing.T) {
			rsAPI.createRoomRequest = nil
			_, err := transport.CreateRoom(context.Background(), CreateRoomRequest{
				CreatorMXID:        "@owner:test",
				CreatorDisplayName: "Owner",
				CreatorAvatarURL:   "mxc://test/owner-avatar",
				Name:               "Product Room",
				Visibility:         "private",
				RoomType:           roomType,
				IsDirect:           roomType == DirextalkRoomTypeDirect,
				InitialState: []RoomStateEvent{{
					Type:     DirextalkRoomProfileEventType,
					StateKey: "",
					Content: map[string]any{
						"room_type": roomType,
					},
				}},
			})
			if err != nil {
				t.Fatalf("expected create room to succeed, got %v", err)
			}
			if rsAPI.createRoomRequest == nil {
				t.Fatalf("expected PerformCreateRoom request")
			}
			if rsAPI.createRoomRequest.UserDisplayName != "Owner" || rsAPI.createRoomRequest.UserAvatarURL != "mxc://test/owner-avatar" {
				t.Fatalf("expected creator profile on PerformCreateRoom, got %#v", rsAPI.createRoomRequest)
			}
		})
	}
}

func TestDendriteTransportCreateRoomIdempotencyKeyReusesCommittedRoom(t *testing.T) {
	rsAPI := &policyTransportRoomserver{knownRooms: make(map[string]bool)}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)
	request := CreateRoomRequest{
		CreatorMXID: "@owner:test", Name: "Direct", Visibility: "private",
		RoomType: DirextalkRoomTypeDirect, IsDirect: true, IdempotencyKey: "op_contact_123",
	}

	first, err := transport.CreateRoom(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := transport.CreateRoom(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.RoomID == "" || second.RoomID != first.RoomID || rsAPI.createRoomCalls != 1 {
		t.Fatalf("idempotent create = first=%#v second=%#v calls=%d", first, second, rsAPI.createRoomCalls)
	}
	if len(rsAPI.purgedRooms) != 0 {
		t.Fatalf("committed room was purged: %#v", rsAPI.purgedRooms)
	}
}

func TestDendriteTransportCreateRoomIdempotencyRepairsMissingInvite(t *testing.T) {
	rsAPI := &policyTransportRoomserver{knownRooms: make(map[string]bool), omitCreateInvites: true}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)
	request := CreateRoomRequest{
		CreatorMXID: "@owner:test", Name: "Direct", Visibility: "private",
		RoomType: DirextalkRoomTypeDirect, IsDirect: true, IdempotencyKey: "op_contact_partial_invite",
		InviteMXIDs: []string{"@alice:remote.test"},
		InitialState: []RoomStateEvent{{
			Type: DirextalkRoomProfileEventType, StateKey: "", Content: map[string]any{"room_type": DirextalkRoomTypeDirect},
		}},
	}

	first, err := transport.CreateRoom(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := transport.CreateRoom(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.RoomID == "" || second.RoomID != first.RoomID || rsAPI.createRoomCalls != 1 || rsAPI.inviteCalls != 1 {
		t.Fatalf("partial create recovery = first=%#v second=%#v createCalls=%d inviteCalls=%d", first, second, rsAPI.createRoomCalls, rsAPI.inviteCalls)
	}
	if got := rsAPI.memberships[first.RoomID]["@alice:remote.test"]; got != string(spec.Invite) {
		t.Fatalf("missing invite was not repaired: membership=%q", got)
	}
}

func TestDendriteTransportCreateRoomIdempotencyReconcilesCommittedCreateError(t *testing.T) {
	rsAPI := &policyTransportRoomserver{
		knownRooms: make(map[string]bool), omitCreateInvites: true,
		createResponse: &util.JSONResponse{Code: http.StatusInternalServerError, JSON: map[string]any{"error": "response lost"}},
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)
	request := CreateRoomRequest{
		CreatorMXID: "@owner:test", Name: "Direct", Visibility: "private",
		RoomType: DirextalkRoomTypeDirect, IsDirect: true, IdempotencyKey: "op_contact_lost_create_response",
		InviteMXIDs: []string{"@alice:remote.test"},
		InitialState: []RoomStateEvent{{
			Type: DirextalkRoomProfileEventType, StateKey: "", Content: map[string]any{"room_type": DirextalkRoomTypeDirect},
		}},
	}

	result, err := transport.CreateRoom(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.RoomID == "" || rsAPI.createRoomCalls != 1 || rsAPI.inviteCalls != 1 {
		t.Fatalf("committed create error was not reconciled: result=%#v createCalls=%d inviteCalls=%d", result, rsAPI.createRoomCalls, rsAPI.inviteCalls)
	}
}

func TestDendriteTransportCreateRoomIdempotencyRebuildsOwnedCreateOnlyRoom(t *testing.T) {
	rsAPI := &policyTransportRoomserver{
		knownRooms:        make(map[string]bool),
		omitCreateCreator: true,
		createOnlyPurge:   true,
		createResponse:    &util.JSONResponse{Code: http.StatusInternalServerError, JSON: map[string]any{"error": "creator input failed"}},
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)
	request := CreateRoomRequest{
		CreatorMXID: "@owner:test", Name: "Direct", Visibility: "private",
		RoomType: DirextalkRoomTypeDirect, IsDirect: true, IdempotencyKey: "op_contact_missing_creator",
	}

	first, err := transport.CreateRoom(context.Background(), request)
	if err == nil || first.RoomID == "" || !strings.Contains(err.Error(), "creator is not joined") {
		t.Fatalf("fault-injected create should expose a retryable partial room: result=%#v err=%v", first, err)
	}
	createEvent := mustPolicyTransportCreateEvent(t, first.RoomID, request.CreatorMXID, request.IdempotencyKey, rsAPI.createRoomRequest)
	rsAPI.latestStateResponses = []roomserverAPI.QueryLatestEventsAndStateResponse{{
		RoomExists:  true,
		RoomVersion: gomatrixserverlib.RoomVersionV10,
		LatestEvents: []string{
			createEvent.EventID(),
		},
		StateEvents: []*types.HeaderedEvent{createEvent},
	}}
	rsAPI.omitCreateCreator = false
	rsAPI.createResponse = nil

	second, err := transport.CreateRoom(context.Background(), request)
	if err != nil {
		t.Fatalf("expected create-only room to be rebuilt, got %v", err)
	}
	if second.RoomID != first.RoomID || rsAPI.createRoomCalls != 2 {
		t.Fatalf("create-only recovery = first=%#v second=%#v createCalls=%d", first, second, rsAPI.createRoomCalls)
	}
	if len(rsAPI.createRoomIDs) != 2 || rsAPI.createRoomIDs[0] != first.RoomID || rsAPI.createRoomIDs[1] != first.RoomID {
		t.Fatalf("rebuilt room IDs = %#v, want two calls for %q", rsAPI.createRoomIDs, first.RoomID)
	}
	if len(rsAPI.purgedRooms) != 1 || rsAPI.purgedRooms[0] != first.RoomID {
		t.Fatalf("purged rooms = %#v, want [%q]", rsAPI.purgedRooms, first.RoomID)
	}

	third, err := transport.CreateRoom(context.Background(), request)
	if err != nil || third.RoomID != first.RoomID || rsAPI.createRoomCalls != 2 || len(rsAPI.purgedRooms) != 1 {
		t.Fatalf("rebuilt room did not become stable: result=%#v err=%v createCalls=%d purges=%#v", third, err, rsAPI.createRoomCalls, rsAPI.purgedRooms)
	}
}

func TestDendriteTransportCreateRoomIdempotencyDoesNotPurgeCreatorJoinLandingDuringRecovery(t *testing.T) {
	rsAPI := &policyTransportRoomserver{
		knownRooms:        make(map[string]bool),
		omitCreateCreator: true,
		createResponse:    &util.JSONResponse{Code: http.StatusInternalServerError, JSON: map[string]any{"error": "creator input response lost"}},
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)
	request := CreateRoomRequest{
		CreatorMXID: "@owner:test", Name: "Direct", Visibility: "private",
		RoomType: DirextalkRoomTypeDirect, IsDirect: true, IdempotencyKey: "op_contact_creator_join_in_flight",
	}

	first, err := transport.CreateRoom(context.Background(), request)
	if err == nil || first.RoomID == "" || !strings.Contains(err.Error(), "creator is not joined") {
		t.Fatalf("fault-injected create should expose a retryable partial room: result=%#v err=%v", first, err)
	}
	createEvent := mustPolicyTransportCreateEvent(t, first.RoomID, request.CreatorMXID, request.IdempotencyKey, rsAPI.createRoomRequest)
	rsAPI.latestStateResponses = []roomserverAPI.QueryLatestEventsAndStateResponse{{
		RoomExists: true, RoomVersion: gomatrixserverlib.RoomVersionV10,
		LatestEvents: []string{createEvent.EventID()}, StateEvents: []*types.HeaderedEvent{createEvent},
	}}
	rsAPI.beforeCreateOnlyPurge = func() {
		rsAPI.memberships[first.RoomID][request.CreatorMXID] = string(spec.Join)
	}
	rsAPI.createResponse = nil

	second, err := transport.CreateRoom(context.Background(), request)
	if err != nil || second.RoomID != first.RoomID {
		t.Fatalf("creator join landing during recovery should reconcile the original room: result=%#v err=%v", second, err)
	}
	if len(rsAPI.purgedRooms) != 0 || rsAPI.createRoomCalls != 1 {
		t.Fatalf("creator join landing during recovery was destroyed: createCalls=%d purges=%#v", rsAPI.createRoomCalls, rsAPI.purgedRooms)
	}
	if rsAPI.createOnlyPurgeCalls != 1 {
		t.Fatalf("conditional recovery calls = %d, want 1", rsAPI.createOnlyPurgeCalls)
	}
}

func TestDendriteTransportCreateRoomIdempotencyDoesNotPurgeUnownedCreateOnlyRoom(t *testing.T) {
	rsAPI := &policyTransportRoomserver{knownRooms: make(map[string]bool), omitCreateCreator: true}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)
	request := CreateRoomRequest{
		CreatorMXID: "@owner:test", Name: "Direct", Visibility: "private",
		RoomType: DirextalkRoomTypeDirect, IsDirect: true, IdempotencyKey: "op_contact_unowned_collision",
	}

	first, err := transport.CreateRoom(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	createEvent := mustPolicyTransportCreateEvent(t, first.RoomID, request.CreatorMXID, request.IdempotencyKey, rsAPI.createRoomRequest)
	content := map[string]any{}
	if err := json.Unmarshal(createEvent.Content(), &content); err != nil {
		t.Fatal(err)
	}
	delete(content, "io.dirextalk.create_operation")
	createEvent = mustPolicyTransportEvent(t, first.RoomID, request.CreatorMXID, spec.MRoomCreate, "", content, 1, nil, nil)
	rsAPI.latestStateResponses = []roomserverAPI.QueryLatestEventsAndStateResponse{{
		RoomExists: true, RoomVersion: gomatrixserverlib.RoomVersionV10,
		LatestEvents: []string{createEvent.EventID()}, StateEvents: []*types.HeaderedEvent{createEvent},
	}}

	second, err := transport.CreateRoom(context.Background(), request)
	if err == nil || second.RoomID != first.RoomID || !strings.Contains(err.Error(), "creator is not joined") {
		t.Fatalf("unowned room should remain an incomplete conflict: first=%#v second=%#v err=%v", first, second, err)
	}
	if rsAPI.createRoomCalls != 1 || len(rsAPI.purgedRooms) != 0 {
		t.Fatalf("unowned room was mutated: createCalls=%d purges=%#v", rsAPI.createRoomCalls, rsAPI.purgedRooms)
	}
}

func TestDendriteTransportCreateRoomIdempotencyDoesNotPurgeOwnedRoomWithOtherState(t *testing.T) {
	rsAPI := &policyTransportRoomserver{knownRooms: make(map[string]bool), omitCreateCreator: true}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)
	request := CreateRoomRequest{
		CreatorMXID: "@owner:test", Name: "Direct", Visibility: "private",
		RoomType: DirextalkRoomTypeDirect, IsDirect: true, IdempotencyKey: "op_contact_partial_state",
	}

	first, err := transport.CreateRoom(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	createEvent := mustPolicyTransportCreateEvent(t, first.RoomID, request.CreatorMXID, request.IdempotencyKey, rsAPI.createRoomRequest)
	profileEvent := mustPolicyTransportEvent(t, first.RoomID, request.CreatorMXID, DirextalkRoomProfileEventType, "", map[string]any{
		"room_type": DirextalkRoomTypeDirect,
	}, 2, []any{createEvent.EventID()}, []any{createEvent.EventID()})
	rsAPI.latestStateResponses = []roomserverAPI.QueryLatestEventsAndStateResponse{{
		RoomExists: true, RoomVersion: gomatrixserverlib.RoomVersionV10,
		LatestEvents: []string{profileEvent.EventID()}, StateEvents: []*types.HeaderedEvent{createEvent, profileEvent},
	}}

	second, err := transport.CreateRoom(context.Background(), request)
	if err == nil || second.RoomID != first.RoomID || !strings.Contains(err.Error(), "creator is not joined") {
		t.Fatalf("room with additional state should remain an incomplete conflict: first=%#v second=%#v err=%v", first, second, err)
	}
	if rsAPI.createRoomCalls != 1 || len(rsAPI.purgedRooms) != 0 {
		t.Fatalf("room with additional state was mutated: createCalls=%d purges=%#v", rsAPI.createRoomCalls, rsAPI.purgedRooms)
	}
}

func mustPolicyTransportCreateEvent(t *testing.T, roomID, creatorMXID, idempotencyKey string, request *roomserverAPI.PerformCreateRoomRequest) *types.HeaderedEvent {
	t.Helper()
	if request == nil {
		t.Fatal("missing PerformCreateRoom request")
	}
	content := map[string]any{}
	if len(request.CreationContent) > 0 {
		if err := json.Unmarshal(request.CreationContent, &content); err != nil {
			t.Fatal(err)
		}
	}
	digest := sha256.Sum256([]byte(idempotencyKey))
	wantMarker := hex.EncodeToString(digest[:])
	if gotMarker, _ := content["io.dirextalk.create_operation"].(string); gotMarker != wantMarker {
		t.Fatalf("deterministic create operation marker = %q, want %q", gotMarker, wantMarker)
	}
	content["creator"] = creatorMXID
	content["room_version"] = request.RoomVersion
	return mustPolicyTransportEvent(t, roomID, creatorMXID, spec.MRoomCreate, "", content, 1, nil, nil)
}

func mustPolicyTransportEvent(
	t *testing.T,
	roomID, senderID, eventType, stateKey string,
	content map[string]any,
	depth int64,
	prevEvents, authEvents []any,
) *types.HeaderedEvent {
	t.Helper()
	return mustPolicyTransportEventVersion(t, gomatrixserverlib.RoomVersionV10, roomID, senderID, eventType, stateKey, content, depth, prevEvents, authEvents)
}

func mustPolicyTransportEventVersion(
	t *testing.T,
	roomVersion gomatrixserverlib.RoomVersion,
	roomID, senderID, eventType, stateKey string,
	content map[string]any,
	depth int64,
	prevEvents, authEvents []any,
) *types.HeaderedEvent {
	t.Helper()
	if prevEvents == nil {
		prevEvents = []any{}
	}
	if authEvents == nil {
		authEvents = []any{}
	}
	stateKeyCopy := stateKey
	builder := gomatrixserverlib.MustGetRoomVersion(roomVersion).NewEventBuilderFromProtoEvent(&gomatrixserverlib.ProtoEvent{
		SenderID: senderID, RoomID: roomID, Type: eventType, StateKey: &stateKeyCopy, Depth: depth, PrevEvents: prevEvents,
	})
	builder.AuthEvents = authEvents
	if err := builder.SetContent(content); err != nil {
		t.Fatal(err)
	}
	event, err := builder.Build(time.Now(), spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	return &types.HeaderedEvent{PDU: event}
}

func TestDendriteTransportGetRoomChannelRequiresChannelRoomType(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile map[string]any
		wantOK  bool
	}{
		{
			name:    "empty profile is not a channel",
			profile: map[string]any{},
		},
		{
			name: "group profile is not a channel",
			profile: map[string]any{
				"room_type": DirextalkRoomTypeGroup,
				"name":      "Group with A, B",
			},
		},
		{
			name: "channel profile without product id is not a channel",
			profile: map[string]any{
				"room_type": DirextalkRoomTypeChannel,
				"name":      "Posts",
			},
		},
		{
			name: "channel profile with Matrix room id as product id is not a channel",
			profile: map[string]any{
				"room_type":  DirextalkRoomTypeChannel,
				"channel_id": "!posts:test",
				"name":       "Posts",
			},
		},
		{
			name: "channel profile is a channel",
			profile: map[string]any{
				"room_type":  DirextalkRoomTypeChannel,
				"channel_id": "ch",
				"name":       "Posts",
			},
			wantOK: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := dendritetest.NewUser(t)
			room := dendritetest.NewRoom(t, owner)
			room.CreateAndInsert(t, owner, DirextalkRoomProfileEventType, tc.profile, dendritetest.WithStateKey(""))
			rsAPI := &policyTransportRoomserver{
				roomID: room.ID,
				state:  room.CurrentState(),
			}
			transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

			ch, ok, err := transport.GetRoomChannel(context.Background(), room.ID)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if ok != tc.wantOK {
				t.Fatalf("expected ok=%v, got ok=%v channel=%#v", tc.wantOK, ok, ch)
			}
			if tc.wantOK && ch.ChannelID != "ch" {
				t.Fatalf("expected channel id ch, got %#v", ch)
			}
		})
	}
}

func TestDendriteTransportReadRoomCreatorUsesCreateSenderAuthority(t *testing.T) {
	const roomID = "!creator:test"
	pseudoSender := spec.SenderIDFromPseudoIDKey(ed25519.NewKeyFromSeed(make([]byte, 32)))
	resolvedCreator, err := spec.NewUserID("@resolved:test", true)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name        string
		event       *types.HeaderedEvent
		senderUsers map[spec.SenderID]*spec.UserID
		wantCreator string
		wantQueries int
	}{
		{
			name: "pseudo sender resolves through roomserver",
			event: mustPolicyTransportEventVersion(t, gomatrixserverlib.RoomVersionPseudoIDs, roomID, string(pseudoSender), spec.MRoomCreate, "", map[string]any{
				"creator": "@spoofed:test",
			}, 1, nil, nil),
			senderUsers: map[spec.SenderID]*spec.UserID{pseudoSender: resolvedCreator},
			wantCreator: "@resolved:test",
			wantQueries: 1,
		},
		{
			name: "raw full mxid is validated fallback",
			event: mustPolicyTransportEvent(t, roomID, "@raw:test", spec.MRoomCreate, "", map[string]any{
				"creator": "@spoofed:test",
			}, 1, nil, nil),
			wantCreator: "@raw:test",
			wantQueries: 1,
		},
		{
			name: "non-authoritative legacy creator is ignored",
			event: mustPolicyTransportEventVersion(t, gomatrixserverlib.RoomVersionPseudoIDs, roomID, string(pseudoSender), spec.MRoomCreate, "", map[string]any{
				"creator": "@spoofed:test",
			}, 1, nil, nil),
			wantQueries: 1,
		},
		{name: "missing create state"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := []*types.HeaderedEvent(nil)
			if tc.event != nil {
				state = []*types.HeaderedEvent{tc.event}
			}
			rsAPI := &policyTransportRoomserver{roomID: roomID, state: state, senderUsers: tc.senderUsers}
			transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

			creator, err := transport.ReadRoomCreator(context.Background(), roomID)
			if err != nil {
				t.Fatal(err)
			}
			if creator != tc.wantCreator || len(rsAPI.senderQueries) != tc.wantQueries {
				t.Fatalf("ReadRoomCreator() = %q queries=%#v, want %q queries=%d", creator, rsAPI.senderQueries, tc.wantCreator, tc.wantQueries)
			}
		})
	}
}

func TestDendriteTransportListRoomMembersResolvesPseudoStateKeys(t *testing.T) {
	const roomID = "!members:test"
	pseudoSender := spec.SenderIDFromPseudoIDKey(ed25519.NewKeyFromSeed(make([]byte, 32)))
	resolvedMember, err := spec.NewUserID("@member:test", true)
	if err != nil {
		t.Fatal(err)
	}
	membership := mustPolicyTransportEventVersion(t, gomatrixserverlib.RoomVersionPseudoIDs, roomID, string(pseudoSender), spec.MRoomMember, string(pseudoSender), map[string]any{
		"membership": "join",
	}, 2, nil, nil)

	for _, tc := range []struct {
		name        string
		senderUsers map[spec.SenderID]*spec.UserID
		wantMembers int
	}{
		{name: "resolved", senderUsers: map[spec.SenderID]*spec.UserID{pseudoSender: resolvedMember}, wantMembers: 1},
		{name: "unresolved is skipped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rsAPI := &policyTransportRoomserver{roomID: roomID, state: []*types.HeaderedEvent{membership}, senderUsers: tc.senderUsers}
			transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)
			members, err := transport.ListRoomMembers(context.Background(), roomID)
			if err != nil {
				t.Fatal(err)
			}
			if len(members) != tc.wantMembers {
				t.Fatalf("ListRoomMembers() = %#v, want %d members", members, tc.wantMembers)
			}
			if tc.wantMembers == 1 && (members[0].UserID != resolvedMember.String() || members[0].Domain != "test") {
				t.Fatalf("resolved pseudo member = %#v", members[0])
			}
		})
	}
}

type policyTransportRoomserver struct {
	roomserverAPI.ClientRoomserverAPI
	roomID                string
	state                 []*types.HeaderedEvent
	events                map[string]*types.HeaderedEvent
	signingIdentityCalled bool
	inputRoomEventsCalled bool
	inviteCalled          bool
	joinCalled            bool
	allowJoin             bool
	joinContent           map[string]interface{}
	createRoomRequest     *roomserverAPI.PerformCreateRoomRequest
	createRoomCalls       int
	createRoomIDs         []string
	knownRooms            map[string]bool
	memberships           map[string]map[string]string
	statePresence         map[string]map[gomatrixserverlib.StateKeyTuple]bool
	omitCreateCreator     bool
	omitCreateInvites     bool
	inviteCalls           int
	createResponse        *util.JSONResponse
	joinRequests          []roomserverAPI.PerformJoinRequest
	purgedRooms           []string
	latestStateResponses  []roomserverAPI.QueryLatestEventsAndStateResponse
	latestStateQueries    int
	createOnlyPurge       bool
	createOnlyPurgeCalls  int
	beforeCreateOnlyPurge func()
	senderUsers           map[spec.SenderID]*spec.UserID
	senderQueryErr        error
	senderQueries         []spec.SenderID
}

func (r *policyTransportRoomserver) InputRoomEvents(_ context.Context, _ *roomserverAPI.InputRoomEventsRequest, _ *roomserverAPI.InputRoomEventsResponse) {
	r.inputRoomEventsCalled = true
}

func (r *policyTransportRoomserver) QueryLatestEventsAndState(_ context.Context, _ *roomserverAPI.QueryLatestEventsAndStateRequest, res *roomserverAPI.QueryLatestEventsAndStateResponse) error {
	responseIndex := r.latestStateQueries
	r.latestStateQueries++
	if responseIndex >= len(r.latestStateResponses) {
		*res = roomserverAPI.QueryLatestEventsAndStateResponse{}
		return nil
	}
	*res = r.latestStateResponses[responseIndex]
	return nil
}

func (r *policyTransportRoomserver) QueryCurrentState(ctx context.Context, req *roomserverAPI.QueryCurrentStateRequest, res *roomserverAPI.QueryCurrentStateResponse) error {
	res.StateEvents = map[gomatrixserverlib.StateKeyTuple]*types.HeaderedEvent{}
	for _, tuple := range req.StateTuples {
		if r.statePresence[req.RoomID][tuple] {
			res.StateEvents[tuple] = &types.HeaderedEvent{}
		}
	}
	for _, tuple := range req.StateTuples {
		if req.AllowWildcards && tuple.StateKey == "*" {
			for _, event := range r.state {
				if event.Type() == tuple.EventType && event.StateKey() != nil {
					res.StateEvents[gomatrixserverlib.StateKeyTuple{EventType: event.Type(), StateKey: *event.StateKey()}] = event
				}
			}
			continue
		}
		for _, event := range r.state {
			if event.Type() == tuple.EventType && event.StateKey() != nil && *event.StateKey() == tuple.StateKey {
				res.StateEvents[tuple] = event
			}
		}
	}
	return nil
}

func (r *policyTransportRoomserver) QueryUserIDForSender(_ context.Context, _ spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	r.senderQueries = append(r.senderQueries, senderID)
	if r.senderQueryErr != nil {
		return nil, r.senderQueryErr
	}
	return r.senderUsers[senderID], nil
}

func (r *policyTransportRoomserver) QueryMembershipForUser(ctx context.Context, req *roomserverAPI.QueryMembershipForUserRequest, res *roomserverAPI.QueryMembershipForUserResponse) error {
	res.RoomExists = true
	if membership := r.memberships[req.RoomID][req.UserID.String()]; membership != "" {
		res.HasBeenInRoom = membership == string(spec.Join) || membership == string(spec.Leave)
		res.IsInRoom = membership == string(spec.Join)
		res.Membership = membership
		return nil
	}
	for _, event := range r.state {
		if event.Type() == spec.MRoomMember && event.StateKey() != nil && *event.StateKey() == req.UserID.String() {
			res.HasBeenInRoom = true
			res.IsInRoom = true
			res.Membership = string(spec.Join)
			return nil
		}
	}
	return nil
}

func (r *policyTransportRoomserver) QuerySenderIDForUser(ctx context.Context, roomID spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	if roomID.String() != r.roomID {
		return nil, fmt.Errorf("unknown room %s", roomID.String())
	}
	senderID := spec.SenderIDFromUserID(userID)
	return &senderID, nil
}

func (r *policyTransportRoomserver) QueryEventsByID(ctx context.Context, req *roomserverAPI.QueryEventsByIDRequest, res *roomserverAPI.QueryEventsByIDResponse) error {
	for _, eventID := range req.EventIDs {
		if event := r.events[eventID]; event != nil {
			res.Events = append(res.Events, event)
		}
	}
	return nil
}

func (r *policyTransportRoomserver) SigningIdentityFor(ctx context.Context, roomID spec.RoomID, sender spec.UserID) (fclient.SigningIdentity, error) {
	r.signingIdentityCalled = true
	return fclient.SigningIdentity{}, fmt.Errorf("policy was not applied before signing")
}

func (r *policyTransportRoomserver) PerformInvite(ctx context.Context, req *roomserverAPI.PerformInviteRequest) error {
	r.inviteCalled = true
	r.inviteCalls++
	if r.memberships != nil {
		roomID := req.InviteInput.RoomID.String()
		if r.memberships[roomID] == nil {
			r.memberships[roomID] = make(map[string]string)
		}
		r.memberships[roomID][req.InviteInput.Invitee.String()] = string(spec.Invite)
		return nil
	}
	return fmt.Errorf("policy was not applied before invite")
}

func (r *policyTransportRoomserver) PerformJoin(ctx context.Context, req *roomserverAPI.PerformJoinRequest) (string, spec.ServerName, error) {
	r.joinCalled = true
	r.joinContent = req.Content
	r.joinRequests = append(r.joinRequests, *req)
	if r.allowJoin {
		return req.RoomIDOrAlias, spec.ServerName("test"), nil
	}
	return "", "", fmt.Errorf("policy was not applied before join")
}

func (r *policyTransportRoomserver) PerformAdminPurgeRoom(_ context.Context, roomID string) error {
	r.purgedRooms = append(r.purgedRooms, roomID)
	return nil
}

func (r *policyTransportRoomserver) PerformAdminPurgeRoomIfCreateOnly(_ context.Context, req *roomserverAPI.PerformAdminPurgeCreateOnlyRoomRequest) (bool, error) {
	r.createOnlyPurgeCalls++
	if r.beforeCreateOnlyPurge != nil {
		r.beforeCreateOnlyPurge()
	}
	if !r.createOnlyPurge {
		return false, nil
	}
	r.purgedRooms = append(r.purgedRooms, req.RoomID)
	return true, nil
}

func (r *policyTransportRoomserver) DefaultRoomVersion() gomatrixserverlib.RoomVersion {
	return gomatrixserverlib.RoomVersionV10
}

func (r *policyTransportRoomserver) PerformCreateRoom(ctx context.Context, userID spec.UserID, roomID spec.RoomID, createRequest *roomserverAPI.PerformCreateRoomRequest) (string, *util.JSONResponse) {
	r.createRoomRequest = createRequest
	r.createRoomCalls++
	r.createRoomIDs = append(r.createRoomIDs, roomID.String())
	if r.knownRooms != nil {
		r.knownRooms[roomID.String()] = true
	}
	if r.memberships == nil {
		r.memberships = make(map[string]map[string]string)
	}
	if r.memberships[roomID.String()] == nil {
		r.memberships[roomID.String()] = make(map[string]string)
	}
	if !r.omitCreateCreator {
		r.memberships[roomID.String()][userID.String()] = string(spec.Join)
	}
	if !r.omitCreateInvites {
		for _, invitee := range createRequest.InvitedUsers {
			r.memberships[roomID.String()][invitee] = string(spec.Invite)
		}
	}
	if r.statePresence == nil {
		r.statePresence = make(map[string]map[gomatrixserverlib.StateKeyTuple]bool)
	}
	if r.statePresence[roomID.String()] == nil {
		r.statePresence[roomID.String()] = make(map[gomatrixserverlib.StateKeyTuple]bool)
	}
	for _, state := range createRequest.InitialState {
		r.statePresence[roomID.String()][gomatrixserverlib.StateKeyTuple{EventType: state.Type, StateKey: state.StateKey}] = true
	}
	return roomID.String(), r.createResponse
}

func (r *policyTransportRoomserver) IsKnownRoom(_ context.Context, roomID spec.RoomID) (bool, error) {
	return r.knownRooms != nil && r.knownRooms[roomID.String()], nil
}
