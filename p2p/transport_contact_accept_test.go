package p2p

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
)

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

func TestContactAcceptRetriesFederatedJoinInProgress(t *testing.T) {
	transport := &failOnceJoinTransport{
		err: errors.New(`contents=[] msg={
				"errcode": "M_LIMIT_EXCEEDED",
				"error": "There is already a federated join to this room in progress. Please wait for it to finish."
			} code=429 wrapped=`),
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
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
		"room_id":   "!dm:remote.example",
		"peer_mxid": "@alice:remote.example",
		"domain":    "remote.example",
	})

	if accepted.Status != "accepted" || accepted.RoomID != "!dm:remote.example" {
		t.Fatalf("expected accepted contact after retry, got %#v", accepted)
	}
	if transport.attempts != 2 || len(transport.joins) != 2 {
		t.Fatalf("expected one retry for federated join in progress, attempts=%d joins=%#v", transport.attempts, transport.joins)
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

func TestAcceptDirectContactRoomUsesCompatibleJoinResultRoom(t *testing.T) {
	tests := []struct {
		name          string
		joinRoomID    string
		wantFinalRoom string
	}{
		{name: "new room uses raw result", joinRoomID: " !joined:example.com ", wantFinalRoom: " !joined:example.com "},
		{name: "empty result keeps requested room", wantFinalRoom: "!old:example.com"},
		{name: "blank result keeps requested room", joinRoomID: "  ", wantFinalRoom: "!old:example.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &contactAcceptJoinResultTransport{resultRoomID: tt.joinRoomID}
			service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
			bootstrapService(t, service)
			roomID, apiErr := service.acceptDirectContactRoom(context.Background(), contactStorageRecord{
				RoomID: "!old:example.com", PeerMXID: "@alice:example.com", Status: "pending_inbound",
			}, nil)
			if apiErr != nil || roomID != tt.wantFinalRoom {
				t.Fatalf("acceptDirectContactRoom = (%q, %#v), want %q", roomID, apiErr, tt.wantFinalRoom)
			}
			if len(transport.joinRequests) != 1 || transport.joinRequests[0].RoomIDOrAlias != "!old:example.com" {
				t.Fatalf("join requests = %#v", transport.joinRequests)
			}
		})
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
	if room.CreatorMXID != "@owner:example.com" || room.CreatorDisplayName != "Owner B" || room.CreatorAvatarURL != "mxc://example.com/owner-b" ||
		room.Name != "Alice" || room.Visibility != "private" || len(room.InviteMXIDs) != 1 || room.InviteMXIDs[0] != "@alice:remote.example" ||
		!room.IsDirect || room.RoomType != DirextalkRoomTypeDirect {
		t.Fatalf("unexpected replacement direct room request: %#v", room)
	}
	wantState := []RoomStateEvent{roomProfileForDirect(
		"Alice", "@owner:example.com", "@alice:remote.example", "Owner B", "mxc://example.com/owner-b", "", false,
	)}
	if !reflect.DeepEqual(room.InitialState, wantState) {
		t.Fatalf("replacement initial state = %#v, want %#v", room.InitialState, wantState)
	}
}

func TestContactAcceptDoesNotPersistWhenReplacementCreateFails(t *testing.T) {
	transport := &failingAcceptReplacementTransport{
		failOnceJoinTransport: failOnceJoinTransport{
			err:      productpolicy.Forbidden("direct room join requires invite"),
			failures: 100,
		},
		createErr: productpolicy.Forbidden("replacement denied"),
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	existing := contactRecord{
		RoomID: "!old-dm:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice", Status: "pending_inbound",
	}
	if err := service.saveContact(context.Background(), existing); err != nil {
		t.Fatal(err)
	}

	result, apiErr := service.Handle(context.Background(), "contacts.requests.accept", map[string]any{"room_id": existing.RoomID})
	if result != nil || apiErr == nil || apiErr.Status != http.StatusForbidden || apiErr.Error != "replacement denied" {
		t.Fatalf("replacement create failure = (%#v, %#v)", result, apiErr)
	}
	contact, ok, err := service.lookupContactByRoom(context.Background(), existing.RoomID)
	if err != nil || !ok || contact.Status != existing.Status {
		t.Fatalf("contact after replacement failure = %#v ok=%v err=%v, want pending snapshot", contact, ok, err)
	}
	if len(transport.joinRequests) != 1 || len(transport.createRooms) != 1 {
		t.Fatalf("replacement failure calls = joins %#v creates %#v", transport.joinRequests, transport.createRooms)
	}
}

func TestContactAcceptDoesNotPersistWhenJoinFails(t *testing.T) {
	transport := &failOnceJoinTransport{
		err:      productpolicy.Forbidden("join denied"),
		failures: 100,
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	existing := contactRecord{
		RoomID: "!dm:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice", Status: "pending_inbound",
	}
	if err := service.saveContact(context.Background(), existing); err != nil {
		t.Fatal(err)
	}

	result, apiErr := service.Handle(context.Background(), "contacts.requests.accept", map[string]any{"room_id": existing.RoomID})
	if result != nil || apiErr == nil || apiErr.Status != http.StatusForbidden || apiErr.Error != "join denied" {
		t.Fatalf("contacts.requests.accept join failure = (%#v, %#v)", result, apiErr)
	}
	contact, ok, err := service.lookupContactByRoom(context.Background(), existing.RoomID)
	if err != nil || !ok || contact.Status != existing.Status {
		t.Fatalf("contact after failed join = %#v ok=%v err=%v, want pending snapshot", contact, ok, err)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("non-reactivation join failure created replacement room: %#v", transport.createRooms)
	}
}
