package p2p

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
)

func TestContactReactivateTreatsAlreadyJoinedAsRestored(t *testing.T) {
	transport := &failingInviteTransport{err: errors.New("user is already joined to room")}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:example.com",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "contacts.reactivate", map[string]any{
		"requester_mxid": "@alice:remote.example",
	})

	if result["status"] != "invited" || result["room_id"] != "!old-dm:example.com" {
		t.Fatalf("expected already-joined reactivation to return old room, got %#v", result)
	}
	if len(transport.inviteRequests) != 1 || transport.inviteRequests[0].RoomID != "!old-dm:example.com" {
		t.Fatalf("expected one retained-room invite attempt, got %#v", transport.inviteRequests)
	}
}

func TestContactReactivateInvitesOnlyRetainedAcceptedPeer(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "remote.example"}, transport)
	bootstrapService(t, service)
	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Remote Owner",
		"avatar_url":   "mxc://remote.example/owner",
	})
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
	if len(transport.inviteRequests) != 1 {
		t.Fatalf("expected one retained-room invite attempt, got %#v", transport.inviteRequests)
	}
	invite := transport.inviteRequests[0]
	if invite.RoomID != "!old-dm:example.com" || invite.InviterMXID != "@owner:remote.example" || invite.InviteeMXID != "@owner:example.com" || !invite.IsDirect {
		t.Fatalf("unexpected retained-room invite: %#v", invite)
	}
	wantState := []RoomStateEvent{roomProfileForDirect(
		"Remote Owner", "@owner:remote.example", "@owner:example.com", "Remote Owner", "mxc://remote.example/owner", "", false,
	)}
	if !reflect.DeepEqual(invite.InviteRoomState, wantState) {
		t.Fatalf("retained-room invite state = %#v, want %#v", invite.InviteRoomState, wantState)
	}

	if _, apiErr := service.Handle(context.Background(), "contacts.reactivate", map[string]any{
		"room_id":        "!other:example.com",
		"requester_mxid": "@owner:example.com",
	}); apiErr == nil || apiErr.Status != http.StatusNotFound {
		t.Fatalf("expected mismatched room reactivation to be rejected, got %#v", apiErr)
	}
}

func TestContactReactivateMapsInviteFailureWithoutChangingContact(t *testing.T) {
	transport := &failingInviteTransport{err: productpolicy.Forbidden("invite denied")}
	service := NewServiceWithTransport(Config{ServerName: "remote.example"}, transport)
	bootstrapService(t, service)
	existing := contactRecord{
		RoomID: "!old-dm:example.com", PeerMXID: "@owner:example.com", DisplayName: "Owner", Status: "accepted",
	}
	if err := service.saveContact(context.Background(), existing); err != nil {
		t.Fatal(err)
	}

	result, apiErr := service.Handle(context.Background(), "contacts.reactivate", map[string]any{
		"room_id": existing.RoomID, "requester_mxid": existing.PeerMXID,
	})
	if result != nil || apiErr == nil || apiErr.Status != http.StatusForbidden || apiErr.Error != "invite denied" {
		t.Fatalf("contacts.reactivate invite failure = (%#v, %#v)", result, apiErr)
	}
	contact, ok, err := service.lookupContactByPeer(context.Background(), existing.PeerMXID)
	if err != nil || !ok || !reflect.DeepEqual(contact, existing) {
		t.Fatalf("contact after invite failure = %#v ok=%v err=%v, want %#v", contact, ok, err, existing)
	}
}
