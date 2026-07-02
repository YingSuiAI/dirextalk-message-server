package p2p

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestBlockActionsListContactsAndRemove(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)

	mustHandle[map[string]any](t, service, "blocks.add", map[string]any{
		"target_type":  "contact",
		"peer_mxid":    "@alice:remote.example",
		"display_name": "Alice",
		"avatar_url":   "mxc://remote.example/alice",
	})

	list := mustHandle[map[string]any](t, service, "blocks.list", nil)
	if _, ok := list["blocks"]; ok {
		t.Fatalf("blocks.list should only return contacts, got %#v", list)
	}
	if _, ok := list["groups"]; ok {
		t.Fatalf("blocks.list should not return groups, got %#v", list)
	}
	if _, ok := list["channels"]; ok {
		t.Fatalf("blocks.list should not return channels, got %#v", list)
	}
	contacts, ok := list["contacts"].([]blockRecord)
	if !ok || len(contacts) != 1 {
		t.Fatalf("expected blacklist contacts list, got %#v", list)
	}
	if contacts[0].DisplayName != "Alice" || contacts[0].AvatarURL != "mxc://remote.example/alice" {
		t.Fatalf("expected contact block display snapshot, got %#v", contacts[0])
	}

	removed := mustHandle[map[string]any](t, service, "blocks.remove", map[string]any{
		"target_type": "contact",
		"peer_mxid":   "@alice:remote.example",
	})
	if removed["status"] != "ok" || removed["removed"] != true {
		t.Fatalf("expected contact block removal response, got %#v", removed)
	}
	list = mustHandle[map[string]any](t, service, "blocks.list", nil)
	contacts, ok = list["contacts"].([]blockRecord)
	if !ok || len(contacts) != 0 {
		t.Fatalf("expected contact block removed from list, got %#v", list)
	}
}

func TestBlockActionsRejectNonContactTargets(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	for _, params := range []map[string]any{
		{"target_type": "group", "room_id": "!group:example.com"},
		{"target_type": "channel", "room_id": "!channel:example.com"},
		{"target_type": "room", "room_id": "!room:example.com"},
	} {
		if _, apiErr := service.Handle(context.Background(), "blocks.add", params); apiErr == nil || apiErr.Status != http.StatusBadRequest || !strings.Contains(apiErr.Error, "target_type must be contact") {
			t.Fatalf("expected non-contact block target to be rejected, params=%#v err=%#v", params, apiErr)
		}
	}
}

func TestBlockedContactsRejectApplications(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)

	mustHandle[map[string]any](t, service, "blocks.add", map[string]any{
		"target_type": "contact",
		"peer_mxid":   "@alice:remote.example",
	})

	assertAlreadyBlocked(t, service, "contacts.request", map[string]any{
		"mxid": "@alice:remote.example",
	})
}

func TestBlockedContactInviteProjectionIsIgnored(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	mustHandle[map[string]any](t, service, "blocks.add", map[string]any{
		"target_type": "contact",
		"peer_mxid":   "@alice:remote.example",
	})

	if err := service.savePendingInboundContact(context.Background(), contactRecord{
		RoomID:      "!direct:example.com",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	contacts := mustHandle[map[string]any](t, service, "contacts.list", nil)["contacts"].([]contactRecord)
	if len(contacts) != 0 {
		t.Fatalf("expected blocked inbound contact invite to stay out of contacts list, got %#v", contacts)
	}
}

func assertAlreadyBlocked(t *testing.T, service *Service, action string, params map[string]any) {
	t.Helper()
	_, apiErr := service.Handle(context.Background(), action, params)
	if apiErr == nil {
		t.Fatalf("%s succeeded unexpectedly", action)
	}
	if apiErr.Status != http.StatusForbidden || !strings.Contains(apiErr.Error, "already blocked") {
		t.Fatalf("expected %s to fail with already blocked 403, got %#v", action, apiErr)
	}
}
