package p2p

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

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

	event := trustedStateEvent(t, "!direct:example.com", "@alice:remote.example", "m.room.member", service.OwnerMXID(), map[string]any{
		"membership": "invite",
		"is_direct":  true,
	})
	if err := service.ProjectRoomEvent(context.Background(), event); err != nil {
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
