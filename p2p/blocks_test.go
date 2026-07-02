package p2p

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestBlockActionsListByTypeAndRemove(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Blocked Group",
	})
	channel := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "blocked_channel",
		"room_id":    "!channel:example.com",
		"name":       "Blocked Channel",
	})

	mustHandle[map[string]any](t, service, "blocks.add", map[string]any{
		"target_type":  "contact",
		"peer_mxid":    "@alice:remote.example",
		"display_name": "Alice",
		"avatar_url":   "mxc://remote.example/alice",
	})
	mustHandle[map[string]any](t, service, "blocks.add", map[string]any{
		"target_type": "group",
		"room_id":     group.RoomID,
	})
	mustHandle[map[string]any](t, service, "blocks.add", map[string]any{
		"target_type": "channel",
		"room_id":     channel.RoomID,
	})

	list := mustHandle[map[string]any](t, service, "blocks.list", nil)
	if _, ok := list["blocks"]; ok {
		t.Fatalf("blocks.list should only return grouped contacts/groups/channels, got %#v", list)
	}
	if sliceLen(list["contacts"]) != 1 || sliceLen(list["groups"]) != 1 || sliceLen(list["channels"]) != 1 {
		t.Fatalf("expected blacklist grouped by contact/group/channel, got %#v", list)
	}
	contactBlocks := list["contacts"].([]blockRecord)
	if contactBlocks[0].DisplayName != "Alice" || contactBlocks[0].AvatarURL != "mxc://remote.example/alice" {
		t.Fatalf("expected contact block display snapshot, got %#v", contactBlocks[0])
	}
	groupBlocks := list["groups"].([]blockRecord)
	if groupBlocks[0].DisplayName != "Blocked Group" {
		t.Fatalf("expected group block display name, got %#v", groupBlocks[0])
	}
	channelBlocks := list["channels"].([]blockRecord)
	if channelBlocks[0].TargetID != channel.RoomID || channelBlocks[0].RoomID != channel.RoomID || channelBlocks[0].ChannelID != "" || channelBlocks[0].DisplayName != "Blocked Channel" {
		t.Fatalf("expected channel block to be keyed only by room_id, got %#v", channelBlocks[0])
	}

	removed := mustHandle[map[string]any](t, service, "blocks.remove", map[string]any{
		"target_type": "group",
		"room_id":     group.RoomID,
	})
	if removed["status"] != "ok" || removed["removed"] != true {
		t.Fatalf("expected group block removal response, got %#v", removed)
	}
	list = mustHandle[map[string]any](t, service, "blocks.list", nil)
	if sliceLen(list["contacts"]) != 1 || sliceLen(list["groups"]) != 0 || sliceLen(list["channels"]) != 1 {
		t.Fatalf("expected group block removed from grouped list, got %#v", list)
	}
}

func TestBlockedTargetsRejectApplications(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!blocked-group:example.com",
		"name":    "Blocked Group",
	})
	channel := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "blocked_channel",
		"room_id":     "!blocked-channel:example.com",
		"name":        "Blocked Channel",
		"visibility":  "public",
		"join_policy": "approval",
	})

	mustHandle[map[string]any](t, service, "blocks.add", map[string]any{
		"target_type": "contact",
		"peer_mxid":   "@alice:remote.example",
	})
	mustHandle[map[string]any](t, service, "blocks.add", map[string]any{
		"target_type": "group",
		"room_id":     group.RoomID,
	})
	mustHandle[map[string]any](t, service, "blocks.add", map[string]any{
		"target_type": "channel",
		"room_id":     channel.RoomID,
	})

	assertAlreadyBlocked(t, service, "contacts.request", map[string]any{
		"mxid": "@alice:remote.example",
	})
	assertAlreadyBlocked(t, service, "groups.join", map[string]any{
		"room_id": group.RoomID,
	})
	assertAlreadyBlocked(t, service, "channels.public.join_request", map[string]any{
		"channel_id": channel.ChannelID,
		"room_id":    channel.RoomID,
		"user_mxid":  "@bob:remote.example",
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

func sliceLen(value any) int {
	v := reflect.ValueOf(value)
	if !v.IsValid() || v.Kind() != reflect.Slice {
		return -1
	}
	return v.Len()
}
