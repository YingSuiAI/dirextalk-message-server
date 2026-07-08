package dirextalkdomain

import (
	"strings"
	"testing"
)

func TestNormalizeConversationRecordTrimsDefaultsAndStableID(t *testing.T) {
	record := NormalizeConversationRecord(ConversationRecord{
		MatrixRoomID:     " !room:example.com ",
		Kind:             ConversationKindDirect,
		CreatedByMXID:    " @owner:example.com ",
		PeerMXID:         " @peer:example.com ",
		Title:            " Chat ",
		AvatarURL:        " mxc://avatar ",
		LastEventID:      " $event ",
		LastMessage:      " hello ",
		ProjectionReason: " pending ",
		LastActivityAt:   123,
		ConversationID:   " ",
		Lifecycle:        "",
		ProjectionState:  "",
		CreatedAt:        0,
		UpdatedAt:        0,
	})

	if record.MatrixRoomID != "!room:example.com" {
		t.Fatalf("expected trimmed room id, got %q", record.MatrixRoomID)
	}
	if record.ConversationID != ConversationIDForRoomID("!room:example.com") {
		t.Fatalf("expected stable conversation id, got %q", record.ConversationID)
	}
	if record.Lifecycle != ConversationLifecycleActive {
		t.Fatalf("expected active lifecycle default, got %q", record.Lifecycle)
	}
	if record.ProjectionState != ConversationProjectionReady {
		t.Fatalf("expected ready projection default, got %q", record.ProjectionState)
	}
	if record.CreatedAt <= 0 || record.UpdatedAt != record.CreatedAt {
		t.Fatalf("expected created/updated timestamps to default together, got created=%d updated=%d", record.CreatedAt, record.UpdatedAt)
	}
	if record.Title != "Chat" || record.PeerMXID != "@peer:example.com" || record.LastMessage != "hello" {
		t.Fatalf("expected string fields to be trimmed, got %#v", record)
	}
}

func TestConversationIDForRoomIDIsTrimmedHashPrefix(t *testing.T) {
	id := ConversationIDForRoomID(" !room:example.com ")
	if id != ConversationIDForRoomID("!room:example.com") {
		t.Fatalf("expected room id whitespace not to affect conversation id")
	}
	if !strings.HasPrefix(id, "conv_") || len(id) != len("conv_")+24 {
		t.Fatalf("expected 12-byte hex hash prefix id, got %q", id)
	}
}

func TestConversationFromGroupAndChannelUseSharedRecords(t *testing.T) {
	group := ConversationFromGroup(GroupRecord{
		RoomID:    "!group:example.com",
		Name:      "Group",
		AvatarURL: "mxc://group",
	})
	if group.Kind != ConversationKindGroup || group.MatrixRoomID != "!group:example.com" || group.Title != "Group" {
		t.Fatalf("unexpected group conversation: %#v", group)
	}

	channel := ConversationFromChannel(Channel{
		RoomID:    "!channel:example.com",
		Name:      "Channel",
		AvatarURL: "mxc://channel",
	})
	if channel.Kind != ConversationKindChannel || channel.MatrixRoomID != "!channel:example.com" || channel.Title != "Channel" {
		t.Fatalf("unexpected channel conversation: %#v", channel)
	}
}

func TestConversationFromContactUsesSharedRecord(t *testing.T) {
	accepted := ConversationFromContact(ContactRecord{
		RoomID:      "!dm:example.com",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		AvatarURL:   "mxc://remote.example/alice",
		Status:      "accepted",
	})
	if accepted.Kind != ConversationKindDirect ||
		accepted.Lifecycle != ConversationLifecycleActive ||
		accepted.MatrixRoomID != "!dm:example.com" ||
		accepted.PeerMXID != "@alice:remote.example" ||
		accepted.Title != "Alice" ||
		accepted.AvatarURL != "mxc://remote.example/alice" {
		t.Fatalf("unexpected accepted contact conversation: %#v", accepted)
	}

	pending := ConversationFromContact(ContactRecord{RoomID: "!pending:example.com", PeerMXID: "@bob:remote.example", Status: "pending_outbound"})
	if pending.Lifecycle != ConversationLifecyclePending || pending.Title != "@bob:remote.example" {
		t.Fatalf("unexpected pending contact conversation: %#v", pending)
	}

	deleted := ConversationFromContact(ContactRecord{RoomID: "!deleted:example.com", PeerMXID: "@carol:remote.example", Status: "deleted"})
	if deleted.Lifecycle != ConversationLifecycleDeleted {
		t.Fatalf("unexpected deleted contact conversation: %#v", deleted)
	}
}

func TestContactDeletedAndFallbackString(t *testing.T) {
	if !ContactDeleted(" Deleted ") {
		t.Fatalf("expected deleted status to be normalized")
	}
	if ContactDeleted("accepted") {
		t.Fatalf("accepted contact must not be deleted")
	}
	if FallbackString(" value ", "fallback") != " value " {
		t.Fatalf("non-empty value should be preserved")
	}
	if FallbackString(" ", "fallback") != "fallback" {
		t.Fatalf("blank value should use fallback")
	}
}
