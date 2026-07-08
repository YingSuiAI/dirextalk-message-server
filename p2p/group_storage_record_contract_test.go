package p2p

import (
	"reflect"
	"testing"
)

func TestGroupStorageRecordExcludesFacadeFields(t *testing.T) {
	groupType := reflect.TypeOf(groupStorageRecord{})
	for _, fieldName := range []string{"Operation", "Conversation"} {
		if _, ok := groupType.FieldByName(fieldName); ok {
			t.Fatalf("durable group storage record must not expose response-only field %s", fieldName)
		}
	}
}

func TestGroupStorageRecordConversionUsesOnlyDurableFields(t *testing.T) {
	conversation := conversationView{ConversationID: "conv_group"}
	group := groupRecord{
		RoomID:       "!group:example.com",
		Name:         "Team",
		Topic:        "topic",
		AvatarURL:    "mxc://example.com/team",
		MemberCount:  7,
		InvitePolicy: "owner",
		Muted:        true,
		Operation:    map[string]any{"action": "groups.create"},
		Conversation: &conversation,
	}

	stored := groupStorageRecordFromGroup(group)
	if stored.RoomID != group.RoomID ||
		stored.Name != group.Name ||
		stored.Topic != group.Topic ||
		stored.AvatarURL != group.AvatarURL ||
		stored.MemberCount != group.MemberCount ||
		stored.InvitePolicy != group.InvitePolicy ||
		stored.Muted != group.Muted {
		t.Fatalf("storage conversion lost durable group fields: %#v", stored)
	}

	restored := groupRecordFromStorage(stored)
	if restored.Operation != nil || restored.Conversation != nil {
		t.Fatalf("storage conversion must not restore facade fields, got %#v", restored)
	}
	if restored.RoomID != group.RoomID ||
		restored.Name != group.Name ||
		restored.Topic != group.Topic ||
		restored.AvatarURL != group.AvatarURL ||
		restored.MemberCount != group.MemberCount ||
		restored.InvitePolicy != group.InvitePolicy ||
		restored.Muted != group.Muted {
		t.Fatalf("facade conversion lost durable group fields: %#v", restored)
	}
}
