package p2p

import "testing"

func TestContactStorageRecordConversionUsesOnlyDurableFields(t *testing.T) {
	conversation := conversationView{ConversationID: "conv_direct"}
	contact := contactRecord{
		PeerMXID:            "@alice:remote.example",
		DisplayName:         "Alice",
		DisplayNameOverride: true,
		AvatarURL:           "mxc://remote.example/alice",
		Domain:              "remote.example",
		RoomID:              "!dm:example.com",
		Status:              "accepted",
		Remark:              "project",
		Operation:           map[string]any{"action": "contacts.request"},
		Conversation:        &conversation,
	}

	stored := contactStorageRecordFromContact(contact)
	if stored.PeerMXID != contact.PeerMXID ||
		stored.DisplayName != contact.DisplayName ||
		stored.DisplayNameOverride != contact.DisplayNameOverride ||
		stored.AvatarURL != contact.AvatarURL ||
		stored.Domain != contact.Domain ||
		stored.RoomID != contact.RoomID ||
		stored.Status != contact.Status ||
		stored.Remark != contact.Remark {
		t.Fatalf("storage conversion lost durable contact fields: %#v", stored)
	}

	restored := contactRecordFromStorage(stored)
	if restored.Operation != nil || restored.Conversation != nil {
		t.Fatalf("storage conversion must not restore facade fields, got %#v", restored)
	}
	if restored.PeerMXID != contact.PeerMXID ||
		restored.DisplayName != contact.DisplayName ||
		restored.DisplayNameOverride != contact.DisplayNameOverride ||
		restored.AvatarURL != contact.AvatarURL ||
		restored.Domain != contact.Domain ||
		restored.RoomID != contact.RoomID ||
		restored.Status != contact.Status ||
		restored.Remark != contact.Remark {
		t.Fatalf("facade conversion lost durable contact fields: %#v", restored)
	}
}
