package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

func TestMemoryStoreConversationMergeConflictAndOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryStore()

	original := conversationRecord{
		ConversationID:  "conv_direct",
		MatrixRoomID:    "!direct:example.com",
		Kind:            conversationKindDirect,
		CreatedByMXID:   "@creator:example.com",
		Title:           "Original",
		AvatarURL:       "mxc://example/avatar",
		LastEventID:     "$old",
		LastMessage:     "old",
		LastActivityAt:  20,
		ProjectionState: dirextalkdomain.ConversationProjectionReady,
		CreatedAt:       10,
		UpdatedAt:       20,
	}
	if err := store.UpsertConversation(ctx, original); err != nil {
		t.Fatalf("UpsertConversation original: %v", err)
	}
	if err := store.UpsertConversation(ctx, conversationRecord{
		ConversationID:  original.ConversationID,
		MatrixRoomID:    original.MatrixRoomID,
		Kind:            original.Kind,
		CreatedByMXID:   "@profile-writer:example.com",
		Lifecycle:       dirextalkdomain.ConversationLifecycleActive,
		ProjectionState: dirextalkdomain.ConversationProjectionReady,
		UpdatedAt:       30,
	}); err != nil {
		t.Fatalf("UpsertConversation update: %v", err)
	}

	got, ok, err := store.GetConversationByID(ctx, original.ConversationID)
	if err != nil || !ok {
		t.Fatalf("GetConversationByID = (_, %v, %v), want (_, true, nil)", ok, err)
	}
	if got.CreatedAt != 10 || got.CreatedByMXID != original.CreatedByMXID || got.Title != "Original" || got.AvatarURL != original.AvatarURL || got.LastEventID != "$old" || got.LastMessage != "old" || got.LastActivityAt != 20 {
		t.Fatalf("conversation merge lost existing non-empty fields: %#v", got)
	}
	if err := store.SetConversationCreator(ctx, original.MatrixRoomID, "@authoritative:example.com"); err != nil {
		t.Fatalf("SetConversationCreator: %v", err)
	}
	got, ok, err = store.GetConversationByID(ctx, original.ConversationID)
	if err != nil || !ok || got.CreatedByMXID != "@authoritative:example.com" {
		t.Fatalf("authoritative creator update = (%#v, %v, %v)", got, ok, err)
	}

	err = store.UpsertConversation(ctx, conversationRecord{
		ConversationID: "conv_group",
		MatrixRoomID:   original.MatrixRoomID,
		Kind:           conversationKindGroup,
	})
	if err == nil || !strings.Contains(err.Error(), "conversation kind conflict") {
		t.Fatalf("kind conflict error = %v, want conversation kind conflict", err)
	}

	for _, record := range []conversationRecord{
		{ConversationID: "conv_old", MatrixRoomID: "!old:example.com", Kind: conversationKindGroup, LastActivityAt: 5, UpdatedAt: 100},
		{ConversationID: "conv_new_b", MatrixRoomID: "!new-b:example.com", Kind: conversationKindGroup, LastActivityAt: 50, UpdatedAt: 60},
		{ConversationID: "conv_new_a", MatrixRoomID: "!new-a:example.com", Kind: conversationKindGroup, LastActivityAt: 50, UpdatedAt: 60},
		{ConversationID: "conv_deleted", MatrixRoomID: "!deleted:example.com", Kind: conversationKindGroup, Lifecycle: conversationLifecycleDeleted, LastActivityAt: 100},
	} {
		if err := store.UpsertConversation(ctx, record); err != nil {
			t.Fatalf("UpsertConversation(%s): %v", record.ConversationID, err)
		}
	}

	listed, err := store.ListConversations(ctx)
	if err != nil {
		t.Fatalf("ListConversations: %v", err)
	}
	ids := make([]string, len(listed))
	for i := range listed {
		ids[i] = listed[i].ConversationID
	}
	want := []string{"conv_new_a", "conv_new_b", "conv_direct", "conv_old"}
	if len(ids) != len(want) {
		t.Fatalf("conversation ids = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("conversation ids = %v, want %v", ids, want)
		}
	}

	if err := store.DeleteConversationByRoomID(ctx, original.MatrixRoomID); err != nil {
		t.Fatalf("DeleteConversationByRoomID: %v", err)
	}
	if _, ok, err := store.GetConversationByRoomID(ctx, original.MatrixRoomID); err != nil || ok {
		t.Fatalf("GetConversationByRoomID after delete = (_, %v, %v), want (_, false, nil)", ok, err)
	}
}
