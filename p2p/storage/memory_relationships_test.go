package storage

import (
	"context"
	"testing"
)

func TestMemoryStoreContactAndBlockSemantics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryStore()

	if err := store.UpsertContact(ctx, contactRecord{RoomID: "!old:example.com", PeerMXID: "@peer:example.com", AvatarURL: "old"}); err != nil {
		t.Fatalf("UpsertContact old: %v", err)
	}
	if err := store.UpsertContact(ctx, contactRecord{RoomID: "!new:example.com", PeerMXID: "@peer:example.com"}); err != nil {
		t.Fatalf("UpsertContact replacement: %v", err)
	}
	contacts, err := store.ListContacts(ctx)
	if err != nil {
		t.Fatalf("ListContacts: %v", err)
	}
	if len(contacts) != 1 || contacts[0].RoomID != "!new:example.com" || contacts[0].AvatarURL != "" {
		t.Fatalf("contact replacement = %#v", contacts)
	}

	block := blockRecord{TargetType: "contact", TargetID: "@peer:example.com", CreatedAt: 10}
	if err := store.UpsertBlock(ctx, block); err != nil {
		t.Fatalf("UpsertBlock: %v", err)
	}
	block.CreatedAt = 20
	if err := store.UpsertBlock(ctx, block); err != nil {
		t.Fatalf("UpsertBlock overwrite: %v", err)
	}
	blocks, err := store.ListBlocks(ctx)
	if err != nil {
		t.Fatalf("ListBlocks: %v", err)
	}
	if len(blocks) != 1 || blocks[0].CreatedAt != 20 {
		t.Fatalf("block created_at = %#v, want overwrite to 20", blocks)
	}
}

func TestMemoryStoreGroupCallAndSocialSemantics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryStore()

	if err := store.UpsertGroup(ctx, groupRecord{RoomID: "!group:example.com", Name: "Group"}); err != nil {
		t.Fatalf("UpsertGroup: %v", err)
	}
	if err := store.UpsertMember(ctx, memberRecord{RoomID: "!group:example.com", UserID: "@owner:example.com", Membership: "JOIN", Role: "owner"}); err != nil {
		t.Fatalf("UpsertMember group owner: %v", err)
	}
	groups, err := store.ListJoinedGroupsForUser(ctx, "@owner:example.com")
	if err != nil {
		t.Fatalf("ListJoinedGroupsForUser: %v", err)
	}
	if len(groups) != 1 || groups[0].RoomID != "!group:example.com" {
		t.Fatalf("joined groups = %#v", groups)
	}

	for _, call := range []callRecord{
		{CallID: "active", State: "ringing"},
		{CallID: "terminal", State: " ENDED "},
	} {
		if err := store.UpsertCall(ctx, call); err != nil {
			t.Fatalf("UpsertCall(%s): %v", call.CallID, err)
		}
	}
	activeCalls, err := store.ListCalls(ctx, "", true)
	if err != nil {
		t.Fatalf("ListCalls active: %v", err)
	}
	if len(activeCalls) != 1 || activeCalls[0].CallID != "active" {
		t.Fatalf("active calls = %#v", activeCalls)
	}

	if err := store.UpsertFavorite(ctx, favoriteRecord{ID: 1, EventID: "$event", RoomID: "", CreatedAt: "2026-01-01T00:00:00Z"}); err != nil {
		t.Fatalf("UpsertFavorite: %v", err)
	}
	favorite, ok, err := store.FindFavoriteByEvent(ctx, "$event", "!room:example.com")
	if err != nil || !ok || favorite.ID != 1 {
		t.Fatalf("FindFavoriteByEvent with legacy empty-room match = (%#v, %v, %v)", favorite, ok, err)
	}

	reaction := reactionRecord{TargetType: "post", TargetID: "post", Reaction: "like", UserID: "@owner:example.com", Active: true}
	if err := store.UpsertReaction(ctx, reaction); err != nil {
		t.Fatalf("UpsertReaction: %v", err)
	}
	count, err := store.CountActiveReactions(ctx, "post", "post", "like")
	if err != nil || count != 1 {
		t.Fatalf("CountActiveReactions = (%d, %v), want (1, nil)", count, err)
	}
	reaction.Active = false
	if err := store.UpsertReaction(ctx, reaction); err != nil {
		t.Fatalf("UpsertReaction deactivate: %v", err)
	}
	listedReactions, err := store.ListReactions(ctx, "@owner:example.com")
	if err != nil || len(listedReactions) != 0 {
		t.Fatalf("ListReactions after deactivate = (%#v, %v)", listedReactions, err)
	}
}

func TestMemoryStorePluginReturnsIndependentCopies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryStore()
	plugin := pluginInstance{
		ID:        "io.dirextalk.test",
		Name:      "Test",
		CreatedAt: 10,
		Config: map[string]any{
			"nested": map[string]any{"enabled": true},
		},
	}
	if err := store.UpsertPlugin(ctx, plugin); err != nil {
		t.Fatalf("UpsertPlugin: %v", err)
	}
	plugin.Config["nested"].(map[string]any)["enabled"] = false

	got, ok, err := store.GetPlugin(ctx, plugin.ID)
	if err != nil || !ok {
		t.Fatalf("GetPlugin = (_, %v, %v), want (_, true, nil)", ok, err)
	}
	if got.Config["nested"].(map[string]any)["enabled"] != true {
		t.Fatalf("plugin input aliased stored config: %#v", got.Config)
	}
	got.Config["nested"].(map[string]any)["enabled"] = false
	reloaded, _, _ := store.GetPlugin(ctx, plugin.ID)
	if reloaded.Config["nested"].(map[string]any)["enabled"] != true {
		t.Fatalf("GetPlugin returned aliased config: %#v", reloaded.Config)
	}

	plugin = reloaded
	plugin.CreatedAt = 20
	if err := store.UpsertPlugin(ctx, plugin); err != nil {
		t.Fatalf("UpsertPlugin overwrite: %v", err)
	}
	reloaded, _, _ = store.GetPlugin(ctx, plugin.ID)
	if reloaded.CreatedAt != 20 {
		t.Fatalf("plugin CreatedAt = %d, want legacy overwrite to 20", reloaded.CreatedAt)
	}
}
