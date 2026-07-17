package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
)

func TestConversationStoreUpsertListAndGet(t *testing.T) {
	ctx := context.Background()
	store := newConversationTestStore(t, ctx)

	direct := conversationRecord{
		ConversationID:  "conv_direct",
		MatrixRoomID:    "!dm:example.com",
		Kind:            conversationKindDirect,
		Lifecycle:       conversationLifecycleActive,
		CreatedByMXID:   "@alice:example.com",
		PeerMXID:        "@bob:example.com",
		Title:           "Alice",
		AvatarURL:       "mxc://example.com/alice",
		LastEventID:     "$event1",
		LastActivityAt:  100,
		ProjectionState: conversationProjectionReady,
		CreatedAt:       10,
		UpdatedAt:       100,
	}
	if upsertErr := store.UpsertConversation(ctx, direct); upsertErr != nil {
		t.Fatal(upsertErr)
	}

	got, ok, err := store.GetConversationByID(ctx, "conv_direct")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected conversation by id")
	}
	if got.MatrixRoomID != direct.MatrixRoomID ||
		got.Kind != conversationKindDirect ||
		got.PeerMXID != "@bob:example.com" ||
		got.Title != "Alice" {
		t.Fatalf("unexpected conversation by id: %#v", got)
	}

	byRoom, ok, err := store.GetConversationByRoomID(ctx, direct.MatrixRoomID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || byRoom.ConversationID != direct.ConversationID {
		t.Fatalf("expected conversation by room, got %#v ok=%v", byRoom, ok)
	}

	direct.Title = "Alice Renamed"
	direct.CreatedByMXID = "@profile-writer:example.com"
	direct.UpdatedAt = 200
	if err = store.UpsertConversation(ctx, direct); err != nil {
		t.Fatal(err)
	}
	list, err := store.ListConversations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Title != "Alice Renamed" || list[0].CreatedByMXID != "@alice:example.com" {
		t.Fatalf("expected one updated active conversation, got %#v", list)
	}
	if err = store.SetConversationCreator(ctx, direct.MatrixRoomID, "@authoritative:example.com"); err != nil {
		t.Fatal(err)
	}
	byRoom, ok, err = store.GetConversationByRoomID(ctx, direct.MatrixRoomID)
	if err != nil || !ok || byRoom.CreatedByMXID != "@authoritative:example.com" {
		t.Fatalf("expected authoritative creator update, got %#v ok=%v err=%v", byRoom, ok, err)
	}
	if err = store.SetConversationCreator(ctx, direct.MatrixRoomID, ""); err != nil {
		t.Fatal(err)
	}
	byRoom, ok, err = store.GetConversationByRoomID(ctx, direct.MatrixRoomID)
	if err != nil || !ok || byRoom.CreatedByMXID != "" {
		t.Fatalf("expected authoritative creator clear, got %#v ok=%v err=%v", byRoom, ok, err)
	}

	activityOnly := conversationRecord{
		ConversationID:  direct.ConversationID,
		MatrixRoomID:    direct.MatrixRoomID,
		Kind:            conversationKindDirect,
		Lifecycle:       conversationLifecycleActive,
		LastEventID:     "$event2",
		LastActivityAt:  300,
		ProjectionState: conversationProjectionReady,
		UpdatedAt:       300,
	}
	if err = store.UpsertConversation(ctx, activityOnly); err != nil {
		t.Fatal(err)
	}
	got, ok, err = store.GetConversationByID(ctx, "conv_direct")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got.PeerMXID != "@bob:example.com" {
		t.Fatalf("expected activity-only update to preserve peer mxid, got %#v ok=%v", got, ok)
	}
	if got.Title != "Alice Renamed" || got.AvatarURL != "mxc://example.com/alice" {
		t.Fatalf("expected activity-only update to preserve profile fields, got %#v", got)
	}

	deleted := conversationRecord{
		ConversationID:  "conv_deleted",
		MatrixRoomID:    "!deleted:example.com",
		Kind:            conversationKindGroup,
		Lifecycle:       conversationLifecycleDeleted,
		Title:           "Deleted Group",
		ProjectionState: conversationProjectionReady,
		CreatedAt:       20,
		UpdatedAt:       300,
	}
	if err = store.UpsertConversation(ctx, deleted); err != nil {
		t.Fatal(err)
	}
	list, err = store.ListConversations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ConversationID != "conv_direct" {
		t.Fatalf("deleted conversations should be excluded from list, got %#v", list)
	}

	if _, ok, err := store.GetConversationByID(ctx, "missing"); err != nil || ok {
		t.Fatalf("expected missing conversation to return ok=false err=nil, ok=%v err=%v", ok, err)
	}
}

func TestConversationStoreRejectsRoomKindConflict(t *testing.T) {
	ctx := context.Background()
	store := newConversationTestStore(t, ctx)

	base := conversationRecord{
		ConversationID:  "conv_room",
		MatrixRoomID:    "!room:example.com",
		Kind:            conversationKindDirect,
		Lifecycle:       conversationLifecycleActive,
		ProjectionState: conversationProjectionReady,
		CreatedAt:       10,
		UpdatedAt:       10,
	}
	if err := store.UpsertConversation(ctx, base); err != nil {
		t.Fatal(err)
	}
	conflict := base
	conflict.ConversationID = "conv_conflict"
	conflict.Kind = conversationKindGroup
	if err := store.UpsertConversation(ctx, conflict); err == nil || !strings.Contains(err.Error(), "kind conflict") {
		t.Fatalf("expected kind conflict, got %v", err)
	}
}

func TestConversationStoreDeletesConversationByRoomID(t *testing.T) {
	ctx := context.Background()
	store := newConversationTestStore(t, ctx)

	record := conversationRecord{
		ConversationID:  "conv_room",
		MatrixRoomID:    "!room:example.com",
		Kind:            conversationKindGroup,
		Lifecycle:       conversationLifecycleActive,
		ProjectionState: conversationProjectionReady,
	}
	if err := store.UpsertConversation(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteConversationByRoomID(ctx, record.MatrixRoomID); err != nil {
		t.Fatal(err)
	}

	if _, ok, err := store.GetConversationByRoomID(ctx, record.MatrixRoomID); err != nil || ok {
		t.Fatalf("expected deleted conversation to be missing, ok=%v err=%v", ok, err)
	}
	list, err := store.ListConversations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("expected deleted conversation to be excluded, got %#v", list)
	}
}

func TestConversationStoreBackfillsProductRecords(t *testing.T) {
	ctx := context.Background()
	store := newConversationTestStore(t, ctx)

	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO p2p_contacts (room_id, peer_mxid, display_name, avatar_url, domain, status)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, "!dm:example.com", "@alice:example.com", "Alice", "mxc://alice", "example.com", "accepted"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO p2p_groups (room_id, name, topic, avatar_url, member_count, invite_policy, muted)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, "!group:example.com", "Group", "", "mxc://group", 3, "owner", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO p2p_channels (
			channel_id, room_id, name, description, avatar_url, visibility,
			join_policy, channel_type, comments_enabled, member_count,
			pending_join_count, muted
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, "channel", "!channel:example.com", "Channel", "", "mxc://channel", "public", "open", "chat", 1, 1, 0, 0); err != nil {
		t.Fatal(err)
	}

	if err := store.BackfillProductConversations(ctx); err != nil {
		t.Fatal(err)
	}

	list, err := store.ListConversations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byKind := map[conversationKind]conversationRecord{}
	for _, conversation := range list {
		byKind[conversation.Kind] = conversation
	}
	if byKind[conversationKindDirect].ConversationID != conversationIDForRoomID("!dm:example.com") ||
		byKind[conversationKindDirect].PeerMXID != "@alice:example.com" ||
		byKind[conversationKindDirect].Title != "Alice" ||
		byKind[conversationKindDirect].AvatarURL != "mxc://alice" {
		t.Fatalf("expected direct conversation backfill, got %#v", byKind[conversationKindDirect])
	}
	if byKind[conversationKindGroup].Title != "Group" ||
		byKind[conversationKindGroup].AvatarURL != "mxc://group" {
		t.Fatalf("expected group conversation backfill, got %#v", byKind[conversationKindGroup])
	}
	if byKind[conversationKindChannel].Title != "Channel" ||
		byKind[conversationKindChannel].AvatarURL != "mxc://channel" {
		t.Fatalf("expected channel conversation backfill, got %#v", byKind[conversationKindChannel])
	}
}

func newConversationTestStore(t *testing.T, ctx context.Context) *DatabaseStore {
	t.Helper()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(closeDB)
	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}
