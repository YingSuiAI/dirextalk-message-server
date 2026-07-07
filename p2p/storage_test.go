package p2p

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
)

func TestDatabaseStoreCreatesBusinessIndexes(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	expected := []string{
		"p2p_channels_room_idx",
		"p2p_channels_type_visibility_idx",
		"p2p_channels_visibility_idx",
		"p2p_channel_posts_channel_idx",
		"p2p_channel_posts_event_idx",
		"p2p_channel_posts_author_idx",
		"p2p_channel_comments_post_idx",
		"p2p_channel_comments_channel_idx",
		"p2p_channel_comments_event_idx",
		"p2p_contacts_peer_idx",
		"p2p_contacts_status_idx",
		"p2p_blocks_type_idx",
		"p2p_blocks_room_idx",
		"p2p_blocks_peer_idx",
		"p2p_reports_target_idx",
		"p2p_reports_reporter_idx",
		"p2p_calls_room_idx",
		"p2p_calls_state_idx",
		"p2p_favorites_type_idx",
		"p2p_favorites_event_idx",
		"p2p_reactions_user_idx",
		"p2p_reactions_target_idx",
		"p2p_members_channel_idx",
		"p2p_members_room_idx",
		"p2p_members_user_idx",
		"p2p_members_room_joined_idx",
		"p2p_members_channel_joined_idx",
		"p2p_members_user_room_idx",
		"p2p_members_user_channel_idx",
		"p2p_events_room_idx",
		"p2p_events_type_idx",
	}
	for _, indexName := range expected {
		t.Run(indexName, func(t *testing.T) {
			var name string
			if err := store.DB().QueryRowContext(ctx, `SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND indexname = $1`, indexName).Scan(&name); err != nil {
				t.Fatalf("expected index %s to exist: %v", indexName, err)
			}
		})
	}
	var contactPeerIndex string
	if err := store.DB().QueryRowContext(ctx, `SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'p2p_contacts_peer_idx'`).Scan(&contactPeerIndex); err != nil {
		t.Fatalf("expected contact peer index definition: %v", err)
	}
	if !strings.Contains(strings.ToUpper(contactPeerIndex), "UNIQUE") {
		t.Fatalf("expected p2p_contacts_peer_idx to be unique, got %s", contactPeerIndex)
	}
	var messageTableCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'p2p_messages'`).Scan(&messageTableCount); err != nil {
		t.Fatal(err)
	}
	if messageTableCount != 0 {
		t.Fatalf("p2p_messages table must not be created after Matrix-source migration")
	}
}

func TestDatabaseStoreUpsertContactIsUniqueByPeer(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if storeErr := store.UpsertContact(ctx, contactRecord{
		RoomID:      "!first:example.com",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		AvatarURL:   "mxc://remote.example/alice-old",
		Remark:      "first request",
		Domain:      "remote.example",
		Status:      "pending_outbound",
	}); storeErr != nil {
		t.Fatal(storeErr)
	}
	if storeErr := store.UpsertContact(ctx, contactRecord{
		RoomID:      "!second:example.com",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice Updated",
		AvatarURL:   "mxc://remote.example/alice",
		Remark:      "updated request",
		Domain:      "remote.example",
		Status:      "accepted",
	}); storeErr != nil {
		t.Fatal(storeErr)
	}
	contacts, err := store.ListContacts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(contacts) != 1 || contacts[0].RoomID != "!second:example.com" || contacts[0].AvatarURL != "mxc://remote.example/alice" || contacts[0].Remark != "updated request" || contacts[0].Status != "accepted" {
		t.Fatalf("expected contact upsert to keep one row per peer, got %#v", contacts)
	}
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO p2p_contacts (room_id, peer_mxid, display_name, domain, status)
		VALUES ($1, $2, $3, $4, $5)
	`, "!third:example.com", "@alice:remote.example", "Alice Duplicate", "remote.example", "pending_outbound"); err == nil {
		t.Fatalf("expected raw duplicate contact insert to fail unique peer constraint")
	}
}

func TestDatabaseStorePersistsBlocks(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	block := blockRecord{
		TargetType:  "channel",
		TargetID:    "!blocked:example.com",
		RoomID:      "!blocked:example.com",
		DisplayName: "Blocked Channel",
		AvatarURL:   "mxc://example.com/blocked",
		CreatedAt:   123,
	}
	if err := store.UpsertBlock(ctx, block); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertBlock(ctx, blockRecord{
		TargetType:  "channel",
		TargetID:    "!blocked:example.com",
		RoomID:      "!blocked:example.com",
		DisplayName: "Blocked Channel Updated",
		CreatedAt:   456,
	}); err != nil {
		t.Fatal(err)
	}

	reloaded, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()
	blocks, err := reloaded.ListBlocks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || blocks[0].TargetID != "!blocked:example.com" || blocks[0].ChannelID != "" || blocks[0].DisplayName != "Blocked Channel Updated" || blocks[0].CreatedAt != 123 {
		t.Fatalf("expected one persisted block preserving original created_at, got %#v", blocks)
	}
	removed, err := reloaded.DeleteBlock(ctx, "channel", "!blocked:example.com")
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatalf("expected persisted block to be removed")
	}
	blocks, err = reloaded.ListBlocks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 0 {
		t.Fatalf("expected no blocks after removal, got %#v", blocks)
	}
}

func TestDatabaseStorePersistsPluginsAndJobs(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	plugin := pluginInstance{
		ID:        "io.dirextalk.agent",
		Name:      "Dirextalk Agent",
		Version:   "0.1.0",
		Image:     "docker.io/dirextalk/agent-plugin:latest",
		Digest:    "",
		Status:    "enabled",
		Enabled:   true,
		Config:    map[string]any{"provider": "openai", "model": "gpt-4.1"},
		LastJobID: "job-install",
	}
	if err := store.UpsertPlugin(ctx, plugin); err != nil {
		t.Fatal(err)
	}
	job := pluginJob{
		JobID:    "job-install",
		PluginID: "io.dirextalk.agent",
		Action:   "install",
		Status:   "succeeded",
		Message:  "installed",
	}
	if err := store.UpsertPluginJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPluginSecret(ctx, pluginSecret{
		PluginID:  plugin.ID,
		Name:      "api_key",
		Value:     "sk-plugin-secret",
		UpdatedAt: 1710000000000,
	}); err != nil {
		t.Fatal(err)
	}

	reloaded, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()

	plugins, err := reloaded.ListPlugins(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(plugins) != 1 || plugins[0].ID != plugin.ID || !plugins[0].Enabled || plugins[0].Config["model"] != "gpt-4.1" {
		t.Fatalf("expected persisted enabled plugin with config, got %#v", plugins)
	}
	gotJob, ok, err := reloaded.GetPluginJob(ctx, "job-install")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || gotJob.PluginID != plugin.ID || gotJob.Status != "succeeded" || gotJob.Message != "installed" {
		t.Fatalf("expected persisted plugin job, got ok=%v job=%#v", ok, gotJob)
	}
	gotSecret, ok, err := reloaded.GetPluginSecret(ctx, plugin.ID, "api_key")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || gotSecret.Value != "sk-plugin-secret" || gotSecret.UpdatedAt != 1710000000000 {
		t.Fatalf("expected persisted plugin secret metadata, got ok=%v secret=%#v", ok, gotSecret)
	}
}

func TestDatabaseStoreListsJoinedGroupsAndChannelsForUser(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ownerMXID := "@owner:example.com"
	otherMXID := "@other:example.com"
	groups := []groupRecord{
		{RoomID: "!group-owner:example.com", Name: "Owner Group", InvitePolicy: "owner"},
		{RoomID: "!group-pending:example.com", Name: "Pending Group", InvitePolicy: "owner"},
		{RoomID: "!group-other:example.com", Name: "Other Group", InvitePolicy: "owner"},
	}
	for _, group := range groups {
		if err := store.UpsertGroup(ctx, group); err != nil {
			t.Fatal(err)
		}
	}
	channels := []channel{
		{ChannelID: "owner_channel", RoomID: "!channel-owner:example.com", Name: "Owner Channel", Visibility: "public", JoinPolicy: "open", ChannelType: "chat"},
		{ChannelID: "left_channel", RoomID: "!channel-left:example.com", Name: "Left Channel", Visibility: "public", JoinPolicy: "open", ChannelType: "chat"},
		{ChannelID: "other_channel", RoomID: "!channel-other:example.com", Name: "Other Channel", Visibility: "public", JoinPolicy: "open", ChannelType: "chat"},
	}
	for _, ch := range channels {
		if err := store.UpsertChannel(ctx, ch); err != nil {
			t.Fatal(err)
		}
	}
	members := []memberRecord{
		{RoomID: "!group-owner:example.com", UserID: ownerMXID, Domain: "example.com", Membership: "join", Role: "owner", JoinedAt: 10},
		{RoomID: "!group-pending:example.com", UserID: ownerMXID, Domain: "example.com", Membership: "pending", Role: "member", JoinedAt: 20},
		{RoomID: "!group-other:example.com", UserID: otherMXID, Domain: "example.com", Membership: "join", Role: "owner", JoinedAt: 30},
		{RoomID: "!channel-owner:example.com", ChannelID: "owner_channel", UserID: ownerMXID, Domain: "example.com", Membership: "join", Role: "owner", JoinedAt: 40},
		{RoomID: "!channel-left:example.com", ChannelID: "left_channel", UserID: ownerMXID, Domain: "example.com", Membership: "leave", Role: "member", JoinedAt: 50},
		{RoomID: "!channel-other:example.com", ChannelID: "other_channel", UserID: otherMXID, Domain: "example.com", Membership: "join", Role: "owner", JoinedAt: 60},
	}
	for _, member := range members {
		if err := store.UpsertMember(ctx, member); err != nil {
			t.Fatal(err)
		}
	}

	joinedGroups, err := store.ListJoinedGroupsForUser(ctx, ownerMXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(joinedGroups) != 1 || joinedGroups[0].RoomID != "!group-owner:example.com" {
		t.Fatalf("expected only owner joined group, got %#v", joinedGroups)
	}
	joinedChannels, err := store.ListJoinedChannelsForUser(ctx, ownerMXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(joinedChannels) != 1 || joinedChannels[0].ChannelID != "owner_channel" || !joinedChannels[0].IsOwned || joinedChannels[0].Role != "owner" || joinedChannels[0].MemberStatus != "join" {
		t.Fatalf("expected only owner joined channel with membership fields, got %#v", joinedChannels)
	}
	ownerMembers, err := store.ListMembersForUser(ctx, ownerMXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ownerMembers) != 3 {
		t.Fatalf("expected owner visible memberships only, got %#v", ownerMembers)
	}
	for _, member := range ownerMembers {
		if member.UserID != ownerMXID || member.Membership == "leave" {
			t.Fatalf("expected owner non-left membership, got %#v", member)
		}
	}
}

func TestDatabaseStoreLooksUpGroupAndChannelWithoutFullList(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	group := groupRecord{RoomID: "!group:example.com", Name: "Group", Topic: "topic", InvitePolicy: "owner"}
	if err := store.UpsertGroup(ctx, group); err != nil {
		t.Fatal(err)
	}
	ch := channel{ChannelID: "channel_1", RoomID: "!channel:example.com", Name: "Channel", Visibility: "public", JoinPolicy: "open", ChannelType: "post", CommentsEnabled: true}
	if err := store.UpsertChannel(ctx, ch); err != nil {
		t.Fatal(err)
	}

	gotGroup, ok, err := store.GetGroupByRoom(ctx, group.RoomID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || gotGroup.RoomID != group.RoomID || gotGroup.Name != group.Name {
		t.Fatalf("expected group lookup by room, got ok=%v group=%#v", ok, gotGroup)
	}
	gotByID, ok, err := store.GetChannelByIDOrRoom(ctx, ch.ChannelID, "")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || gotByID.ChannelID != ch.ChannelID || !gotByID.CommentsEnabled {
		t.Fatalf("expected channel lookup by id, got ok=%v channel=%#v", ok, gotByID)
	}
	gotByRoom, ok, err := store.GetChannelByIDOrRoom(ctx, "", ch.RoomID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || gotByRoom.ChannelID != ch.ChannelID {
		t.Fatalf("expected channel lookup by room, got ok=%v channel=%#v", ok, gotByRoom)
	}
	missing, ok, err := store.GetChannelByIDOrRoom(ctx, "missing", "")
	if err != nil {
		t.Fatal(err)
	}
	if ok || missing.ChannelID != "" {
		t.Fatalf("expected missing channel lookup, got ok=%v channel=%#v", ok, missing)
	}
}

func TestDatabaseStoreCountsJoinedMembers(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	members := []memberRecord{
		{RoomID: "!group:example.com", UserID: "@owner:example.com", Membership: "join", Role: "owner"},
		{RoomID: "!group:example.com", UserID: "@alice:example.com", Membership: "join", Role: "member"},
		{RoomID: "!group:example.com", UserID: "@left:example.com", Membership: "leave", Role: "member"},
		{RoomID: "!channel:example.com", ChannelID: "channel_1", UserID: "@owner:example.com", Membership: "join", Role: "owner"},
		{RoomID: "!channel:example.com", ChannelID: "channel_1", UserID: "@bob:example.com", Membership: "join", Role: "member"},
		{RoomID: "!channel:example.com", ChannelID: "channel_1", UserID: "@pending:example.com", Membership: "pending", Role: "member"},
	}
	for _, member := range members {
		if err := store.UpsertMember(ctx, member); err != nil {
			t.Fatal(err)
		}
	}

	groupCount, err := store.CountJoinedMembers(ctx, "!group:example.com", "")
	if err != nil {
		t.Fatal(err)
	}
	if groupCount != 2 {
		t.Fatalf("expected 2 joined group members, got %d", groupCount)
	}
	channelCount, err := store.CountJoinedMembers(ctx, "!channel:example.com", "channel_1")
	if err != nil {
		t.Fatal(err)
	}
	if channelCount != 2 {
		t.Fatalf("expected 2 joined channel members, got %d", channelCount)
	}
	joined, pending, err := store.CountProductMembers(ctx, "!channel:example.com", "channel_1")
	if err != nil {
		t.Fatal(err)
	}
	if joined != 2 || pending != 1 {
		t.Fatalf("expected channel joined=2 pending=1, got joined=%d pending=%d", joined, pending)
	}
}

func TestDatabaseStoreSearchesPublicChannelsAndListsOwnerPublicChannels(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ownerMXID := "@owner:example.com"
	channels := []channel{
		{ChannelID: "public_news", RoomID: "!public-news:example.com", Name: "Public News", Description: "daily updates", Visibility: "public", JoinPolicy: "open", ChannelType: "post"},
		{ChannelID: "public_other", RoomID: "!public-other:example.com", Name: "Public Other", Visibility: "public", JoinPolicy: "open", ChannelType: "chat"},
		{ChannelID: "private_news", RoomID: "!private-news:example.com", Name: "Private News", Visibility: "private", JoinPolicy: "invite", ChannelType: "chat"},
	}
	for _, ch := range channels {
		if err := store.UpsertChannel(ctx, ch); err != nil {
			t.Fatal(err)
		}
	}
	members := []memberRecord{
		{RoomID: "!public-news:example.com", ChannelID: "public_news", UserID: ownerMXID, Membership: "join", Role: "owner"},
		{RoomID: "!public-news:example.com", ChannelID: "public_news", UserID: "@alice:example.com", Membership: "join", Role: "member"},
		{RoomID: "!public-other:example.com", ChannelID: "public_other", UserID: "@other:example.com", Membership: "join", Role: "owner"},
		{RoomID: "!private-news:example.com", ChannelID: "private_news", UserID: ownerMXID, Membership: "join", Role: "owner"},
	}
	for _, member := range members {
		if err := store.UpsertMember(ctx, member); err != nil {
			t.Fatal(err)
		}
	}

	results, err := store.SearchPublicChannels(ctx, "news", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ChannelID != "public_news" {
		t.Fatalf("expected public news search result only, got %#v", results)
	}
	limited, err := store.SearchPublicChannels(ctx, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 1 {
		t.Fatalf("expected search limit to apply, got %#v", limited)
	}
	owned, err := store.ListPublicChannelsForOwner(ctx, ownerMXID)
	if err != nil {
		t.Fatal(err)
	}
	if len(owned) != 1 || owned[0].ChannelID != "public_news" {
		t.Fatalf("expected owner public channel only, got %#v", owned)
	}
}

func TestDatabaseStorePrunesP2PEventsBeforeSeq(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for seq := int64(1); seq <= 5; seq++ {
		if _, err := store.InsertEvent(ctx, p2pEvent{
			Seq:       seq,
			Type:      "test.event",
			RoomID:    "!room:example.com",
			EventID:   "$event",
			CreatedAt: "2026-06-29T00:00:00Z",
		}); err != nil {
			t.Fatal(err)
		}
	}
	bounds, err := store.EventBounds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if bounds.MinSeq != 1 || bounds.MaxSeq != 5 || bounds.Count != 5 {
		t.Fatalf("expected initial event bounds 1..5 count 5, got %#v", bounds)
	}
	deleted, err := store.PruneEventsBefore(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 3 {
		t.Fatalf("expected 3 deleted events, got %d", deleted)
	}
	remaining, err := store.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 2 || remaining[0].Seq != 4 || remaining[1].Seq != 5 {
		t.Fatalf("expected events 4 and 5 after prune, got %#v", remaining)
	}
}

func TestDatabaseStoreSkipsDuplicateP2PEventsByDedupeKey(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	inserted, err := store.InsertEvent(ctx, p2pEvent{
		Seq:       1,
		Type:      "room.member.projected",
		RoomID:    "!room:example.com",
		EventID:   "$event",
		DedupeKey: "room.member.projected:$event:@owner:example.com",
		CreatedAt: "2026-06-29T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatalf("expected first event insert to report inserted")
	}
	inserted, err = store.InsertEvent(ctx, p2pEvent{
		Seq:       2,
		Type:      "room.member.projected",
		RoomID:    "!room:example.com",
		EventID:   "$event",
		DedupeKey: "room.member.projected:$event:@owner:example.com",
		CreatedAt: "2026-06-29T00:00:01Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatalf("expected duplicate dedupe key to be skipped")
	}
	events, err := store.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Seq != 1 {
		t.Fatalf("expected only first deduped event, got %#v", events)
	}
}

func TestServicePrunesP2PEventsWhenRetentionMaxRowsIsConfigured(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err := NewServiceWithStore(ctx, Config{
		ServerName:                    "example.com",
		P2PEventRetentionMaxRows:      3,
		P2PEventRetentionPruneOnWrite: true,
	}, store)
	if err != nil {
		t.Fatal(err)
	}

	for seq := int64(1); seq <= 5; seq++ {
		if err := service.appendP2PEvent(ctx, p2pEvent{
			Seq:     seq,
			Type:    "test.event",
			RoomID:  "!room:example.com",
			EventID: "$event",
		}); err != nil {
			t.Fatal(err)
		}
	}
	remaining, err := store.ListEvents(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 3 || remaining[0].Seq != 3 || remaining[2].Seq != 5 {
		t.Fatalf("expected retained events 3..5, got %#v", remaining)
	}
}

func TestDatabaseStoreDeleteChannelContentReportsDeletedRows(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.InsertChannelPost(ctx, channelPostRecord{
		PostID:         "post_1",
		ChannelID:      "ch_1",
		RoomID:         "!room:example.com",
		EventID:        "$post-event",
		AuthorMXID:     "@owner:example.com",
		OriginServerTS: 1,
	}); err != nil {
		t.Fatal(err)
	}
	postDeleted, err := store.DeleteChannelPost(ctx, "$post-event")
	if err != nil {
		t.Fatal(err)
	}
	if !postDeleted {
		t.Fatalf("expected post delete by event id to report a deleted row")
	}
	postDeletedAgain, err := store.DeleteChannelPost(ctx, "$post-event")
	if err != nil {
		t.Fatal(err)
	}
	if postDeletedAgain {
		t.Fatalf("expected second post delete to report no deleted row")
	}

	if err := store.InsertChannelComment(ctx, channelCommentRecord{
		CommentID:      "comment_1",
		PostID:         "post_1",
		ChannelID:      "ch_1",
		EventID:        "$comment-event",
		AuthorMXID:     "@owner:example.com",
		OriginServerTS: 2,
	}); err != nil {
		t.Fatal(err)
	}
	commentDeleted, err := store.DeleteChannelComment(ctx, "$comment-event")
	if err != nil {
		t.Fatal(err)
	}
	if !commentDeleted {
		t.Fatalf("expected comment delete by event id to report a deleted row")
	}
	commentDeletedAgain, err := store.DeleteChannelComment(ctx, "$comment-event")
	if err != nil {
		t.Fatal(err)
	}
	if commentDeletedAgain {
		t.Fatalf("expected second comment delete to report no deleted row")
	}
}

func TestDatabaseStoreGetsChannelContentByIDAndEventID(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.InsertChannelPost(ctx, channelPostRecord{
		PostID:         "post_lookup",
		ChannelID:      "ch_lookup",
		RoomID:         "!room:example.com",
		EventID:        "$post-lookup-event",
		AuthorMXID:     "@owner:example.com",
		Body:           "post lookup",
		OriginServerTS: 1,
	}); err != nil {
		t.Fatal(err)
	}
	postByID, ok, err := store.GetChannelPostByID(ctx, "post_lookup", "ch_lookup")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || postByID.EventID != "$post-lookup-event" {
		t.Fatalf("expected post lookup by id, got ok=%v post=%#v", ok, postByID)
	}
	if _, ok, err = store.GetChannelPostByID(ctx, "post_lookup", "other_channel"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("expected channel-scoped post lookup to reject another channel")
	}
	postByEvent, ok, err := store.GetChannelPostByEventID(ctx, "$post-lookup-event", "")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || postByEvent.PostID != "post_lookup" {
		t.Fatalf("expected post lookup by event id, got ok=%v post=%#v", ok, postByEvent)
	}

	if err := store.InsertChannelComment(ctx, channelCommentRecord{
		CommentID:      "comment_lookup",
		PostID:         "post_lookup",
		ChannelID:      "ch_lookup",
		EventID:        "$comment-lookup-event",
		AuthorMXID:     "@owner:example.com",
		Body:           "comment lookup",
		OriginServerTS: 2,
	}); err != nil {
		t.Fatal(err)
	}
	commentByID, ok, err := store.GetChannelCommentByID(ctx, "comment_lookup", "post_lookup")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || commentByID.EventID != "$comment-lookup-event" {
		t.Fatalf("expected comment lookup by id, got ok=%v comment=%#v", ok, commentByID)
	}
	if _, ok, err = store.GetChannelCommentByID(ctx, "comment_lookup", "other_post"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("expected post-scoped comment lookup to reject another post")
	}
	commentByEvent, ok, err := store.GetChannelCommentByEventID(ctx, "$comment-lookup-event", "ch_lookup")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || commentByEvent.CommentID != "comment_lookup" {
		t.Fatalf("expected comment lookup by event id, got ok=%v comment=%#v", ok, commentByEvent)
	}
}

func TestDatabaseStoreContactPeerUniqueMigrationDeduplicatesExistingRows(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	db, writer, err := sqlutil.NewConnectionManager(nil, dbOpts).Connection(&dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	store := NewUnmigratedDatabaseStore(db, writer)
	defer store.Close()

	if _, execErr := store.DB().ExecContext(ctx, `
		CREATE TABLE p2p_contacts (
			room_id TEXT PRIMARY KEY NOT NULL,
			peer_mxid TEXT NOT NULL,
			display_name TEXT NOT NULL,
			domain TEXT NOT NULL,
			status TEXT NOT NULL
		)
	`); execErr != nil {
		t.Fatal(execErr)
	}
	if _, execErr := store.DB().ExecContext(ctx, `CREATE INDEX p2p_contacts_peer_idx ON p2p_contacts(peer_mxid)`); execErr != nil {
		t.Fatal(execErr)
	}
	duplicates := []contactRecord{
		{RoomID: "!pending:example.com", PeerMXID: "@alice:remote.example", DisplayName: "Pending Alice", Domain: "remote.example", Status: "pending_outbound"},
		{RoomID: "!accepted:example.com", PeerMXID: "@alice:remote.example", DisplayName: "Accepted Alice", Domain: "remote.example", Status: "accepted"},
		{RoomID: "!deleted:example.com", PeerMXID: "@alice:remote.example", DisplayName: "Deleted Alice", Domain: "remote.example", Status: "deleted"},
		{RoomID: "!bob:example.com", PeerMXID: "@bob:remote.example", DisplayName: "Bob", Domain: "remote.example", Status: "pending_outbound"},
	}
	for _, contact := range duplicates {
		if _, execErr := store.DB().ExecContext(ctx, `
			INSERT INTO p2p_contacts (room_id, peer_mxid, display_name, domain, status)
			VALUES ($1, $2, $3, $4, $5)
		`, contact.RoomID, contact.PeerMXID, contact.DisplayName, contact.Domain, contact.Status); execErr != nil {
			t.Fatal(execErr)
		}
	}
	if migrationErr := markP2PMigrationsBeforeContactPeerUnique(ctx, store.DB()); migrationErr != nil {
		t.Fatal(migrationErr)
	}

	if migrationErr := store.Migrate(ctx); migrationErr != nil {
		t.Fatal(migrationErr)
	}

	contacts, err := store.ListContacts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(contacts) != 2 {
		t.Fatalf("expected duplicate peers to be compacted, got %#v", contacts)
	}
	alice := findContact(contacts, "@alice:remote.example")
	if alice.RoomID != "!accepted:example.com" || alice.Status != "accepted" {
		t.Fatalf("expected migration to keep accepted contact for duplicate peer, got %#v", alice)
	}
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO p2p_contacts (room_id, peer_mxid, display_name, domain, status)
		VALUES ($1, $2, $3, $4, $5)
	`, "!new-alice:example.com", "@alice:remote.example", "Alice Duplicate", "remote.example", "pending_outbound"); err == nil {
		t.Fatalf("expected migrated contact peer index to reject duplicates")
	}
}

func markP2PMigrationsBeforeContactPeerUnique(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS db_migrations (
			version TEXT PRIMARY KEY NOT NULL,
			time TEXT NOT NULL,
			dendrite_version TEXT NOT NULL
		)
	`); err != nil {
		return err
	}
	versions := []string{
		"p2p: integrated appservice tables v1",
		"p2p: integrated appservice tables v2",
		"p2p: integrated appservice tables v3",
		"p2p: integrated appservice tables v4 member avatars",
		"p2p: integrated appservice tables v5 product mute state",
		"p2p: integrated appservice tables v6 member join order",
		"p2p: integrated appservice tables v7 portal matrix device",
		"p2p: integrated appservice tables v11 channel comment replies",
		"p2p: integrated appservice tables v12 channel comment media",
		"p2p: integrated appservice tables v13 event outbox",
		"p2p: integrated appservice tables v14 channel invite grants",
		"p2p: drop legacy message mirror table v15",
	}
	for _, version := range versions {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO db_migrations (version, time, dendrite_version)
			VALUES ($1, $2, $3)
		`, version, "2026-06-21T00:00:00Z", "test"); err != nil {
			return err
		}
	}
	return nil
}

func TestDatabaseStoreRoomSendActionRemainsDeprecated(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service, err := NewServiceWithStoreAndTransport(ctx, Config{ServerName: "example.com"}, store, transport)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "db-channel",
		"name":       "DB Channel",
	})

	mustHandle[map[string]any](t, service, "channels.mute", map[string]any{
		"channel_id": ch.ChannelID,
	})
	mustHandle[map[string]any](t, service, "channels.unmute", map[string]any{
		"channel_id": ch.ChannelID,
	})
	_, apiErr := service.Handle(ctx, "rooms.send", map[string]any{
		"room_id":      ch.RoomID,
		"content":      "after db unmute",
		"message_type": "text",
	})

	if apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected removed room send to be unknown after db-backed channel unmute, got %#v", apiErr)
	}
}

func TestDatabaseStoreRestoresPortalAndBusinessState(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	cm := sqlutil.NewConnectionManager(nil, dbOpts)
	store, err := NewDatabaseStore(ctx, cm, &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, store)
	if err != nil {
		t.Fatal(err)
	}

	defaultPassword := service.password
	session := bootstrapService(t, service)
	accessToken, _ := session["access_token"].(string)
	if accessToken == "" {
		t.Fatalf("expected access token in session: %#v", session)
	}
	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{"display_name": "Owner Name", "email": "owner@example.com"})
	mustHandle[contactRecord](t, service, "contacts.request", map[string]any{"mxid": "@alice:remote.example", "display_name": "Alice", "avatar_url": "mxc://remote.example/alice"})
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"room_id": "!group:example.com", "name": "Group"})
	mustHandle[callRecord](t, service, "calls.create", map[string]any{"call_id": "call_1", "room_id": "!room:example.com"})
	mustHandle[callRecord](t, service, "calls.event", map[string]any{
		"call_id":        "call_1",
		"event":          "connected",
		"answered_at_ms": int64(1767225600000),
	})
	mustHandle[callRecord](t, service, "calls.event", map[string]any{
		"call_id":       "call_1",
		"event":         "ended",
		"ended_at_ms":   int64(1767225605000),
		"ended_by_mxid": "@alice:remote.example",
		"reason":        "remote_hangup",
		"duration_ms":   int64(5000),
	})
	favorite := mustHandle[favoriteRecord](t, service, "favorites.add", map[string]any{"event_id": "$event", "room_id": "!room:example.com", "content": "fav", "message_type": "text"})
	mustHandle[followRecord](t, service, "follows.add", map[string]any{"domain": "remote.example"})
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{"channel_id": "ch_news", "name": "News", "channel_type": "post"})
	mustHandle[groupRecord](t, service, "groups.invite_policy.update", map[string]any{"room_id": group.RoomID, "invite_policy": "owner"})
	mustHandle[map[string]any](t, service, "channels.mute", map[string]any{"channel_id": ch.ChannelID})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{"channel_id": ch.ChannelID, "body": "post body"})
	mustHandle[channelCommentRecord](t, service, "channels.comments.create", map[string]any{"channel_id": ch.ChannelID, "post_id": post.PostID, "body": "comment body"})
	mustHandle[map[string]any](t, service, "agent.config.update", map[string]any{
		"display_name":         "Storage Agent",
		"avatar_url":           "mxc://example.com/storage-agent",
		"context_window":       float64(96),
		"enabled":              true,
		"model":                "storage-model",
		"system_prompt":        "stored prompt",
		"mcp_blocked_room_ids": []any{"!secret:example.com", ch.RoomID},
	})
	service.systemRoomID = "!system:example.com"
	if err := store.SavePortal(ctx, service.portalStateLocked()); err != nil {
		t.Fatal(err)
	}
	report := reportRecord{
		ReportID:            "report_1",
		TargetType:          "channel",
		TargetRoomID:        ch.RoomID,
		TargetChannelID:     ch.ChannelID,
		TargetName:          ch.Name,
		ReporterMXID:        "@alice:remote.example",
		ReporterDisplayName: "Alice",
		Reason:              "Spam / Advertisement",
		Body:                "ads",
		ImageURLs:           []string{"mxc://example.com/evidence"},
		SystemRoomID:        "!system:example.com",
		EventID:             "$report:example.com",
		OriginServerTS:      1783433640000,
		CreatedAt:           "2026-07-07T10:14:00Z",
	}
	if err := store.InsertReport(ctx, report); err != nil {
		t.Fatal(err)
	}

	reloadedStore, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer reloadedStore.Close()
	reloaded, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, reloadedStore)
	if err != nil {
		t.Fatal(err)
	}

	reloadedSession := mustHandle[map[string]any](t, reloaded, "portal.auth", map[string]any{"password": defaultPassword})
	if reloadedSession["access_token"] != accessToken {
		t.Fatalf("expected access token to survive reload, got %#v want %q", reloadedSession, accessToken)
	}
	if reloadedSession["system_room_id"] != "!system:example.com" {
		t.Fatalf("expected system room id to survive reload, got %#v", reloadedSession)
	}
	profile := mustHandle[ownerProfile](t, reloaded, "profile.get", nil)
	if profile.DisplayName != "Owner Name" || profile.Email != "owner@example.com" {
		t.Fatalf("expected profile to survive reload, got %#v", profile)
	}
	agentConfig := mustHandle[map[string]any](t, reloaded, "agent.config.get", nil)
	blockedRooms := agentConfig["mcp_blocked_room_ids"].([]string)
	if agentConfig["display_name"] != "Storage Agent" ||
		agentConfig["avatar_url"] != "mxc://example.com/storage-agent" ||
		agentConfig["model"] != "storage-model" ||
		agentConfig["system_prompt"] != "stored prompt" ||
		int64Param(agentConfig["context_window"]) != 96 ||
		len(blockedRooms) != 2 ||
		blockedRooms[0] != "!secret:example.com" ||
		blockedRooms[1] != ch.RoomID {
		t.Fatalf("expected agent config to survive reload, got %#v", agentConfig)
	}
	channels := mustHandle[map[string]any](t, reloaded, "channels.list", nil)
	if got, ok := channels["channels"].([]channel); !ok || len(got) != 1 || got[0].Name != "News" || !got[0].Muted {
		t.Fatalf("expected restored channel, got %#v", channels)
	}
	bootstrap := mustHandle[map[string]any](t, reloaded, "sync.bootstrap", nil)
	if got, ok := bootstrap["contacts"].([]contactRecord); !ok || len(got) != 1 || got[0].PeerMXID != "@alice:remote.example" || got[0].AvatarURL != "mxc://remote.example/alice" {
		t.Fatalf("expected restored contacts in sync bootstrap, got %#v", bootstrap)
	}
	if got, ok := bootstrap["groups"].([]groupRecord); !ok || len(got) != 1 || got[0].RoomID != "!group:example.com" || got[0].InvitePolicy != "owner" {
		t.Fatalf("expected restored groups in sync bootstrap, got %#v", bootstrap)
	}
	calls := mustHandle[map[string]any](t, reloaded, "calls.list", nil)
	if got, ok := calls["calls"].([]callRecord); !ok || len(got) != 1 || got[0].CallID != "call_1" || got[0].State != "ended" || got[0].AnsweredAt == "" || got[0].EndedAt == "" || got[0].EndedByMXID != "@alice:remote.example" || got[0].EndReason != "remote_hangup" || got[0].DurationMS != 5000 {
		t.Fatalf("expected restored call, got %#v", calls)
	}
	favorites := mustHandle[map[string]any](t, reloaded, "favorites.list", map[string]any{"message_type": "text"})
	if got, ok := favorites["favorites"].([]favoriteRecord); !ok || len(got) != 1 || got[0].ID != favorite.ID {
		t.Fatalf("expected restored favorite, got %#v", favorites)
	}
	follows := mustHandle[map[string]any](t, reloaded, "follows.list", nil)
	if got, ok := follows["follows"].([]followRecord); !ok || len(got) != 1 || got[0].Domain != "remote.example" {
		t.Fatalf("expected restored follow, got %#v", follows)
	}
	posts := mustHandle[map[string]any](t, reloaded, "channels.posts.list", map[string]any{"channel_id": ch.ChannelID})
	if got, ok := posts["posts"].([]channelPostRecord); !ok || len(got) != 1 || got[0].Body != "post body" {
		t.Fatalf("expected restored post, got %#v", posts)
	}
	comments := mustHandle[map[string]any](t, reloaded, "channels.comments.list", map[string]any{"post_id": post.PostID})
	if got, ok := comments["comments"].([]channelCommentRecord); !ok || len(got) != 1 || got[0].Body != "comment body" {
		t.Fatalf("expected restored comment, got %#v", comments)
	}
	reports, err := reloadedStore.ListReports(ctx, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 ||
		reports[0].ReportID != "report_1" ||
		reports[0].TargetChannelID != ch.ChannelID ||
		reports[0].ReporterMXID != "@alice:remote.example" ||
		len(reports[0].ImageURLs) != 1 ||
		reports[0].ImageURLs[0] != "mxc://example.com/evidence" {
		t.Fatalf("expected restored report, got %#v", reports)
	}
}

func TestDatabaseStorePreservesKickedChannelMemberAutoReject(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, store)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "moderated",
		"room_id":     "!moderated:example.com",
		"name":        "Moderated",
		"visibility":  "public",
		"join_policy": "approval",
	})
	mustHandle[map[string]any](t, service, "channels.invite", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@kicked:remote.example",
	})
	mustHandle[map[string]any](t, service, "channels.member.remove", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@kicked:remote.example",
	})

	reloadedStore, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer reloadedStore.Close()
	reloaded, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, reloadedStore)
	if err != nil {
		t.Fatal(err)
	}
	rejected := mustHandle[map[string]any](t, reloaded, "channels.public.join_request", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@kicked:remote.example",
	})
	if rejected["status"] != "rejected" {
		t.Fatalf("expected kicked member auto reject after reload, got %#v", rejected)
	}
}

func TestDatabaseStorePersistsMemberRequesterNodeBaseURL(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	member := memberRecord{
		RoomID:               "!remote:c.example",
		ChannelID:            "remote_ch",
		UserID:               "@owner:b.example",
		Domain:               "b.example",
		Membership:           "pending",
		Role:                 "member",
		RequesterNodeBaseURL: "https://b.example/_p2p",
	}
	if err := store.UpsertMember(ctx, member); err != nil {
		t.Fatal(err)
	}

	reloadedStore, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer reloadedStore.Close()
	reloaded, ok, err := reloadedStore.LookupMember(ctx, member.RoomID, member.UserID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || reloaded.RequesterNodeBaseURL != member.RequesterNodeBaseURL {
		t.Fatalf("expected requester node base URL to survive reload, got ok=%v member=%#v", ok, reloaded)
	}
}

func TestDatabaseStoreRestoresDeletedContactRequest(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, store)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapService(t, service)
	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice",
	})
	contact = mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":      contact.RoomID,
		"peer_mxid":    contact.PeerMXID,
		"display_name": contact.DisplayName,
		"domain":       contact.Domain,
	})
	mustHandle[map[string]any](t, service, "contacts.delete", map[string]any{
		"room_id": contact.RoomID,
	})

	reloadedStore, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer reloadedStore.Close()
	reloaded, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, reloadedStore)
	if err != nil {
		t.Fatal(err)
	}
	restored := mustHandle[contactRecord](t, reloaded, "contacts.request", map[string]any{
		"mxid":         contact.PeerMXID,
		"display_name": contact.DisplayName,
	})
	if restored.Status != "accepted" || restored.RoomID != contact.RoomID {
		t.Fatalf("expected deleted contact request to restore original room after reload, got %#v", restored)
	}
}

func TestDatabaseStoreRestoresContactRequestRemarkAfterReload(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, store)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapService(t, service)
	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice",
		"remark":       "我是 Adam，请通过好友申请",
	})
	if contact.Remark != "我是 Adam，请通过好友申请" {
		t.Fatalf("expected contact request response to include remark, got %#v", contact)
	}

	reloadedStore, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer reloadedStore.Close()
	reloaded, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, reloadedStore)
	if err != nil {
		t.Fatal(err)
	}
	contacts := mustHandle[map[string]any](t, reloaded, "contacts.list", nil)["contacts"].([]contactRecord)
	if len(contacts) != 1 || contacts[0].Remark != "我是 Adam，请通过好友申请" {
		t.Fatalf("expected reloaded contacts.list to include remark, got %#v", contacts)
	}
}

func TestPortalCredentialsFileIsWrittenAndUpdated(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	credentialsPath := filepath.Join(t.TempDir(), "ops", "bootstrap.json")
	t.Setenv("P2P_PORTAL_CREDENTIALS_FILE", credentialsPath)

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, store)
	if err != nil {
		t.Fatal(err)
	}

	initial := readCredentialsFile(t, credentialsPath)
	requireEightDigitPassword(t, initial.Password)
	if initial.AccessToken == "" || initial.AgentToken == "" || initial.DeviceID != "P2P_PORTAL" {
		t.Fatalf("expected default credentials file with tokens, got %#v", initial)
	}
	session := mustHandle[map[string]any](t, service, "portal.bootstrap", map[string]any{"password": initial.Password})
	accessToken := session["access_token"].(string)
	if accessToken == "" {
		t.Fatalf("expected access token from bootstrap, got %#v", session)
	}
	rotated := mustHandle[map[string]any](t, service, "portal.password", map[string]any{
		"old_password": initial.Password,
		"new_password": "new-secret",
	})
	nextAccessToken := rotated["access_token"].(string)
	if nextAccessToken == "" || nextAccessToken == accessToken {
		t.Fatalf("expected rotated access token, got %#v", rotated)
	}
	updated := readCredentialsFile(t, credentialsPath)
	if updated.Password != "new-secret" || updated.AccessToken != nextAccessToken {
		t.Fatalf("expected credentials file to update after password rotation, got %#v", updated)
	}
}

func TestDatabaseStoreRecallChannelPostRemovesPostAfterReload(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()

	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, store)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapService(t, service)
	createdChannel := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch_delete",
		"room_id":    "!delete:example.com",
		"name":       "Delete",
	})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": createdChannel.ChannelID,
		"room_id":    createdChannel.RoomID,
		"body":       "delete me",
	})

	reloadedStore, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer reloadedStore.Close()
	reloaded, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, reloadedStore)
	if err != nil {
		t.Fatal(err)
	}
	mustHandle[map[string]any](t, reloaded, "channels.posts.recall", map[string]any{
		"post_id": post.PostID,
		"room_id": createdChannel.RoomID,
	})

	againStore, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer againStore.Close()
	again, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, againStore)
	if err != nil {
		t.Fatal(err)
	}
	posts := mustHandle[map[string]any](t, again, "channels.posts.list", map[string]any{"channel_id": createdChannel.ChannelID})
	if got := posts["posts"].([]channelPostRecord); len(got) != 0 {
		t.Fatalf("expected recalled post to stay deleted after reload, got %#v", got)
	}
}

func mustHandle[T any](t *testing.T, service *Service, action string, params map[string]any) T {
	t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	result, apiErr := service.Handle(context.Background(), action, params)
	if apiErr != nil {
		t.Fatalf("%s failed: %#v", action, apiErr)
	}
	typed, ok := result.(T)
	if !ok {
		t.Fatalf("%s returned %T: %#v", action, result, result)
	}
	return typed
}

func readCredentialsFile(t *testing.T, path string) portalCredentialsFile {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var credentials portalCredentialsFile
	if err := json.Unmarshal(data, &credentials); err != nil {
		t.Fatal(err)
	}
	return credentials
}
