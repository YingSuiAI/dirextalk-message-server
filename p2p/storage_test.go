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

	"github.com/YingSuiAI/direxio-message-server/internal/sqlutil"
	"github.com/YingSuiAI/direxio-message-server/setup/config"
	"github.com/YingSuiAI/direxio-message-server/test"
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
		"p2p: integrated appservice tables v9 reports",
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
	profile := mustHandle[ownerProfile](t, reloaded, "profile.get", nil)
	if profile.DisplayName != "Owner Name" || profile.Email != "owner@example.com" {
		t.Fatalf("expected profile to survive reload, got %#v", profile)
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
