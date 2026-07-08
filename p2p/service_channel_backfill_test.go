package p2p

import (
	"context"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/matrixhistory"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
)

func TestChannelContentBackfillProjectsReactionsAfterTargetsDespiteTimestamps(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":       "post_channel",
		"name":             "Post Channel",
		"channel_type":     "post",
		"comments_enabled": true,
	})
	service.SetMatrixMessageReader(&fakeChannelBackfillReader{events: []matrixhistory.Event{
		{
			Type:           "m.reaction",
			EventID:        "$reaction-before-post:example.com",
			Sender:         "@alice:example.com",
			OriginServerTS: 500,
			Content: map[string]any{
				"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": "$post-one:example.com", "key": "like"},
			},
		},
		{
			Type:           "m.room.message",
			EventID:        "$post-one:example.com",
			Sender:         "@owner:example.com",
			OriginServerTS: 1000,
			Content: map[string]any{
				"p2p_kind": "channel_post",
				"post_id":  "post_one",
				"body":     "historical post",
				"msgtype":  "m.text",
			},
		},
	}})

	if err := service.backfillJoinedChannelContent(context.Background(), ch.RoomID, ch.ChannelID); err != nil {
		t.Fatalf("backfillJoinedChannelContent returned error: %v", err)
	}
	posts := mustHandle[map[string]any](t, service, "channels.posts.list", map[string]any{
		"channel_id": ch.ChannelID,
	})["posts"].([]channelPostRecord)
	if len(posts) != 1 || posts[0].PostID != "post_one" || posts[0].ReactionCount != 1 {
		t.Fatalf("expected backfilled reaction to resolve after post projection, got %#v", posts)
	}
}

func TestChannelContentBackfillPersistsProjectionAfterReload(t *testing.T) {
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
		"channel_id":       "persisted_channel",
		"room_id":          "!persisted-channel:example.com",
		"name":             "Persisted Channel",
		"channel_type":     "post",
		"comments_enabled": true,
	})
	service.SetMatrixMessageReader(&fakeChannelBackfillReader{events: []matrixhistory.Event{
		{
			Type:           "m.room.message",
			EventID:        "$post-one:example.com",
			Sender:         "@owner:example.com",
			OriginServerTS: 1000,
			Content: map[string]any{
				"p2p_kind": "channel_post",
				"post_id":  "post_one",
				"body":     "persisted historical post",
				"msgtype":  "m.text",
			},
		},
		{
			Type:           "m.room.message",
			EventID:        "$comment-one:example.com",
			Sender:         "@owner:example.com",
			OriginServerTS: 1100,
			Content: map[string]any{
				"p2p_kind":   "channel_comment",
				"post_id":    "post_one",
				"comment_id": "comment_one",
				"body":       "persisted historical comment",
				"msgtype":    "m.text",
			},
		},
		{
			Type:           "m.reaction",
			EventID:        "$reaction-post-one:example.com",
			Sender:         "@owner:example.com",
			OriginServerTS: 1200,
			Content: map[string]any{
				"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": "$post-one:example.com", "key": "like"},
			},
		},
		{
			Type:           "m.reaction",
			EventID:        "$reaction-comment-one:example.com",
			Sender:         "@owner:example.com",
			OriginServerTS: 1300,
			Content: map[string]any{
				"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": "$comment-one:example.com", "key": "like"},
			},
		},
	}})
	if err := service.backfillJoinedChannelContent(ctx, ch.RoomID, ch.ChannelID); err != nil {
		t.Fatalf("backfillJoinedChannelContent returned error: %v", err)
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
	posts := mustHandle[map[string]any](t, reloaded, "channels.posts.list", map[string]any{
		"channel_id": ch.ChannelID,
	})["posts"].([]channelPostRecord)
	if len(posts) != 1 || posts[0].PostID != "post_one" || posts[0].CommentCount != 1 || posts[0].ReactionCount != 1 || !posts[0].ReactedByMe {
		t.Fatalf("expected persisted backfilled post counts after reload, got %#v", posts)
	}
	comments := mustHandle[map[string]any](t, reloaded, "channels.comments.list", map[string]any{
		"post_id": "post_one",
	})["comments"].([]channelCommentRecord)
	if len(comments) != 1 || comments[0].CommentID != "comment_one" || comments[0].ReactionCount != 1 || !comments[0].ReactedByMe {
		t.Fatalf("expected persisted backfilled comment counts after reload, got %#v", comments)
	}
}
