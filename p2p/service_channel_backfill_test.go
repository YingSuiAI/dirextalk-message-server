package p2p

import (
	"context"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/matrixhistory"
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
