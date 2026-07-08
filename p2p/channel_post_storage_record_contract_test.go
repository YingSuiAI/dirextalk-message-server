package p2p

import (
	"reflect"
	"testing"
)

func TestChannelPostStorageRecordExcludesFacadeFields(t *testing.T) {
	postType := reflect.TypeOf(channelPostStorageRecord{})
	for _, fieldName := range []string{"ReactionCount", "ReactedByMe", "Operation", "Conversation"} {
		if _, ok := postType.FieldByName(fieldName); ok {
			t.Fatalf("durable channel post storage record must not expose response-only field %s", fieldName)
		}
	}
}

func TestChannelPostStorageRecordConversionUsesOnlyDurableFields(t *testing.T) {
	conversation := conversationView{ConversationID: "conv_channel"}
	post := channelPostRecord{
		PostID:         "post_1",
		ChannelID:      "channel_1",
		RoomID:         "!channel:example.com",
		EventID:        "$post",
		AuthorMXID:     "@owner:example.com",
		AuthorName:     "Owner",
		Body:           "hello",
		MessageType:    "text",
		MediaJSON:      `{"url":"mxc://example.com/media"}`,
		OriginServerTS: 123,
		CommentCount:   4,
		ReactionCount:  9,
		ReactedByMe:    true,
		Operation:      map[string]any{"action": "channels.posts.create"},
		Conversation:   &conversation,
	}

	stored := channelPostStorageRecordFromPost(post)
	if stored.PostID != post.PostID ||
		stored.ChannelID != post.ChannelID ||
		stored.RoomID != post.RoomID ||
		stored.EventID != post.EventID ||
		stored.AuthorMXID != post.AuthorMXID ||
		stored.AuthorName != post.AuthorName ||
		stored.Body != post.Body ||
		stored.MessageType != post.MessageType ||
		stored.MediaJSON != post.MediaJSON ||
		stored.OriginServerTS != post.OriginServerTS ||
		stored.CommentCount != post.CommentCount {
		t.Fatalf("storage conversion lost durable channel post fields: %#v", stored)
	}

	restored := channelPostRecordFromStorage(stored)
	if restored.ReactionCount != 0 || restored.ReactedByMe || restored.Operation != nil || restored.Conversation != nil {
		t.Fatalf("storage conversion must not restore facade fields, got %#v", restored)
	}
	if restored.PostID != post.PostID ||
		restored.ChannelID != post.ChannelID ||
		restored.RoomID != post.RoomID ||
		restored.EventID != post.EventID ||
		restored.AuthorMXID != post.AuthorMXID ||
		restored.AuthorName != post.AuthorName ||
		restored.Body != post.Body ||
		restored.MessageType != post.MessageType ||
		restored.MediaJSON != post.MediaJSON ||
		restored.OriginServerTS != post.OriginServerTS ||
		restored.CommentCount != post.CommentCount {
		t.Fatalf("facade conversion lost durable channel post fields: %#v", restored)
	}
}
