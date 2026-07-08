package p2p

import (
	"reflect"
	"testing"
)

func TestChannelCommentStorageRecordExcludesFacadeFields(t *testing.T) {
	commentType := reflect.TypeOf(channelCommentStorageRecord{})
	for _, fieldName := range []string{"ReactionCount", "ReactedByMe", "Operation", "Conversation"} {
		if _, ok := commentType.FieldByName(fieldName); ok {
			t.Fatalf("durable channel comment storage record must not expose response-only field %s", fieldName)
		}
	}
}

func TestChannelCommentStorageRecordConversionUsesOnlyDurableFields(t *testing.T) {
	conversation := conversationView{ConversationID: "conv_channel"}
	comment := channelCommentRecord{
		CommentID:         "comment_1",
		PostID:            "post_1",
		ChannelID:         "channel_1",
		EventID:           "$comment",
		AuthorMXID:        "@owner:example.com",
		AuthorName:        "Owner",
		Body:              "hello",
		MessageType:       "text",
		MediaJSON:         `{"url":"mxc://example.com/media"}`,
		ReplyToCommentID:  "comment_parent",
		ReplyToAuthorMXID: "@peer:example.com",
		MentionsJSON:      `["@peer:example.com"]`,
		OriginServerTS:    123,
		ReactionCount:     9,
		ReactedByMe:       true,
		Operation:         map[string]any{"action": "channels.comments.create"},
		Conversation:      &conversation,
	}

	stored := channelCommentStorageRecordFromComment(comment)
	if stored.CommentID != comment.CommentID ||
		stored.PostID != comment.PostID ||
		stored.ChannelID != comment.ChannelID ||
		stored.EventID != comment.EventID ||
		stored.AuthorMXID != comment.AuthorMXID ||
		stored.AuthorName != comment.AuthorName ||
		stored.Body != comment.Body ||
		stored.MessageType != comment.MessageType ||
		stored.MediaJSON != comment.MediaJSON ||
		stored.ReplyToCommentID != comment.ReplyToCommentID ||
		stored.ReplyToAuthorMXID != comment.ReplyToAuthorMXID ||
		stored.MentionsJSON != comment.MentionsJSON ||
		stored.OriginServerTS != comment.OriginServerTS {
		t.Fatalf("storage conversion lost durable channel comment fields: %#v", stored)
	}

	restored := channelCommentRecordFromStorage(stored)
	if restored.ReactionCount != 0 || restored.ReactedByMe || restored.Operation != nil || restored.Conversation != nil {
		t.Fatalf("storage conversion must not restore facade fields, got %#v", restored)
	}
	if restored.CommentID != comment.CommentID ||
		restored.PostID != comment.PostID ||
		restored.ChannelID != comment.ChannelID ||
		restored.EventID != comment.EventID ||
		restored.AuthorMXID != comment.AuthorMXID ||
		restored.AuthorName != comment.AuthorName ||
		restored.Body != comment.Body ||
		restored.MessageType != comment.MessageType ||
		restored.MediaJSON != comment.MediaJSON ||
		restored.ReplyToCommentID != comment.ReplyToCommentID ||
		restored.ReplyToAuthorMXID != comment.ReplyToAuthorMXID ||
		restored.MentionsJSON != comment.MentionsJSON ||
		restored.OriginServerTS != comment.OriginServerTS {
		t.Fatalf("facade conversion lost durable channel comment fields: %#v", restored)
	}
}
