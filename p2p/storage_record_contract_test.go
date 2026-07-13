package p2p

import (
	"reflect"
	"testing"
)

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
	expectedStored := channelCommentStorageRecord{
		CommentID:         comment.CommentID,
		PostID:            comment.PostID,
		ChannelID:         comment.ChannelID,
		EventID:           comment.EventID,
		AuthorMXID:        comment.AuthorMXID,
		AuthorName:        comment.AuthorName,
		Body:              comment.Body,
		MessageType:       comment.MessageType,
		MediaJSON:         comment.MediaJSON,
		ReplyToCommentID:  comment.ReplyToCommentID,
		ReplyToAuthorMXID: comment.ReplyToAuthorMXID,
		MentionsJSON:      comment.MentionsJSON,
		OriginServerTS:    comment.OriginServerTS,
	}
	if !reflect.DeepEqual(stored, expectedStored) {
		t.Fatalf("storage conversion lost durable channel comment fields: %#v", stored)
	}

	restored := channelCommentRecordFromStorage(stored)
	expectedRestored := channelCommentRecord{
		CommentID:         comment.CommentID,
		PostID:            comment.PostID,
		ChannelID:         comment.ChannelID,
		EventID:           comment.EventID,
		AuthorMXID:        comment.AuthorMXID,
		AuthorName:        comment.AuthorName,
		Body:              comment.Body,
		MessageType:       comment.MessageType,
		MediaJSON:         comment.MediaJSON,
		ReplyToCommentID:  comment.ReplyToCommentID,
		ReplyToAuthorMXID: comment.ReplyToAuthorMXID,
		MentionsJSON:      comment.MentionsJSON,
		OriginServerTS:    comment.OriginServerTS,
	}
	if !reflect.DeepEqual(restored, expectedRestored) {
		t.Fatalf("facade conversion lost durable channel comment fields: %#v", restored)
	}
}
