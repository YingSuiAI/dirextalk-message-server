package p2p

import (
	"context"
	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/matrixhistory"
	"github.com/matrix-org/gomatrixserverlib"
	"net/http"
	"strings"
	"testing"
)

func TestServiceCreatesChannelRoomStateThroughTransport(t *testing.T) {
	transport := &alreadyJoinedOnceInviteTransport{recordingTransport: recordingTransport{roomID: "!channel:example.com"}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "ch_post",
		"name":         "Posts",
		"description":  "Announcements",
		"avatar_url":   "mxc://example.com/ch",
		"visibility":   "private",
		"join_policy":  "approval",
		"channel_type": "post",
	})
	if ch.RoomID != transport.roomID {
		t.Fatalf("expected transport room_id, got %#v", ch)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("expected one transport room create, got %#v", transport.createRooms)
	}
	state := transport.createRooms[0].InitialState
	profileState, ok := initialStateOfType(state, DirextalkRoomProfileEventType)
	if len(state) != 2 || !ok || profileState.Content["room_type"] != DirextalkRoomTypeChannel || profileState.Content["channel_type"] != "post" {
		t.Fatalf("expected Dirextalk channel profile state, got %#v", state)
	}
	if got, ok := initialHistoryVisibility(transport.createRooms[0]); !ok || got != string(gomatrixserverlib.HistoryVisibilityShared) {
		t.Fatalf("expected shared post channel history visibility, got %q ok=%v in %#v", got, ok, state)
	}
	content := profileState.Content
	for key, want := range map[string]any{
		"channel_id":       "ch_post",
		"name":             "Posts",
		"description":      "Announcements",
		"avatar_url":       "mxc://example.com/ch",
		"visibility":       "private",
		"join_policy":      "approval",
		"channel_type":     "post",
		"comments_enabled": true,
		"dissolved":        false,
	} {
		if content[key] != want {
			t.Fatalf("expected channel state %s=%#v, got %#v", key, want, content)
		}
	}
}

func TestChannelUpdateAndDissolvePublishRoomStateThroughTransport(t *testing.T) {
	transport := &alreadyJoinedOnceInviteTransport{recordingTransport: recordingTransport{roomID: "!channel:example.com"}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "ch_lifecycle",
		"name":        "Before",
		"visibility":  "public",
		"join_policy": "open",
	})

	updated := mustHandle[channel](t, service, "channels.update", map[string]any{
		"channel_id":  ch.ChannelID,
		"name":        "After",
		"visibility":  "private",
		"join_policy": "approval",
	})
	if updated.Name != "After" || updated.Visibility != "private" || updated.JoinPolicy != "approval" {
		t.Fatalf("expected updated channel response, got %#v", updated)
	}
	if len(transport.stateEvents) != 1 {
		t.Fatalf("expected channel update to publish one state event, got %#v", transport.stateEvents)
	}
	updateState := transport.stateEvents[0]
	if updateState.RoomID != ch.RoomID || updateState.Event.Type != DirextalkRoomProfileEventType || updateState.Event.Content["room_type"] != DirextalkRoomTypeChannel || updateState.Event.Content["name"] != "After" || updateState.Event.Content["join_policy"] != "approval" {
		t.Fatalf("expected updated channel metadata state, got %#v", updateState)
	}

	mustHandle[map[string]any](t, service, "channels.dissolve", map[string]any{"channel_id": ch.ChannelID})
	if len(transport.stateEvents) != 2 {
		t.Fatalf("expected channel dissolve to publish second state event, got %#v", transport.stateEvents)
	}
	dissolveState := transport.stateEvents[1]
	if dissolveState.RoomID != ch.RoomID || dissolveState.Event.Content["dissolved"] != true {
		t.Fatalf("expected dissolved channel state, got %#v", dissolveState)
	}
}

func TestChannelUpdateIgnoresChannelTypeChanges(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "ch_to_post",
		"name":         "Before",
		"channel_type": "chat",
	})

	updated := mustHandle[channel](t, service, "channels.update", map[string]any{
		"channel_id":   ch.ChannelID,
		"name":         "Still Chat",
		"channel_type": "post",
	})
	if updated.ChannelType != "chat" || updated.Name != "Still Chat" {
		t.Fatalf("expected channel_type update to be ignored while mutable fields apply, got %#v", updated)
	}
	if len(transport.stateEvents) != 1 {
		t.Fatalf("expected ignored channel_type update to publish only metadata state, got %#v", transport.stateEvents)
	}
	if transport.stateEvents[0].Event.Content["channel_type"] != "chat" {
		t.Fatalf("expected published profile to preserve original channel_type, got %#v", transport.stateEvents[0])
	}
}

func TestGroupCreateUpdateAndDissolvePublishRoomStateThroughTransport(t *testing.T) {
	transport := &recordingTransport{roomID: "!group:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"name":          "Before",
		"topic":         "Original",
		"avatar_url":    "mxc://example.com/group",
		"invite_policy": "member",
	})
	if len(transport.createRooms) != 1 {
		t.Fatalf("expected group create to publish initial state, got %#v", transport.createRooms)
	}
	createState, ok := initialStateOfType(transport.createRooms[0].InitialState, DirextalkRoomProfileEventType)
	if !ok || createState.Content["room_type"] != DirextalkRoomTypeGroup || createState.Content["name"] != "Before" || createState.Content["invite_policy"] != "member" {
		t.Fatalf("expected group metadata initial state, got %#v", transport.createRooms[0].InitialState)
	}

	updated := mustHandle[groupRecord](t, service, "groups.update", map[string]any{
		"room_id":       group.RoomID,
		"name":          "After",
		"invite_policy": "owner",
	})
	if updated.Name != "After" || updated.InvitePolicy != "owner" {
		t.Fatalf("expected updated group response, got %#v", updated)
	}
	if len(transport.stateEvents) != 1 {
		t.Fatalf("expected group update to publish one state event, got %#v", transport.stateEvents)
	}
	updateState := transport.stateEvents[0]
	if updateState.RoomID != group.RoomID || updateState.Event.Type != DirextalkRoomProfileEventType || updateState.Event.Content["room_type"] != DirextalkRoomTypeGroup || updateState.Event.Content["name"] != "After" || updateState.Event.Content["invite_policy"] != "owner" {
		t.Fatalf("expected updated group metadata state, got %#v", updateState)
	}

	mustHandle[map[string]any](t, service, "groups.dissolve", map[string]any{"room_id": group.RoomID})
	if len(transport.stateEvents) != 2 {
		t.Fatalf("expected group dissolve to publish second state event, got %#v", transport.stateEvents)
	}
	dissolveState := transport.stateEvents[1]
	if dissolveState.RoomID != group.RoomID || dissolveState.Event.Content["dissolved"] != true {
		t.Fatalf("expected dissolved group state, got %#v", dissolveState)
	}
}

func TestChannelPostAndCommentUseChannelRoomAndMediaThroughTransport(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "ch_media",
		"name":         "Media Posts",
		"channel_type": "post",
	})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id":   ch.ChannelID,
		"message_type": "m.image",
		"body":         "photo.jpg",
		"media_json":   `{"url":"mxc://example.com/photo","info":{"mimetype":"image/jpeg"}}`,
	})
	comment := mustHandle[channelCommentRecord](t, service, "channels.comments.create", map[string]any{
		"channel_id":   ch.ChannelID,
		"post_id":      post.PostID,
		"message_type": "m.image",
		"body":         "reply.jpg",
		"media_json":   `{"url":"mxc://example.com/reply","info":{"mimetype":"image/jpeg"}}`,
	})

	if post.RoomID != ch.RoomID || comment.ChannelID != ch.ChannelID {
		t.Fatalf("expected post/comment to use channel identity, post=%#v comment=%#v channel=%#v", post, comment, ch)
	}
	if len(transport.messages) != 2 {
		t.Fatalf("expected post and comment to be sent through Matrix transport, got %#v", transport.messages)
	}
	postContent := transport.messages[0].Content
	if transport.messages[0].RoomID != ch.RoomID || postContent["msgtype"] != "m.image" || postContent["url"] != "mxc://example.com/photo" {
		t.Fatalf("expected image post Matrix content with channel room, got %#v", transport.messages[0])
	}
	commentContent := transport.messages[1].Content
	if transport.messages[1].RoomID != ch.RoomID || commentContent["msgtype"] != "m.image" || commentContent["url"] != "mxc://example.com/reply" {
		t.Fatalf("expected image comment Matrix content with channel room, got %#v", transport.messages[1])
	}
}

func TestChannelReactionDoesNotSaveProjectionWhenMatrixSendFails(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "ch_reaction",
		"room_id":      "!channel:example.com",
		"name":         "Reaction Channel",
		"channel_type": "post",
	})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": ch.ChannelID,
		"body":       "post",
	})
	service.transport = &failingSendTransport{err: productpolicy.Forbidden("channel comments are disabled")}

	_, apiErr := service.Handle(context.Background(), "channels.post_reaction.toggle", map[string]any{
		"channel_id": ch.ChannelID,
		"post_id":    post.PostID,
		"reaction":   "like",
	})

	if apiErr == nil || apiErr.Status != http.StatusForbidden {
		t.Fatalf("expected ProductPolicy failure to return 403, got %#v", apiErr)
	}
	service.mu.Lock()
	ownerMXID := service.ownerMXID
	service.mu.Unlock()
	if reaction, ok, err := service.getReaction(context.Background(), "post", post.PostID, "like", ownerMXID); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("reaction projection should not be saved when Matrix send fails, got %#v", reaction)
	}
}

type fakeChannelBackfillReader struct {
	events    []matrixhistory.Event
	responses [][]matrixhistory.Event
	err       error
	errs      []error
	calls     int
}

func (r *fakeChannelBackfillReader) ListOrdinaryMessages(ctx context.Context, roomID string, page mcpMessagePage) (mcpMessagePageResult, error) {
	return mcpMessagePageResult{}, nil
}

func (r *fakeChannelBackfillReader) ListChannelContent(ctx context.Context, roomID string, limit int) ([]matrixhistory.Event, error) {
	r.calls++
	if len(r.errs) > 0 {
		index := r.calls - 1
		if index >= len(r.errs) {
			index = len(r.errs) - 1
		}
		if r.errs[index] != nil {
			return nil, r.errs[index]
		}
	} else if r.err != nil {
		return nil, r.err
	}
	events := r.events
	if len(r.responses) > 0 {
		index := r.calls - 1
		if index >= len(r.responses) {
			index = len(r.responses) - 1
		}
		events = r.responses[index]
	}
	if limit > 0 && len(events) > limit {
		return events[:limit], nil
	}
	return events, nil
}

func TestChannelJoinBackfillsHistoricalPostsCommentsAndReactions(t *testing.T) {
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
			EventID:        "$reaction-post:example.com",
			Sender:         "@alice:example.com",
			OriginServerTS: 3000,
			Content: map[string]any{
				"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": "$post-one:example.com", "key": "like"},
			},
		},
		{
			Type:           "m.room.message",
			EventID:        "$comment-one:example.com",
			Sender:         "@owner:example.com",
			OriginServerTS: 2000,
			Content: map[string]any{
				"p2p_kind":   "channel_comment",
				"channel_id": ch.ChannelID,
				"post_id":    "post_one",
				"comment_id": "comment_one",
				"body":       "historical comment",
				"msgtype":    "m.text",
			},
		},
		{
			Type:           "m.reaction",
			EventID:        "$reaction-comment:example.com",
			Sender:         "@alice:example.com",
			OriginServerTS: 4000,
			Content: map[string]any{
				"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": "$comment-one:example.com", "key": "like"},
			},
		},
		{
			Type:           "m.room.message",
			EventID:        "$post-one:example.com",
			Sender:         "@owner:example.com",
			OriginServerTS: 1000,
			Content: map[string]any{
				"p2p_kind":   "channel_post",
				"channel_id": ch.ChannelID,
				"post_id":    "post_one",
				"body":       "historical post",
				"msgtype":    "m.text",
			},
		},
		{
			Type:           "m.room.message",
			EventID:        "$post-two:example.com",
			Sender:         "@owner:example.com",
			OriginServerTS: 1100,
			Content: map[string]any{
				"p2p_kind":   "channel_post",
				"channel_id": ch.ChannelID,
				"post_id":    "post_two",
				"body":       "unliked post",
				"msgtype":    "m.text",
			},
		},
		{
			Type:           "m.reaction",
			EventID:        "$reaction-post-two-on:example.com",
			Sender:         "@alice:example.com",
			OriginServerTS: 1200,
			Content: map[string]any{
				"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": "$post-two:example.com", "key": "like"},
				"active":       true,
			},
		},
		{
			Type:           "m.reaction",
			EventID:        "$reaction-post-two-off:example.com",
			Sender:         "@alice:example.com",
			OriginServerTS: 1300,
			Content: map[string]any{
				"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": "$post-two:example.com", "key": "like"},
				"active":       false,
			},
		},
	}})

	joined := mustHandle[map[string]any](t, service, "channels.join", map[string]any{
		"room_id":    ch.RoomID,
		"channel_id": ch.ChannelID,
		"user_id":    "@alice:example.com",
	})
	if joined["status"] != "ok" {
		t.Fatalf("expected channels.join ok, got %#v", joined)
	}

	posts := mustHandle[map[string]any](t, service, "channels.posts.list", map[string]any{
		"channel_id": ch.ChannelID,
	})["posts"].([]channelPostRecord)
	if len(posts) != 2 || posts[0].PostID != "post_one" || posts[0].Body != "historical post" || posts[0].CommentCount != 1 || posts[0].ReactionCount != 1 {
		t.Fatalf("expected backfilled post with comment/reaction counts, got %#v", posts)
	}
	if posts[1].PostID != "post_two" || posts[1].ReactionCount != 0 {
		t.Fatalf("expected active=false reaction event to clear backfilled reaction count, got %#v", posts)
	}
	comments := mustHandle[map[string]any](t, service, "channels.comments.list", map[string]any{
		"post_id": "post_one",
	})["comments"].([]channelCommentRecord)
	if len(comments) != 1 || comments[0].CommentID != "comment_one" || comments[0].Body != "historical comment" || comments[0].ReactionCount != 1 {
		t.Fatalf("expected backfilled comment with reaction count, got %#v", comments)
	}
}

func TestUnifiedChatChannelJoinBackfillsHistoricalContent(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":   "chat_channel",
		"name":         "Chat Channel",
		"channel_type": "chat",
	})
	reader := &fakeChannelBackfillReader{events: []matrixhistory.Event{
		{
			Type:           "m.room.message",
			EventID:        "$post-one:example.com",
			Sender:         "@owner:example.com",
			OriginServerTS: 1000,
			Content: map[string]any{
				"p2p_kind": "channel_post",
				"post_id":  "post_one",
				"body":     "should sync",
				"msgtype":  "m.text",
			},
		},
	}}
	service.SetMatrixMessageReader(reader)

	joined := mustHandle[map[string]any](t, service, "channels.join", map[string]any{
		"room_id":    ch.RoomID,
		"channel_id": ch.ChannelID,
		"user_id":    "@alice:example.com",
	})
	if joined["status"] != "ok" {
		t.Fatalf("expected channels.join ok, got %#v", joined)
	}
	if reader.calls != 1 {
		t.Fatalf("unified channel join should backfill historical channel content once, called reader %d times", reader.calls)
	}
	posts := mustHandle[map[string]any](t, service, "channels.posts.list", map[string]any{
		"channel_id": ch.ChannelID,
	})["posts"].([]channelPostRecord)
	if len(posts) != 1 || posts[0].PostID != "post_one" || posts[0].Body != "should sync" {
		t.Fatalf("unified channel join should project historical post content, got %#v", posts)
	}
}

func TestChannelPostRecallDoesNotDeleteProjectionWhenMatrixRedactionFails(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch_recall",
		"room_id":    "!channel:example.com",
		"name":       "Recall Channel",
	})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": ch.ChannelID,
		"body":       "post",
	})
	service.transport = &failingRedactTransport{err: productpolicy.Forbidden("sender cannot redact another sender in dirextalk room")}

	_, apiErr := service.Handle(context.Background(), "channels.posts.recall", map[string]any{
		"channel_id": ch.ChannelID,
		"post_id":    post.PostID,
	})

	if apiErr == nil || apiErr.Status != http.StatusForbidden {
		t.Fatalf("expected ProductPolicy redaction failure to return 403, got %#v", apiErr)
	}
	posts := mustHandle[map[string]any](t, service, "channels.posts.list", map[string]any{"channel_id": ch.ChannelID})["posts"].([]channelPostRecord)
	if len(posts) != 1 || posts[0].PostID != post.PostID {
		t.Fatalf("post projection should remain when Matrix redaction fails, got %#v", posts)
	}
}

func TestChannelCommentRecallDoesNotDeleteProjectionWhenMatrixRedactionFails(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch_comment_recall",
		"room_id":    "!channel:example.com",
		"name":       "Comment Recall Channel",
	})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": ch.ChannelID,
		"body":       "post",
	})
	comment := mustHandle[channelCommentRecord](t, service, "channels.comments.create", map[string]any{
		"channel_id": ch.ChannelID,
		"post_id":    post.PostID,
		"body":       "comment",
	})
	service.transport = &failingRedactTransport{err: productpolicy.Forbidden("sender cannot redact another sender in dirextalk room")}

	_, apiErr := service.Handle(context.Background(), "channels.comments.recall", map[string]any{
		"channel_id":  ch.ChannelID,
		"post_id":     post.PostID,
		"comment_id":  comment.CommentID,
		"target_type": "comment",
	})

	if apiErr == nil || apiErr.Status != http.StatusForbidden {
		t.Fatalf("expected ProductPolicy redaction failure to return 403, got %#v", apiErr)
	}
	comments := mustHandle[map[string]any](t, service, "channels.comments.list", map[string]any{"post_id": post.PostID})["comments"].([]channelCommentRecord)
	if len(comments) != 1 || comments[0].CommentID != comment.CommentID {
		t.Fatalf("comment projection should remain when Matrix redaction fails, got %#v", comments)
	}
}

func TestChannelPostRecallWithTransportDoesNotRequireLocalOwnerProjection(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch_stale_owner",
		"name":       "Stale Owner Projection",
	})
	service.mu.Lock()
	delete(service.members, ch.RoomID+"|"+service.ownerMXID)
	service.posts = append(service.posts, channelPostRecord{
		PostID:     "post_remote",
		ChannelID:  ch.ChannelID,
		RoomID:     ch.RoomID,
		EventID:    "$remote-post:example.com",
		AuthorMXID: "@remote:example.com",
		Body:       "remote post",
	})
	service.mu.Unlock()

	_, apiErr := service.Handle(context.Background(), "channels.posts.recall", map[string]any{
		"channel_id": ch.ChannelID,
		"post_id":    "post_remote",
	})

	if apiErr != nil {
		t.Fatalf("expected transport ProductPolicy to be authoritative for recall, got %#v", apiErr)
	}
	if len(transport.redactions) != 1 || !strings.Contains(transport.redactions[0], "$remote-post:example.com") {
		t.Fatalf("expected recall to send Matrix redaction, got %#v", transport.redactions)
	}
}
