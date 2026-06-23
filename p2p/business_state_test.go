package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/sqlutil"
	"github.com/YingSuiAI/direxio-message-server/setup/config"
	"github.com/YingSuiAI/direxio-message-server/test"
)

func TestReactionTogglePersistsAndListsActiveReaction(t *testing.T) {
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

	first := mustHandle[map[string]any](t, service, "channels.post_reaction.toggle", map[string]any{
		"channel_id": "ch",
		"post_id":    "post_1",
		"reaction":   "like",
	})
	if first["active"] != true || int64Param(first["reaction_count"]) != 1 {
		t.Fatalf("expected first toggle to activate reaction, got %#v", first)
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
	reactions := mustHandle[map[string]any](t, reloaded, "channels.my_reactions", nil)
	got, ok := reactions["reactions"].([]reactionRecord)
	if !ok || len(got) != 1 || got[0].PostID != "post_1" || !got[0].Active {
		t.Fatalf("expected active reaction after reload, got %#v", reactions)
	}

	second := mustHandle[map[string]any](t, reloaded, "channels.post_reaction.toggle", map[string]any{
		"channel_id": "ch",
		"post_id":    "post_1",
		"reaction":   "like",
	})
	if second["active"] != false || int64Param(second["reaction_count"]) != 0 {
		t.Fatalf("expected second toggle to deactivate reaction, got %#v", second)
	}
}

func TestCommentReactionTogglePersistsAndListsActiveReaction(t *testing.T) {
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
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": "ch",
		"body":       "post",
	})
	comment := mustHandle[channelCommentRecord](t, service, "channels.comments.create", map[string]any{
		"channel_id": "ch",
		"post_id":    post.PostID,
		"body":       "comment",
	})

	first := mustHandle[map[string]any](t, service, "channels.comment_reaction.toggle", map[string]any{
		"channel_id": "ch",
		"post_id":    post.PostID,
		"comment_id": comment.CommentID,
		"reaction":   "like",
	})
	if first["active"] != true || int64Param(first["reaction_count"]) != 1 || first["comment_id"] != comment.CommentID {
		t.Fatalf("expected first comment toggle to activate reaction, got %#v", first)
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
	reactions := mustHandle[map[string]any](t, reloaded, "channels.my_reactions", nil)
	got, ok := reactions["reactions"].([]reactionRecord)
	if !ok || len(got) != 1 || got[0].TargetType != "comment" || got[0].CommentID != comment.CommentID || !got[0].Active {
		t.Fatalf("expected active comment reaction after reload, got %#v", reactions)
	}

	second := mustHandle[map[string]any](t, reloaded, "channels.comment_reaction.toggle", map[string]any{
		"channel_id": "ch",
		"post_id":    post.PostID,
		"comment_id": comment.CommentID,
		"reaction":   "like",
	})
	if second["active"] != false || int64Param(second["reaction_count"]) != 0 {
		t.Fatalf("expected second comment toggle to deactivate reaction, got %#v", second)
	}
}

func TestChannelPostAndCommentListsExposeCountsMediaAndReactionState(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id":   "ch",
		"body":         "post",
		"message_type": "m.image",
		"media_json":   `{"url":"mxc://example.com/post"}`,
	})
	comment := mustHandle[channelCommentRecord](t, service, "channels.comments.create", map[string]any{
		"channel_id":   "ch",
		"post_id":      post.PostID,
		"body":         "comment",
		"message_type": "m.image",
		"media_json":   `{"url":"mxc://example.com/comment"}`,
	})
	mustHandle[map[string]any](t, service, "channels.post_reaction.toggle", map[string]any{
		"channel_id": "ch",
		"post_id":    post.PostID,
	})
	mustHandle[map[string]any](t, service, "channels.comment_reaction.toggle", map[string]any{
		"channel_id": "ch",
		"post_id":    post.PostID,
		"comment_id": comment.CommentID,
		"reaction":   "like",
	})

	posts := mustHandle[map[string]any](t, service, "channels.posts.list", map[string]any{"channel_id": "ch"})["posts"].([]channelPostRecord)
	if len(posts) != 1 || posts[0].CommentCount != 1 || posts[0].ReactionCount != 1 || !posts[0].ReactedByMe || !strings.Contains(posts[0].MediaJSON, "mxc://example.com/post") {
		t.Fatalf("expected post list counts, reaction state, and media, got %#v", posts)
	}
	comments := mustHandle[map[string]any](t, service, "channels.comments.list", map[string]any{"post_id": post.PostID})["comments"].([]channelCommentRecord)
	if len(comments) != 1 || comments[0].ReactionCount != 1 || !comments[0].ReactedByMe || !strings.Contains(comments[0].MediaJSON, "mxc://example.com/comment") {
		t.Fatalf("expected comment list reaction state and media, got %#v", comments)
	}
}

func TestChannelCommentCreateRequiresExistingPost(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)

	if _, apiErr := service.Handle(context.Background(), "channels.comments.create", map[string]any{
		"channel_id": "ch",
		"body":       "orphan",
	}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected missing post_id to return 400, got %#v", apiErr)
	}
	if _, apiErr := service.Handle(context.Background(), "channels.comments.create", map[string]any{
		"channel_id": "ch",
		"post_id":    "post_missing",
		"body":       "orphan",
	}); apiErr == nil || apiErr.Status != http.StatusNotFound {
		t.Fatalf("expected unknown post to return 404, got %#v", apiErr)
	}

	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": "ch",
		"body":       "post",
	})
	if _, apiErr := service.Handle(context.Background(), "channels.comments.create", map[string]any{
		"channel_id": "other",
		"post_id":    post.PostID,
		"body":       "wrong channel",
	}); apiErr == nil || apiErr.Status != http.StatusNotFound {
		t.Fatalf("expected wrong channel post lookup to return 404, got %#v", apiErr)
	}
	comment := mustHandle[channelCommentRecord](t, service, "channels.comments.create", map[string]any{
		"channel_id": "ch",
		"post_id":    post.PostID,
		"body":       "valid",
	})
	if comment.PostID != post.PostID || comment.Body != "valid" {
		t.Fatalf("expected valid comment on existing post, got %#v", comment)
	}
}

func TestFavoriteAddIsIdempotentByEventAndRoom(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)

	first := mustHandle[favoriteRecord](t, service, "favorites.add", map[string]any{
		"event_id":     "$event",
		"room_id":      "!room:example.com",
		"content":      "first",
		"message_type": "text",
	})
	second := mustHandle[favoriteRecord](t, service, "favorites.add", map[string]any{
		"event_id":     "$event",
		"room_id":      "!room:example.com",
		"content":      "updated",
		"message_type": "text",
	})
	if second.ID != first.ID {
		t.Fatalf("expected duplicate favorite to reuse id %d, got %d", first.ID, second.ID)
	}
	favorites := mustHandle[map[string]any](t, service, "favorites.list", map[string]any{"message_type": "text"})["favorites"].([]favoriteRecord)
	if len(favorites) != 1 || favorites[0].ID != first.ID || favorites[0].Content != "updated" {
		t.Fatalf("expected one updated favorite, got %#v", favorites)
	}

	otherRoom := mustHandle[favoriteRecord](t, service, "favorites.add", map[string]any{
		"event_id":     "$event",
		"room_id":      "!other:example.com",
		"content":      "other",
		"message_type": "text",
	})
	if otherRoom.ID == first.ID {
		t.Fatalf("expected same event in a different room to get a separate favorite, got %#v", otherRoom)
	}
}

func TestStoredFavoriteAddIsIdempotentByEventAndRoom(t *testing.T) {
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

	first := mustHandle[favoriteRecord](t, service, "favorites.add", map[string]any{
		"event_id":     "$stored",
		"room_id":      "!room:example.com",
		"content":      "first",
		"message_type": "text",
	})
	second := mustHandle[favoriteRecord](t, service, "favorites.add", map[string]any{
		"event_id":     "$stored",
		"room_id":      "!room:example.com",
		"content":      "updated",
		"message_type": "text",
	})
	if second.ID != first.ID {
		t.Fatalf("expected stored duplicate favorite to reuse id %d, got %d", first.ID, second.ID)
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
	favorites := mustHandle[map[string]any](t, reloaded, "favorites.list", map[string]any{"message_type": "text"})["favorites"].([]favoriteRecord)
	if len(favorites) != 1 || favorites[0].ID != first.ID || favorites[0].Content != "updated" {
		t.Fatalf("expected one stored updated favorite after reload, got %#v", favorites)
	}
}

func TestContactAcceptRequiresPendingInboundContact(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)

	if _, apiErr := service.Handle(context.Background(), "contacts.requests.accept", map[string]any{
		"room_id":   "!missing:example.com",
		"peer_mxid": "@alice:remote.example",
	}); apiErr == nil || apiErr.Status != http.StatusNotFound {
		t.Fatalf("expected accept without pending inbound contact to return 404, got %#v", apiErr)
	}
}

func TestRemovedP2PMessageActionsAreUnknown(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	for _, action := range []string{
		"rooms.send",
		"rooms.send_media",
		"rooms.messages.delete",
		"rooms.messages.delete_batch",
		"rooms.messages.delete_range",
		"rooms.messages.recall",
		"sync.messages",
		"sync.unread",
		"search",
	} {
		if result, apiErr := service.Handle(context.Background(), action, map[string]any{
			"room_id":  "!room:example.com",
			"event_id": "$event:example.com",
			"content":  "removed",
		}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
			t.Fatalf("expected removed %s to be unknown, result=%#v err=%#v", action, result, apiErr)
		}
	}
}

func TestGroupsAndChannelsExposeOwnerMember(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"room_id": "!group:example.com", "name": "Group"})
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{"channel_id": "ch", "room_id": "!channel:example.com", "name": "Channel"})

	groupMembers := mustHandle[map[string]any](t, service, "groups.members", map[string]any{"room_id": group.RoomID})
	if got, ok := groupMembers["members"].([]memberRecord); !ok || len(got) != 1 || got[0].UserID != "@owner:example.com" {
		t.Fatalf("expected owner group member, got %#v", groupMembers)
	}
	channelMembers := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID, "room_id": ch.RoomID})
	if got, ok := channelMembers["members"].([]memberRecord); !ok || len(got) != 1 || got[0].UserID != "@owner:example.com" {
		t.Fatalf("expected owner channel member, got %#v", channelMembers)
	}
}

func TestChannelOwnerRoleSurvivesMatrixMemberProjection(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":       "ch",
		"room_id":          "!channel:example.com",
		"name":             "Channel",
		"channel_type":     "post",
		"comments_enabled": true,
	})
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:      ch.RoomID,
		ChannelID:   ch.ChannelID,
		UserID:      "@owner:example.com",
		DisplayName: "Owner",
		Domain:      "example.com",
		Membership:  "join",
		Role:        "member",
	}); err != nil {
		t.Fatal(err)
	}

	channels := mustHandle[map[string]any](t, service, "channels.list", nil)["channels"].([]channel)
	if len(channels) != 1 || !channels[0].IsOwned || channels[0].Role != "owner" {
		t.Fatalf("expected local channel owner role to survive Matrix projection, got %#v", channels)
	}
	got := mustHandle[conversationView](t, service, "conversations.get", map[string]any{"room_id": ch.RoomID})
	if got.Role != "owner" || !got.Capabilities.PostCreate {
		t.Fatalf("expected channel conversation to preserve owner post capability, got %#v", got)
	}
}

func TestStoredChannelOwnerRoleRepairAfterReload(t *testing.T) {
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
		"channel_id":       "stored_ch",
		"room_id":          "!stored-channel:example.com",
		"name":             "Stored Channel",
		"channel_type":     "post",
		"comments_enabled": true,
	})
	if upsertErr := store.UpsertMember(ctx, memberRecord{
		RoomID:      ch.RoomID,
		ChannelID:   ch.ChannelID,
		UserID:      "@owner:example.com",
		DisplayName: "Owner",
		Domain:      "example.com",
		Membership:  "join",
		Role:        "member",
	}); upsertErr != nil {
		t.Fatal(upsertErr)
	}

	reloaded, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, store)
	if err != nil {
		t.Fatal(err)
	}
	member, ok, err := store.LookupMember(ctx, ch.RoomID, "@owner:example.com")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || member.Role != "owner" {
		t.Fatalf("expected stored local channel owner role to be repaired, got ok=%v member=%#v", ok, member)
	}
	got := mustHandle[conversationView](t, reloaded, "conversations.get", map[string]any{"room_id": ch.RoomID})
	if got.Role != "owner" || !got.Capabilities.PostCreate {
		t.Fatalf("expected repaired channel conversation to preserve owner post capability, got %#v", got)
	}
}

func TestPublicChannelSearchFindsPublicChannelsOnly(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	public := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "ch_public",
		"room_id":     "!public:example.com",
		"name":        "Public Garden",
		"description": "open discussion",
		"visibility":  "public",
	})
	mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch_private",
		"room_id":    "!private:example.com",
		"name":       "Private Garden",
		"visibility": "private",
	})

	search := mustHandle[map[string]any](t, service, "channels.public.search", map[string]any{"q": "garden"})
	results, ok := search["channels"].([]channel)
	if !ok || len(results) != 1 || results[0].ChannelID != public.ChannelID {
		t.Fatalf("expected only public matching channel, got %#v", search)
	}
}

func TestRecallChannelPostAndCommentHideFromLists(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": "ch",
		"body":       "recall post",
	})
	comment := mustHandle[channelCommentRecord](t, service, "channels.comments.create", map[string]any{
		"channel_id": "ch",
		"post_id":    post.PostID,
		"body":       "recall comment",
	})

	mustHandle[map[string]any](t, service, "channels.posts.recall", map[string]any{"post_id": post.PostID})
	posts := mustHandle[map[string]any](t, service, "channels.posts.list", map[string]any{"channel_id": "ch"})
	if got, ok := posts["posts"].([]channelPostRecord); !ok || len(got) != 0 {
		t.Fatalf("expected recalled post hidden, got %#v", posts)
	}

	mustHandle[map[string]any](t, service, "channels.comments.recall", map[string]any{"comment_id": comment.CommentID, "post_id": post.PostID})
	comments := mustHandle[map[string]any](t, service, "channels.comments.list", map[string]any{"post_id": post.PostID})
	if got, ok := comments["comments"].([]channelCommentRecord); !ok || len(got) != 0 {
		t.Fatalf("expected recalled comment hidden, got %#v", comments)
	}
}

func TestRecallChannelContentRequiresAuthorOrChannelOwner(t *testing.T) {
	ctx := context.Background()
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch_auth",
		"room_id":    "!ch-auth:example.com",
		"name":       "Auth Channel",
	})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"body":       "owner post",
	})
	if err := service.saveMember(ctx, memberRecord{
		RoomID:      ch.RoomID,
		ChannelID:   ch.ChannelID,
		UserID:      "@bob:example.com",
		DisplayName: "Bob",
		Membership:  "join",
		Role:        "member",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(ctx, memberRecord{
		RoomID:      ch.RoomID,
		ChannelID:   ch.ChannelID,
		UserID:      "@alice:example.com",
		DisplayName: "Alice",
		Membership:  "join",
		Role:        "member",
	}); err != nil {
		t.Fatal(err)
	}

	setServiceOwnerForTest(service, "@bob:example.com", "Bob")
	if _, apiErr := service.Handle(ctx, "channels.posts.recall", map[string]any{"post_id": post.PostID}); apiErr == nil || apiErr.Status != http.StatusForbidden {
		t.Fatalf("expected member recall of owner post to be forbidden, got %#v", apiErr)
	}
	posts := mustHandle[map[string]any](t, service, "channels.posts.list", map[string]any{"channel_id": ch.ChannelID})
	if got := posts["posts"].([]channelPostRecord); len(got) != 1 || got[0].PostID != post.PostID {
		t.Fatalf("expected forbidden post recall to keep post, got %#v", got)
	}

	ownComment := mustHandle[channelCommentRecord](t, service, "channels.comments.create", map[string]any{
		"channel_id": ch.ChannelID,
		"post_id":    post.PostID,
		"room_id":    ch.RoomID,
		"body":       "bob comment",
	})
	if _, apiErr := service.Handle(ctx, "channels.comments.recall", map[string]any{"comment_id": ownComment.CommentID, "post_id": post.PostID}); apiErr != nil {
		t.Fatalf("expected author to recall own comment, got %#v", apiErr)
	}

	foreignComment := channelCommentRecord{
		CommentID:      "comment_alice",
		PostID:         post.PostID,
		ChannelID:      ch.ChannelID,
		EventID:        "$comment_alice:example.com",
		AuthorMXID:     "@alice:example.com",
		AuthorName:     "Alice",
		Body:           "alice comment",
		MessageType:    "text",
		MentionsJSON:   "[]",
		OriginServerTS: time.Now().UTC().UnixMilli(),
	}
	service.mu.Lock()
	service.comments = append(service.comments, foreignComment)
	service.mu.Unlock()
	if _, apiErr := service.Handle(ctx, "channels.comments.recall", map[string]any{"comment_id": foreignComment.CommentID, "post_id": post.PostID}); apiErr == nil || apiErr.Status != http.StatusForbidden {
		t.Fatalf("expected member recall of another member comment to be forbidden, got %#v", apiErr)
	}
	comments := mustHandle[map[string]any](t, service, "channels.comments.list", map[string]any{"post_id": post.PostID})
	if got := comments["comments"].([]channelCommentRecord); len(got) != 1 || got[0].CommentID != foreignComment.CommentID {
		t.Fatalf("expected forbidden comment recall to keep comment, got %#v", got)
	}

	setServiceOwnerForTest(service, "@owner:example.com", "")
	if _, apiErr := service.Handle(ctx, "channels.comments.recall", map[string]any{"comment_id": foreignComment.CommentID, "post_id": post.PostID}); apiErr != nil {
		t.Fatalf("expected channel owner to recall member comment, got %#v", apiErr)
	}
}

func TestChannelCommentPersistsReplyAndMentions(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": "ch",
		"body":       "post",
	})
	parent := mustHandle[channelCommentRecord](t, service, "channels.comments.create", map[string]any{
		"channel_id": "ch",
		"post_id":    post.PostID,
		"body":       "parent",
	})

	reply := mustHandle[channelCommentRecord](t, service, "channels.comments.create", map[string]any{
		"channel_id":           "ch",
		"post_id":              post.PostID,
		"body":                 "reply @alice",
		"reply_to_comment_id":  parent.CommentID,
		"reply_to_author_mxid": parent.AuthorMXID,
		"mentions": []any{
			map[string]any{"user_id": "@alice:remote.example", "display_name": "Alice"},
		},
	})
	if reply.ReplyToCommentID != parent.CommentID || !strings.Contains(reply.MentionsJSON, "@alice:remote.example") {
		t.Fatalf("expected reply metadata on created comment, got %#v", reply)
	}
	comments := mustHandle[map[string]any](t, service, "channels.comments.list", map[string]any{"post_id": post.PostID})
	got := comments["comments"].([]channelCommentRecord)
	if len(got) != 2 || got[1].ReplyToCommentID != parent.CommentID || !strings.Contains(got[1].MentionsJSON, "@alice:remote.example") {
		t.Fatalf("expected reply metadata in comments list, got %#v", got)
	}
}

func setServiceOwnerForTest(service *Service, mxid, displayName string) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.ownerMXID = mxid
	service.profile.UserID = mxid
	service.profile.DisplayName = displayName
}

func TestGroupAndChannelMemberLifecycle(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"room_id": "!group:example.com", "name": "Group"})
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{"channel_id": "ch", "room_id": "!channel:example.com", "name": "Channel"})

	mustHandle[map[string]any](t, service, "groups.invite", map[string]any{
		"room_id":    group.RoomID,
		"user_id":    "@alice:example.com",
		"peer_mxids": []any{"@bob:example.com"},
	})
	groupMembers := mustHandle[map[string]any](t, service, "groups.members", map[string]any{"room_id": group.RoomID})
	groupList := groupMembers["members"].([]memberRecord)
	if len(groupList) != 3 {
		t.Fatalf("expected owner plus two invited group members, got %#v", groupMembers)
	}
	mustHandle[map[string]any](t, service, "groups.member.mute", map[string]any{"room_id": group.RoomID, "user_id": "@alice:example.com"})
	muted := mustHandle[map[string]any](t, service, "groups.members", map[string]any{"room_id": group.RoomID})["members"].([]memberRecord)
	if !findMember(muted, "@alice:example.com").Muted {
		t.Fatalf("expected alice muted, got %#v", muted)
	}
	mustHandle[map[string]any](t, service, "groups.member.remove", map[string]any{"room_id": group.RoomID, "user_id": "@alice:example.com"})
	afterRemove := mustHandle[map[string]any](t, service, "groups.members", map[string]any{"room_id": group.RoomID})["members"].([]memberRecord)
	if findMember(afterRemove, "@alice:example.com").UserID != "" {
		t.Fatalf("expected alice removed from joined member list, got %#v", afterRemove)
	}
	if _, apiErr := service.Handle(context.Background(), "groups.member.remove", map[string]any{"room_id": group.RoomID, "user_id": "@owner:example.com"}); apiErr == nil || apiErr.Status != 409 {
		t.Fatalf("expected group owner remove to return 409, got %#v", apiErr)
	}
	if _, apiErr := service.Handle(context.Background(), "groups.leave", map[string]any{"room_id": group.RoomID}); apiErr == nil || apiErr.Status != 409 {
		t.Fatalf("expected group owner leave to return 409, got %#v", apiErr)
	}
	afterLeave := mustHandle[map[string]any](t, service, "groups.members", map[string]any{"room_id": group.RoomID})["members"].([]memberRecord)
	if findMember(afterLeave, "@owner:example.com").UserID == "" {
		t.Fatalf("expected owner to remain after rejected leave, got %#v", afterLeave)
	}

	mustHandle[map[string]any](t, service, "channels.invite", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_ids":   []any{"@carol:example.com"},
	})
	channelMembers := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	if findMember(channelMembers, "@carol:example.com").UserID == "" {
		t.Fatalf("expected invited channel member, got %#v", channelMembers)
	}
	mustHandle[map[string]any](t, service, "channels.member.mute", map[string]any{"channel_id": ch.ChannelID, "room_id": ch.RoomID, "user_id": "@carol:example.com"})
	channelMembers = mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	if !findMember(channelMembers, "@carol:example.com").Muted {
		t.Fatalf("expected carol muted, got %#v", channelMembers)
	}
	mustHandle[map[string]any](t, service, "channels.member.remove", map[string]any{"channel_id": ch.ChannelID, "room_id": ch.RoomID, "user_id": "@carol:example.com"})
	channelMembers = mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	if findMember(channelMembers, "@carol:example.com").UserID != "" {
		t.Fatalf("expected carol removed from joined channel members, got %#v", channelMembers)
	}
	if _, apiErr := service.Handle(context.Background(), "channels.member.remove", map[string]any{"channel_id": ch.ChannelID, "room_id": ch.RoomID, "user_id": "@owner:example.com"}); apiErr == nil || apiErr.Status != 409 {
		t.Fatalf("expected channel owner remove to return 409, got %#v", apiErr)
	}
	if _, apiErr := service.Handle(context.Background(), "channels.leave", map[string]any{"channel_id": ch.ChannelID, "room_id": ch.RoomID}); apiErr == nil || apiErr.Status != 409 {
		t.Fatalf("expected channel owner leave to return 409, got %#v", apiErr)
	}
	channelMembers = mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	if findMember(channelMembers, "@owner:example.com").UserID == "" {
		t.Fatalf("expected channel owner to remain after rejected leave, got %#v", channelMembers)
	}

	groupDissolve := mustHandle[map[string]any](t, service, "groups.dissolve", map[string]any{"room_id": group.RoomID})
	if groupDissolve["status"] != "ok" {
		t.Fatalf("expected group dissolve ok, got %#v", groupDissolve)
	}
	groupsAfterDissolve := mustHandle[map[string]any](t, service, "groups.list", nil)["groups"].([]groupRecord)
	if len(groupsAfterDissolve) != 0 {
		t.Fatalf("expected dissolved group removed from list, got %#v", groupsAfterDissolve)
	}

	channelDissolve := mustHandle[map[string]any](t, service, "channels.dissolve", map[string]any{"channel_id": ch.ChannelID, "room_id": ch.RoomID})
	if channelDissolve["status"] != "ok" {
		t.Fatalf("expected channel dissolve ok, got %#v", channelDissolve)
	}
	channelsAfterDissolve := mustHandle[map[string]any](t, service, "channels.list", nil)["channels"].([]channel)
	if len(channelsAfterDissolve) != 0 {
		t.Fatalf("expected dissolved channel removed from list, got %#v", channelsAfterDissolve)
	}
}

func TestGroupAndChannelWideMuteAndInvitePolicyActions(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"room_id": "!group:example.com", "name": "Group"})
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{"channel_id": "ch", "room_id": "!channel:example.com", "name": "Channel"})

	mustHandle[map[string]any](t, service, "groups.invite", map[string]any{
		"room_id": group.RoomID,
		"user_id": "@alice:example.com",
	})
	mustHandle[map[string]any](t, service, "channels.invite", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_id":    "@carol:example.com",
	})

	groupMute := mustHandle[map[string]any](t, service, "groups.mute", map[string]any{"room_id": group.RoomID})
	if groupMute["muted"] != true {
		t.Fatalf("expected group mute response, got %#v", groupMute)
	}
	groupMembers := mustHandle[map[string]any](t, service, "groups.members", map[string]any{"room_id": group.RoomID})["members"].([]memberRecord)
	if findMember(groupMembers, "@owner:example.com").Muted || !findMember(groupMembers, "@alice:example.com").Muted {
		t.Fatalf("expected only ordinary group member muted, got %#v", groupMembers)
	}
	updatedPolicy := mustHandle[groupRecord](t, service, "groups.invite_policy.update", map[string]any{"room_id": group.RoomID, "invite_policy": "owner"})
	if !updatedPolicy.Muted || updatedPolicy.InvitePolicy != "owner" {
		t.Fatalf("expected muted group with updated invite policy, got %#v", updatedPolicy)
	}
	mustHandle[map[string]any](t, service, "groups.unmute", map[string]any{"room_id": group.RoomID})
	groupMembers = mustHandle[map[string]any](t, service, "groups.members", map[string]any{"room_id": group.RoomID})["members"].([]memberRecord)
	if findMember(groupMembers, "@alice:example.com").Muted {
		t.Fatalf("expected group unmute to clear ordinary member mute, got %#v", groupMembers)
	}

	channelMute := mustHandle[map[string]any](t, service, "channels.mute", map[string]any{"channel_id": ch.ChannelID, "room_id": ch.RoomID})
	if channelMute["muted"] != true {
		t.Fatalf("expected channel mute response, got %#v", channelMute)
	}
	channels := mustHandle[map[string]any](t, service, "channels.list", nil)["channels"].([]channel)
	if !channels[0].Muted {
		t.Fatalf("expected channel list to expose muted state, got %#v", channels)
	}
	channelMembers := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	if findMember(channelMembers, "@owner:example.com").Muted || !findMember(channelMembers, "@carol:example.com").Muted {
		t.Fatalf("expected only ordinary channel member muted, got %#v", channelMembers)
	}
	mustHandle[map[string]any](t, service, "channels.unmute", map[string]any{"channel_id": ch.ChannelID, "room_id": ch.RoomID})
	channelMembers = mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	if findMember(channelMembers, "@carol:example.com").Muted {
		t.Fatalf("expected channel unmute to clear ordinary member mute, got %#v", channelMembers)
	}
}

func TestProductUpdatesPreserveExistingFieldsAndPublicGetDoesNotCreate(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id":       "!group:example.com",
		"name":          "Original Group",
		"topic":         "original topic",
		"avatar_url":    "mxc://old-group-avatar",
		"invite_policy": "member",
	})
	updatedGroup := mustHandle[groupRecord](t, service, "groups.update", map[string]any{
		"room_id":    group.RoomID,
		"avatar_url": "mxc://new-group-avatar",
	})
	if updatedGroup.Name != "Original Group" || updatedGroup.Topic != "original topic" || updatedGroup.InvitePolicy != "member" || updatedGroup.AvatarURL != "mxc://new-group-avatar" {
		t.Fatalf("expected partial group update to preserve existing fields, got %#v", updatedGroup)
	}

	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":       "news",
		"room_id":          "!news:example.com",
		"name":             "News",
		"description":      "original description",
		"visibility":       "private",
		"join_policy":      "approval",
		"channel_type":     "post",
		"comments_enabled": true,
	})
	updatedChannel := mustHandle[channel](t, service, "channels.update", map[string]any{
		"channel_id":  ch.ChannelID,
		"description": "new description",
	})
	if updatedChannel.Name != "News" || updatedChannel.Visibility != "private" || updatedChannel.JoinPolicy != "approval" || updatedChannel.ChannelType != "post" || updatedChannel.Description != "new description" {
		t.Fatalf("expected partial channel update to preserve existing fields, got %#v", updatedChannel)
	}

	if _, apiErr := service.Handle(context.Background(), "channels.public.get", map[string]any{"room_id": ch.RoomID}); apiErr == nil || apiErr.Status != 404 {
		t.Fatalf("expected private channel public get to return 404, got %#v", apiErr)
	}
	updatedChannel = mustHandle[channel](t, service, "channels.update", map[string]any{
		"channel_id": ch.ChannelID,
		"visibility": "public",
	})
	detail := mustHandle[channel](t, service, "channels.public.get", map[string]any{"room_id": updatedChannel.RoomID})
	if detail.ChannelID != ch.ChannelID || detail.Description != "new description" {
		t.Fatalf("expected public get to return public existing channel, got %#v", detail)
	}
	if _, apiErr := service.Handle(context.Background(), "channels.public.get", map[string]any{"room_id": "!missing:example.com"}); apiErr == nil || apiErr.Status != 404 {
		t.Fatalf("expected missing public channel to return 404, got %#v", apiErr)
	}
	channels := mustHandle[map[string]any](t, service, "channels.list", nil)["channels"].([]channel)
	if len(channels) != 1 {
		t.Fatalf("expected public get missing not to create a channel, got %#v", channels)
	}
	if !channels[0].IsOwned || channels[0].Role != "owner" || channels[0].MemberStatus != "join" {
		t.Fatalf("expected channel list to expose owner membership fields, got %#v", channels[0])
	}
	bootstrap := mustHandle[map[string]any](t, service, "sync.bootstrap", nil)
	bootstrapChannels := bootstrap["channels"].([]channel)
	if len(bootstrapChannels) != 1 || !bootstrapChannels[0].IsOwned || bootstrapChannels[0].Role != "owner" || bootstrapChannels[0].MemberStatus != "join" {
		t.Fatalf("expected sync bootstrap to expose owner membership fields, got %#v", bootstrapChannels)
	}
}

func TestCallGetAndEventsDoNotCreateMissingCalls(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	if _, apiErr := service.Handle(context.Background(), "calls.get", map[string]any{"call_id": "missing"}); apiErr == nil || apiErr.Status != 404 {
		t.Fatalf("expected missing calls.get to return 404, got %#v", apiErr)
	}
	if _, apiErr := service.Handle(context.Background(), "calls.event", map[string]any{"call_id": "missing", "event": "ended"}); apiErr == nil || apiErr.Status != 404 {
		t.Fatalf("expected missing calls.event to return 404, got %#v", apiErr)
	}
	calls := mustHandle[map[string]any](t, service, "calls.list", nil)
	if got := calls["calls"].([]callRecord); len(got) != 0 {
		t.Fatalf("expected missing call queries not to create rows, got %#v", calls)
	}

	created := mustHandle[callRecord](t, service, "calls.create", map[string]any{
		"call_id":    "call_1",
		"room_id":    "!room:example.com",
		"media_type": "video",
	})
	loaded := mustHandle[callRecord](t, service, "calls.get", map[string]any{"call_id": created.CallID})
	if loaded.CallID != "call_1" || loaded.State != "ringing" || loaded.MediaType != "video" {
		t.Fatalf("expected calls.get to load existing session, got %#v", loaded)
	}
	connected := mustHandle[callRecord](t, service, "calls.event", map[string]any{"call_id": created.CallID, "event": "connected"})
	if connected.State != "connected" || connected.MediaType != "video" || connected.AnsweredAt == "" {
		t.Fatalf("expected connected event to update existing call, got %#v", connected)
	}
	if _, apiErr := service.Handle(context.Background(), "calls.event", map[string]any{"call_id": created.CallID, "event": "ringing"}); apiErr == nil || apiErr.Status != 400 {
		t.Fatalf("expected invalid call event to return 400, got %#v", apiErr)
	}
	rejected := mustHandle[callRecord](t, service, "calls.event", map[string]any{
		"call_id":       created.CallID,
		"event":         "rejected",
		"reason":        "user_reject",
		"duration_ms":   3000,
		"ended_by_mxid": "@bob:example.com",
	})
	if rejected.State != "rejected" || rejected.EndedAt == "" || rejected.EndedByMXID != "@bob:example.com" || rejected.EndReason != "user_reject" || rejected.DurationMS != 3000 {
		t.Fatalf("expected rejected event to persist lifecycle details, got %#v", rejected)
	}
	if len(service.events) < 3 || service.events[len(service.events)-1].Type != "call.changed" {
		t.Fatalf("expected call state changes to emit call.changed events, got %#v", service.events)
	}
	if payloadCall, ok := service.events[len(service.events)-1].Payload["call"].(callRecord); !ok || payloadCall.State != "rejected" {
		t.Fatalf("expected call.changed payload to include rejected call, got %#v", service.events[len(service.events)-1].Payload)
	}
	reopened := mustHandle[callRecord](t, service, "calls.incoming", map[string]any{
		"call_id":    created.CallID,
		"room_id":    created.RoomID,
		"media_type": "voice",
	})
	if reopened.State != "rejected" || reopened.MediaType != "video" {
		t.Fatalf("expected terminal call not to reopen through calls.incoming, got %#v", reopened)
	}
	reconnected := mustHandle[callRecord](t, service, "calls.event", map[string]any{
		"call_id": created.CallID,
		"event":   "connected",
	})
	if reconnected.State != "rejected" {
		t.Fatalf("expected terminal call not to reconnect, got %#v", reconnected)
	}

	mustHandle[callRecord](t, service, "calls.create", map[string]any{"call_id": "missed", "room_id": "!room:example.com"})
	mustHandle[callRecord](t, service, "calls.event", map[string]any{"call_id": "missed", "event": "missed"})
	mustHandle[callRecord](t, service, "calls.create", map[string]any{"call_id": "failed", "room_id": "!room:example.com"})
	mustHandle[callRecord](t, service, "calls.event", map[string]any{"call_id": "failed", "event": "failed"})
	active := mustHandle[map[string]any](t, service, "calls.active", nil)
	activeCalls := active["calls"].([]callRecord)
	for _, call := range activeCalls {
		if call.State == "missed" || call.State == "failed" || call.State == "ended" || call.State == "rejected" {
			t.Fatalf("expected terminal calls hidden from calls.active, got %#v", activeCalls)
		}
	}
}

func TestSyncBootstrapIncludesGroupAndChannelInvites(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	group := groupRecord{
		RoomID: "!group:example.com",
		Name:   "产品群",
	}
	if err := service.saveGroup(context.Background(), group); err != nil {
		t.Fatal(err)
	}
	ch := channel{
		ChannelID: "product",
		RoomID:    "!product:example.com",
		Name:      "产品频道",
	}
	if err := service.saveChannel(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     group.RoomID,
		UserID:     "@owner:example.com",
		Domain:     "example.com",
		Membership: "invite",
		Role:       "member",
		JoinedAt:   1770000000000,
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@owner:example.com",
		Domain:     "example.com",
		Membership: "invite",
		Role:       "member",
		JoinedAt:   1770000000001,
	}); err != nil {
		t.Fatal(err)
	}

	bootstrap := mustHandle[map[string]any](t, service, "sync.bootstrap", nil)
	pending := bootstrap["pending"].(map[string]any)
	groupInvites := pending["group_invites"].([]map[string]any)
	channelNotices := pending["channel_notices"].([]map[string]any)
	if len(groupInvites) != 1 {
		t.Fatalf("expected one pending group invite, got %#v", pending["group_invites"])
	}
	groupInvite := groupInvites[0]
	if groupInvite["id"] != group.RoomID || groupInvite["title"] != group.Name {
		t.Fatalf("expected pending group invite, got %#v", pending["group_invites"])
	}
	if len(channelNotices) != 1 {
		t.Fatalf("expected one pending channel invite, got %#v", pending["channel_notices"])
	}
	channelNotice := channelNotices[0]
	if channelNotice["id"] != ch.RoomID || channelNotice["title"] != ch.Name {
		t.Fatalf("expected pending channel invite notice, got %#v", pending["channel_notices"])
	}
	if groups := bootstrap["groups"].([]groupRecord); len(groups) != 0 {
		t.Fatalf("expected invited group hidden from bootstrap main groups, got %#v", groups)
	}
	if channels := bootstrap["channels"].([]channel); len(channels) != 0 {
		t.Fatalf("expected invited channel hidden from bootstrap main channels, got %#v", channels)
	}
}

func TestGroupAndChannelListsOnlyExposeJoinedOwnerMembership(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	joinedGroup := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!joined-group:example.com",
		"name":    "Joined group",
	})
	invitedGroup := groupRecord{RoomID: "!invited-group:example.com", Name: "Invited group"}
	if err := service.saveGroup(context.Background(), invitedGroup); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     invitedGroup.RoomID,
		UserID:     "@owner:example.com",
		Domain:     "example.com",
		Membership: "invite",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}
	joinedChannel := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "joined-channel",
		"room_id":    "!joined-channel:example.com",
		"name":       "Joined channel",
	})
	invitedChannel := channel{ChannelID: "invited-channel", RoomID: "!invited-channel:example.com", Name: "Invited channel"}
	if err := service.saveChannel(context.Background(), invitedChannel); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     invitedChannel.RoomID,
		ChannelID:  invitedChannel.ChannelID,
		UserID:     "@owner:example.com",
		Domain:     "example.com",
		Membership: "invite",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	groups := mustHandle[map[string]any](t, service, "groups.list", nil)["groups"].([]groupRecord)
	if len(groups) != 1 || groups[0].RoomID != joinedGroup.RoomID {
		t.Fatalf("expected only joined group in groups.list, got %#v", groups)
	}
	channels := mustHandle[map[string]any](t, service, "channels.list", nil)["channels"].([]channel)
	if len(channels) != 1 || channels[0].ChannelID != joinedChannel.ChannelID {
		t.Fatalf("expected only joined channel in channels.list, got %#v", channels)
	}
}

func TestGroupCardJoinRequiresRecordedInvite(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"name": "产品群",
	})

	_, apiErr := service.Handle(context.Background(), "groups.join", map[string]any{
		"room_id":         group.RoomID,
		"user_id":         "@alice:remote.example",
		"group_name":      group.Name,
		"invite_event_id": "$invite",
		"direct_room_id":  "!dm:remote.example",
	})
	if apiErr == nil || apiErr.Status != 403 {
		t.Fatalf("expected forbidden card join without invite, got %#v", apiErr)
	}
}

func TestGroupCardJoinConsumesRecordedInvite(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"name": "产品群",
	})
	mustHandle[map[string]any](t, service, "groups.invite", map[string]any{
		"room_id": group.RoomID,
		"user_id": "@alice:remote.example",
	})

	result := mustHandle[map[string]any](t, service, "groups.join", map[string]any{
		"room_id":         group.RoomID,
		"user_id":         "@alice:remote.example",
		"group_name":      group.Name,
		"invite_event_id": "$invite",
		"direct_room_id":  "!dm:remote.example",
	})

	member := result["member"].(memberRecord)
	if member.UserID != "@alice:remote.example" || member.Membership != "join" {
		t.Fatalf("expected invited user to join, got %#v", member)
	}
	stored, ok, err := service.lookupMember(context.Background(), group.RoomID, "@alice:remote.example")
	if err != nil || !ok || stored.Membership != "join" {
		t.Fatalf("expected stored joined member, got member=%#v ok=%v err=%v", stored, ok, err)
	}
	if result["room_id"] != group.RoomID {
		t.Fatalf("expected join response to include room_id %s, got %#v", group.RoomID, result)
	}
}

func TestGroupInviteRejectUsesCurrentLocalUserAndHidesInvitation(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	group := groupRecord{
		RoomID:       "!remote-group:remote.example",
		Name:         "Remote Group",
		InvitePolicy: "member",
	}
	if err := service.saveGroup(context.Background(), group); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:      group.RoomID,
		UserID:      "@owner:example.com",
		DisplayName: "Owner",
		Domain:      "example.com",
		Membership:  "invite",
		Role:        "member",
	}); err != nil {
		t.Fatal(err)
	}

	rejected := mustHandle[map[string]any](t, service, "groups.invite.reject", map[string]any{
		"room_id": group.RoomID,
	})

	if rejected["status"] != "rejected" {
		t.Fatalf("expected rejected status, got %#v", rejected)
	}
	member := rejected["member"].(memberRecord)
	if member.UserID != "@owner:example.com" || member.Membership != "reject" {
		t.Fatalf("expected current local user invite to become rejected, got %#v", member)
	}
	members := mustHandle[map[string]any](t, service, "groups.members", map[string]any{"room_id": group.RoomID})["members"].([]memberRecord)
	if findMember(members, "@owner:example.com").UserID != "" {
		t.Fatalf("expected rejected invite hidden from visible group members, got %#v", members)
	}
	groups := mustHandle[map[string]any](t, service, "groups.list", nil)["groups"].([]groupRecord)
	if len(groups) != 0 {
		t.Fatalf("expected rejected invite hidden from joined groups, got %#v", groups)
	}
}

func TestStoredGroupOwnerCannotLeaveOrRemoveAfterReload(t *testing.T) {
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
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!stored-owner:example.com",
		"name":    "Stored Owner",
	})

	reloaded, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, apiErr := reloaded.Handle(ctx, "groups.leave", map[string]any{"room_id": group.RoomID}); apiErr == nil || apiErr.Status != 409 {
		t.Fatalf("expected stored owner leave to be rejected after reload, got %#v", apiErr)
	}
	if _, apiErr := reloaded.Handle(ctx, "groups.member.remove", map[string]any{
		"room_id": group.RoomID,
		"user_id": "@owner:example.com",
	}); apiErr == nil || apiErr.Status != 409 {
		t.Fatalf("expected stored owner removal to be rejected after reload, got %#v", apiErr)
	}
	members := mustHandle[map[string]any](t, reloaded, "groups.members", map[string]any{"room_id": group.RoomID})["members"].([]memberRecord)
	owner := findMember(members, "@owner:example.com")
	if owner.UserID == "" || owner.Membership != "join" || owner.Role != "owner" {
		t.Fatalf("expected stored owner to remain joined owner after rejected mutations, got %#v", members)
	}
}

func TestContactListDeduplicatesPeerAndPrefersAcceptedContact(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		PeerMXID:    "@owner:peer.example",
		DisplayName: "owner",
		AvatarURL:   "mxc://peer.example/pending",
		Domain:      "peer.example",
		RoomID:      "!pending:example.com",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveContact(context.Background(), contactRecord{
		PeerMXID:    "@owner:peer.example",
		DisplayName: "Bob Nickname",
		AvatarURL:   "mxc://peer.example/accepted",
		Domain:      "peer.example",
		RoomID:      "!accepted:example.com",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "contacts.list", nil)
	contacts := result["contacts"].([]contactRecord)
	if len(contacts) != 1 {
		t.Fatalf("expected one visible contact after peer dedupe, got %#v", contacts)
	}
	if contacts[0].RoomID != "!accepted:example.com" || contacts[0].DisplayName != "Bob Nickname" || contacts[0].AvatarURL != "mxc://peer.example/accepted" || contacts[0].Status != "accepted" {
		t.Fatalf("expected accepted contact with latest nickname to win, got %#v", contacts[0])
	}

	bootstrap := mustHandle[map[string]any](t, service, "sync.bootstrap", nil)
	syncedContacts := bootstrap["contacts"].([]contactRecord)
	if len(syncedContacts) != 1 || syncedContacts[0].AvatarURL != "mxc://peer.example/accepted" {
		t.Fatalf("expected sync bootstrap contact to include avatar_url, got %#v", syncedContacts)
	}
}

func TestChannelJoinRequestPersistsPendingMemberAndResolves(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "approval",
		"room_id":     "!approval:example.com",
		"name":        "Approval",
		"visibility":  "public",
		"join_policy": "approval",
	})

	request := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id":   ch.ChannelID,
		"room_id":      ch.RoomID,
		"user_mxid":    "@alice:remote.example",
		"display_name": "Alice",
	})
	if request["status"] != "pending" {
		t.Fatalf("expected pending join request, got %#v", request)
	}
	members := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	alice := findMember(members, "@alice:remote.example")
	if alice.Membership != "pending" || alice.DisplayName != "Alice" || alice.ChannelID != ch.ChannelID {
		t.Fatalf("expected persisted pending member, got %#v in %#v", alice, members)
	}

	approved := mustHandle[map[string]any](t, service, "channels.join_request.approve", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@alice:remote.example",
	})
	approvedMember := approved["member"].(memberRecord)
	if approved["status"] != "approved" || approvedMember.Membership != "approved" {
		t.Fatalf("expected remote approved request without callback to remain approved until requester node joins, got %#v", approved)
	}

	mustHandle[map[string]any](t, service, "channels.invite", map[string]any{
		"channel_id":   ch.ChannelID,
		"room_id":      ch.RoomID,
		"user_mxid":    "@charlie:remote.example",
		"display_name": "Charlie",
	})
	if _, apiErr := service.Handle(context.Background(), "channels.join_request.approve", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@charlie:remote.example",
	}); apiErr == nil || apiErr.Status != http.StatusNotFound {
		t.Fatalf("expected join_request.approve to ignore ordinary Matrix invites, got %#v", apiErr)
	}

	mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@bob:remote.example",
	})
	rejected := mustHandle[map[string]any](t, service, "channels.join_request.reject", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@bob:remote.example",
	})
	rejectedMember := rejected["member"].(memberRecord)
	if rejectedMember.Membership != "reject" {
		t.Fatalf("expected rejected request to become rejected member, got %#v", rejected)
	}
	members = mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	if findMember(members, "@bob:remote.example").UserID != "" {
		t.Fatalf("expected rejected request hidden from visible members, got %#v", members)
	}
}

func TestChannelJoinRequestApproveCanRetryJoinFailedRemoteRequest(t *testing.T) {
	callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"temporary requester node failure"}`, http.StatusBadGateway)
	}))
	defer callback.Close()

	service := NewService(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "approval-retry",
		"room_id":     "!approval-retry:example.com",
		"name":        "Approval Retry",
		"visibility":  "public",
		"join_policy": "approval",
	})

	mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id":              ch.ChannelID,
		"room_id":                 ch.RoomID,
		"user_mxid":               "@alice:remote.example",
		"requester_node_base_url": callback.URL + "/_p2p",
	})
	first := mustHandle[map[string]any](t, service, "channels.join_request.approve", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@alice:remote.example",
	})
	if first["status"] != "join_failed" {
		t.Fatalf("expected first approval to record join_failed, got %#v", first)
	}

	second := mustHandle[map[string]any](t, service, "channels.join_request.approve", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@alice:remote.example",
	})
	if second["status"] != "join_failed" {
		t.Fatalf("expected retry approval to run instead of 404, got %#v", second)
	}
	member := second["member"].(memberRecord)
	if member.Membership != "join_failed" || member.UserID != "@alice:remote.example" {
		t.Fatalf("expected retry to preserve join_failed member, got %#v", member)
	}
}

func TestChannelJoinAcceptsRoomScopedInviteGrant(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "private",
		"room_id":     "!private:example.com",
		"name":        "Private",
		"visibility":  "private",
		"join_policy": "invite",
	})
	shareRoomID := "!share:example.com"
	if err := service.saveGroup(context.Background(), groupRecord{RoomID: shareRoomID, Name: "Share Room"}); err != nil {
		t.Fatal(err)
	}
	for _, member := range []memberRecord{
		{RoomID: shareRoomID, UserID: "@owner:example.com", Domain: "example.com", Membership: "join", Role: "owner"},
		{RoomID: shareRoomID, UserID: "@alice:remote.example", Domain: "remote.example", Membership: "join", Role: "member"},
	} {
		if err := service.saveMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
	}
	grant := mustHandle[map[string]any](t, service, "channels.invite_grant.create", map[string]any{
		"room_id":       ch.RoomID,
		"channel_id":    ch.ChannelID,
		"share_room_id": shareRoomID,
	})

	joined := mustHandle[map[string]any](t, service, "channels.join", map[string]any{
		"grant_id":      grant["grant_id"],
		"share_room_id": shareRoomID,
		"user_id":       "@alice:remote.example",
	})
	if joined["room_id"] != ch.RoomID {
		t.Fatalf("expected grant join to return channel room id, got %#v", joined)
	}
	member := joined["member"].(memberRecord)
	if member.UserID != "@alice:remote.example" || member.Membership != "join" || member.ChannelID != ch.ChannelID {
		t.Fatalf("expected grant user to join channel, got %#v", member)
	}
	if _, apiErr := service.Handle(context.Background(), "channels.join", map[string]any{
		"grant_id":      grant["grant_id"],
		"share_room_id": shareRoomID,
		"user_id":       "@eve:remote.example",
	}); apiErr == nil || apiErr.Status != http.StatusForbidden {
		t.Fatalf("expected non share-room member to be rejected, got %#v", apiErr)
	}
}

func TestChannelJoinWithMatrixInviteDoesNotRequireLocalInviteGrant(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "private",
		"room_id":     "!private:example.com",
		"name":        "Private",
		"visibility":  "private",
		"join_policy": "invite",
	})
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:      ch.RoomID,
		ChannelID:   ch.ChannelID,
		UserID:      "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Membership:  "invite",
		Role:        "member",
	}); err != nil {
		t.Fatal(err)
	}

	joined := mustHandle[map[string]any](t, service, "channels.join", map[string]any{
		"room_id":       ch.RoomID,
		"grant_id":      "grant-from-owner-node",
		"share_room_id": "!share-on-owner-node:example.com",
		"user_id":       "@alice:remote.example",
	})

	member := joined["member"].(memberRecord)
	if joined["room_id"] != ch.RoomID || member.Membership != "join" {
		t.Fatalf("expected existing Matrix invite to permit join without local grant, got %#v", joined)
	}
}

func TestPublicOpenChannelJoinRequestApprovesUntilRequesterNodeJoins(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "open",
		"room_id":     "!open:example.com",
		"name":        "Open",
		"visibility":  "public",
		"join_policy": "open",
	})

	request := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id":   ch.ChannelID,
		"room_id":      ch.RoomID,
		"user_mxid":    "@alice:remote.example",
		"display_name": "Alice",
	})
	if request["status"] != "approved" {
		t.Fatalf("expected remote open public channel request without callback to approve until requester node joins, got %#v", request)
	}
	member := request["member"].(memberRecord)
	if member.Membership != "approved" || member.UserID != "@alice:remote.example" || member.ChannelID != ch.ChannelID {
		t.Fatalf("expected approved member for open public channel without callback, got %#v", member)
	}
	members := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	alice := findMember(members, "@alice:remote.example")
	if alice.Membership != "approved" || alice.DisplayName != "Alice" {
		t.Fatalf("expected approved member to be visible as approved, got %#v in %#v", alice, members)
	}
}

func TestPrivateChannelRejectsPublicJoinRequest(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "private",
		"room_id":     "!private:example.com",
		"name":        "Private",
		"visibility":  "private",
		"join_policy": "invite",
	})

	if _, apiErr := service.Handle(context.Background(), "channels.public.join_request", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@alice:remote.example",
	}); apiErr == nil || apiErr.Status != 403 {
		t.Fatalf("expected private channel public join request to return 403, got %#v", apiErr)
	}
	members := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	if findMember(members, "@alice:remote.example").UserID != "" {
		t.Fatalf("expected rejected private public request to avoid creating member, got %#v", members)
	}
}

func TestKickedChannelMemberJoinRequestRejectedButLeaverCanReapply(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
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
	rejected := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@kicked:remote.example",
	})
	if rejected["status"] != "rejected" {
		t.Fatalf("expected kicked member join request to be auto rejected, got %#v", rejected)
	}
	members := mustHandle[map[string]any](t, service, "channels.members", map[string]any{
		"channel_id": ch.ChannelID,
	})["members"].([]memberRecord)
	if findMember(members, "@kicked:remote.example").UserID != "" {
		t.Fatalf("expected kicked member to stay hidden, got %#v", members)
	}

	shareRoomID := "!share:example.com"
	if err := service.saveGroup(context.Background(), groupRecord{RoomID: shareRoomID, Name: "Share Room"}); err != nil {
		t.Fatal(err)
	}
	for _, member := range []memberRecord{
		{RoomID: shareRoomID, UserID: "@owner:example.com", Domain: "example.com", Membership: "join", Role: "owner"},
		{RoomID: shareRoomID, UserID: "@kicked:remote.example", Domain: "remote.example", Membership: "join", Role: "member"},
	} {
		if err := service.saveMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
	}
	grant := mustHandle[map[string]any](t, service, "channels.invite_grant.create", map[string]any{
		"room_id":       ch.RoomID,
		"channel_id":    ch.ChannelID,
		"share_room_id": shareRoomID,
	})
	joined := mustHandle[map[string]any](t, service, "channels.join", map[string]any{
		"grant_id":      grant["grant_id"],
		"share_room_id": shareRoomID,
		"user_mxid":     "@kicked:remote.example",
	})
	member := joined["member"].(memberRecord)
	if member.UserID != "@kicked:remote.example" || member.Membership != "join" || member.ChannelID != ch.ChannelID {
		t.Fatalf("expected fresh channel invite grant to let kicked member rejoin, got %#v", joined)
	}

	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@projected-left:remote.example",
		Domain:     "remote.example",
		Membership: "leave",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}
	joinedAfterKickProjection := mustHandle[map[string]any](t, service, "channels.join", map[string]any{
		"room_id":       ch.RoomID,
		"channel_id":    ch.ChannelID,
		"grant_id":      "fresh-grant-from-owner-node",
		"share_room_id": shareRoomID,
		"user_mxid":     "@projected-left:remote.example",
	})
	projectedLeftMember := joinedAfterKickProjection["member"].(memberRecord)
	if projectedLeftMember.UserID != "@projected-left:remote.example" || projectedLeftMember.Membership != "join" || projectedLeftMember.ChannelID != ch.ChannelID {
		t.Fatalf("expected fresh channel invite grant to let projected-left kicked member rejoin, got %#v", joinedAfterKickProjection)
	}

	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@left:remote.example",
		Domain:     "remote.example",
		Membership: "leave",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}
	pending := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@left:remote.example",
	})
	if pending["status"] != "pending" {
		t.Fatalf("expected self-left member to be able to reapply, got %#v", pending)
	}
}

func TestChannelMemberLeaveActionCanReapply(t *testing.T) {
	ch := channel{
		ChannelID:  "moderated",
		RoomID:     "!moderated:example.com",
		Name:       "Moderated",
		Visibility: "public",
		JoinPolicy: "approval",
	}
	remoteActions := []string{}
	remoteOwner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_p2p/query" {
			http.NotFound(w, r)
			return
		}
		var env envelope
		if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		remoteActions = append(remoteActions, env.Action)
		w.Header().Set("Content-Type", "application/json")
		switch env.Action {
		case "channels.public.get":
			_ = json.NewEncoder(w).Encode(ch)
		case "channels.public.join_request":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "pending",
				"member": memberRecord{
					RoomID:     ch.RoomID,
					ChannelID:  ch.ChannelID,
					UserID:     "@owner:remote.example",
					Domain:     "remote.example",
					Membership: "pending",
					Role:       "member",
				},
				"channel": ch,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer remoteOwner.Close()

	service := NewService(Config{
		ServerName:                     "remote.example",
		RemoteNodeAllowPrivateBaseURLs: true,
	})
	bootstrapService(t, service)
	if err := service.saveChannel(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@owner:remote.example",
		Domain:     "remote.example",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	left := mustHandle[map[string]any](t, service, "channels.leave", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
	})
	member := left["member"].(memberRecord)
	if member.UserID != "@owner:remote.example" || member.Membership != "leave" || member.Role != "member" {
		t.Fatalf("expected current member to leave through real action, got %#v", left)
	}
	pending := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id":           ch.ChannelID,
		"room_id":              ch.RoomID,
		"user_mxid":            "@owner:remote.example",
		"remote_node_base_url": remoteOwner.URL,
	})
	if pending["status"] != "pending" {
		t.Fatalf("expected action-left channel member to be able to reapply, got %#v", pending)
	}
	if strings.Join(remoteActions, ",") != "channels.public.get,channels.public.join_request" {
		t.Fatalf("expected reapply to query and post to remote owner node, got %#v", remoteActions)
	}
}

func TestRemotePublicLookupRejectsMalformedAndUnconfiguredRoomTargets(t *testing.T) {
	service := NewService(Config{ServerName: "local.example"})
	bootstrapService(t, service)

	if _, apiErr := service.Handle(context.Background(), "channels.public.get", map[string]any{
		"room_id": "!room:https://127.0.0.1:8448",
	}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected URL-shaped room server to be rejected before outbound lookup, got %#v", apiErr)
	}

	if _, apiErr := service.Handle(context.Background(), "channels.public.get", map[string]any{
		"room_id": "!room:remote.example",
	}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected missing remote node base URL to return 400, got %#v", apiErr)
	}
}

func TestOpenPublicJoinRequestUsesMatrixJoinForLocalRequester(t *testing.T) {
	transport := &recordingTransport{roomID: "!open:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "open",
		"name":        "Open",
		"visibility":  "public",
		"join_policy": "open",
	})

	result := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_mxid":  "@owner:example.com",
	})
	if result["status"] != "joined" {
		t.Fatalf("expected open public join request to auto join local requester, got %#v", result)
	}
	member := result["member"].(memberRecord)
	if member.Membership != "join" {
		t.Fatalf("expected product member to join after Matrix join, got %#v", member)
	}
	if len(transport.invites) != 0 {
		t.Fatalf("expected open public join request not to create Matrix invite, got %#v", transport.invites)
	}
	if len(transport.joins) != 1 || transport.joins[0] != "@owner:example.com in !open:example.com" {
		t.Fatalf("expected open public join request to join through Matrix, got %#v", transport.joins)
	}
}

func TestKickedGroupMemberCanRejoinWithFreshInviteButNotDirectly(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!moderated-group:example.com",
		"name":    "Moderated Group",
	})

	mustHandle[map[string]any](t, service, "groups.invite", map[string]any{
		"room_id":   group.RoomID,
		"user_mxid": "@kicked:remote.example",
	})
	mustHandle[map[string]any](t, service, "groups.member.remove", map[string]any{
		"room_id":   group.RoomID,
		"user_mxid": "@kicked:remote.example",
	})
	if _, apiErr := service.Handle(context.Background(), "groups.join", map[string]any{
		"room_id":   group.RoomID,
		"user_mxid": "@kicked:remote.example",
	}); apiErr == nil || apiErr.Status != 403 {
		t.Fatalf("expected kicked group member direct rejoin to be rejected, got %#v", apiErr)
	}
	rejoined := mustHandle[map[string]any](t, service, "groups.join", map[string]any{
		"room_id":         group.RoomID,
		"user_mxid":       "@kicked:remote.example",
		"group_name":      group.Name,
		"invite_event_id": "$fresh-invite",
		"direct_room_id":  "!direct:remote.example",
	})
	rejoinedMember := rejoined["member"].(memberRecord)
	if rejoinedMember.UserID != "@kicked:remote.example" || rejoinedMember.Membership != "join" {
		t.Fatalf("expected fresh invite to let kicked group member rejoin, got %#v", rejoined)
	}

	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     group.RoomID,
		UserID:     "@projected-left:remote.example",
		Domain:     "remote.example",
		Membership: "leave",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}
	rejoinedAfterKickProjection := mustHandle[map[string]any](t, service, "groups.join", map[string]any{
		"room_id":         group.RoomID,
		"user_mxid":       "@projected-left:remote.example",
		"group_name":      group.Name,
		"invite_event_id": "$fresh-invite-after-kick",
		"direct_room_id":  "!direct:remote.example",
	})
	rejoinedAfterKickProjectionMember := rejoinedAfterKickProjection["member"].(memberRecord)
	if rejoinedAfterKickProjectionMember.UserID != "@projected-left:remote.example" || rejoinedAfterKickProjectionMember.Membership != "join" {
		t.Fatalf("expected fresh invite to let projected-left kicked group member rejoin, got %#v", rejoinedAfterKickProjection)
	}

	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     group.RoomID,
		UserID:     "@left:remote.example",
		Domain:     "remote.example",
		Membership: "leave",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}
	joined := mustHandle[map[string]any](t, service, "groups.join", map[string]any{
		"room_id":   group.RoomID,
		"user_mxid": "@left:remote.example",
	})
	if joined["status"] != "ok" {
		t.Fatalf("expected self-left group member to be able to rejoin, got %#v", joined)
	}
}

func TestGroupMemberLeaveActionCanRejoin(t *testing.T) {
	service := NewService(Config{ServerName: "remote.example"})
	bootstrapService(t, service)
	group := groupRecord{
		RoomID:       "!moderated-group:example.com",
		Name:         "Moderated Group",
		MemberCount:  1,
		InvitePolicy: "member",
	}
	if err := service.saveGroup(context.Background(), group); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     group.RoomID,
		UserID:     "@owner:remote.example",
		Domain:     "remote.example",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	left := mustHandle[map[string]any](t, service, "groups.leave", map[string]any{
		"room_id": group.RoomID,
	})
	member := left["member"].(memberRecord)
	if member.UserID != "@owner:remote.example" || member.Membership != "leave" || member.Role != "member" {
		t.Fatalf("expected current member to leave through real action, got %#v", left)
	}
	joined := mustHandle[map[string]any](t, service, "groups.join", map[string]any{
		"room_id": group.RoomID,
	})
	member = joined["member"].(memberRecord)
	if joined["status"] != "ok" || member.UserID != "@owner:remote.example" || member.Membership != "join" {
		t.Fatalf("expected action-left group member to be able to rejoin, got %#v", joined)
	}
}

func TestSaveMemberDoesNotDowngradeRemovedMembershipToLeave(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	removed := memberRecord{
		RoomID:     "!room:example.com",
		UserID:     "@alice:remote.example",
		Domain:     "remote.example",
		Membership: "remove",
		Role:       "member",
	}
	if err := service.saveMember(context.Background(), removed); err != nil {
		t.Fatal(err)
	}
	leave := removed
	leave.Membership = "leave"
	if err := service.saveMember(context.Background(), leave); err != nil {
		t.Fatal(err)
	}
	got, ok, err := service.lookupMember(context.Background(), removed.RoomID, removed.UserID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got.Membership != "remove" {
		t.Fatalf("expected removed membership to survive later leave projection, got %#v ok=%v", got, ok)
	}
}

func TestDeletedContactRequestRestoresOriginalRoom(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice",
	})
	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":      contact.RoomID,
		"peer_mxid":    contact.PeerMXID,
		"display_name": contact.DisplayName,
		"domain":       contact.Domain,
	})
	if accepted.Status != "accepted" {
		t.Fatalf("expected accepted contact, got %#v", accepted)
	}
	mustHandle[map[string]any](t, service, "contacts.delete", map[string]any{
		"room_id": accepted.RoomID,
	})
	contacts := mustHandle[map[string]any](t, service, "contacts.list", nil)["contacts"].([]contactRecord)
	if findContact(contacts, accepted.PeerMXID).PeerMXID != "" {
		t.Fatalf("expected deleted contact hidden from ordinary contact list, got %#v", contacts)
	}

	restored := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         accepted.PeerMXID,
		"display_name": accepted.DisplayName,
	})
	if restored.Status != "accepted" || restored.RoomID != accepted.RoomID {
		t.Fatalf("expected deleted peer re-request to restore original room, got %#v", restored)
	}
	contacts = mustHandle[map[string]any](t, service, "contacts.list", nil)["contacts"].([]contactRecord)
	if findContact(contacts, accepted.PeerMXID).Status != "accepted" {
		t.Fatalf("expected restored contact visible as accepted, got %#v", contacts)
	}
}

func TestDeletedContactRequestRestoresOriginalRoomAfterReload(t *testing.T) {
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
	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice",
	})
	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":      contact.RoomID,
		"peer_mxid":    contact.PeerMXID,
		"display_name": contact.DisplayName,
		"domain":       contact.Domain,
	})
	mustHandle[map[string]any](t, service, "contacts.delete", map[string]any{
		"room_id": accepted.RoomID,
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
		"mxid":         accepted.PeerMXID,
		"display_name": accepted.DisplayName,
	})
	if restored.Status != "accepted" || restored.RoomID != accepted.RoomID {
		t.Fatalf("expected deleted peer re-request to restore original room after reload, got %#v", restored)
	}
	contacts := mustHandle[map[string]any](t, reloaded, "contacts.list", nil)["contacts"].([]contactRecord)
	if findContact(contacts, accepted.PeerMXID).Status != "accepted" {
		t.Fatalf("expected restored contact visible after reload, got %#v", contacts)
	}
}

func TestContactRemarkUpdatePersistsAfterReload(t *testing.T) {
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
	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice",
	})
	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":      contact.RoomID,
		"peer_mxid":    contact.PeerMXID,
		"display_name": contact.DisplayName,
		"domain":       contact.Domain,
	})
	updated := mustHandle[contactRecord](t, service, "contacts.update", map[string]any{
		"room_id":      accepted.RoomID,
		"display_name": "Alice Remark",
	})
	if updated.DisplayName != "Alice Remark" || updated.PeerMXID != accepted.PeerMXID || updated.Status != "accepted" {
		t.Fatalf("expected updated contact remark, got %#v", updated)
	}
	contacts := mustHandle[map[string]any](t, service, "contacts.list", nil)["contacts"].([]contactRecord)
	if got := findContact(contacts, accepted.PeerMXID); got.DisplayName != "Alice Remark" {
		t.Fatalf("expected updated remark in contacts list, got %#v", contacts)
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
	reloadedContacts := mustHandle[map[string]any](t, reloaded, "contacts.list", nil)["contacts"].([]contactRecord)
	if got := findContact(reloadedContacts, accepted.PeerMXID); got.DisplayName != "Alice Remark" {
		t.Fatalf("expected updated remark after reload, got %#v", reloadedContacts)
	}
}

func TestPortalStatusReportsStorageAndProjectorMode(t *testing.T) {
	memoryService := NewService(Config{ServerName: "example.com"})
	memoryStatus := mustHandle[map[string]any](t, memoryService, "portal.status", nil)
	if memoryStatus["store_mode"] != "memory" || memoryStatus["projector_started"] != false || memoryStatus["policy_index_mode"] != "unavailable" || memoryStatus["policy_index_ready"] != false {
		t.Fatalf("expected memory service status to expose storage and projector mode, got %#v", memoryStatus)
	}

	transportService := NewServiceWithTransport(Config{ServerName: "example.com"}, &recordingTransport{})
	transportStatus := mustHandle[map[string]any](t, transportService, "portal.status", nil)
	if transportStatus["policy_index_mode"] != "matrix_state" || transportStatus["policy_index_ready"] != true {
		t.Fatalf("expected transport service status to expose Matrix-state policy index mode, got %#v", transportStatus)
	}

	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()
	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	databaseService, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, store)
	if err != nil {
		t.Fatal(err)
	}
	databaseService.SetProjectorStarted(true)
	databaseStatus := mustHandle[map[string]any](t, databaseService, "portal.status", nil)
	if databaseStatus["store_mode"] != "database" || databaseStatus["projector_started"] != true || databaseStatus["policy_index_ready"] != false {
		t.Fatalf("expected database service status to expose storage and projector mode, got %#v", databaseStatus)
	}
}

func TestPortalPasswordSetupAndAgentActions(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	status := mustHandle[map[string]any](t, service, "portal.status", nil)
	if status["initialized"] != true {
		t.Fatalf("expected default initialized portal, got %#v", status)
	}
	requireEightDigitPassword(t, service.password)
	auth := mustHandle[map[string]any](t, service, "portal.auth", map[string]any{"password": service.password})
	authDeviceID, _ := auth["device_id"].(string)
	if !strings.HasPrefix(authDeviceID, "PORTALIM") {
		t.Fatalf("expected auth session without requested device_id to expose generated Matrix device id, got %#v", auth)
	}
	if auth["profile_initialized"] != false {
		t.Fatalf("expected default owner profile to require first-time setup, got %#v", auth)
	}
	if auth["account_initialized"] != false {
		t.Fatalf("expected default auth to expose incomplete account initialization, got %#v", auth)
	}
	if auth["setup_completed"] != false {
		t.Fatalf("expected default auth to expose incomplete setup status, got %#v", auth)
	}
	if auth["already_initialized"] != false {
		t.Fatalf("expected default auth to expose incomplete already initialized status, got %#v", auth)
	}
	if auth["initialization_completed"] != false {
		t.Fatalf("expected default auth to expose incomplete initialization completion, got %#v", auth)
	}
	if auth["initialized"] != true || auth["password_initialized"] != false {
		t.Fatalf("expected default auth to expose initialization flags, got %#v", auth)
	}
	profile := mustHandle[ownerProfile](t, service, "profile.get", nil)
	if profile.DisplayName != "" {
		t.Fatalf("expected default owner display name to be empty, got %#v", profile)
	}

	defaultPassword := service.password
	bootstrap := bootstrapService(t, service)
	oldAccessToken := bootstrap["access_token"].(string)
	if bootstrap["initialized"] != true || bootstrap["password_initialized"] != false || bootstrap["profile_initialized"] != false {
		t.Fatalf("expected bootstrap to expose first-time initialization flags, got %#v", bootstrap)
	}
	if _, ok := bootstrap["admin_access_token"]; ok {
		t.Fatalf("bootstrap must not expose admin_access_token: %#v", bootstrap)
	}
	if _, ok := bootstrap["matrix_access_token"]; ok {
		t.Fatalf("bootstrap must not expose matrix_access_token: %#v", bootstrap)
	}
	if bootstrap["account_initialized"] != false {
		t.Fatalf("expected bootstrap to expose incomplete account initialization, got %#v", bootstrap)
	}
	if bootstrap["setup_completed"] != false {
		t.Fatalf("expected bootstrap to expose incomplete setup status, got %#v", bootstrap)
	}
	if bootstrap["already_initialized"] != false {
		t.Fatalf("expected bootstrap to expose incomplete already initialized status, got %#v", bootstrap)
	}

	if _, apiErr := service.Handle(context.Background(), "portal.setup", nil); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected portal.setup compatibility action to be removed, got %#v", apiErr)
	}

	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Alice",
	})
	profileOnlyAuth := mustHandle[map[string]any](t, service, "portal.auth", map[string]any{"password": defaultPassword})
	if profileOnlyAuth["profile_initialized"] != true {
		t.Fatalf("expected display name setup to be exposed before password change, got %#v", profileOnlyAuth)
	}
	if profileOnlyAuth["account_initialized"] != false || profileOnlyAuth["initialization_completed"] != false {
		t.Fatalf("expected account initialization to still require password change, got %#v", profileOnlyAuth)
	}
	password := mustHandle[map[string]any](t, service, "portal.password", map[string]any{
		"old_password": defaultPassword,
		"new_password": "new-secret",
	})
	if password["access_token"] == "" {
		t.Fatalf("expected refreshed access token after password change, got %#v", password)
	}
	if _, ok := password["admin_access_token"]; ok {
		t.Fatalf("password response must not expose admin_access_token: %#v", password)
	}
	if _, ok := password["matrix_access_token"]; ok {
		t.Fatalf("password response must not expose matrix_access_token: %#v", password)
	}
	if password["profile_initialized"] != true {
		t.Fatalf("expected password change after display name setup to complete profile initialization, got %#v", password)
	}
	if password["account_initialized"] != true {
		t.Fatalf("expected password change after display name setup to complete account initialization, got %#v", password)
	}
	if password["setup_completed"] != true {
		t.Fatalf("expected password change after display name setup to complete setup, got %#v", password)
	}
	if password["already_initialized"] != true {
		t.Fatalf("expected password change after display name setup to expose already initialized, got %#v", password)
	}
	if password["initialization_completed"] != true {
		t.Fatalf("expected password change after display name setup to expose completed initialization, got %#v", password)
	}
	if password["initialized"] != true || password["password_initialized"] != true {
		t.Fatalf("expected password change to expose completed initialization flags, got %#v", password)
	}
	passwordDeviceID, _ := password["device_id"].(string)
	if !strings.HasPrefix(passwordDeviceID, "PORTALIM") {
		t.Fatalf("expected password session without requested device_id to expose generated Matrix device id, got %#v", password)
	}
	if _, err := service.Handle(context.Background(), "portal.auth", map[string]any{"password": defaultPassword}); err == nil {
		t.Fatalf("expected old password to fail after password change")
	}
	nextAuth := mustHandle[map[string]any](t, service, "portal.auth", map[string]any{"password": "new-secret"})
	if nextAuth["profile_initialized"] != true {
		t.Fatalf("expected profile initialization flag to persist into auth, got %#v", nextAuth)
	}
	if nextAuth["account_initialized"] != true {
		t.Fatalf("expected account initialization flag to persist into auth, got %#v", nextAuth)
	}
	if nextAuth["setup_completed"] != true {
		t.Fatalf("expected setup completion flag to persist into auth, got %#v", nextAuth)
	}
	if nextAuth["already_initialized"] != true {
		t.Fatalf("expected already initialized flag to persist into auth, got %#v", nextAuth)
	}
	if nextAuth["initialization_completed"] != true {
		t.Fatalf("expected initialization completed flag to persist into auth, got %#v", nextAuth)
	}
	if nextAuth["initialized"] != true || nextAuth["password_initialized"] != true {
		t.Fatalf("expected completed initialization flags to persist into auth, got %#v", nextAuth)
	}

	agentPassword := mustHandle[map[string]any](t, service, "agent.password", nil)
	if agentPassword["password"] != "new-secret" {
		t.Fatalf("expected agent password lookup to return current password, got %#v", agentPassword)
	}
	if service.Authenticate(oldAccessToken) {
		t.Fatalf("expected old access token to be rotated after password change")
	}
}

func TestPortalProfileInitializationCompletesWhenPasswordChangesBeforeName(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	defaultPassword := service.password

	password := mustHandle[map[string]any](t, service, "portal.password", map[string]any{
		"old_password": defaultPassword,
		"new_password": "new-secret",
	})
	if password["profile_initialized"] != false {
		t.Fatalf("expected password-only setup to still require profile name, got %#v", password)
	}
	if password["account_initialized"] != false {
		t.Fatalf("expected password-only setup to keep account initialization incomplete, got %#v", password)
	}
	if password["setup_completed"] != false {
		t.Fatalf("expected password-only setup to keep setup incomplete, got %#v", password)
	}
	if password["already_initialized"] != false {
		t.Fatalf("expected password-only setup to keep already initialized incomplete, got %#v", password)
	}
	if password["initialization_completed"] != false {
		t.Fatalf("expected password-only setup to keep initialization incomplete, got %#v", password)
	}

	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Alice",
	})
	auth := mustHandle[map[string]any](t, service, "portal.auth", map[string]any{"password": "new-secret"})
	if auth["profile_initialized"] != true {
		t.Fatalf("expected profile initialization after password and display name setup, got %#v", auth)
	}
	if auth["account_initialized"] != true {
		t.Fatalf("expected account initialization after password and display name setup, got %#v", auth)
	}
	if auth["setup_completed"] != true {
		t.Fatalf("expected setup completion after password and display name setup, got %#v", auth)
	}
	if auth["already_initialized"] != true {
		t.Fatalf("expected already initialized after password and display name setup, got %#v", auth)
	}
	if auth["initialization_completed"] != true {
		t.Fatalf("expected initialization completed after password and display name setup, got %#v", auth)
	}
}

func TestContactRequestPreservesPeerDomainWithPort(t *testing.T) {
	service := NewService(Config{ServerName: "dendrite-a:8448"})

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@owner:dendrite-b:8448",
		"display_name": "Owner B",
	})

	if contact.Domain != "dendrite-b:8448" {
		t.Fatalf("expected MXID domain with port to be preserved, got %#v", contact)
	}
}

func TestContactRequestRemarkIsReturnedInContactListAndPendingNotice(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice",
		"remark":       "我是 Adam，请通过好友申请",
	})
	if contact.Remark != "我是 Adam，请通过好友申请" {
		t.Fatalf("expected contact request response to include remark, got %#v", contact)
	}
	contacts := mustHandle[map[string]any](t, service, "contacts.list", nil)["contacts"].([]contactRecord)
	if len(contacts) != 1 || contacts[0].Remark != "我是 Adam，请通过好友申请" {
		t.Fatalf("expected contacts.list to include request remark, got %#v", contacts)
	}
	contact.Status = "pending_inbound"
	if err := service.saveContact(context.Background(), contact); err != nil {
		t.Fatal(err)
	}
	bootstrap := mustHandle[map[string]any](t, service, "sync.bootstrap", nil)
	pending := bootstrap["pending"].(map[string]any)
	friendRequests := pending["friend_requests"].([]map[string]any)
	if len(friendRequests) != 1 || friendRequests[0]["remark"] != "我是 Adam，请通过好友申请" {
		t.Fatalf("expected pending friend request to include remark, got %#v", friendRequests)
	}
}

func TestAgentConfigContactsFavoritesAndDeprecatedMessageActions(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)

	cfg := mustHandle[map[string]any](t, service, "agent.config.update", map[string]any{
		"display_name":    "Ops Agent",
		"context_window":  float64(64),
		"enabled":         true,
		"model":           "local-model",
		"system_prompt":   "help users",
		"allowed_actions": []any{"contacts.request", "channels.create"},
	})
	if cfg["display_name"] != "Ops Agent" || int64Param(cfg["context_window"]) != 64 || cfg["enabled"] != true {
		t.Fatalf("expected updated agent config, got %#v", cfg)
	}
	cfg = mustHandle[map[string]any](t, service, "agent.config.get", nil)
	if cfg["display_name"] != "Ops Agent" || cfg["model"] != "local-model" {
		t.Fatalf("expected persisted agent config, got %#v", cfg)
	}
	status := mustHandle[map[string]any](t, service, "agent.status", nil)
	if status["configured"] != true {
		t.Fatalf("expected configured agent status, got %#v", status)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:example.com",
		"display_name": "Alice",
	})
	contacts := mustHandle[map[string]any](t, service, "contacts.list", nil)
	if got := contacts["contacts"].([]contactRecord); len(got) != 1 || got[0].RoomID != contact.RoomID {
		t.Fatalf("expected contact list with alice, got %#v", contacts)
	}
	mustHandle[map[string]any](t, service, "contacts.requests.delete", map[string]any{"room_id": contact.RoomID})
	contacts = mustHandle[map[string]any](t, service, "contacts.list", nil)
	if got := contacts["contacts"].([]contactRecord); len(got) != 0 {
		t.Fatalf("expected deleted contact request gone, got %#v", contacts)
	}

	if err := service.saveContact(context.Background(), contactRecord{
		PeerMXID:    "@bob:remote.example",
		DisplayName: "Bob",
		Domain:      "remote.example",
		RoomID:      "!bob:remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatalf("failed to seed inbound contact: %s", err)
	}
	bootstrap := mustHandle[map[string]any](t, service, "sync.bootstrap", nil)
	pending := bootstrap["pending"].(map[string]any)
	friendRequests := pending["friend_requests"].([]map[string]any)
	if len(friendRequests) != 1 || friendRequests[0]["id"] != "!bob:remote.example" || friendRequests[0]["title"] != "Bob" {
		t.Fatalf("expected pending inbound contact in friend requests, got %#v", pending)
	}

	fav1 := mustHandle[favoriteRecord](t, service, "favorites.add", map[string]any{"content": "one"})
	fav2 := mustHandle[favoriteRecord](t, service, "favorites.add", map[string]any{"content": "two"})
	mustHandle[map[string]any](t, service, "favorites.delete_batch", map[string]any{"ids": []any{float64(fav1.ID), float64(fav2.ID)}})
	favorites := mustHandle[map[string]any](t, service, "favorites.list", nil)
	if got := favorites["favorites"].([]favoriteRecord); len(got) != 0 {
		t.Fatalf("expected batch-deleted favorites gone, got %#v", favorites)
	}

	report := mustHandle[reportRecord](t, service, "reports.submit", map[string]any{
		"reporterDomain": "alice.example",
		"reportedDomain": "channel.example",
		"targetType":     float64(2),
		"reason":         "spam",
		"images":         []any{"mxc://example/report"},
	})
	if report.ReporterDomain != "alice.example" || report.ReportedDomain != "channel.example" || report.TargetType != 2 || report.ImagesJSON != `["mxc://example/report"]` {
		t.Fatalf("expected saved report with legacy-compatible params, got %#v", report)
	}

	if _, apiErr := service.Handle(context.Background(), "sync.messages", map[string]any{"room_id": "!room:example.com"}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected removed sync.messages to be unknown, got %#v", apiErr)
	}
}

func TestRemovedSyncMessagesIsUnknown(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})

	ctx := context.Background()
	for _, params := range []map[string]any{
		nil,
		{"page": float64(1)},
		{"page_size": float64(50)},
		{"limit": float64(50)},
		{"cursor": "not-a-cursor"},
	} {
		if result, apiErr := service.Handle(ctx, "sync.messages", params); apiErr == nil || apiErr.Status != http.StatusBadRequest {
			t.Fatalf("expected removed sync.messages to be unknown, result=%#v err=%#v", result, apiErr)
		}
	}
}

func TestGroupInviteAcceptsInviteArrayAlias(t *testing.T) {
	service := NewService(Config{ServerName: "im1.direxio.ai"})
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"room_id": "!group:im1.direxio.ai", "name": "Group"})

	res, apiErr := service.Handle(context.Background(), "groups.invite", map[string]any{
		"room_id": group.RoomID,
		"invite":  []any{"@owner:dm1.direxio.ai"},
	})
	if apiErr != nil {
		t.Fatalf("expected invite array alias to be accepted, got %#v", apiErr)
	}
	invite := res.(map[string]any)
	members := invite["members"].([]memberRecord)
	if len(members) != 1 || members[0].UserID != "@owner:dm1.direxio.ai" || members[0].Membership != "invite" {
		t.Fatalf("expected invite array alias to create invited member, got %#v", invite)
	}
}

func TestAgentPermissionCatalogCoversMigratedBusinessActions(t *testing.T) {
	perms := defaultAPIPermissions()
	for _, action := range []string{
		"apis.list",
		"contacts.list",
		"contacts.request",
		"contacts.delete",
		"channels.create",
		"channels.list",
		"channels.dissolve",
		"channels.members",
		"channels.posts.list",
		"channels.posts.create",
		"channels.posts.recall",
		"channels.comments.list",
		"channels.comments.create",
		"channels.my_comments",
		"channels.my_reactions",
		"users.public_channels",
		"groups.list",
		"groups.dissolve",
		"groups.members",
		"groups.invite",
		"favorites.list",
		"favorites.add",
		"favorites.delete",
		"reports.submit",
	} {
		if perm, ok := perms[action]; !ok || !perm.Enabled {
			t.Fatalf("expected Agent permission for %s, got %#v", action, perm)
		}
	}
	for _, action := range []string{
		"sync.messages",
		"sync.unread",
		"search",
		"rooms.send",
		"rooms.send_media",
		"rooms.messages.delete",
		"rooms.messages.delete_batch",
		"rooms.messages.delete_range",
		"rooms.messages.recall",
	} {
		if perm, ok := perms[action]; ok {
			t.Fatalf("expected removed Agent permission %s to be absent, got %#v", action, perm)
		}
	}
}

func TestContactBackupActionsRemoved(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	for _, action := range []string{"contacts.export", "contacts.download", "contacts.import"} {
		if result, apiErr := service.Handle(context.Background(), action, map[string]any{}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
			t.Fatalf("expected removed %s action to be unknown, result=%#v err=%#v", action, result, apiErr)
		}
		if _, ok := defaultAPIPermissions()[action]; ok {
			t.Fatalf("expected removed %s action to be absent from default API permissions", action)
		}
	}
}

func TestMyChannelCommentsListsOwnerHistory(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": "ch",
		"body":       "post",
	})
	comment := mustHandle[channelCommentRecord](t, service, "channels.comments.create", map[string]any{
		"channel_id": "ch",
		"post_id":    post.PostID,
		"body":       "my comment",
	})

	history := mustHandle[map[string]any](t, service, "channels.my_comments", nil)
	comments := jsonList(t, history["comments"])
	if len(comments) != 1 || comments[0]["comment_id"] != comment.CommentID || comments[0]["body"] != "my comment" {
		t.Fatalf("expected my channel comment history, got %#v", history)
	}
}

func TestMembersListIncludesAvatarsAndJoinOrder(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Group",
	})
	time.Sleep(time.Millisecond)
	mustHandle[map[string]any](t, service, "groups.join", map[string]any{
		"room_id":    group.RoomID,
		"user_mxid":  "@alice:example.com",
		"avatar_url": "mxc://example.com/alice",
	})
	time.Sleep(time.Millisecond)
	mustHandle[map[string]any](t, service, "groups.join", map[string]any{
		"room_id":    group.RoomID,
		"user_mxid":  "@bob:example.com",
		"avatar_url": "mxc://example.com/bob",
	})

	list := mustHandle[map[string]any](t, service, "groups.members", map[string]any{"room_id": group.RoomID})
	members := list["members"].([]memberRecord)
	if len(members) != 3 {
		t.Fatalf("expected owner plus two members, got %#v", members)
	}
	if members[0].Role != "owner" || members[1].UserID != "@alice:example.com" || members[2].UserID != "@bob:example.com" {
		t.Fatalf("expected members in join order, got %#v", members)
	}
	if members[1].AvatarURL != "mxc://example.com/alice" || members[2].AvatarURL != "mxc://example.com/bob" {
		t.Fatalf("expected member avatars to be preserved, got %#v", members)
	}
	if members[0].JoinedAt == 0 || members[1].JoinedAt == 0 || members[2].JoinedAt == 0 {
		t.Fatalf("expected joined_at on every member, got %#v", members)
	}
	groups := mustHandle[map[string]any](t, service, "groups.list", nil)["groups"].([]groupRecord)
	if len(groups) != 1 || groups[0].RoomID != group.RoomID || groups[0].MemberCount != 3 {
		t.Fatalf("expected group member_count to match joined members, got %#v", groups)
	}

	createdChannel := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch_members",
		"room_id":    "!channel:example.com",
		"name":       "Members",
	})
	time.Sleep(time.Millisecond)
	mustHandle[map[string]any](t, service, "channels.join", map[string]any{
		"channel_id":   createdChannel.ChannelID,
		"user_mxid":    "@carol:example.com",
		"display_name": "Carol",
		"avatar_url":   "mxc://example.com/carol",
	})
	channelMembersList := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": createdChannel.ChannelID})
	channelMembers := channelMembersList["members"].([]memberRecord)
	if len(channelMembers) != 2 || channelMembers[0].Role != "owner" || channelMembers[1].UserID != "@carol:example.com" {
		t.Fatalf("expected channel owner to see channel members in join order, got %#v", channelMembers)
	}
	if channelMembers[1].DisplayName != "Carol" || channelMembers[1].AvatarURL != "mxc://example.com/carol" || channelMembers[1].Membership != "join" {
		t.Fatalf("expected channel member profile and status, got %#v", channelMembers[1])
	}
}

func TestGroupJoinCreatesLocalGroupRecord(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	joined := mustHandle[map[string]any](t, service, "groups.join", map[string]any{
		"room_id":    "!remote:remote.example",
		"group_name": "Remote Group",
	})
	member := joined["member"].(memberRecord)
	if member.RoomID != "!remote:remote.example" || member.Membership != "join" {
		t.Fatalf("expected local joined member for remote group, got %#v", joined)
	}

	list := mustHandle[map[string]any](t, service, "groups.list", nil)
	groups := list["groups"].([]groupRecord)
	if len(groups) != 1 {
		t.Fatalf("expected joined remote group in groups.list, got %#v", list)
	}
	if groups[0].RoomID != "!remote:remote.example" || groups[0].Name != "Remote Group" {
		t.Fatalf("expected joined remote group summary, got %#v", groups[0])
	}
}

func TestUserPublicChannelsReturnsOwnedAndJoinedPublicChannels(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	publicChannel := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "public_owned",
		"room_id":     "!public-owned:example.com",
		"name":        "Public Owned",
		"avatar_url":  "mxc://example.com/public-owned",
		"visibility":  "public",
		"description": "visible",
	})
	privateChannel := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "private_owned",
		"room_id":    "!private-owned:example.com",
		"name":       "Private Owned",
		"visibility": "private",
	})
	memberOnly := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "member_only",
		"room_id":    "!member-only:example.com",
		"name":       "Member Only",
		"visibility": "public",
	})
	mustHandle[map[string]any](t, service, "channels.join", map[string]any{
		"channel_id": memberOnly.ChannelID,
		"user_mxid":  "@alice:example.com",
	})
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:      publicChannel.RoomID,
		ChannelID:   publicChannel.ChannelID,
		UserID:      "@alice:example.com",
		DisplayName: "Alice",
		Domain:      "example.com",
		Membership:  "join",
		Role:        "owner",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:      privateChannel.RoomID,
		ChannelID:   privateChannel.ChannelID,
		UserID:      "@alice:example.com",
		DisplayName: "Alice",
		Domain:      "example.com",
		Membership:  "join",
		Role:        "owner",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "users.public_channels", map[string]any{"user_mxid": "@alice:example.com"})
	channels := result["channels"].([]channel)
	if len(channels) != 2 {
		t.Fatalf("expected alice owned and joined public channels, got %#v", result)
	}
	got := map[string]channel{}
	for _, ch := range channels {
		got[ch.ChannelID] = ch
	}
	if ch := got[publicChannel.ChannelID]; ch.RoomID != publicChannel.RoomID || ch.AvatarURL != "mxc://example.com/public-owned" {
		t.Fatalf("expected alice owned public channel with display fields, got %#v", result)
	}
	if ch := got[memberOnly.ChannelID]; ch.RoomID != memberOnly.RoomID {
		t.Fatalf("expected alice joined public channel, got %#v", result)
	}
	if _, ok := got[privateChannel.ChannelID]; ok {
		t.Fatalf("expected private channel to be hidden, got %#v", result)
	}
}

func jsonList(t *testing.T, value any) []map[string]any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var result []map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func findMember(members []memberRecord, userID string) memberRecord {
	for _, member := range members {
		if member.UserID == userID {
			return member
		}
	}
	return memberRecord{}
}

func findContact(contacts []contactRecord, peerMXID string) contactRecord {
	for _, contact := range contacts {
		if contact.PeerMXID == peerMXID {
			return contact
		}
	}
	return contactRecord{}
}
