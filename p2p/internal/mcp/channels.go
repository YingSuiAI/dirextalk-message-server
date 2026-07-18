package mcp

import (
	"context"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
	channelsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/channels"
)

func (m *Module) channelPostsList(ctx context.Context, params map[string]any) (any, *dirextalkmcp.Error) {
	roomID := dirextalkmcp.TrimString(params["room_id"])
	if roomID == "" {
		return nil, dirextalkmcp.BadRequest("room_id is required")
	}
	if mcpErr := m.requireRoomAllowed(roomID); mcpErr != nil {
		return nil, mcpErr
	}
	page, mcpErr := dirextalkmcp.PageFromParams(params, dirextalkmcp.ActionChannelPostsList, roomID)
	if mcpErr != nil {
		return nil, mcpErr
	}
	channel, ok, err := m.channels.ByIDOrRoom(ctx, "", roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, dirextalkmcp.StatusError(http.StatusNotFound, "channel not found")
	}
	rawPosts, hasMore, err := m.content.PostPage(ctx, channel.ChannelID, page.FromTS, page.SnapshotTS, page.CursorTS, page.CursorID, page.Limit)
	if err != nil {
		return nil, internalError(err)
	}
	posts := make([]dirextalkmcp.PostSummary, 0, len(rawPosts))
	for _, post := range rawPosts {
		posts = append(posts, m.postSummary(ctx, post))
	}
	result := map[string]any{
		"channel_id": channel.ChannelID,
		"room_id":    channel.RoomID,
		"name":       channel.Name,
		"posts":      posts,
	}
	lastTS, lastID := lastPostKey(rawPosts)
	if mcpErr := dirextalkmcp.AttachPagination(result, dirextalkmcp.ActionChannelPostsList, roomID, page, hasMore, lastTS, lastID); mcpErr != nil {
		return nil, mcpErr
	}
	return result, nil
}

func (m *Module) channelCommentsList(ctx context.Context, params map[string]any) (any, *dirextalkmcp.Error) {
	postID := dirextalkmcp.TrimString(params["post_id"])
	if postID == "" {
		return nil, dirextalkmcp.BadRequest("post_id is required")
	}
	page, mcpErr := dirextalkmcp.PageFromParams(params, dirextalkmcp.ActionChannelCommentsList, postID)
	if mcpErr != nil {
		return nil, mcpErr
	}
	post, ok, err := m.content.PostByID(ctx, postID, "")
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, dirextalkmcp.StatusError(http.StatusNotFound, "post not found")
	}
	if mcpErr := m.requireRoomAllowed(post.RoomID); mcpErr != nil {
		return nil, mcpErr
	}
	rawComments, hasMore, err := m.content.CommentPage(ctx, postID, page.FromTS, page.SnapshotTS, page.CursorTS, page.CursorID, page.Limit)
	if err != nil {
		return nil, internalError(err)
	}
	comments := make([]dirextalkmcp.CommentSummary, 0, len(rawComments))
	for _, comment := range rawComments {
		comments = append(comments, commentSummary(comment))
	}
	result := map[string]any{"post_id": postID, "comments": comments}
	lastTS, lastID := lastCommentKey(rawComments)
	if mcpErr := dirextalkmcp.AttachPagination(result, dirextalkmcp.ActionChannelCommentsList, postID, page, hasMore, lastTS, lastID); mcpErr != nil {
		return nil, mcpErr
	}
	return result, nil
}

func (m *Module) channelCommentCreate(ctx context.Context, params map[string]any) (any, *dirextalkmcp.Error) {
	postID := dirextalkmcp.TrimString(params["post_id"])
	msg := fallback(dirextalkmcp.TrimString(params["msg"]), dirextalkmcp.TrimString(params["body"]))
	if postID == "" {
		return nil, dirextalkmcp.BadRequest("post_id is required")
	}
	if msg == "" {
		return nil, dirextalkmcp.BadRequest("msg is required")
	}
	post, ok, err := m.content.PostByID(ctx, postID, "")
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, dirextalkmcp.StatusError(http.StatusNotFound, "post not found")
	}
	if mcpErr := m.requireRoomAllowed(post.RoomID); mcpErr != nil {
		return nil, mcpErr
	}
	commentAny, actionErr := m.content.CreateComment(ctx, map[string]any{
		"channel_id":   post.ChannelID,
		"room_id":      post.RoomID,
		"post_id":      postID,
		"body":         msg,
		"message_type": "text",
	})
	if actionErr != nil {
		return nil, actionError(actionErr)
	}
	comment := commentAny.(channelsmodule.Comment)
	return map[string]any{
		"ok":         true,
		"post_id":    comment.PostID,
		"comment_id": comment.CommentID,
		"created_at": dirextalkmcp.FormatTime(comment.OriginServerTS),
	}, nil
}

func (m *Module) postSummary(ctx context.Context, post channelsmodule.Post) dirextalkmcp.PostSummary {
	favoriteCount, favoritedByMe := m.FavoriteStateForPost(ctx, post)
	return dirextalkmcp.PostSummary{
		PostID:        post.PostID,
		CreatedAt:     dirextalkmcp.FormatTime(post.OriginServerTS),
		Sender:        fallback(post.AuthorName, post.AuthorMXID),
		Msg:           post.Body,
		CommentCount:  post.CommentCount,
		LikeCount:     post.ReactionCount,
		FavoriteCount: favoriteCount,
		FavoritedByMe: favoritedByMe,
	}
}

func commentSummary(comment channelsmodule.Comment) dirextalkmcp.CommentSummary {
	return dirextalkmcp.CommentSummary{
		CommentID: comment.CommentID,
		CreatedAt: dirextalkmcp.FormatTime(comment.OriginServerTS),
		Sender:    fallback(comment.AuthorName, comment.AuthorMXID),
		Msg:       comment.Body,
	}
}

// FavoriteStateForPost is exposed to the root compatibility facade while the
// owning implementation remains in this module.
func (m *Module) FavoriteStateForPost(ctx context.Context, post channelsmodule.Post) (int64, bool) {
	if m == nil || m.social == nil {
		return 0, false
	}
	favorites, err := m.social.ListFavorites(ctx, "")
	if err != nil {
		return 0, false
	}
	var count int64
	for _, favorite := range favorites {
		if favoriteMatchesPost(favorite, post) {
			count++
		}
	}
	return count, count > 0
}

func favoriteMatchesPost(favorite dirextalkdomain.FavoriteRecord, post channelsmodule.Post) bool {
	if strings.TrimSpace(post.EventID) == "" || strings.TrimSpace(favorite.EventID) != strings.TrimSpace(post.EventID) {
		return false
	}
	if strings.TrimSpace(favorite.RoomID) != "" && strings.TrimSpace(post.RoomID) != "" && strings.TrimSpace(favorite.RoomID) != strings.TrimSpace(post.RoomID) {
		return false
	}
	switch strings.TrimSpace(favorite.MessageType) {
	case "", "post", "channel_post":
		return true
	default:
		return false
	}
}

func lastPostKey(posts []channelsmodule.Post) (int64, string) {
	if len(posts) == 0 {
		return 0, ""
	}
	last := posts[len(posts)-1]
	return last.OriginServerTS, last.PostID
}

func lastCommentKey(comments []channelsmodule.Comment) (int64, string) {
	if len(comments) == 0 {
		return 0, ""
	}
	last := comments[len(comments)-1]
	return last.OriginServerTS, last.CommentID
}
