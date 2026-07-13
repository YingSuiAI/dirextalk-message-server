package p2p

import (
	"context"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
)

type mcpMessagePage = dirextalkmcp.Page
type mcpMessagePageResult = dirextalkmcp.MessagePageResult

func mcpPageFromParams(params map[string]any, action, targetID string) (mcpMessagePage, *apiError) {
	page, apiErr := dirextalkmcp.PageFromParams(params, action, targetID)
	return page, dirextalkMCPErrorToAPI(apiErr)
}

func mcpAttachPagination(payload map[string]any, action, targetID string, page mcpMessagePage, hasMore bool, lastTS int64, lastID string) *apiError {
	return dirextalkMCPErrorToAPI(dirextalkmcp.AttachPagination(payload, action, targetID, page, hasMore, lastTS, lastID))
}

func mcpPageIncludes(ts int64, id string, page mcpMessagePage) bool {
	return dirextalkmcp.InPage(ts, id, page)
}

func mcpFormatTime(ts int64) string {
	return dirextalkmcp.FormatTime(ts)
}

func (s *Service) mcpChannelPostPage(ctx context.Context, channelID string, page mcpMessagePage) ([]channelPostRecord, bool, error) {
	return s.channelContentModule.PostPage(
		ctx, channelID, page.FromTS, page.SnapshotTS, page.CursorTS, page.CursorID, page.Limit,
	)
}

func (s *Service) mcpChannelCommentPage(ctx context.Context, postID string, page mcpMessagePage) ([]channelCommentRecord, bool, error) {
	return s.channelContentModule.CommentPage(
		ctx, postID, page.FromTS, page.SnapshotTS, page.CursorTS, page.CursorID, page.Limit,
	)
}

func lastMCPMessageKey(messages []mcpMessageSummary) (int64, string) {
	if len(messages) == 0 {
		return 0, ""
	}
	last := messages[len(messages)-1]
	return last.OriginServerTS, last.EventID
}

func lastMCPPostKey(posts []channelPostRecord) (int64, string) {
	if len(posts) == 0 {
		return 0, ""
	}
	last := posts[len(posts)-1]
	return last.OriginServerTS, last.PostID
}

func lastMCPCommentKey(comments []channelCommentRecord) (int64, string) {
	if len(comments) == 0 {
		return 0, ""
	}
	last := comments[len(comments)-1]
	return last.OriginServerTS, last.CommentID
}

func (s *Service) mcpPostSummary(ctx context.Context, post channelPostRecord) mcpPostSummary {
	favoriteCount, favoritedByMe := s.mcpFavoriteStateForPost(ctx, post)
	return mcpPostSummary{
		PostID:        post.PostID,
		CreatedAt:     mcpFormatTime(post.OriginServerTS),
		Sender:        fallbackString(post.AuthorName, post.AuthorMXID),
		Msg:           post.Body,
		CommentCount:  post.CommentCount,
		LikeCount:     post.ReactionCount,
		FavoriteCount: favoriteCount,
		FavoritedByMe: favoritedByMe,
	}
}

func (s *Service) mcpCommentSummary(comment channelCommentRecord) mcpCommentSummary {
	return mcpCommentSummary{
		CommentID: comment.CommentID,
		CreatedAt: mcpFormatTime(comment.OriginServerTS),
		Sender:    fallbackString(comment.AuthorName, comment.AuthorMXID),
		Msg:       comment.Body,
	}
}

func (s *Service) mcpFavoriteStateForPost(ctx context.Context, post channelPostRecord) (int64, bool) {
	favorites, err := s.socialModule.ListFavorites(ctx, "")
	if err != nil {
		return 0, false
	}
	var count int64
	for _, favorite := range favorites {
		if mcpFavoriteMatchesPost(favorite, post) {
			count++
		}
	}
	return count, count > 0
}

func mcpFavoriteMatchesPost(favorite favoriteRecord, post channelPostRecord) bool {
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
