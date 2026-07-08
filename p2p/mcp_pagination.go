package p2p

import (
	"context"
	"sort"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
)

type mcpMessagePage = dirextalkmcp.Page
type mcpMessagePageResult = dirextalkmcp.MessagePageResult
type mcpCursorPayload = dirextalkmcp.CursorPayload

func mcpPageFromParams(params map[string]any, action, targetID string) (mcpMessagePage, *apiError) {
	page, apiErr := dirextalkmcp.PageFromParams(params, action, targetID)
	return page, dirextalkMCPErrorToAPI(apiErr)
}

func rejectLegacyMCPTimeParams(params map[string]any) *apiError {
	return dirextalkMCPErrorToAPI(dirextalkmcp.RejectLegacyTimeParams(params))
}

func mcpTimeParam(params map[string]any, key string) (int64, bool, *apiError) {
	ts, ok, apiErr := dirextalkmcp.TimeParam(params, key)
	return ts, ok, dirextalkMCPErrorToAPI(apiErr)
}

func decodeMCPCursor(cursor string) (mcpCursorPayload, *apiError) {
	payload, apiErr := dirextalkmcp.DecodeCursor(cursor)
	return payload, dirextalkMCPErrorToAPI(apiErr)
}

func encodeMCPCursor(action, targetID string, page mcpMessagePage, lastTS int64, lastID string) (string, error) {
	return dirextalkmcp.EncodeCursor(action, targetID, page, lastTS, lastID)
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

func mcpPagePostRecords(records []channelPostRecord, page mcpMessagePage) ([]channelPostRecord, bool) {
	filtered := make([]channelPostRecord, 0, len(records))
	for _, record := range records {
		if mcpPageIncludes(record.OriginServerTS, record.PostID, page) {
			filtered = append(filtered, record)
		}
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if filtered[i].OriginServerTS == filtered[j].OriginServerTS {
			return filtered[i].PostID > filtered[j].PostID
		}
		return filtered[i].OriginServerTS > filtered[j].OriginServerTS
	})
	hasMore := len(filtered) > page.Limit
	if hasMore {
		filtered = filtered[:page.Limit]
	}
	return filtered, hasMore
}

func mcpPageCommentRecords(records []channelCommentRecord, page mcpMessagePage) ([]channelCommentRecord, bool) {
	filtered := make([]channelCommentRecord, 0, len(records))
	for _, record := range records {
		if mcpPageIncludes(record.OriginServerTS, record.CommentID, page) {
			filtered = append(filtered, record)
		}
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if filtered[i].OriginServerTS == filtered[j].OriginServerTS {
			return filtered[i].CommentID > filtered[j].CommentID
		}
		return filtered[i].OriginServerTS > filtered[j].OriginServerTS
	})
	hasMore := len(filtered) > page.Limit
	if hasMore {
		filtered = filtered[:page.Limit]
	}
	return filtered, hasMore
}

func (s *Service) mcpChannelPostPage(ctx context.Context, channelID string, page mcpMessagePage) ([]channelPostRecord, bool, error) {
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if s.store != nil {
		posts, hasMore, err := s.store.ListChannelPostsPage(ctx, channelID, page.FromTS, page.SnapshotTS, page.CursorTS, page.CursorID, page.Limit)
		if err != nil {
			return nil, false, err
		}
		s.enrichChannelPosts(ctx, posts, ownerMXID)
		return posts, hasMore, nil
	}
	s.mu.Lock()
	posts := make([]channelPostRecord, 0, len(s.posts))
	for _, post := range s.posts {
		if channelID == "" || post.ChannelID == channelID {
			posts = append(posts, post)
		}
	}
	s.mu.Unlock()
	s.enrichChannelPosts(ctx, posts, ownerMXID)
	paged, hasMore := mcpPagePostRecords(posts, page)
	return paged, hasMore, nil
}

func (s *Service) mcpChannelCommentPage(ctx context.Context, postID string, page mcpMessagePage) ([]channelCommentRecord, bool, error) {
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if s.store != nil {
		comments, hasMore, err := s.store.ListChannelCommentsPage(ctx, postID, page.FromTS, page.SnapshotTS, page.CursorTS, page.CursorID, page.Limit)
		if err != nil {
			return nil, false, err
		}
		s.enrichChannelComments(ctx, comments, ownerMXID)
		return comments, hasMore, nil
	}
	s.mu.Lock()
	comments := make([]channelCommentRecord, 0, len(s.comments))
	for _, comment := range s.comments {
		if postID == "" || comment.PostID == postID {
			comments = append(comments, comment)
		}
	}
	s.mu.Unlock()
	s.enrichChannelComments(ctx, comments, ownerMXID)
	paged, hasMore := mcpPageCommentRecords(comments, page)
	return paged, hasMore, nil
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
	var favorites []favoriteRecord
	if s.store != nil {
		stored, err := s.store.ListFavorites(ctx, "")
		if err != nil {
			return 0, false
		}
		favorites = stored
	} else {
		s.mu.Lock()
		favorites = make([]favoriteRecord, 0, len(s.favorites))
		for _, favorite := range s.favorites {
			favorites = append(favorites, favorite)
		}
		s.mu.Unlock()
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
