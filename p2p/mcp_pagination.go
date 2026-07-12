package p2p

import (
	"context"
	"sort"
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

func mcpPagePostRecords(records []channelPostRecord, page mcpMessagePage) ([]channelPostRecord, bool) {
	return mcpPageRecords(records, page, func(record channelPostRecord) (int64, string) {
		return record.OriginServerTS, record.PostID
	})
}

func mcpPageCommentRecords(records []channelCommentRecord, page mcpMessagePage) ([]channelCommentRecord, bool) {
	return mcpPageRecords(records, page, func(record channelCommentRecord) (int64, string) {
		return record.OriginServerTS, record.CommentID
	})
}

func mcpPageRecords[T any](records []T, page mcpMessagePage, key func(T) (int64, string)) ([]T, bool) {
	filtered := make([]T, 0, len(records))
	for _, record := range records {
		ts, id := key(record)
		if mcpPageIncludes(ts, id, page) {
			filtered = append(filtered, record)
		}
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		iTS, iID := key(filtered[i])
		jTS, jID := key(filtered[j])
		if iTS == jTS {
			return iID > jID
		}
		return iTS > jTS
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
	if store := s.channelContentStore(); store != nil {
		storedPosts, hasMore, err := store.ListChannelPostsPage(ctx, channelID, page.FromTS, page.SnapshotTS, page.CursorTS, page.CursorID, page.Limit)
		if err != nil {
			return nil, false, err
		}
		posts := channelPostRecordsFromStorage(storedPosts)
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
	if store := s.channelContentStore(); store != nil {
		storedComments, hasMore, err := store.ListChannelCommentsPage(ctx, postID, page.FromTS, page.SnapshotTS, page.CursorTS, page.CursorID, page.Limit)
		if err != nil {
			return nil, false, err
		}
		comments := channelCommentRecordsFromStorage(storedComments)
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
