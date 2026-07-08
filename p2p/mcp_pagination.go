package p2p

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/matrixhistory"
)

type mcpMessagePage = matrixhistory.Page
type mcpMessagePageResult = matrixhistory.MessagePageResult

type mcpCursorPayload struct {
	Version        int    `json:"v"`
	Action         string `json:"action"`
	TargetID       string `json:"target_id"`
	FromTimeMS     int64  `json:"from_time_ms,omitempty"`
	SnapshotTimeMS int64  `json:"snapshot_time_ms"`
	LastTimeMS     int64  `json:"last_time_ms"`
	LastID         string `json:"last_id"`
}

func mcpPageFromParams(params map[string]any, action, targetID string) (mcpMessagePage, *apiError) {
	if apiErr := rejectLegacyMCPTimeParams(params); apiErr != nil {
		return mcpMessagePage{}, apiErr
	}
	limit := mcpLimit(params)
	if cursor := trimString(params["cursor"]); cursor != "" {
		payload, apiErr := decodeMCPCursor(cursor)
		if apiErr != nil {
			return mcpMessagePage{}, apiErr
		}
		if payload.Action != action || payload.TargetID != targetID || payload.SnapshotTimeMS <= 0 || payload.LastTimeMS <= 0 || strings.TrimSpace(payload.LastID) == "" {
			return mcpMessagePage{}, badRequest("cursor is invalid for this query")
		}
		return mcpMessagePage{
			FromTS:     payload.FromTimeMS,
			SnapshotTS: payload.SnapshotTimeMS,
			CursorTS:   payload.LastTimeMS,
			CursorID:   payload.LastID,
			Limit:      limit,
		}, nil
	}
	fromTS, _, apiErr := mcpTimeParam(params, "from_time")
	if apiErr != nil {
		return mcpMessagePage{}, apiErr
	}
	toTS, hasTo, apiErr := mcpTimeParam(params, "to_time")
	if apiErr != nil {
		return mcpMessagePage{}, apiErr
	}
	if !hasTo {
		toTS = time.Now().UTC().UnixMilli()
	}
	if fromTS > 0 && fromTS > toTS {
		return mcpMessagePage{}, badRequest("from_time must be less than or equal to to_time")
	}
	return mcpMessagePage{FromTS: fromTS, SnapshotTS: toTS, Limit: limit}, nil
}

func rejectLegacyMCPTimeParams(params map[string]any) *apiError {
	if _, ok := params["from_ts"]; ok {
		return badRequest("use from_time/to_time instead of from_ts/to_ts")
	}
	if _, ok := params["to_ts"]; ok {
		return badRequest("use from_time/to_time instead of from_ts/to_ts")
	}
	return nil
}

func mcpTimeParam(params map[string]any, key string) (int64, bool, *apiError) {
	value, ok := params[key]
	if !ok {
		return 0, false, nil
	}
	text := trimString(value)
	if text == "" {
		return 0, false, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return 0, false, badRequest(key + " must be RFC3339 UTC")
	}
	if _, offset := parsed.Zone(); offset != 0 {
		return 0, false, badRequest(key + " must be RFC3339 UTC")
	}
	return parsed.UTC().UnixMilli(), true, nil
}

func decodeMCPCursor(cursor string) (mcpCursorPayload, *apiError) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(cursor))
	if err != nil {
		return mcpCursorPayload{}, badRequest("cursor is invalid")
	}
	var payload mcpCursorPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return mcpCursorPayload{}, badRequest("cursor is invalid")
	}
	if payload.Version != 1 {
		return mcpCursorPayload{}, badRequest("cursor is invalid")
	}
	return payload, nil
}

func encodeMCPCursor(action, targetID string, page mcpMessagePage, lastTS int64, lastID string) (string, error) {
	raw, err := json.Marshal(mcpCursorPayload{
		Version:        1,
		Action:         action,
		TargetID:       targetID,
		FromTimeMS:     page.FromTS,
		SnapshotTimeMS: page.SnapshotTS,
		LastTimeMS:     lastTS,
		LastID:         lastID,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func mcpAttachPagination(payload map[string]any, action, targetID string, page mcpMessagePage, hasMore bool, lastTS int64, lastID string) *apiError {
	payload["has_more"] = hasMore
	if !hasMore || lastTS <= 0 || strings.TrimSpace(lastID) == "" {
		return nil
	}
	cursor, err := encodeMCPCursor(action, targetID, page, lastTS, lastID)
	if err != nil {
		return internalError(err)
	}
	payload["next_cursor"] = cursor
	return nil
}

func mcpPageIncludes(ts int64, id string, page mcpMessagePage) bool {
	return matrixhistory.InPage(ts, id, page)
}

func mcpFormatTime(ts int64) string {
	return matrixhistory.FormatTime(ts)
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
