package p2p

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/YingSuiAI/direxio-message-server/p2p/mcp"
)

const defaultMCPLimit = mcp.DefaultLimit
const maxMCPLimit = mcp.MaxLimit

type mcpRoomSummary = mcp.RoomSummary
type mcpMessageSummary = mcp.MessageSummary
type mcpPostSummary = mcp.PostSummary
type mcpCommentSummary = mcp.CommentSummary
type matrixMessageReader = mcp.MessageReader

func mcpLimit(params map[string]any) int {
	limit := int(int64Param(params["limit"]))
	if limit <= 0 {
		return defaultMCPLimit
	}
	if limit > maxMCPLimit {
		return maxMCPLimit
	}
	return limit
}

func inMCPTimeRange(ts, fromTS, toTS int64) bool {
	return mcp.InTimeRange(ts, fromTS, toTS)
}

func (s *Service) mcpRoomsSearch(ctx context.Context, params map[string]any) (any, *apiError) {
	query := strings.ToLower(trimString(params["query"]))
	kind := strings.ToLower(trimString(params["type"]))
	if kind == "" {
		kind = "all"
	}
	if kind != "all" && kind != "contact" && kind != "group" && kind != "channel" {
		return nil, badRequest("type must be contact, group, channel, or all")
	}
	records, err := s.listConversations(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	rooms := make([]mcpRoomSummary, 0, len(records))
	for _, record := range records {
		view, err := s.conversationView(ctx, record)
		if err != nil {
			return nil, internalError(err)
		}
		summary := mcpRoomSummaryFromConversation(view)
		if summary.RoomID == "" {
			continue
		}
		if kind != "all" && summary.Type != kind {
			continue
		}
		if query != "" && !mcpRoomMatches(summary, query) {
			continue
		}
		rooms = append(rooms, summary)
	}
	sort.SliceStable(rooms, func(i, j int) bool {
		if rooms[i].LastTS == rooms[j].LastTS {
			return rooms[i].Name < rooms[j].Name
		}
		return rooms[i].LastTS > rooms[j].LastTS
	})
	limit := mcpLimit(params)
	if len(rooms) > limit {
		rooms = rooms[:limit]
	}
	return map[string]any{"rooms": rooms}, nil
}

func (s *Service) mcpMessagesSend(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	msg := fallbackString(trimString(params["msg"]), trimString(params["text"]))
	if roomID == "" {
		return nil, badRequest("room_id is required")
	}
	if msg == "" {
		return nil, badRequest("msg is required")
	}
	gatewayMarked := mcpGatewayMarked(params)
	s.mu.Lock()
	agentRoomID := strings.TrimSpace(s.agentRoomID)
	senderMXID := s.ownerMXID
	if gatewayMarked {
		senderMXID = s.agentMXIDLocked()
	}
	s.mu.Unlock()
	if !gatewayMarked && agentRoomID != "" && roomID == agentRoomID {
		return nil, badRequest("mcp.messages.send cannot send owner messages to the agent room; use the Matrix agent gateway")
	}
	if s.transport == nil {
		return nil, internalError(errors.New("matrix transport is unavailable"))
	}
	content := map[string]any{
		"msgtype": "m.text",
		"body":    msg,
	}
	if gatewayMarked {
		content[AgentGatewayContentKey] = true
		if source := trimString(params["gateway_source"]); source != "" {
			content[AgentGatewaySourceContentKey] = source
		} else {
			content[AgentGatewaySourceContentKey] = "agent_gateway"
		}
	}
	res, err := s.transport.SendMessage(ctx, SendMessageRequest{
		SenderMXID:  senderMXID,
		RoomID:      roomID,
		MessageType: "text",
		Timestamp:   time.Now().UTC(),
		Content:     content,
	})
	if err != nil {
		return nil, transportWriteError(err)
	}
	return map[string]any{
		"ok":       true,
		"room_id":  roomID,
		"event_id": res.EventID,
		"ts":       res.OriginServerTS,
	}, nil
}

func mcpGatewayMarked(params map[string]any) bool {
	return boolParam(params["agent_gateway"]) ||
		boolParam(params["gateway"]) ||
		boolParam(params[AgentGatewayContentKey]) ||
		trimString(params["gateway_source"]) != ""
}

func (s *Service) mcpMessagesList(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	if roomID == "" {
		return nil, badRequest("room_id is required")
	}
	fromTS := int64Param(params["from_ts"])
	toTS := int64Param(params["to_ts"])
	if fromTS > 0 && toTS > 0 && fromTS > toTS {
		return nil, badRequest("from_ts must be less than or equal to to_ts")
	}
	limit := mcpLimit(params)
	s.mu.Lock()
	reader := s.matrixMessages
	s.mu.Unlock()
	if reader == nil {
		return nil, internalError(errors.New("MCP message reader is unavailable"))
	}
	messages, err := reader.ListOrdinaryMessages(ctx, roomID, fromTS, toTS, limit)
	if err != nil {
		return nil, internalError(err)
	}
	name, apiErr := s.mcpMessagesRoomName(ctx, roomID)
	if apiErr != nil {
		return nil, apiErr
	}
	messages = s.normalizeMCPMessageSenders(roomID, messages)
	return map[string]any{"room_id": roomID, "name": name, "messages": messages}, nil
}

func (s *Service) mcpMessagesRoomName(ctx context.Context, roomID string) (string, *apiError) {
	s.mu.Lock()
	agentRoomID := strings.TrimSpace(s.agentRoomID)
	s.mu.Unlock()
	if roomID != "" && roomID == agentRoomID {
		return agentRoomName, nil
	}
	if record, ok, err := s.getConversation(ctx, "", roomID); err != nil {
		return "", internalError(err)
	} else if ok {
		view, err := s.conversationView(ctx, record)
		if err != nil {
			return "", internalError(err)
		}
		return fallbackString(view.Title, roomID), nil
	}
	return roomID, nil
}

func (s *Service) normalizeMCPMessageSenders(roomID string, messages []mcpMessageSummary) []mcpMessageSummary {
	s.mu.Lock()
	agentRoomID := strings.TrimSpace(s.agentRoomID)
	agentMXID := s.agentMXIDLocked()
	agentDisplayName := s.agentDisplayNameLocked()
	s.mu.Unlock()
	if roomID == "" || roomID != agentRoomID {
		return messages
	}
	for i := range messages {
		if strings.EqualFold(strings.TrimSpace(messages[i].SenderMXID), agentMXID) {
			messages[i].Sender = agentDisplayName
		}
	}
	return messages
}

func (s *Service) mcpChannelPostsList(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	if roomID == "" {
		return nil, badRequest("room_id is required")
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, "", roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(http.StatusNotFound, "channel not found")
	}
	postsAny := s.channelPosts(ctx, map[string]any{"channel_id": ch.ChannelID})
	rawPosts := postsAny.(map[string]any)["posts"].([]channelPostRecord)
	fromTS := int64Param(params["from_ts"])
	toTS := int64Param(params["to_ts"])
	if fromTS > 0 && toTS > 0 && fromTS > toTS {
		return nil, badRequest("from_ts must be less than or equal to to_ts")
	}
	limit := mcpLimit(params)
	posts := make([]mcpPostSummary, 0, len(rawPosts))
	for _, post := range rawPosts {
		if !inMCPTimeRange(post.OriginServerTS, fromTS, toTS) {
			continue
		}
		posts = append(posts, mcpPostSummary{
			PostID:       post.PostID,
			TS:           post.OriginServerTS,
			Sender:       fallbackString(post.AuthorName, post.AuthorMXID),
			Msg:          post.Body,
			CommentCount: post.CommentCount,
		})
		if len(posts) >= limit {
			break
		}
	}
	return map[string]any{"room_id": ch.RoomID, "name": ch.Name, "posts": posts}, nil
}

func (s *Service) mcpChannelCommentsList(ctx context.Context, params map[string]any) (any, *apiError) {
	postID := trimString(params["post_id"])
	if postID == "" {
		return nil, badRequest("post_id is required")
	}
	if _, ok, err := s.channelPostByID(ctx, postID, ""); err != nil {
		return nil, internalError(err)
	} else if !ok {
		return nil, statusError(http.StatusNotFound, "post not found")
	}
	commentsAny := s.channelComments(ctx, map[string]any{"post_id": postID})
	rawComments := commentsAny.(map[string]any)["comments"].([]channelCommentRecord)
	fromTS := int64Param(params["from_ts"])
	toTS := int64Param(params["to_ts"])
	if fromTS > 0 && toTS > 0 && fromTS > toTS {
		return nil, badRequest("from_ts must be less than or equal to to_ts")
	}
	limit := mcpLimit(params)
	comments := make([]mcpCommentSummary, 0, len(rawComments))
	for _, comment := range rawComments {
		if !inMCPTimeRange(comment.OriginServerTS, fromTS, toTS) {
			continue
		}
		comments = append(comments, mcpCommentSummary{
			CommentID: comment.CommentID,
			TS:        comment.OriginServerTS,
			Sender:    fallbackString(comment.AuthorName, comment.AuthorMXID),
			Msg:       comment.Body,
		})
		if len(comments) >= limit {
			break
		}
	}
	return map[string]any{"post_id": postID, "comments": comments}, nil
}

func (s *Service) mcpChannelCommentCreate(ctx context.Context, params map[string]any) (any, *apiError) {
	postID := trimString(params["post_id"])
	msg := fallbackString(trimString(params["msg"]), trimString(params["body"]))
	if postID == "" {
		return nil, badRequest("post_id is required")
	}
	if msg == "" {
		return nil, badRequest("msg is required")
	}
	post, ok, err := s.channelPostByID(ctx, postID, "")
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(http.StatusNotFound, "post not found")
	}
	commentAny, apiErr := s.channelComment(ctx, map[string]any{
		"channel_id":   post.ChannelID,
		"room_id":      post.RoomID,
		"post_id":      postID,
		"body":         msg,
		"message_type": "text",
	})
	if apiErr != nil {
		return nil, apiErr
	}
	comment := commentAny.(channelCommentRecord)
	return map[string]any{
		"ok":         true,
		"post_id":    comment.PostID,
		"comment_id": comment.CommentID,
		"ts":         comment.OriginServerTS,
	}, nil
}

func (s *Service) MatrixHistoryAccessToken(ctx context.Context) (string, error) {
	return s.matrixHistoryAccessToken(ctx)
}

func (s *Service) matrixHistoryAccessToken(_ context.Context) (string, error) {
	s.mu.Lock()
	token := trimString(s.accessToken)
	s.mu.Unlock()
	if token == "" {
		return "", errors.New("matrix access token is unavailable")
	}
	return token, nil
}

func mcpRoomSummaryFromConversation(view conversationView) mcpRoomSummary {
	roomType := string(view.Kind)
	if roomType == "direct" {
		roomType = "contact"
	}
	subtitle := view.PeerMXID
	if roomType == "group" || roomType == "channel" {
		subtitle = mcpFormatMemberCount(view.MemberCount)
	}
	return mcpRoomSummary{
		Type:     roomType,
		Name:     fallbackString(view.Title, view.MatrixRoomID),
		RoomID:   view.MatrixRoomID,
		Subtitle: subtitle,
		LastMsg:  view.LastMessage,
		LastTS:   view.LastActivityAt,
	}
}

func mcpRoomMatches(room mcpRoomSummary, query string) bool {
	return strings.Contains(strings.ToLower(room.Name), query) ||
		strings.Contains(strings.ToLower(room.Subtitle), query) ||
		strings.Contains(strings.ToLower(room.RoomID), query)
}

func mcpFormatMemberCount(count int64) string {
	if count <= 0 {
		return ""
	}
	return strconv.FormatInt(count, 10) + " members"
}
