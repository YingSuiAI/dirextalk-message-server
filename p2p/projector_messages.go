package p2p

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
	"github.com/sirupsen/logrus"
)

type eventProjectionMeta struct {
	RoomID         string
	EventID        string
	SenderMXID     string
	OriginServerTS int64
}

const agentRoomTurnDedupeMax = 2048

func (s *Service) projectMessage(ctx context.Context, event *types.HeaderedEvent) error {
	content := map[string]any{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		return err
	}
	if handled, err := s.projectAgentRoomMessage(ctx, event, content); handled || err != nil {
		return err
	}
	if !s.shouldProjectRoomMessage(ctx, event.RoomID().String(), content) {
		return nil
	}
	body := trimString(content["body"])
	msgType := fallbackString(trimString(content["client_type"]), trimString(content["msgtype"]))
	if msgType == "" {
		msgType = "text"
	}
	kind := trimString(content["p2p_kind"])
	if kind == "" {
		if err := s.projectConversationActivity(ctx, event, body, msgType); err != nil {
			return err
		}
	}
	switch kind {
	case "channel_post":
		return s.projectChannelPost(ctx, event, content, body, msgType)
	case "channel_comment":
		return s.projectChannelComment(ctx, event, content, body, msgType)
	default:
		return nil
	}
}

func (s *Service) projectAgentRoomMessage(ctx context.Context, event *types.HeaderedEvent, content map[string]any) (bool, error) {
	roomID := event.RoomID().String()
	if !s.isAgentRoom(roomID) {
		return false, nil
	}
	if mcpGatewayMarked(content) {
		return true, nil
	}
	body := trimString(content["body"])
	if body == "" || !agentRoomMessageTypeSupported(content) {
		return true, nil
	}
	s.mu.Lock()
	productAgent := s.productAgent
	nodeID := s.serverName
	ownerMXID := s.ownerMXID
	agentMXID := s.agentMXIDLocked()
	s.mu.Unlock()
	senderMXID := strings.TrimSpace(string(event.SenderID()))
	if productAgent == nil || senderMXID == "" || senderMXID == agentMXID || senderMXID != strings.TrimSpace(ownerMXID) {
		return true, nil
	}
	if !s.claimAgentRoomTurn(event.EventID()) {
		return true, nil
	}
	s.dispatchProductAgentRoomMessage(productAgent, ProductAgentMessageRequest{
		NodeID:           nodeID,
		RoomID:           roomID,
		ConversationType: "agent",
		SenderID:         senderMXID,
		SenderKind:       "user",
		Content:          body,
		AgentConfig:      s.agentPluginConfigSnapshot(ctx),
	})
	return true, nil
}

/**
 * Function: Claims one Matrix event for a product-agent turn.
 * Inputs:
 * - eventID: Matrix event id for the user's agent-room message.
 * Output:
 * - True only for the first projection of this event in the current service process.
 * Side effects:
 * - Records the event id before the async product-agent call starts, so projector
 *   replays or concurrent duplicate projections cannot send multiple replies.
 * Errors:
 * - Empty event ids are treated as unclaimable and allowed through.
 */
func (s *Service) claimAgentRoomTurn(eventID string) bool {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return true
	}
	now := time.Now().UTC().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.agentRoomTurns == nil {
		s.agentRoomTurns = map[string]int64{}
	}
	if _, ok := s.agentRoomTurns[eventID]; ok {
		return false
	}
	s.agentRoomTurns[eventID] = now
	if len(s.agentRoomTurns) > agentRoomTurnDedupeMax {
		s.pruneAgentRoomTurnsLocked(agentRoomTurnDedupeMax / 2)
	}
	return true
}

func (s *Service) pruneAgentRoomTurnsLocked(target int) {
	for len(s.agentRoomTurns) > target {
		oldestEventID := ""
		var oldest int64
		for eventID, seenAt := range s.agentRoomTurns {
			if oldestEventID == "" || seenAt < oldest {
				oldestEventID = eventID
				oldest = seenAt
			}
		}
		if oldestEventID == "" {
			return
		}
		delete(s.agentRoomTurns, oldestEventID)
	}
}

/**
 * Function: Runs a product-agent turn outside the roomserver projection path.
 * Inputs:
 * - productAgent: Configured product-agent bridge client.
 * - req: Fully normalized agent-room request, including any plugin config snapshot.
 * Output:
 * - None.
 * Side effects:
 * - Starts a goroutine that may call product-agent and send one Matrix reply.
 * Errors:
 * - Logs product-agent or Matrix send failures and intentionally avoids
 *   returning them to the projector, so roomserver event consumption is not
 *   retried for transient AI/runtime failures.
 */
func (s *Service) dispatchProductAgentRoomMessage(productAgent ProductAgentClient, req ProductAgentMessageRequest) {
	go func() {
		agentCtx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
		defer cancel()
		response, err := productAgent.HandleMessage(agentCtx, req)
		if err != nil {
			logrus.WithError(err).Warn("Product agent bridge failed to handle agent-room message")
			return
		}
		if response.Ignored {
			return
		}
		reply, structuredContent := productAgentReplyMatrixPayload(response)
		if reply == "" && len(structuredContent) == 0 {
			return
		}
		sendCtx, sendCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer sendCancel()
		if err := s.sendProductAgentRoomReply(sendCtx, req.RoomID, reply, structuredContent); err != nil {
			logrus.WithError(err).Warn("Product agent bridge failed to send agent-room reply")
		}
	}()
}

/**
 * Function: Returns whether an agent-room Matrix message should be bridged.
 * Inputs:
 * - content: Matrix m.room.message content map.
 * Output:
 * - True for ordinary text messages; false for media/custom messages.
 * Side effects:
 * - None.
 * Errors:
 * - None.
 */
func agentRoomMessageTypeSupported(content map[string]any) bool {
	msgType := strings.ToLower(fallbackString(trimString(content["msgtype"]), trimString(content["client_type"])))
	return msgType == "" || msgType == "m.text" || msgType == "text"
}

/**
 * Function: Reads the saved official Agent plugin config for a product-agent turn.
 * Inputs:
 * - ctx: Request context used for store reads.
 * Output:
 * - A shallow clone of `io.dirextalk.agent` config, or nil when unavailable.
 * Side effects:
 * - Reads from the configured plugin store.
 * Errors:
 * - Store errors are logged and treated as missing config so Matrix projection
 *   does not retry and duplicate user messages.
 */
func (s *Service) agentPluginConfigSnapshot(ctx context.Context) map[string]any {
	plugin, ok, err := s.getPlugin(ctx, "io.dirextalk.agent")
	if err != nil {
		logrus.WithError(err).Warn("Product agent bridge could not read official Agent plugin config")
		return nil
	}
	if !ok || plugin.Config == nil {
		return nil
	}
	return cloneAnyMap(plugin.Config)
}

/**
 * Function: Sends product-agent output back into the Matrix agent room.
 * Inputs:
 * - ctx: Request context for the Matrix transport write.
 * - roomID: Real Matrix agent_room_id.
 * - body: Plain text fallback shown by Matrix clients that do not understand cards.
 * - structuredContent: Optional card fields for Direxio clients.
 * Output:
 * - Error from Matrix transport, if sending fails.
 * Side effects:
 * - Writes one gateway-marked Matrix m.room.message as local @agent:<server>.
 * Errors:
 * - Returns transport errors; callers log and avoid projection retry loops.
 */
func (s *Service) sendProductAgentRoomReply(ctx context.Context, roomID, body string, structuredContent map[string]any) error {
	body = strings.TrimSpace(body)
	if body == "" && len(structuredContent) == 0 {
		return nil
	}
	s.mu.Lock()
	transport := s.transport
	agentMXID := s.agentMXIDLocked()
	s.mu.Unlock()
	if transport == nil {
		return nil
	}
	content := map[string]any{
		"msgtype":                    "m.text",
		"body":                       body,
		AgentGatewayContentKey:       true,
		AgentGatewaySourceContentKey: productAgentGatewaySource,
	}
	for key, value := range structuredContent {
		content[key] = value
	}
	_, err := transport.SendMessage(ctx, SendMessageRequest{
		SenderMXID:  agentMXID,
		RoomID:      strings.TrimSpace(roomID),
		MessageType: "text",
		Timestamp:   time.Now().UTC(),
		Content:     content,
	})
	return err
}

func (s *Service) isAgentRoom(roomID string) bool {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return roomID == strings.TrimSpace(s.agentRoomID)
}

func (s *Service) projectConversationActivity(ctx context.Context, event *types.HeaderedEvent, body, msgType string) error {
	record, ok, err := s.getConversation(ctx, "", event.RoomID().String())
	if err != nil || !ok {
		return err
	}
	record.LastEventID = event.EventID()
	record.LastActivityAt = int64(event.OriginServerTS())
	record.LastMessage = conversationActivityPreview(body, msgType)
	record.UpdatedAt = time.Now().UTC().UnixMilli()
	return s.saveConversation(ctx, record)
}

func conversationActivityPreview(body, msgType string) string {
	body = strings.TrimSpace(body)
	if body != "" {
		return body
	}
	switch strings.ToLower(strings.TrimSpace(msgType)) {
	case "m.image", "image":
		return "图片"
	case "m.video", "video":
		return "视频"
	case "m.audio", "audio":
		return "语音"
	case "m.file", "file":
		return "文件"
	default:
		return ""
	}
}

func (s *Service) projectChannelPost(ctx context.Context, event *types.HeaderedEvent, content map[string]any, body, msgType string) error {
	return s.projectChannelPostContent(ctx, eventProjectionMeta{
		RoomID:         event.RoomID().String(),
		EventID:        event.EventID(),
		SenderMXID:     string(event.SenderID()),
		OriginServerTS: int64(event.OriginServerTS()),
	}, content, body, msgType)
}

func (s *Service) projectChannelPostContent(ctx context.Context, meta eventProjectionMeta, content map[string]any, body, msgType string) error {
	postID := trimString(content["post_id"])
	if postID == "" {
		postID = "post_" + strings.TrimPrefix(meta.EventID, "$")
	}
	post := channelPostRecord{
		PostID:         postID,
		ChannelID:      trimString(content["channel_id"]),
		RoomID:         meta.RoomID,
		EventID:        meta.EventID,
		AuthorMXID:     meta.SenderMXID,
		AuthorName:     trimString(content["sender_name"]),
		Body:           body,
		MessageType:    msgType,
		MediaJSON:      trimString(content["media_json"]),
		OriginServerTS: meta.OriginServerTS,
		CommentCount:   0,
	}
	s.mu.Lock()
	upserted := false
	for i := range s.posts {
		if s.posts[i].PostID == post.PostID {
			s.posts[i] = post
			upserted = true
			break
		}
	}
	if !upserted {
		s.posts = append(s.posts, post)
	}
	s.mu.Unlock()
	if s.store != nil {
		return s.store.InsertChannelPost(ctx, post)
	}
	return nil
}

func (s *Service) projectChannelComment(ctx context.Context, event *types.HeaderedEvent, content map[string]any, body, msgType string) error {
	return s.projectChannelCommentContent(ctx, eventProjectionMeta{
		RoomID:         event.RoomID().String(),
		EventID:        event.EventID(),
		SenderMXID:     string(event.SenderID()),
		OriginServerTS: int64(event.OriginServerTS()),
	}, content, body, msgType)
}

func (s *Service) projectChannelCommentContent(ctx context.Context, meta eventProjectionMeta, content map[string]any, body, msgType string) error {
	commentID := trimString(content["comment_id"])
	if commentID == "" {
		commentID = "comment_" + strings.TrimPrefix(meta.EventID, "$")
	}
	mentionsJSON := "[]"
	if rawMentionsJSON, ok := content["mentions_json"]; ok {
		var err error
		mentionsJSON, err = jsonArrayStringParam(rawMentionsJSON)
		if err != nil {
			mentionsJSON = "[]"
		}
	} else if rawMentions, ok := content["mentions"]; ok {
		var err error
		mentionsJSON, err = jsonArrayStringParam(rawMentions)
		if err != nil {
			mentionsJSON = "[]"
		}
	}
	comment := channelCommentRecord{
		CommentID:         commentID,
		PostID:            trimString(content["post_id"]),
		ChannelID:         trimString(content["channel_id"]),
		EventID:           meta.EventID,
		AuthorMXID:        meta.SenderMXID,
		AuthorName:        trimString(content["sender_name"]),
		Body:              body,
		MessageType:       msgType,
		MediaJSON:         trimString(content["media_json"]),
		ReplyToCommentID:  trimString(content["reply_to_comment_id"]),
		ReplyToAuthorMXID: trimString(content["reply_to_author_mxid"]),
		MentionsJSON:      mentionsJSON,
		OriginServerTS:    meta.OriginServerTS,
		ReactionCount:     0,
		ReactedByMe:       false,
	}
	s.mu.Lock()
	upserted := false
	for i := range s.comments {
		if s.comments[i].CommentID == comment.CommentID {
			s.comments[i] = comment
			upserted = true
			break
		}
	}
	if !upserted {
		s.comments = append(s.comments, comment)
	}
	s.mu.Unlock()
	if s.store != nil {
		return s.store.InsertChannelComment(ctx, comment)
	}
	return nil
}
