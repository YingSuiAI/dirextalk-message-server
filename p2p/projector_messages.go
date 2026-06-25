package p2p

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/YingSuiAI/direxio-message-server/roomserver/types"
)

type eventProjectionMeta struct {
	RoomID         string
	EventID        string
	SenderMXID     string
	OriginServerTS int64
}

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
	if err := s.projectConversationActivity(ctx, event, body, msgType); err != nil {
		return err
	}
	switch trimString(content["p2p_kind"]) {
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
	if contentHasAgentGatewayMarker(content) {
		return true, nil
	}
	body := trimString(content["body"])
	msgType := fallbackString(trimString(content["client_type"]), trimString(content["msgtype"]))
	if msgType == "" {
		msgType = "m.text"
	}
	return true, s.appendP2PEvent(ctx, p2pEvent{
		Type:    AgentRoomMessageEventType,
		RoomID:  roomID,
		EventID: event.EventID(),
		Payload: map[string]any{
			"room_id":          roomID,
			"event_id":         event.EventID(),
			"sender_mxid":      string(event.SenderID()),
			"body":             body,
			"msgtype":          msgType,
			"origin_server_ts": int64(event.OriginServerTS()),
		},
	})
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

func contentHasAgentGatewayMarker(content map[string]any) bool {
	return boolParam(content[AgentGatewayContentKey]) || trimString(content[AgentGatewaySourceContentKey]) != ""
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
