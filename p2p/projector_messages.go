package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	channelsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/channels"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
)

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
	return true, nil
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
	if s.channelContentModule == nil {
		return errors.New("channel content module is not configured")
	}
	return s.channelContentModule.ProjectPost(ctx, channelsmodule.ProjectionEvent{
		RoomID:         event.RoomID().String(),
		EventID:        event.EventID(),
		SenderMXID:     string(event.SenderID()),
		OriginServerTS: int64(event.OriginServerTS()),
		Content:        content,
		Body:           body,
		MessageType:    msgType,
	})
}

func (s *Service) projectChannelComment(ctx context.Context, event *types.HeaderedEvent, content map[string]any, body, msgType string) error {
	if s.channelContentModule == nil {
		return errors.New("channel content module is not configured")
	}
	return s.channelContentModule.ProjectComment(ctx, channelsmodule.ProjectionEvent{
		RoomID:         event.RoomID().String(),
		EventID:        event.EventID(),
		SenderMXID:     string(event.SenderID()),
		OriginServerTS: int64(event.OriginServerTS()),
		Content:        content,
		Body:           body,
		MessageType:    msgType,
	})
}
