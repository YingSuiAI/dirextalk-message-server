package p2p

import (
	"context"
	"sort"

	"github.com/YingSuiAI/direxio-message-server/p2p/matrixhistory"
)

const maxChannelContentBackfillEvents = 5000

type channelContentReader interface {
	ListChannelContent(ctx context.Context, roomID string, limit int) ([]matrixhistory.Event, error)
}

func (s *Service) backfillJoinedPostChannelContent(ctx context.Context, roomID, channelID string) error {
	if roomID == "" {
		return nil
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return err
	}
	if !ok || trimString(ch.ChannelType) != "post" {
		return nil
	}
	return s.backfillJoinedChannelContent(ctx, roomID, fallbackString(channelID, ch.ChannelID))
}

func (s *Service) backfillJoinedChannelContent(ctx context.Context, roomID, channelID string) error {
	if roomID == "" {
		return nil
	}
	s.mu.Lock()
	reader, _ := s.matrixMessages.(channelContentReader)
	s.mu.Unlock()
	if reader == nil {
		return nil
	}
	events, err := reader.ListChannelContent(ctx, roomID, maxChannelContentBackfillEvents)
	if err != nil {
		return err
	}
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].OriginServerTS != events[j].OriginServerTS {
			return events[i].OriginServerTS < events[j].OriginServerTS
		}
		return channelContentBackfillWeight(events[i]) < channelContentBackfillWeight(events[j])
	})
	for _, event := range events {
		if event.RoomID == "" {
			event.RoomID = roomID
		}
		if event.Content == nil {
			event.Content = map[string]any{}
		}
		if trimString(event.Content["channel_id"]) == "" && channelID != "" {
			event.Content["channel_id"] = channelID
		}
		meta := eventProjectionMeta{
			RoomID:         event.RoomID,
			EventID:        event.EventID,
			SenderMXID:     event.Sender,
			OriginServerTS: event.OriginServerTS,
		}
		body := trimString(event.Content["body"])
		msgType := fallbackString(trimString(event.Content["client_type"]), trimString(event.Content["msgtype"]))
		if msgType == "" {
			msgType = "text"
		}
		switch event.Type {
		case "m.room.message":
			switch trimString(event.Content["p2p_kind"]) {
			case "channel_post":
				if err := s.projectChannelPostContent(ctx, meta, event.Content, body, msgType); err != nil {
					return err
				}
			case "channel_comment":
				if err := s.projectChannelCommentContent(ctx, meta, event.Content, body, msgType); err != nil {
					return err
				}
			}
		case "m.reaction":
			if err := s.projectReactionContent(ctx, meta, event.Content); err != nil {
				return err
			}
		}
	}
	return nil
}

func channelContentBackfillWeight(event matrixhistory.Event) int {
	if event.Type == "m.reaction" {
		return 2
	}
	switch trimString(event.Content["p2p_kind"]) {
	case "channel_post":
		return 0
	case "channel_comment":
		return 1
	default:
		return 3
	}
}
