package p2p

import (
	"context"
	"errors"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/projection"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
)

func (s *Service) removeProjectedEvent(ctx context.Context, eventID string) error {
	if eventID == "" {
		return nil
	}
	if s.channelContentModule == nil {
		return errors.New("channel content module is not configured")
	}
	removed, err := s.channelContentModule.RemoveProjectedEvent(ctx, eventID)
	if err != nil {
		return err
	}
	if !removed {
		return nil
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:      "room.redaction.projected",
		EventID:   eventID,
		DedupeKey: projectedEventDedupeKey("room.redaction.projected", eventID, ""),
		Payload:   map[string]any{"redacted_event_id": eventID},
	})
}

func eventTime(event *types.HeaderedEvent) time.Time {
	return projection.EventTime(event)
}
