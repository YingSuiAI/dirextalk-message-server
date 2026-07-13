package p2p

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"time"

	matrixhistory "github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmatrix"
	channelsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/channels"
)

const maxChannelContentBackfillEvents = 5000

var channelContentBackfillRateLimitRetryDelays = []time.Duration{
	600 * time.Millisecond,
}

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
	if !ok {
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
	events, err := listJoinedChannelContentBestEffort(ctx, reader, roomID)
	if err != nil {
		return err
	}
	sort.SliceStable(events, func(i, j int) bool {
		leftWeight := channelContentBackfillWeight(events[i])
		rightWeight := channelContentBackfillWeight(events[j])
		if leftWeight != rightWeight {
			return leftWeight < rightWeight
		}
		if events[i].OriginServerTS != events[j].OriginServerTS {
			return events[i].OriginServerTS < events[j].OriginServerTS
		}
		return events[i].EventID < events[j].EventID
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
		projectionEvent := channelsmodule.ProjectionEvent{
			RoomID:         event.RoomID,
			EventID:        event.EventID,
			SenderMXID:     event.Sender,
			OriginServerTS: event.OriginServerTS,
			Content:        event.Content,
			Body:           trimString(event.Content["body"]),
			MessageType:    fallbackString(trimString(event.Content["client_type"]), trimString(event.Content["msgtype"])),
		}
		if projectionEvent.MessageType == "" {
			projectionEvent.MessageType = "text"
		}
		if s.channelContentModule == nil {
			return errors.New("channel content module is not configured")
		}
		switch event.Type {
		case "m.room.message":
			switch trimString(event.Content["p2p_kind"]) {
			case "channel_post":
				if err := s.channelContentModule.ProjectPost(ctx, projectionEvent); err != nil {
					return err
				}
			case "channel_comment":
				if err := s.channelContentModule.ProjectComment(ctx, projectionEvent); err != nil {
					return err
				}
			}
		case "m.reaction":
			if err := s.channelContentModule.ProjectReaction(ctx, projectionEvent); err != nil {
				return err
			}
		}
	}
	return nil
}

func listJoinedChannelContentBestEffort(ctx context.Context, reader channelContentReader, roomID string) ([]matrixhistory.Event, error) {
	events, err := reader.ListChannelContent(ctx, roomID, maxChannelContentBackfillEvents)
	if err == nil {
		return events, nil
	}
	if !matrixHistoryRateLimited(err) {
		return nil, nil
	}
	for _, delay := range channelContentBackfillRateLimitRetryDelays {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
		events, err = reader.ListChannelContent(ctx, roomID, maxChannelContentBackfillEvents)
		if err == nil {
			return events, nil
		}
		if !matrixHistoryRateLimited(err) {
			return nil, nil
		}
	}
	return nil, nil
}

func matrixHistoryRateLimited(err error) bool {
	var statusErr matrixhistory.StatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusTooManyRequests
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
