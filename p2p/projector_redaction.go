package p2p

import (
	"context"
	"time"

	"github.com/YingSuiAI/direxio-message-server/p2p/projection"
	"github.com/YingSuiAI/direxio-message-server/roomserver/types"
)

func (s *Service) removeProjectedEvent(ctx context.Context, eventID string) error {
	if eventID == "" {
		return nil
	}
	s.mu.Lock()
	removed := false
	posts := s.posts[:0]
	for _, post := range s.posts {
		if post.EventID != eventID {
			posts = append(posts, post)
		} else {
			removed = true
		}
	}
	s.posts = posts
	comments := s.comments[:0]
	for _, comment := range s.comments {
		if comment.EventID != eventID {
			comments = append(comments, comment)
		} else {
			removed = true
		}
	}
	s.comments = comments
	s.mu.Unlock()
	if s.store != nil {
		postRemoved, err := s.store.DeleteChannelPost(ctx, eventID)
		if err != nil {
			return err
		}
		commentRemoved, err := s.store.DeleteChannelComment(ctx, eventID)
		if err != nil {
			return err
		}
		removed = removed || postRemoved || commentRemoved
	}
	if !removed {
		return nil
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:    "room.redaction.projected",
		EventID: eventID,
		Payload: map[string]any{"redacted_event_id": eventID},
	})
}

func eventTime(event *types.HeaderedEvent) time.Time {
	return projection.EventTime(event)
}
