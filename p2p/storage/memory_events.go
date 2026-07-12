package storage

import (
	"context"
	"sort"
)

func (s *MemoryStore) InsertEvent(ctx context.Context, event p2pEvent) (bool, error) {
	event = cloneEvent(event)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.eventSeq[event.Seq]; exists {
		return false, nil
	}
	if event.DedupeKey != "" {
		if _, exists := s.eventDedupe[event.DedupeKey]; exists {
			return false, nil
		}
		s.eventDedupe[event.DedupeKey] = event.Seq
	}
	s.eventSeq[event.Seq] = struct{}{}
	s.events = append(s.events, event)
	return true, nil
}

func (s *MemoryStore) ListEvents(ctx context.Context, since int64, limit int) ([]p2pEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	s.mu.RLock()
	events := make([]p2pEvent, 0, min(limit, len(s.events)))
	for _, event := range s.events {
		if event.Seq <= since {
			continue
		}
		events = append(events, cloneEvent(event))
		if len(events) >= limit {
			break
		}
	}
	s.mu.RUnlock()
	return events, nil
}

func (s *MemoryStore) EventBounds(ctx context.Context) (eventBounds, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var bounds eventBounds
	for i, event := range s.events {
		if i == 0 || event.Seq < bounds.MinSeq {
			bounds.MinSeq = event.Seq
		}
		if i == 0 || event.Seq > bounds.MaxSeq {
			bounds.MaxSeq = event.Seq
		}
		bounds.Count++
	}
	return bounds, nil
}

func (s *MemoryStore) PruneEventsBefore(ctx context.Context, beforeSeq int64) (int64, error) {
	if beforeSeq <= 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.events[:0]
	var deleted int64
	for _, event := range s.events {
		if event.Seq < beforeSeq {
			delete(s.eventSeq, event.Seq)
			if event.DedupeKey != "" {
				delete(s.eventDedupe, event.DedupeKey)
			}
			deleted++
			continue
		}
		kept = append(kept, event)
	}
	s.events = kept
	return deleted, nil
}

func (s *MemoryStore) PruneEventsToMaxRows(ctx context.Context, maxRows int64) (int64, error) {
	if maxRows <= 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if int64(len(s.events)) <= maxRows {
		return 0, nil
	}
	events := append([]p2pEvent(nil), s.events...)
	sort.SliceStable(events, func(i, j int) bool { return events[i].Seq < events[j].Seq })
	deleted := int64(len(events)) - maxRows
	events = append([]p2pEvent(nil), events[len(events)-int(maxRows):]...)
	s.events = events
	s.rebuildEventIndexesLocked()
	return deleted, nil
}

func (s *MemoryStore) rebuildEventIndexesLocked() {
	s.eventSeq = make(map[int64]struct{}, len(s.events))
	s.eventDedupe = make(map[string]int64, len(s.events))
	for _, event := range s.events {
		s.eventSeq[event.Seq] = struct{}{}
		if event.DedupeKey != "" {
			s.eventDedupe[event.DedupeKey] = event.Seq
		}
	}
}
