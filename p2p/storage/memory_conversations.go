package storage

import (
	"context"
	"fmt"
	"sort"
)

func (s *MemoryStore) UpsertConversation(ctx context.Context, record conversationRecord) error {
	record = normalizeConversationRecord(record)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.conversations {
		if existing.MatrixRoomID == record.MatrixRoomID && existing.Kind != record.Kind {
			return fmt.Errorf("conversation kind conflict for room %s: existing %s, incoming %s", record.MatrixRoomID, existing.Kind, record.Kind)
		}
	}
	if existing, ok := s.conversations[record.ConversationID]; ok {
		record = mergeMemoryConversationUpdate(existing, record)
	}
	s.conversations[record.ConversationID] = record
	return nil
}

func mergeMemoryConversationUpdate(existing, incoming conversationRecord) conversationRecord {
	if existing.CreatedAt > 0 {
		incoming.CreatedAt = existing.CreatedAt
	}
	if incoming.LastEventID == "" {
		incoming.LastEventID = existing.LastEventID
	}
	if incoming.LastMessage == "" {
		incoming.LastMessage = existing.LastMessage
	}
	if incoming.LastActivityAt <= 0 {
		incoming.LastActivityAt = existing.LastActivityAt
	}
	if existing.CreatedByMXID != "" {
		incoming.CreatedByMXID = existing.CreatedByMXID
	}
	if incoming.PeerMXID == "" {
		incoming.PeerMXID = existing.PeerMXID
	}
	if incoming.Title == "" {
		incoming.Title = existing.Title
	}
	if incoming.AvatarURL == "" {
		incoming.AvatarURL = existing.AvatarURL
	}
	return incoming
}

func (s *MemoryStore) SetConversationCreator(_ context.Context, matrixRoomID, creatorMXID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, record := range s.conversations {
		if record.MatrixRoomID != matrixRoomID {
			continue
		}
		record.CreatedByMXID = creatorMXID
		s.conversations[id] = record
		return nil
	}
	return fmt.Errorf("conversation not found for room %s", matrixRoomID)
}

func (s *MemoryStore) GetConversationByID(ctx context.Context, conversationID string) (conversationRecord, bool, error) {
	s.mu.RLock()
	record, ok := s.conversations[conversationID]
	s.mu.RUnlock()
	return record, ok, nil
}

func (s *MemoryStore) GetConversationByRoomID(ctx context.Context, matrixRoomID string) (conversationRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, record := range s.conversations {
		if record.MatrixRoomID == matrixRoomID {
			return record, true, nil
		}
	}
	return conversationRecord{}, false, nil
}

func (s *MemoryStore) ListConversations(ctx context.Context) ([]conversationRecord, error) {
	s.mu.RLock()
	records := make([]conversationRecord, 0, len(s.conversations))
	for _, record := range s.conversations {
		if record.Lifecycle != conversationLifecycleDeleted {
			records = append(records, record)
		}
	}
	s.mu.RUnlock()
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].LastActivityAt == records[j].LastActivityAt {
			if records[i].UpdatedAt == records[j].UpdatedAt {
				return records[i].ConversationID < records[j].ConversationID
			}
			return records[i].UpdatedAt > records[j].UpdatedAt
		}
		return records[i].LastActivityAt > records[j].LastActivityAt
	})
	return records, nil
}

func (s *MemoryStore) DeleteConversationByRoomID(ctx context.Context, matrixRoomID string) error {
	s.mu.Lock()
	for id, record := range s.conversations {
		if record.MatrixRoomID == matrixRoomID {
			delete(s.conversations, id)
		}
	}
	s.mu.Unlock()
	return nil
}
