package storage

import "context"

func (s *MemoryStore) UpsertFavorite(ctx context.Context, favorite favoriteRecord) error {
	s.mu.Lock()
	s.favorites[favorite.ID] = favorite
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) FindFavoriteByEvent(ctx context.Context, eventID, roomID string) (favoriteRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, favorite := range s.favorites {
		if eventID == "" || favorite.EventID != eventID {
			continue
		}
		if roomID != "" && favorite.RoomID != "" && favorite.RoomID != roomID {
			continue
		}
		return favorite, true, nil
	}
	return favoriteRecord{}, false, nil
}

func (s *MemoryStore) ListFavorites(ctx context.Context, messageType string) ([]favoriteRecord, error) {
	s.mu.RLock()
	favorites := make([]favoriteRecord, 0, len(s.favorites))
	for _, favorite := range s.favorites {
		if messageType == "" || favorite.MessageType == messageType {
			favorites = append(favorites, favorite)
		}
	}
	s.mu.RUnlock()
	return favorites, nil
}

func (s *MemoryStore) DeleteFavorite(ctx context.Context, id int64) error {
	s.mu.Lock()
	delete(s.favorites, id)
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) UpsertFollow(ctx context.Context, follow followRecord) error {
	s.mu.Lock()
	s.follows[follow.Domain] = follow
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) ListFollows(ctx context.Context) ([]followRecord, error) {
	s.mu.RLock()
	follows := make([]followRecord, 0, len(s.follows))
	for _, follow := range s.follows {
		follows = append(follows, follow)
	}
	s.mu.RUnlock()
	return follows, nil
}

func (s *MemoryStore) DeleteFollow(ctx context.Context, domain string) error {
	s.mu.Lock()
	delete(s.follows, domain)
	s.mu.Unlock()
	return nil
}

func memoryReactionKey(targetType, targetID, reaction, userID string) string {
	return targetType + "|" + targetID + "|" + reaction + "|" + userID
}

func (s *MemoryStore) UpsertReaction(ctx context.Context, reaction reactionRecord) error {
	s.mu.Lock()
	s.reactions[memoryReactionKey(reaction.TargetType, reaction.TargetID, reaction.Reaction, reaction.UserID)] = reaction
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) GetReaction(ctx context.Context, targetType, targetID, reaction, userID string) (reactionRecord, bool, error) {
	s.mu.RLock()
	record, ok := s.reactions[memoryReactionKey(targetType, targetID, reaction, userID)]
	s.mu.RUnlock()
	return record, ok, nil
}

func (s *MemoryStore) CountActiveReactions(ctx context.Context, targetType, targetID, reaction string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var count int64
	for _, record := range s.reactions {
		if record.TargetType == targetType && record.TargetID == targetID && record.Reaction == reaction && record.Active {
			count++
		}
	}
	return count, nil
}

func (s *MemoryStore) ListReactions(ctx context.Context, userID string) ([]reactionRecord, error) {
	s.mu.RLock()
	reactions := make([]reactionRecord, 0, len(s.reactions))
	for _, reaction := range s.reactions {
		if reaction.UserID == userID && reaction.Active {
			reactions = append(reactions, reaction)
		}
	}
	s.mu.RUnlock()
	return reactions, nil
}
