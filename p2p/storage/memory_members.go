package storage

import (
	"context"
	"sort"
	"strings"
)

func memoryMemberKey(roomID, userID string) string {
	return roomID + "|" + userID
}

func (s *MemoryStore) UpsertMember(ctx context.Context, member memberRecord) error {
	s.mu.Lock()
	s.members[memoryMemberKey(member.RoomID, member.UserID)] = member
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) InsertMemberIfAbsent(_ context.Context, member memberRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := memoryMemberKey(member.RoomID, member.UserID)
	if _, exists := s.members[key]; exists {
		return false, nil
	}
	s.members[key] = member
	return true, nil
}

func (s *MemoryStore) CompareAndSwapMemberGeneration(
	_ context.Context,
	member memberRecord,
	expectedRequestID,
	expectedMembership string,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := memoryMemberKey(member.RoomID, member.UserID)
	current, found := s.members[key]
	if !found || current.RequestID != expectedRequestID ||
		(expectedMembership != "" && !strings.EqualFold(strings.TrimSpace(current.Membership), strings.TrimSpace(expectedMembership))) {
		return false, nil
	}
	s.members[key] = member
	return true, nil
}

func (s *MemoryStore) LookupMember(ctx context.Context, roomID, userID string) (memberRecord, bool, error) {
	s.mu.RLock()
	member, ok := s.members[memoryMemberKey(roomID, userID)]
	s.mu.RUnlock()
	return member, ok, nil
}

func (s *MemoryStore) ListMembers(ctx context.Context, roomID, channelID string) ([]memberRecord, error) {
	s.mu.RLock()
	members := make([]memberRecord, 0, len(s.members))
	for _, member := range s.members {
		if roomID != "" && member.RoomID != roomID {
			continue
		}
		if channelID != "" && member.ChannelID != channelID {
			continue
		}
		members = append(members, member)
	}
	s.mu.RUnlock()
	sortMemoryMembers(members)
	return members, nil
}

func (s *MemoryStore) ListMembersForUser(ctx context.Context, userID string) ([]memberRecord, error) {
	s.mu.RLock()
	members := make([]memberRecord, 0, len(s.members))
	for _, member := range s.members {
		if member.UserID == userID && !memoryMemberHidden(member.Membership) {
			members = append(members, member)
		}
	}
	s.mu.RUnlock()
	return members, nil
}

func (s *MemoryStore) CountProductMembers(ctx context.Context, roomID, channelID string) (int64, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var joined, pending int64
	for _, member := range s.members {
		if roomID == "" && channelID == "" {
			continue
		}
		if roomID != "" && member.RoomID != roomID {
			continue
		}
		if channelID != "" && member.ChannelID != channelID {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(member.Membership)) {
		case "join", "joined":
			joined++
		case "pending":
			pending++
		}
	}
	return joined, pending, nil
}

func (s *MemoryStore) CountJoinedMembers(ctx context.Context, roomID, channelID string) (int64, error) {
	joined, _, err := s.CountProductMembers(ctx, roomID, channelID)
	return joined, err
}

func memoryMemberHidden(membership string) bool {
	switch strings.ToLower(strings.TrimSpace(membership)) {
	case "leave", "left", "remove", "removed", "reject", "rejected", "ban", "banned":
		return true
	default:
		return false
	}
}

func sortMemoryMembers(members []memberRecord) {
	sort.SliceStable(members, func(i, j int) bool {
		left, right := members[i], members[j]
		if left.JoinedAt != right.JoinedAt {
			if left.JoinedAt == 0 {
				return false
			}
			if right.JoinedAt == 0 {
				return true
			}
			return left.JoinedAt < right.JoinedAt
		}
		return left.UserID < right.UserID
	})
}
