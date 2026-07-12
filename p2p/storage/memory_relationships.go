package storage

import (
	"context"
	"sort"
	"strings"
)

func (s *MemoryStore) UpsertContact(ctx context.Context, contact contactRecord) error {
	s.mu.Lock()
	if contact.PeerMXID != "" {
		for roomID, existing := range s.contacts {
			if roomID != contact.RoomID && existing.PeerMXID == contact.PeerMXID {
				delete(s.contacts, roomID)
			}
		}
	}
	s.contacts[contact.RoomID] = contact
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) ListContacts(ctx context.Context) ([]contactRecord, error) {
	s.mu.RLock()
	contacts := make([]contactRecord, 0, len(s.contacts))
	for _, contact := range s.contacts {
		contacts = append(contacts, contact)
	}
	s.mu.RUnlock()
	return contacts, nil
}

func memoryBlockKey(targetType, targetID string) string {
	switch strings.ToLower(strings.TrimSpace(targetType)) {
	case "friend", "user", "member", "contact":
		targetType = "contact"
	default:
		targetType = ""
	}
	return targetType + "|" + strings.TrimSpace(targetID)
}

func (s *MemoryStore) UpsertBlock(ctx context.Context, block blockRecord) error {
	s.mu.Lock()
	s.blocks[memoryBlockKey(block.TargetType, block.TargetID)] = block
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) DeleteBlock(ctx context.Context, targetType, targetID string) (bool, error) {
	key := memoryBlockKey(targetType, targetID)
	s.mu.Lock()
	_, removed := s.blocks[key]
	delete(s.blocks, key)
	s.mu.Unlock()
	return removed, nil
}

func (s *MemoryStore) ListBlocks(ctx context.Context) ([]blockRecord, error) {
	s.mu.RLock()
	blocks := make([]blockRecord, 0, len(s.blocks))
	for _, block := range s.blocks {
		blocks = append(blocks, block)
	}
	s.mu.RUnlock()
	sort.SliceStable(blocks, func(i, j int) bool {
		if blocks[i].TargetType != blocks[j].TargetType {
			return blocks[i].TargetType < blocks[j].TargetType
		}
		if blocks[i].DisplayName != blocks[j].DisplayName {
			return strings.ToLower(blocks[i].DisplayName) < strings.ToLower(blocks[j].DisplayName)
		}
		return blocks[i].TargetID < blocks[j].TargetID
	})
	return blocks, nil
}

func (s *MemoryStore) UpsertGroup(ctx context.Context, group groupRecord) error {
	s.mu.Lock()
	s.groups[group.RoomID] = group
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) DeleteGroup(ctx context.Context, roomID string) error {
	s.mu.Lock()
	delete(s.groups, roomID)
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) ListGroups(ctx context.Context) ([]groupRecord, error) {
	s.mu.RLock()
	groups := make([]groupRecord, 0, len(s.groups))
	for _, group := range s.groups {
		groups = append(groups, group)
	}
	s.mu.RUnlock()
	return groups, nil
}

func (s *MemoryStore) GetGroupByRoom(ctx context.Context, roomID string) (groupRecord, bool, error) {
	if roomID == "" {
		return groupRecord{}, false, nil
	}
	s.mu.RLock()
	group, ok := s.groups[roomID]
	s.mu.RUnlock()
	return group, ok, nil
}

func (s *MemoryStore) ListJoinedGroupsForUser(ctx context.Context, userID string) ([]groupRecord, error) {
	s.mu.RLock()
	joinedRooms := make(map[string]bool)
	for _, member := range s.members {
		if member.UserID == userID && member.ChannelID == "" && !memoryMemberHidden(member.Membership) && strings.EqualFold(strings.TrimSpace(member.Membership), "join") {
			joinedRooms[member.RoomID] = true
		}
	}
	groups := make([]groupRecord, 0, len(s.groups))
	for _, group := range s.groups {
		if joinedRooms[group.RoomID] {
			groups = append(groups, group)
		}
	}
	s.mu.RUnlock()
	return groups, nil
}

func (s *MemoryStore) UpsertCall(ctx context.Context, call callRecord) error {
	s.mu.Lock()
	s.calls[call.CallID] = call
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) ListCalls(ctx context.Context, roomID string, activeOnly bool) ([]callRecord, error) {
	s.mu.RLock()
	calls := make([]callRecord, 0, len(s.calls))
	for _, call := range s.calls {
		if roomID != "" && call.RoomID != roomID {
			continue
		}
		if activeOnly && memoryTerminalCallState(call.State) {
			continue
		}
		calls = append(calls, call)
	}
	s.mu.RUnlock()
	return calls, nil
}

func memoryTerminalCallState(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "ended", "rejected", "missed", "failed":
		return true
	default:
		return false
	}
}
