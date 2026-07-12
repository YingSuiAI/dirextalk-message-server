package storage

import (
	"context"
	"sort"
	"strings"
)

func (s *MemoryStore) UpsertChannel(ctx context.Context, ch channel) error {
	s.mu.Lock()
	s.channels[ch.ChannelID] = ch
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) DeleteChannel(ctx context.Context, channelID string) error {
	s.mu.Lock()
	delete(s.channels, channelID)
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) ListChannels(ctx context.Context) ([]channel, error) {
	s.mu.RLock()
	channels := make([]channel, 0, len(s.channels))
	for _, ch := range s.channels {
		channels = append(channels, ch)
	}
	s.mu.RUnlock()
	return channels, nil
}

func (s *MemoryStore) GetChannelByIDOrRoom(ctx context.Context, channelID, roomID string) (channel, bool, error) {
	if channelID == "" && roomID == "" {
		return channel{}, false, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ch := range s.channels {
		if channelID != "" && ch.ChannelID == channelID {
			return ch, true, nil
		}
		if roomID != "" && ch.RoomID == roomID {
			return ch, true, nil
		}
	}
	return channel{}, false, nil
}

func (s *MemoryStore) ListJoinedChannelsForUser(ctx context.Context, userID string) ([]channel, error) {
	s.mu.RLock()
	membersByChannel := make(map[string]memberRecord)
	membersByRoom := make(map[string]memberRecord)
	for _, member := range s.members {
		if member.UserID != userID || memoryMemberHidden(member.Membership) || !strings.EqualFold(strings.TrimSpace(member.Membership), "join") {
			continue
		}
		if member.ChannelID != "" {
			membersByChannel[member.ChannelID] = member
		}
		if member.RoomID != "" {
			membersByRoom[member.RoomID] = member
		}
	}
	channels := make([]channel, 0, len(s.channels))
	for _, ch := range s.channels {
		member, ok := membersByChannel[ch.ChannelID]
		if !ok {
			member, ok = membersByRoom[ch.RoomID]
		}
		if !ok {
			continue
		}
		ch.Role = normalizeStoredProductMemberRole(member.Role)
		ch.MemberStatus = "join"
		ch.IsOwned = strings.EqualFold(ch.Role, "owner")
		channels = append(channels, ch)
	}
	s.mu.RUnlock()
	return channels, nil
}

func (s *MemoryStore) SearchPublicChannels(ctx context.Context, query string, limit int) ([]channel, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	query = strings.ToLower(strings.TrimSpace(query))
	s.mu.RLock()
	results := make([]channel, 0, min(limit, len(s.channels)))
	for _, ch := range s.channels {
		if !strings.EqualFold(ch.Visibility, "public") {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(ch.ChannelID+" "+ch.RoomID+" "+ch.Name+" "+ch.Description), query) {
			continue
		}
		results = append(results, ch)
		if len(results) >= limit {
			break
		}
	}
	s.mu.RUnlock()
	return results, nil
}

func (s *MemoryStore) ListPublicChannelsForOwner(ctx context.Context, userID string) ([]channel, error) {
	s.mu.RLock()
	ownedChannelIDs := make(map[string]bool)
	ownedRoomIDs := make(map[string]bool)
	for _, member := range s.members {
		if member.UserID != userID || memoryMemberHidden(member.Membership) || !strings.EqualFold(member.Role, "owner") {
			continue
		}
		if member.ChannelID != "" {
			ownedChannelIDs[member.ChannelID] = true
		}
		if member.RoomID != "" {
			ownedRoomIDs[member.RoomID] = true
		}
	}
	channels := make([]channel, 0)
	for _, ch := range s.channels {
		if (!ownedChannelIDs[ch.ChannelID] && !ownedRoomIDs[ch.RoomID]) || !strings.EqualFold(ch.Visibility, "public") {
			continue
		}
		channels = append(channels, ch)
	}
	s.mu.RUnlock()
	sort.SliceStable(channels, func(i, j int) bool {
		if channels[i].Name == channels[j].Name {
			return channels[i].ChannelID < channels[j].ChannelID
		}
		return channels[i].Name < channels[j].Name
	})
	return channels, nil
}

func (s *MemoryStore) UpsertChannelInviteGrant(ctx context.Context, grant channelInviteGrant) error {
	s.mu.Lock()
	s.inviteGrants[grant.GrantID] = grant
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) ListChannelInviteGrants(ctx context.Context) ([]channelInviteGrant, error) {
	s.mu.RLock()
	grants := make([]channelInviteGrant, 0, len(s.inviteGrants))
	for _, grant := range s.inviteGrants {
		grants = append(grants, grant)
	}
	s.mu.RUnlock()
	sort.SliceStable(grants, func(i, j int) bool {
		if grants[i].CreatedAt == grants[j].CreatedAt {
			return grants[i].GrantID < grants[j].GrantID
		}
		return grants[i].CreatedAt > grants[j].CreatedAt
	})
	return grants, nil
}
