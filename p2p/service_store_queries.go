package p2p

import (
	"context"
	"strings"
)

func (s *Service) listGroups(ctx context.Context) ([]groupRecord, error) {
	return s.groupsModule.List(ctx)
}

func (s *Service) listChannels(ctx context.Context) ([]channel, error) {
	if store := s.channelStore(); store != nil {
		return store.ListChannels(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channels := make([]channel, 0, len(s.channels))
	for _, ch := range s.channels {
		channels = append(channels, ch)
	}
	return channels, nil
}

func (s *Service) joinedChannelsForOwner(ctx context.Context, channels []channel) ([]channel, error) {
	if len(channels) == 0 {
		return channels, nil
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if strings.TrimSpace(ownerMXID) == "" {
		return []channel{}, nil
	}
	members, err := s.membersForUser(ctx, ownerMXID)
	if err != nil {
		return nil, err
	}
	ownerByChannelID := make(map[string]memberRecord, len(members))
	ownerByRoomID := make(map[string]memberRecord, len(members))
	for _, member := range members {
		if !strings.EqualFold(strings.TrimSpace(member.Membership), "join") {
			continue
		}
		if member.ChannelID != "" {
			ownerByChannelID[member.ChannelID] = member
		}
		if member.RoomID != "" {
			ownerByRoomID[member.RoomID] = member
		}
	}
	visible := make([]channel, 0, len(channels))
	for _, ch := range channels {
		member, ok := ownerByChannelID[ch.ChannelID]
		if !ok {
			member, ok = ownerByRoomID[ch.RoomID]
		}
		if !ok {
			continue
		}
		role := normalizeProductMemberRole(member.Role)
		ch.Role = role
		ch.MemberStatus = "join"
		ch.IsOwned = productOwnerRole(role)
		visible = append(visible, ch)
	}
	return visible, nil
}
