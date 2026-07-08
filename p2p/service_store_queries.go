package p2p

import (
	"context"
	"sort"
	"strings"
)

func (s *Service) listContacts(ctx context.Context) ([]contactRecord, error) {
	contacts, err := s.rawContacts(ctx)
	if err != nil {
		return nil, err
	}
	visible := make([]contactRecord, 0, len(contacts))
	for _, contact := range contacts {
		if contactDeleted(contact.Status) {
			continue
		}
		visible = append(visible, contact)
	}
	return dedupeContactsByPeer(visible), nil
}

func dedupeContactsByPeer(contacts []contactRecord) []contactRecord {
	if len(contacts) <= 1 {
		return contacts
	}
	byPeer := make(map[string]contactRecord, len(contacts))
	for _, contact := range contacts {
		key := strings.TrimSpace(contact.PeerMXID)
		if key == "" {
			key = strings.TrimSpace(contact.RoomID)
		}
		if key == "" {
			continue
		}
		existing, ok := byPeer[key]
		if !ok || contactStatusRank(contact.Status) > contactStatusRank(existing.Status) {
			byPeer[key] = contact
			continue
		}
		if contactStatusRank(contact.Status) == contactStatusRank(existing.Status) {
			if existing.DisplayName == "" && contact.DisplayName != "" {
				existing.DisplayName = contact.DisplayName
			}
			if existing.AvatarURL == "" && contact.AvatarURL != "" {
				existing.AvatarURL = contact.AvatarURL
			}
			if existing.Domain == "" && contact.Domain != "" {
				existing.Domain = contact.Domain
			}
			if existing.Remark == "" && contact.Remark != "" {
				existing.Remark = contact.Remark
			}
			byPeer[key] = existing
		}
	}
	result := make([]contactRecord, 0, len(byPeer))
	for _, contact := range byPeer {
		result = append(result, contact)
	}
	sort.SliceStable(result, func(i, j int) bool {
		left, right := strings.ToLower(result[i].DisplayName), strings.ToLower(result[j].DisplayName)
		if left == right {
			return result[i].PeerMXID < result[j].PeerMXID
		}
		return left < right
	})
	return result
}

func contactStatusRank(status string) int {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "accepted":
		return 4
	case "pending_inbound":
		return 3
	case "pending_outbound":
		return 2
	case "rejected", "reject":
		return 1
	default:
		return 0
	}
}

func (s *Service) rawContacts(ctx context.Context) ([]contactRecord, error) {
	if s.store != nil {
		return s.store.ListContacts(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	contacts := make([]contactRecord, 0, len(s.contacts))
	for _, contact := range s.contacts {
		contacts = append(contacts, contact)
	}
	return contacts, nil
}

func (s *Service) lookupContactByRoom(ctx context.Context, roomID string) (contactRecord, bool, error) {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return contactRecord{}, false, nil
	}
	contacts, err := s.rawContacts(ctx)
	if err != nil {
		return contactRecord{}, false, err
	}
	for _, contact := range contacts {
		if contact.RoomID == roomID {
			return contact, true, nil
		}
	}
	return contactRecord{}, false, nil
}

func (s *Service) lookupContactByPeer(ctx context.Context, peerMXID string) (contactRecord, bool, error) {
	peerMXID = strings.TrimSpace(peerMXID)
	if peerMXID == "" {
		return contactRecord{}, false, nil
	}
	contacts, err := s.rawContacts(ctx)
	if err != nil {
		return contactRecord{}, false, err
	}
	var found contactRecord
	for _, contact := range contacts {
		if contact.PeerMXID == peerMXID {
			if found.PeerMXID == "" || contactStatusRank(contact.Status) > contactStatusRank(found.Status) {
				found = contact
			}
		}
	}
	if found.PeerMXID != "" {
		return found, true, nil
	}
	return contactRecord{}, false, nil
}

func (s *Service) listGroups(ctx context.Context) ([]groupRecord, error) {
	if store := s.groupStore(); store != nil {
		return store.ListGroups(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	groups := make([]groupRecord, 0, len(s.groups))
	for _, group := range s.groups {
		groups = append(groups, group)
	}
	return groups, nil
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

func (s *Service) joinedGroupsForOwner(ctx context.Context, groups []groupRecord) ([]groupRecord, error) {
	if len(groups) == 0 {
		return groups, nil
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if strings.TrimSpace(ownerMXID) == "" {
		return []groupRecord{}, nil
	}
	members, err := s.membersForUser(ctx, ownerMXID)
	if err != nil {
		return nil, err
	}
	joinedByRoom := make(map[string]bool, len(members))
	for _, member := range members {
		if member.ChannelID == "" && strings.EqualFold(strings.TrimSpace(member.Membership), "join") {
			joinedByRoom[member.RoomID] = true
		}
	}
	visible := make([]groupRecord, 0, len(groups))
	for _, group := range groups {
		if joinedByRoom[group.RoomID] {
			visible = append(visible, group)
		}
	}
	return visible, nil
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
