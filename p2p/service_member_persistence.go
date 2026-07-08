package p2p

import (
	"context"
	"errors"
	"strings"
	"time"
)

type memberStore interface {
	UpsertMember(ctx context.Context, member memberRecord) error
	LookupMember(ctx context.Context, roomID, userID string) (memberRecord, bool, error)
	ListMembers(ctx context.Context, roomID, channelID string) ([]memberRecord, error)
	ListMembersForUser(ctx context.Context, userID string) ([]memberRecord, error)
	CountProductMembers(ctx context.Context, roomID, channelID string) (joined, pending int64, err error)
	CountJoinedMembers(ctx context.Context, roomID, channelID string) (int64, error)
}

func (s *Service) memberStore() memberStore {
	if s.store == nil {
		return nil
	}
	return s.store
}

func (s *Service) saveMember(ctx context.Context, member memberRecord) error {
	member.Role = normalizeProductMemberRole(member.Role)
	if member.JoinedAt == 0 {
		member.JoinedAt = time.Now().UTC().UnixMilli()
	}
	var stored memberRecord
	var hasStored bool
	store := s.memberStore()
	if store != nil && member.RoomID != "" && member.UserID != "" {
		var err error
		stored, hasStored, err = store.LookupMember(ctx, member.RoomID, member.UserID)
		if err != nil {
			return err
		}
	}
	s.mu.Lock()
	if existing, ok := s.members[member.RoomID+"|"+member.UserID]; ok && existing.JoinedAt > 0 {
		mergeMemberPersistence(&member, existing)
	} else if hasStored {
		mergeMemberPersistence(&member, stored)
	}
	s.members[member.RoomID+"|"+member.UserID] = member
	if member.ChannelID == "" {
		s.refreshGroupCountsLocked(member.RoomID)
	} else {
		s.refreshChannelCountsLocked(member.ChannelID)
	}
	s.mu.Unlock()
	if store != nil {
		if err := store.UpsertMember(ctx, member); err != nil {
			return err
		}
		if member.ChannelID == "" {
			return s.refreshStoredGroupCounts(ctx, member.RoomID)
		}
		return s.refreshStoredChannelCounts(ctx, member.ChannelID)
	}
	return nil
}

func mergeMemberPersistence(member *memberRecord, existing memberRecord) {
	if existing.JoinedAt > 0 {
		member.JoinedAt = existing.JoinedAt
	}
	if member.RequesterNodeBaseURL == "" {
		member.RequesterNodeBaseURL = existing.RequesterNodeBaseURL
	}
	if memberRemoved(existing.Membership) && memberLeft(member.Membership) {
		member.Membership = existing.Membership
	}
	if productOwnerRole(existing.Role) &&
		!productOwnerRole(member.Role) &&
		!memberHidden(existing.Membership) &&
		!memberHidden(member.Membership) {
		member.Role = existing.Role
	}
}

func productOwnerRole(role string) bool {
	return strings.EqualFold(strings.TrimSpace(role), "owner")
}

func normalizeProductMemberRole(role string) string {
	if productOwnerRole(role) {
		return "owner"
	}
	return "member"
}

func (s *Service) repairLocalChannelOwnerRoles(ctx context.Context) error {
	channelStore := s.channelStore()
	if channelStore == nil {
		return nil
	}
	memberStore := s.memberStore()
	if memberStore == nil {
		return nil
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	serverName := s.serverName
	s.mu.Unlock()
	if strings.TrimSpace(ownerMXID) == "" {
		return nil
	}
	channels, err := channelStore.ListChannels(ctx)
	if err != nil {
		return err
	}
	for _, ch := range channels {
		if !strings.EqualFold(domainFromMatrixID(ch.RoomID, "!"), serverName) {
			continue
		}
		member, ok, err := memberStore.LookupMember(ctx, ch.RoomID, ownerMXID)
		if err != nil {
			return err
		}
		if !ok {
			if err := s.saveOwnerMember(ctx, ch.RoomID, ch.ChannelID); err != nil {
				return err
			}
			continue
		}
		if memberHidden(member.Membership) || productOwnerRole(member.Role) {
			continue
		}
		member.ChannelID = fallbackString(member.ChannelID, ch.ChannelID)
		member.Role = "owner"
		if err := s.saveMember(ctx, member); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) setProductMemberMute(ctx context.Context, roomID, channelID string, muted bool) error {
	members, err := s.membersForProduct(ctx, roomID, channelID)
	if err != nil {
		return err
	}
	for _, member := range members {
		if memberHidden(member.Membership) {
			continue
		}
		if productOwnerRole(member.Role) {
			continue
		}
		member.Muted = muted
		if err := s.saveMember(ctx, member); err != nil {
			return err
		}
		if apiErr := s.publishMemberPolicyState(ctx, member); apiErr != nil {
			return errors.New(apiErr.Error)
		}
	}
	return nil
}

func (s *Service) membersForProduct(ctx context.Context, roomID, channelID string) ([]memberRecord, error) {
	if store := s.memberStore(); store != nil {
		return store.ListMembers(ctx, roomID, channelID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
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
	sortMembersByJoinOrder(members)
	return members, nil
}
