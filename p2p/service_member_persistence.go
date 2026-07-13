package p2p

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

type memberStore interface {
	UpsertMember(ctx context.Context, member memberRecord) error
	InsertMemberIfAbsent(ctx context.Context, member memberRecord) (bool, error)
	CompareAndSwapMemberGeneration(ctx context.Context, member memberRecord, expectedRequestID, expectedMembership string) (bool, error)
	LookupMember(ctx context.Context, roomID, userID string) (memberRecord, bool, error)
	ListMembers(ctx context.Context, roomID, channelID string) ([]memberRecord, error)
	ListMembersForUser(ctx context.Context, userID string) ([]memberRecord, error)
	CountProductMembers(ctx context.Context, roomID, channelID string) (joined, pending int64, err error)
	CountJoinedMembers(ctx context.Context, roomID, channelID string) (int64, error)
}

func (s *Service) saveMemberIfAbsent(ctx context.Context, member memberRecord) (bool, error) {
	release := s.lockMemberWrite(member.RoomID, member.UserID)
	defer release()

	member.Role = normalizeProductMemberRole(member.Role)
	if member.JoinedAt == 0 {
		member.JoinedAt = time.Now().UTC().UnixMilli()
	}
	store := s.memberStore()
	if store == nil {
		return false, errors.New("member store is not configured")
	}
	saved, err := store.InsertMemberIfAbsent(ctx, member)
	if err != nil || !saved {
		return saved, err
	}
	if member.ChannelID == "" {
		return true, s.refreshStoredGroupCounts(ctx, member.RoomID)
	}
	return true, s.refreshStoredChannelCounts(ctx, member.ChannelID)
}

func (s *Service) saveMemberIfState(
	ctx context.Context,
	member memberRecord,
	expectedRequestID,
	expectedMembership string,
) (bool, error) {
	release := s.lockMemberWrite(member.RoomID, member.UserID)
	defer release()

	member.Role = normalizeProductMemberRole(member.Role)
	if member.JoinedAt == 0 {
		member.JoinedAt = time.Now().UTC().UnixMilli()
	}
	store := s.memberStore()
	if store == nil {
		return false, errors.New("member store is not configured")
	}
	stored, found, err := store.LookupMember(ctx, member.RoomID, member.UserID)
	if err != nil {
		return false, err
	}
	if !found || stored.RequestID != expectedRequestID ||
		(expectedMembership != "" && !strings.EqualFold(strings.TrimSpace(stored.Membership), strings.TrimSpace(expectedMembership))) {
		return false, nil
	}
	mergeMemberPersistence(&member, stored)
	saved, err := store.CompareAndSwapMemberGeneration(ctx, member, expectedRequestID, expectedMembership)
	if err != nil || !saved {
		return saved, err
	}
	if member.ChannelID == "" {
		return true, s.refreshStoredGroupCounts(ctx, member.RoomID)
	}
	return true, s.refreshStoredChannelCounts(ctx, member.ChannelID)
}

type memberWriteEntry struct {
	mu   sync.Mutex
	refs int
}

func (s *Service) memberStore() memberStore {
	if s.store == nil {
		return nil
	}
	return s.store
}

func (s *Service) saveMember(ctx context.Context, member memberRecord) error {
	release := s.lockMemberWrite(member.RoomID, member.UserID)
	defer release()

	member.Role = normalizeProductMemberRole(member.Role)
	if member.JoinedAt == 0 {
		member.JoinedAt = time.Now().UTC().UnixMilli()
	}
	store := s.memberStore()
	if store == nil {
		return errors.New("member store is not configured")
	}
	if member.RoomID != "" && member.UserID != "" {
		stored, hasStored, err := store.LookupMember(ctx, member.RoomID, member.UserID)
		if err != nil {
			return err
		}
		if hasStored {
			mergeMemberPersistence(&member, stored)
		}
	}
	if err := store.UpsertMember(ctx, member); err != nil {
		return err
	}
	if member.ChannelID == "" {
		return s.refreshStoredGroupCounts(ctx, member.RoomID)
	}
	return s.refreshStoredChannelCounts(ctx, member.ChannelID)
}

func (s *Service) lockMemberWrite(roomID, userID string) func() {
	key := roomID + "\x00" + userID
	s.memberWritesMu.Lock()
	if s.memberWrites == nil {
		s.memberWrites = make(map[string]*memberWriteEntry)
	}
	entry := s.memberWrites[key]
	if entry == nil {
		entry = &memberWriteEntry{}
		s.memberWrites[key] = entry
	}
	entry.refs++
	s.memberWritesMu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		s.memberWritesMu.Lock()
		entry.refs--
		if entry.refs == 0 && s.memberWrites[key] == entry {
			delete(s.memberWrites, key)
		}
		s.memberWritesMu.Unlock()
	}
}

func mergeMemberPersistence(member *memberRecord, existing memberRecord) {
	if existing.JoinedAt > 0 && !memberStartsNewRequestGeneration(existing.Membership, member.Membership) {
		member.JoinedAt = existing.JoinedAt
	}
	if member.RequesterNodeBaseURL == "" {
		member.RequesterNodeBaseURL = existing.RequesterNodeBaseURL
	}
	if member.RequestID == "" {
		member.RequestID = existing.RequestID
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

func memberStartsNewRequestGeneration(previous, next string) bool {
	switch strings.ToLower(strings.TrimSpace(next)) {
	case "invite", "pending":
		switch strings.ToLower(strings.TrimSpace(previous)) {
		case "invite", "pending", "approved", "joining", "join_failed":
			return false
		default:
			return true
		}
	default:
		return false
	}
}

func productOwnerRole(role string) bool {
	return dirextalkdomain.ProductOwnerRole(role)
}

func normalizeProductMemberRole(role string) string {
	return dirextalkdomain.NormalizeProductMemberRole(role)
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
	store := s.memberStore()
	if store == nil {
		return nil, errors.New("member store is not configured")
	}
	return store.ListMembers(ctx, roomID, channelID)
}
