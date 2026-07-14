package p2p

import (
	"context"
	"strings"
)

func (s *Service) completeChannelJoinRequest(ctx context.Context, approved bool, member memberRecord, params map[string]any) (map[string]any, *apiError) {
	if approved {
		return s.completeApprovedChannelJoin(ctx, member, params)
	}
	if domainFromMXID(member.UserID) != s.serverName {
		result, apiErr := s.notifyRemoteChannelJoinResult(ctx, member, "rejected", params)
		if apiErr != nil {
			return nil, apiErr
		}
		return result, nil
	}
	return map[string]any{
		"status":  "rejected",
		"member":  member,
		"channel": s.channelSnapshot(ctx, member.ChannelID),
	}, nil
}

func (s *Service) memberOwnerMXID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ownerMXID
}

func (s *Service) kickMember(ctx context.Context, roomID, senderMXID, targetMXID, reason string) *apiError {
	if s.transport == nil {
		return nil
	}
	if err := s.transport.KickUser(ctx, KickUserRequest{
		RoomID:     roomID,
		SenderMXID: senderMXID,
		TargetMXID: targetMXID,
		Reason:     reason,
	}); err != nil {
		return transportWriteError(err)
	}
	return nil
}

func (s *Service) leaveMember(ctx context.Context, roomID, userMXID string) *apiError {
	if s.transport == nil {
		return nil
	}
	if err := s.transport.LeaveRoom(ctx, LeaveRoomRequest{RoomID: roomID, UserMXID: userMXID}); err != nil {
		return transportWriteError(err)
	}
	return nil
}

func (s *Service) lookupMember(ctx context.Context, roomID, userID string) (memberRecord, bool, error) {
	if roomID == "" || userID == "" {
		return memberRecord{}, false, nil
	}
	store := s.memberStore()
	if store == nil {
		return memberRecord{}, false, nil
	}
	return store.LookupMember(ctx, roomID, userID)
}

func (s *Service) requireOwnerMember(ctx context.Context, roomID string) *apiError {
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	member, ok, err := s.lookupMember(ctx, roomID, ownerMXID)
	if err != nil {
		return internalError(err)
	}
	if !ok || !strings.EqualFold(member.Role, "owner") || memberHidden(member.Membership) {
		return statusError(403, "owner role is required")
	}
	return nil
}

func (s *Service) memberTarget(ctx context.Context, params map[string]any) (string, string, error) {
	roomID := trimString(params["room_id"])
	channelID := trimString(params["channel_id"])
	if roomID == "" && channelID != "" {
		ch, ok, err := s.channelByIDOrRoom(ctx, channelID, "")
		if err != nil {
			return "", "", err
		}
		if ok {
			roomID = ch.RoomID
		}
	}
	if channelID == "" && roomID != "" {
		ch, ok, err := s.channelByIDOrRoom(ctx, "", roomID)
		if err != nil {
			return "", "", err
		}
		if ok {
			channelID = ch.ChannelID
		}
	}
	return roomID, channelID, nil
}

func (s *Service) memberRecordFor(roomID, channelID, userID string) memberRecord {
	return memberRecord{
		RoomID:      roomID,
		ChannelID:   channelID,
		UserID:      userID,
		DisplayName: displayNameFromMXID(userID),
		Domain:      domainFromMXID(userID),
		Membership:  "join",
		Role:        "member",
	}
}

func (s *Service) applyLocalOwnerMemberProfile(member *memberRecord) {
	if member == nil {
		return
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	profile := s.profile
	serverName := s.serverName
	s.mu.Unlock()
	if strings.TrimSpace(ownerMXID) == "" || member.UserID != ownerMXID {
		return
	}
	if displayName := strings.TrimSpace(profile.DisplayName); displayName != "" {
		member.DisplayName = displayName
	}
	if avatarURL := strings.TrimSpace(profile.AvatarURL); avatarURL != "" {
		member.AvatarURL = avatarURL
	}
	if member.Domain == "" {
		member.Domain = serverName
	}
}
