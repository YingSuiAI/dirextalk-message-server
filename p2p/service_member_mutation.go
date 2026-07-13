package p2p

import (
	"context"
	"strings"
)

func (s *Service) memberMutation(ctx context.Context, scope, action string, params map[string]any) (any, *apiError) {
	roomID, channelID := s.memberTarget(params)
	if roomID == "" && channelID == "" {
		return nil, badRequest("room_id or channel_id is required")
	}
	userID := firstMemberID(params)
	if strings.HasSuffix(action, ".leave") || action == "groups.leave" || action == "channels.leave" || strings.Contains(action, ".invite.reject") {
		s.mu.Lock()
		userID = s.ownerMXID
		s.mu.Unlock()
	}
	if userID == "" {
		return nil, badRequest("user_id is required")
	}
	member := s.memberRecordFor(roomID, channelID, userID)
	existing, ok, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return nil, internalError(err)
	}
	if ok {
		member = existing
		if channelID != "" {
			member.ChannelID = channelID
		}
	}
	if strings.Contains(action, ".join_request.") {
		if !ok || !joinRequestMutationAllowed(action, existing.Membership) {
			return nil, statusError(404, "join request not found")
		}
	}
	if strings.Contains(action, ".invite.reject") {
		if !ok || !strings.EqualFold(strings.TrimSpace(existing.Membership), "invite") {
			return nil, statusError(404, scope+" invite not found")
		}
	}
	if scope == "group" {
		member.ChannelID = ""
	}
	if (strings.HasSuffix(action, ".leave") || strings.Contains(action, ".remove")) && strings.EqualFold(member.Role, "owner") {
		return nil, statusError(409, scope+" owner cannot leave; dissolve the "+scope+" instead")
	}
	switch {
	case strings.Contains(action, ".remove"):
		member.Membership = "remove"
		if s.transport != nil {
			s.mu.Lock()
			senderMXID := s.ownerMXID
			s.mu.Unlock()
			if err := s.transport.KickUser(ctx, KickUserRequest{
				RoomID:     member.RoomID,
				SenderMXID: senderMXID,
				TargetMXID: member.UserID,
				Reason:     trimString(params["reason"]),
			}); err != nil {
				return nil, transportWriteError(err)
			}
		}
	case strings.HasSuffix(action, ".leave"):
		member.Membership = "leave"
		if s.transport != nil {
			if err := s.transport.LeaveRoom(ctx, LeaveRoomRequest{
				RoomID:   member.RoomID,
				UserMXID: member.UserID,
			}); err != nil {
				return nil, transportWriteError(err)
			}
		}
	case strings.Contains(action, ".approve"):
		member.Membership = "approved"
	case strings.Contains(action, ".reject"):
		member.Membership = "reject"
	default:
		member.Membership = fallbackString(member.Membership, "join")
	}
	if err := s.saveMember(ctx, member); err != nil {
		return nil, internalError(err)
	}
	if strings.Contains(action, ".join_request.") {
		stateStatus := ""
		if strings.Contains(action, ".approve") {
			stateStatus = "approved"
		}
		if strings.Contains(action, ".reject") {
			stateStatus = "rejected"
		}
		if stateStatus != "" {
			if apiErr := s.publishJoinRequestState(ctx, member.RoomID, member.UserID, stateStatus, trimString(params["reason"])); apiErr != nil {
				return nil, apiErr
			}
		}
		if strings.Contains(action, ".approve") {
			result, apiErr := s.completeApprovedChannelJoin(ctx, member, params)
			if apiErr != nil {
				return nil, apiErr
			}
			status := fallbackString(trimString(result["status"]), "approved")
			if err := s.attachConversationOperation(ctx, result, action, status, member.RoomID); err != nil {
				return nil, internalError(err)
			}
			return result, nil
		}
		if strings.Contains(action, ".reject") && domainFromMXID(member.UserID) != s.serverName {
			result, apiErr := s.notifyRemoteChannelJoinResult(ctx, member, "rejected", params)
			if apiErr != nil {
				return nil, apiErr
			}
			result["status"] = "rejected"
			if err := s.attachConversationOperation(ctx, result, action, "rejected", member.RoomID); err != nil {
				return nil, internalError(err)
			}
			return result, nil
		}
	}
	result := map[string]any{"status": "ok", "member": member}
	if strings.Contains(action, ".invite.reject") {
		result["status"] = "rejected"
	}
	if strings.Contains(action, ".join_request.") {
		if strings.Contains(action, ".approve") {
			result["status"] = "approved"
		}
		if strings.Contains(action, ".reject") {
			result["status"] = "rejected"
		}
		result["channel"] = s.channelSnapshot(ctx, member.ChannelID)
	}
	status := fallbackString(trimString(result["status"]), "ok")
	if err := s.attachConversationOperation(ctx, result, action, status, member.RoomID); err != nil {
		return nil, internalError(err)
	}
	return result, nil
}

func joinRequestMutationAllowed(action, membership string) bool {
	membership = strings.ToLower(strings.TrimSpace(membership))
	if strings.Contains(action, ".approve") {
		switch membership {
		case "pending", "approved", "join_failed":
			return true
		default:
			return false
		}
	}
	return membership == "pending"
}

func (s *Service) lookupMember(ctx context.Context, roomID, userID string) (memberRecord, bool, error) {
	if roomID == "" || userID == "" {
		return memberRecord{}, false, nil
	}
	if store := s.memberStore(); store != nil {
		return store.LookupMember(ctx, roomID, userID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	member, ok := s.members[roomID+"|"+userID]
	return member, ok, nil
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

func (s *Service) memberTarget(params map[string]any) (string, string) {
	roomID := trimString(params["room_id"])
	channelID := trimString(params["channel_id"])
	if roomID == "" && channelID != "" {
		s.mu.Lock()
		if ch, ok := s.channels[channelID]; ok {
			roomID = ch.RoomID
		}
		s.mu.Unlock()
	}
	if channelID == "" && roomID != "" {
		s.mu.Lock()
		for _, ch := range s.channels {
			if ch.RoomID == roomID {
				channelID = ch.ChannelID
				break
			}
		}
		s.mu.Unlock()
	}
	return roomID, channelID
}

func (s *Service) memberRecordFor(roomID, channelID, userID string) memberRecord {
	s.mu.Lock()
	if existing, ok := s.members[roomID+"|"+userID]; ok {
		if channelID != "" {
			existing.ChannelID = channelID
		}
		s.mu.Unlock()
		return existing
	}
	s.mu.Unlock()
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

func applyMemberProfileParams(member *memberRecord, params map[string]any) {
	if displayName := trimString(params["display_name"]); displayName != "" {
		member.DisplayName = displayName
	}
	if avatarURL := trimString(params["avatar_url"]); avatarURL != "" {
		member.AvatarURL = avatarURL
	}
	if domain := trimString(params["domain"]); domain != "" {
		member.Domain = domain
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
