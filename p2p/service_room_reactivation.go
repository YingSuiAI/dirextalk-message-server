package p2p

import (
	"context"
	"net/http"
	"strings"
	"time"
)

func (s *Service) notifyRemoteRoomReactivation(ctx context.Context, scope string, member memberRecord, params map[string]any) *apiError {
	if member.UserID == "" || domainFromMXID(member.UserID) == s.serverName {
		return nil
	}
	base := trimString(params["requester_node_base_url"])
	if base == "" {
		base = trimString(params["applicant_node_base_url"])
	}
	if base == "" {
		base = trimString(params["remote_node_base_url"])
	}
	if base == "" {
		base = "https://" + domainFromMXID(member.UserID) + "/_p2p"
	}
	reactivationParams := map[string]any{
		"room_id":              member.RoomID,
		"channel_id":           member.ChannelID,
		"room_type":            scope,
		"user_id":              member.UserID,
		"display_name":         member.DisplayName,
		"avatar_url":           member.AvatarURL,
		"domain":               member.Domain,
		"server_names":         retainedRoomServerNames(params, member.RoomID),
		"remote_node_base_url": base,
	}
	if scope == "group" {
		if group, ok, err := s.groupByRoom(ctx, member.RoomID); err != nil {
			return internalError(err)
		} else if ok {
			reactivationParams["name"] = group.Name
			reactivationParams["topic"] = group.Topic
			reactivationParams["avatar_url"] = fallbackString(trimString(reactivationParams["avatar_url"]), group.AvatarURL)
			reactivationParams["invite_policy"] = group.InvitePolicy
		}
	}
	if scope == "channel" {
		if ch, ok, err := s.channelByIDOrRoom(ctx, member.ChannelID, member.RoomID); err != nil {
			return internalError(err)
		} else if ok {
			reactivationParams["channel_id"] = ch.ChannelID
			reactivationParams["name"] = ch.Name
			reactivationParams["description"] = ch.Description
			reactivationParams["avatar_url"] = fallbackString(trimString(reactivationParams["avatar_url"]), ch.AvatarURL)
			reactivationParams["visibility"] = ch.Visibility
			reactivationParams["join_policy"] = ch.JoinPolicy
			reactivationParams["channel_type"] = ch.ChannelType
			reactivationParams["comments_enabled"] = ch.CommentsEnabled
		}
	}
	var remote map[string]any
	status, err := s.remotePublicAction(ctx, domainFromMXID(member.UserID), "rooms.reactivate", reactivationParams, &remote)
	if err != nil {
		if status != 0 && status != http.StatusBadGateway {
			return statusError(status, err.Error())
		}
		return statusError(http.StatusBadGateway, err.Error())
	}
	if status != http.StatusOK {
		return statusError(status, "target node room reactivation failed")
	}
	return nil
}

func (s *Service) reinviteAlreadyJoinedRoomMember(ctx context.Context, scope string, member memberRecord, params map[string]any, inviteReq InviteUserRequest) *apiError {
	if apiErr := s.kickAndInviteStaleJoinedRoomMember(ctx, member, inviteReq); apiErr != nil {
		return apiErr
	}
	return s.notifyRemoteRoomReactivation(ctx, scope, member, params)
}

func (s *Service) kickAndInviteStaleJoinedRoomMember(ctx context.Context, member memberRecord, inviteReq InviteUserRequest) *apiError {
	if s.transport == nil {
		return nil
	}
	if err := s.transport.KickUser(ctx, KickUserRequest{
		RoomID:     member.RoomID,
		SenderMXID: inviteReq.InviterMXID,
		TargetMXID: member.UserID,
		Reason:     "reactivate rebuilt member invite",
		Timestamp:  time.Now().UTC(),
	}); err != nil && !isAlreadyLeftRoomError(err) {
		return transportWriteError(err)
	}
	if err := s.transport.InviteUser(ctx, inviteReq); err != nil {
		return transportWriteError(err)
	}
	return nil
}

func (s *Service) roomReactivate(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	scope := strings.ToLower(trimString(params["room_type"]))
	if scope == "" {
		scope = strings.ToLower(trimString(params["scope"]))
	}
	if roomID == "" || (scope != "group" && scope != "channel") {
		return nil, badRequest("room_id and room_type are required")
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	userID := fallbackString(firstMemberID(params), ownerMXID)
	if userID != ownerMXID {
		return nil, statusError(http.StatusForbidden, "room reactivation user must be local owner")
	}
	channelID := trimString(params["channel_id"])
	member := s.memberRecordFor(roomID, channelID, userID)
	member.Membership = "invite"
	if scope == "group" {
		member.ChannelID = ""
	}
	applyMemberProfileParams(&member, params)
	s.applyLocalOwnerMemberProfile(&member)
	if err := s.saveMember(ctx, member); err != nil {
		return nil, internalError(err)
	}
	if apiErr := s.saveRetainedRoomInviteMetadata(ctx, scope, member, params); apiErr != nil {
		return nil, apiErr
	}
	result := map[string]any{"status": "invite", "room_id": member.RoomID, "member": member}
	if scope == "channel" {
		result["channel"] = s.channelSnapshot(ctx, member.ChannelID)
	}
	if err := s.attachConversationOperation(ctx, result, scope+"s.reactivate", "invite", member.RoomID); err != nil {
		return nil, internalError(err)
	}
	return result, nil
}

func (s *Service) saveRetainedRoomInviteMetadata(ctx context.Context, scope string, member memberRecord, params map[string]any) *apiError {
	switch scope {
	case "group":
		if err := s.ensureJoinedGroupRecord(ctx, member, params); err != nil {
			return internalError(err)
		}
	case "channel":
		ch := channel{
			ChannelID:       fallbackString(member.ChannelID, member.RoomID),
			RoomID:          member.RoomID,
			Name:            fallbackString(trimString(params["name"]), member.RoomID),
			Description:     trimString(params["description"]),
			AvatarURL:       trimString(params["avatar_url"]),
			Visibility:      fallbackString(trimString(params["visibility"]), "private"),
			JoinPolicy:      fallbackString(trimString(params["join_policy"]), "invite"),
			ChannelType:     fallbackString(trimString(params["channel_type"]), "post"),
			CommentsEnabled: boolParam(params["comments_enabled"]),
			MemberStatus:    "invite",
			Role:            fallbackString(member.Role, "member"),
		}
		if existing, ok, err := s.channelByIDOrRoom(ctx, ch.ChannelID, ch.RoomID); err != nil {
			return internalError(err)
		} else if ok {
			mergeRefreshedChannel(&ch, existing)
			ch.MemberStatus = "invite"
		}
		if err := s.saveChannel(ctx, ch); err != nil {
			return internalError(err)
		}
	}
	return nil
}

func (s *Service) joinAndProjectRetainedRoom(ctx context.Context, scope string, member *memberRecord, params map[string]any) *apiError {
	if member == nil {
		return badRequest("member is required")
	}
	if s.transport != nil {
		result, err := s.joinRoomWithRetry(ctx, JoinRoomRequest{
			RoomIDOrAlias: fallbackString(member.RoomID, member.ChannelID),
			UserMXID:      member.UserID,
			DisplayName:   member.DisplayName,
			AvatarURL:     member.AvatarURL,
			ServerNames:   retainedRoomServerNames(params, member.RoomID),
		}, 10, isRetainedRoomJoinRetryable)
		if err != nil && !isAlreadyJoinedRoomError(err) {
			return transportWriteError(err)
		}
		if result.RoomID != "" {
			member.RoomID = result.RoomID
		}
	}
	member.Membership = "join"
	if member.JoinedAt == 0 {
		member.JoinedAt = time.Now().UTC().UnixMilli()
	}
	if err := s.saveMember(ctx, *member); err != nil {
		return internalError(err)
	}
	switch scope {
	case "group":
		if err := s.ensureJoinedGroupRecord(ctx, *member, params); err != nil {
			return internalError(err)
		}
	case "channel":
		if refreshedChannelID, err := s.refreshRoomChannel(ctx, member.RoomID); err != nil {
			return internalError(err)
		} else if refreshedChannelID != "" {
			member.ChannelID = refreshedChannelID
			if err := s.saveMember(ctx, *member); err != nil {
				return internalError(err)
			}
		}
		if err := s.refreshRoomMembers(ctx, member.RoomID, member.ChannelID); err != nil {
			return internalError(err)
		}
		if err := s.backfillJoinedPostChannelContent(ctx, member.RoomID, member.ChannelID); err != nil {
			return internalError(err)
		}
	}
	return nil
}

func (s *Service) joinRoomWithRetry(ctx context.Context, req JoinRoomRequest, maxAttempts int, retryable func(error) bool) (JoinRoomResult, error) {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		result, err := s.transport.JoinRoom(ctx, req)
		if err == nil || retryable == nil || !retryable(err) || attempt == maxAttempts-1 {
			return result, err
		}
		select {
		case <-ctx.Done():
			return JoinRoomResult{}, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 300 * time.Millisecond):
		}
	}
	return JoinRoomResult{}, nil
}

func isRetainedRoomJoinRetryable(err error) bool {
	return isRoomJoinRequiresInvite(err) || isFederatedJoinInProgress(err)
}

func isRoomJoinRequiresInvite(err error) bool {
	if err == nil {
		return false
	}
	if isDirectRoomJoinRequiresInvite(err) {
		return true
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "join rule \"invite\" forbids it")
}

func isFederatedJoinInProgress(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "already a federated join to this room in progress")
}

func retainedRoomServerNames(params map[string]any, roomID string) []string {
	serverNames := stringSliceParam(params["server_names"])
	if len(serverNames) > 0 {
		return serverNames
	}
	if server, ok := roomServerFromMatrixRoomID(roomID); ok && server != "" {
		return []string{server}
	}
	return nil
}
