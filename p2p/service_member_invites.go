package p2p

import (
	"context"
	"strings"
	"time"
)

func (s *Service) inviteMembers(ctx context.Context, scope string, params map[string]any) (any, *apiError) {
	roomID, channelID := s.memberTarget(params)
	if roomID == "" && channelID == "" {
		return nil, badRequest("room_id or channel_id is required")
	}
	users := memberIDsFromParams(params)
	if len(users) == 0 {
		return nil, badRequest("user_id is required")
	}
	inviteRoomState, apiErr := s.productInviteRoomState(ctx, scope, roomID, channelID)
	if apiErr != nil {
		return nil, apiErr
	}
	members := make([]memberRecord, 0, len(users))
	for _, userID := range users {
		member := s.memberRecordFor(roomID, channelID, userID)
		member.Membership = "invite"
		if scope == "group" {
			member.ChannelID = ""
		}
		applyMemberProfileParams(&member, params)
		if s.transport != nil {
			s.mu.Lock()
			inviterMXID := s.ownerMXID
			s.mu.Unlock()
			inviteReq := InviteUserRequest{
				RoomID:          member.RoomID,
				InviterMXID:     inviterMXID,
				InviteeMXID:     userID,
				Reason:          trimString(params["reason"]),
				IsDirect:        boolParam(params["is_direct"]),
				InviteRoomState: inviteRoomState,
			}
			if err := s.transport.InviteUser(ctx, inviteReq); err != nil {
				if !isAlreadyJoinedRoomError(err) {
					return nil, transportWriteError(err)
				}
				if apiErr := s.reinviteAlreadyJoinedRoomMember(ctx, scope, member, params, inviteReq); apiErr != nil {
					return nil, apiErr
				}
			}
		}
		if err := s.saveMember(ctx, member); err != nil {
			return nil, internalError(err)
		}
		members = append(members, member)
	}
	result := map[string]any{"status": "ok", "members": members}
	if err := s.attachConversationOperation(ctx, result, scope+"s.invite", "ok", roomID); err != nil {
		return nil, internalError(err)
	}
	return result, nil
}

//nolint:gocyclo // Invite grants validate channel, share-room, and Matrix invite side effects together.
func (s *Service) channelInviteGrantCreate(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID, channelID := s.memberTarget(params)
	if roomID == "" && channelID == "" {
		return nil, badRequest("room_id or channel_id is required")
	}
	shareRoomID := trimString(params["share_room_id"])
	if shareRoomID == "" {
		shareRoomID = trimString(params["via_room_id"])
	}
	if shareRoomID == "" {
		return nil, badRequest("share_room_id is required")
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "channel not found")
	}
	if apiErr := s.requireOwnerMember(ctx, ch.RoomID); apiErr != nil {
		return nil, apiErr
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	ownerShareMember, ok, err := s.lookupMember(ctx, shareRoomID, ownerMXID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok || !strings.EqualFold(strings.TrimSpace(ownerShareMember.Membership), "join") {
		return nil, statusError(403, "owner must be joined to the share room")
	}
	grantID := trimString(params["grant_id"])
	if grantID == "" {
		grantID = "grant_" + randomToken("channel_invite")
	}
	grant := channelInviteGrant{
		GrantID:     grantID,
		ChannelID:   ch.ChannelID,
		RoomID:      ch.RoomID,
		ShareRoomID: shareRoomID,
		CreatedBy:   ownerMXID,
		CreatedAt:   time.Now().UTC().UnixMilli(),
	}
	if saveErr := s.saveChannelInviteGrant(ctx, grant); saveErr != nil {
		return nil, internalError(saveErr)
	}
	shareMembers, err := s.shareRoomMembersForInviteGrant(ctx, shareRoomID)
	if err != nil {
		return nil, internalError(err)
	}
	inviteRoomState, apiErr := s.productInviteRoomState(ctx, "channel", ch.RoomID, ch.ChannelID)
	if apiErr != nil {
		return nil, apiErr
	}
	invited := make([]memberRecord, 0, len(shareMembers))
	for _, shareMember := range shareMembers {
		if shareMember.UserID == "" ||
			shareMember.UserID == ownerMXID ||
			!strings.EqualFold(strings.TrimSpace(shareMember.Membership), "join") {
			continue
		}
		if existing, ok, err := s.lookupMember(ctx, ch.RoomID, shareMember.UserID); err != nil {
			return nil, internalError(err)
		} else if ok && (strings.EqualFold(existing.Membership, "join") || strings.EqualFold(existing.Membership, "invite")) {
			continue
		}
		member := s.memberRecordFor(ch.RoomID, ch.ChannelID, shareMember.UserID)
		member.Membership = "invite"
		member.Role = fallbackString(member.Role, "member")
		member.DisplayName = fallbackString(shareMember.DisplayName, member.DisplayName)
		member.AvatarURL = fallbackString(shareMember.AvatarURL, member.AvatarURL)
		member.Domain = fallbackString(shareMember.Domain, member.Domain)
		if s.transport != nil {
			inviteReq := InviteUserRequest{
				RoomID:          ch.RoomID,
				InviterMXID:     ownerMXID,
				InviteeMXID:     shareMember.UserID,
				Reason:          trimString(params["reason"]),
				InviteRoomState: inviteRoomState,
			}
			if err := s.transport.InviteUser(ctx, inviteReq); err != nil {
				if !isAlreadyJoinedRoomError(err) {
					return nil, transportWriteError(err)
				}
				if apiErr := s.reinviteAlreadyJoinedRoomMember(ctx, "channel", member, params, inviteReq); apiErr != nil {
					return nil, apiErr
				}
			}
		}
		if err := s.saveMember(ctx, member); err != nil {
			return nil, internalError(err)
		}
		invited = append(invited, member)
	}
	return map[string]any{
		"status":        "ok",
		"grant_id":      grant.GrantID,
		"room_id":       grant.RoomID,
		"channel_id":    grant.ChannelID,
		"share_room_id": grant.ShareRoomID,
		"grant":         grant,
		"channel":       ch,
		"members":       invited,
	}, nil
}

func (s *Service) shareRoomMembersForInviteGrant(ctx context.Context, shareRoomID string) ([]memberRecord, error) {
	members, err := s.membersForProduct(ctx, shareRoomID, "")
	if err != nil {
		return nil, err
	}
	if s.transport == nil || shareRoomID == "" {
		return members, nil
	}
	matrixMembers, err := s.transport.ListRoomMembers(ctx, shareRoomID)
	if err != nil {
		return nil, err
	}
	byUserID := make(map[string]int, len(members)+len(matrixMembers))
	for index, member := range members {
		if member.UserID == "" {
			continue
		}
		byUserID[member.UserID] = index
	}
	for _, member := range matrixMembers {
		if member.UserID == "" {
			continue
		}
		member.RoomID = shareRoomID
		if member.ChannelID != "" {
			member.ChannelID = ""
		}
		if member.Membership == "" {
			member.Membership = "join"
		}
		if member.Role == "" {
			member.Role = "member"
		}
		if index, ok := byUserID[member.UserID]; ok {
			mergeRefreshedMember(&member, members[index])
			members[index] = member
			continue
		}
		byUserID[member.UserID] = len(members)
		members = append(members, member)
	}
	sortMembersByJoinOrder(members)
	return members, nil
}

func (s *Service) productInviteRoomState(ctx context.Context, scope, roomID, channelID string) ([]RoomStateEvent, *apiError) {
	switch scope {
	case "group":
		group, ok, err := s.groupByRoom(ctx, roomID)
		if err != nil {
			return nil, transportWriteError(err)
		}
		if !ok {
			group = groupRecord{
				RoomID:       roomID,
				Name:         roomID,
				InvitePolicy: "member",
			}
		}
		return []RoomStateEvent{groupStateEvent(group, false)}, nil
	case "channel":
		ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
		if err != nil {
			return nil, internalError(err)
		}
		if !ok {
			return nil, statusError(404, "channel not found")
		}
		return []RoomStateEvent{channelStateEvent(ch, false)}, nil
	default:
		return nil, nil
	}
}

//nolint:gocyclo // Join flow intentionally keeps Matrix join, invite-card, and projection refresh ordering together.
func (s *Service) joinMember(ctx context.Context, scope string, params map[string]any) (any, *apiError) {
	roomID, channelID := s.memberTarget(params)
	if scope == "channel" && roomID == "" && channelID == "" {
		if grant, ok, err := s.lookupChannelInviteGrantForParams(ctx, params); err != nil {
			return nil, internalError(err)
		} else if ok {
			roomID = grant.RoomID
			channelID = grant.ChannelID
			params["room_id"] = roomID
			params["channel_id"] = channelID
		}
	}
	if roomID == "" && channelID == "" {
		return nil, badRequest("room_id or channel_id is required")
	}
	if scope == "channel" && channelID == "" && roomID != "" {
		ch, ok, err := s.channelByIDOrRoom(ctx, "", roomID)
		if err != nil {
			return nil, internalError(err)
		}
		if ok {
			channelID = ch.ChannelID
			params["channel_id"] = channelID
		}
	}
	userID := firstMemberID(params)
	if userID == "" {
		s.mu.Lock()
		userID = s.ownerMXID
		s.mu.Unlock()
	}
	if scope == "group" {
		if apiErr := s.requireRecordedGroupInviteForCardJoin(ctx, roomID, userID, params); apiErr != nil {
			return nil, apiErr
		}
	}
	if scope == "channel" {
		if apiErr := s.requireChannelInviteGrantForJoin(ctx, roomID, channelID, userID, params); apiErr != nil {
			return nil, apiErr
		}
	}
	existing, ok, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return nil, internalError(err)
	}
	if ok && memberRemoved(existing.Membership) && !removedMemberHasFreshInvite(scope, params) {
		return nil, statusError(403, scope+" member was removed")
	}
	member := existing
	if !ok {
		member = s.memberRecordFor(roomID, channelID, userID)
	}
	member.Membership = "join"
	if scope == "group" {
		member.ChannelID = ""
	}
	applyMemberProfileParams(&member, params)
	s.applyLocalOwnerMemberProfile(&member)
	if apiErr := s.joinAndProjectRetainedRoom(ctx, scope, &member, params); apiErr != nil {
		return nil, apiErr
	}
	result := map[string]any{"status": "ok", "room_id": member.RoomID, "member": member}
	if scope == "channel" {
		result["channel"] = s.channelSnapshot(ctx, member.ChannelID)
	}
	if err := s.attachConversationOperation(ctx, result, scope+"s.join", "ok", member.RoomID); err != nil {
		return nil, internalError(err)
	}
	return result, nil
}

func (s *Service) requireRecordedGroupInviteForCardJoin(ctx context.Context, roomID, userID string, params map[string]any) *apiError {
	if !hasGroupInviteCardParams(params) {
		return nil
	}
	existing, ok, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return internalError(err)
	}
	if !ok || (!strings.EqualFold(strings.TrimSpace(existing.Membership), "invite") && !removedMemberHasFreshGroupInvite(existing.Membership, params)) {
		return statusError(403, "group invite is missing or expired")
	}
	return nil
}

func hasGroupInviteCardParams(params map[string]any) bool {
	return trimString(params["invite_event_id"]) != "" || trimString(params["direct_room_id"]) != ""
}

func removedMemberHasFreshInvite(scope string, params map[string]any) bool {
	switch scope {
	case "group":
		return trimString(params["invite_event_id"]) != ""
	case "channel":
		return trimString(params["grant_id"]) != "" ||
			trimString(params["share_room_id"]) != "" ||
			trimString(params["via_room_id"]) != ""
	default:
		return false
	}
}

func removedMemberHasFreshGroupInvite(membership string, params map[string]any) bool {
	return (memberRemoved(membership) || memberLeft(membership)) && trimString(params["invite_event_id"]) != ""
}

func (s *Service) requireChannelInviteGrantForJoin(ctx context.Context, roomID, channelID, userID string, params map[string]any) *apiError {
	if trimString(params["grant_id"]) == "" && trimString(params["share_room_id"]) == "" && trimString(params["via_room_id"]) == "" {
		return nil
	}
	grant, ok, err := s.lookupChannelInviteGrantForParams(ctx, params)
	if err != nil {
		return internalError(err)
	}
	if !ok {
		if existing, memberOK, memberErr := s.lookupMember(ctx, roomID, userID); memberErr != nil {
			return internalError(memberErr)
		} else if memberOK {
			membership := strings.TrimSpace(existing.Membership)
			if strings.EqualFold(membership, "invite") ||
				((memberRemoved(membership) || memberLeft(membership)) && removedMemberHasFreshInvite("channel", params)) {
				return nil
			}
		}
		return statusError(403, "channel invite grant is missing or expired")
	}
	if roomID != "" && grant.RoomID != roomID {
		return statusError(403, "channel invite grant room mismatch")
	}
	if channelID != "" && grant.ChannelID != channelID {
		return statusError(403, "channel invite grant channel mismatch")
	}
	member, ok, err := s.lookupMember(ctx, grant.ShareRoomID, userID)
	if err != nil {
		return internalError(err)
	}
	if !ok || !strings.EqualFold(strings.TrimSpace(member.Membership), "join") {
		return statusError(403, "user is not joined to the share room")
	}
	return nil
}

func (s *Service) ensureJoinedGroupRecord(ctx context.Context, member memberRecord, params map[string]any) error {
	if member.RoomID == "" {
		return nil
	}
	group, ok, err := s.groupByRoom(ctx, member.RoomID)
	if err != nil {
		return err
	}
	name := fallbackString(
		trimString(params["name"]),
		fallbackString(trimString(params["group_name"]), trimString(params["room_name"])),
	)
	if !ok {
		group = groupRecord{
			RoomID:       member.RoomID,
			Name:         fallbackString(name, member.RoomID),
			Topic:        trimString(params["topic"]),
			AvatarURL:    trimString(params["avatar_url"]),
			MemberCount:  1,
			InvitePolicy: fallbackString(trimString(params["invite_policy"]), "member"),
		}
		return s.saveGroup(ctx, group)
	}
	changed := false
	if name != "" && group.Name != name {
		group.Name = name
		changed = true
	}
	if avatarURL := trimString(params["avatar_url"]); avatarURL != "" && group.AvatarURL != avatarURL {
		group.AvatarURL = avatarURL
		changed = true
	}
	if topic := trimString(params["topic"]); topic != "" && group.Topic != topic {
		group.Topic = topic
		changed = true
	}
	if group.MemberCount == 0 {
		group.MemberCount = 1
		changed = true
	}
	if !changed {
		return nil
	}
	return s.saveGroup(ctx, group)
}

func (s *Service) refreshRoomChannel(ctx context.Context, roomID string) (string, error) {
	if s.transport == nil || roomID == "" {
		return "", nil
	}
	ch, ok, err := s.transport.GetRoomChannel(ctx, roomID)
	if err != nil || !ok {
		return "", err
	}
	if existing, exists, lookupErr := s.channelByIDOrRoom(ctx, ch.ChannelID, ch.RoomID); lookupErr != nil {
		return "", lookupErr
	} else if exists {
		mergeRefreshedChannel(&ch, existing)
	}
	s.mu.Lock()
	s.channels[ch.ChannelID] = ch
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.UpsertChannel(ctx, ch); err != nil {
			return "", err
		}
	}
	return ch.ChannelID, nil
}

func (s *Service) refreshRoomMembers(ctx context.Context, roomID, channelID string) error {
	if s.transport == nil || roomID == "" {
		return nil
	}
	members, err := s.transport.ListRoomMembers(ctx, roomID)
	if err != nil {
		return err
	}
	if channelID == "" {
		channelID = s.channelIDForRoom(ctx, roomID)
	}
	for _, member := range members {
		member.RoomID = roomID
		if member.ChannelID == "" {
			member.ChannelID = channelID
		}
		if member.Membership == "" {
			member.Membership = "join"
		}
		if member.Role == "" {
			member.Role = "member"
		}
		if existing, ok, lookupErr := s.lookupMember(ctx, member.RoomID, member.UserID); lookupErr != nil {
			return lookupErr
		} else if ok {
			mergeRefreshedMember(&member, existing)
		}
		if err := s.saveMember(ctx, member); err != nil {
			return err
		}
	}
	return nil
}

func mergeRefreshedChannel(ch *channel, existing channel) {
	if ch.Name == "" {
		ch.Name = existing.Name
	}
	if ch.Description == "" {
		ch.Description = existing.Description
	}
	if ch.AvatarURL == "" {
		ch.AvatarURL = existing.AvatarURL
	}
	if ch.Visibility == "" {
		ch.Visibility = existing.Visibility
	}
	if ch.JoinPolicy == "" {
		ch.JoinPolicy = existing.JoinPolicy
	}
	if ch.ChannelType == "" {
		ch.ChannelType = existing.ChannelType
	}
	if !ch.CommentsEnabled && existing.CommentsEnabled {
		ch.CommentsEnabled = true
	}
	if !ch.Muted && existing.Muted {
		ch.Muted = true
	}
}

func mergeRefreshedMember(member *memberRecord, existing memberRecord) {
	if member.DisplayName == "" {
		member.DisplayName = existing.DisplayName
	}
	if member.AvatarURL == "" {
		member.AvatarURL = existing.AvatarURL
	}
	if member.Domain == "" {
		member.Domain = existing.Domain
	}
	if !member.Muted && existing.Muted {
		member.Muted = true
	}
}
