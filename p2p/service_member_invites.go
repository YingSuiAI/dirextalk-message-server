package p2p

import (
	"context"

	membersmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/members"
)

func (s *Service) prepareMemberInvite(
	ctx context.Context,
	scope, roomID, channelID string,
) (membersmodule.InviteSender, *apiError) {
	inviteRoomState, apiErr := s.productInviteRoomState(ctx, scope, roomID, channelID)
	if apiErr != nil {
		return nil, apiErr
	}
	return func(sendCtx context.Context, member *memberRecord, params map[string]any, isDirect bool) *apiError {
		if member == nil {
			return badRequest("member is required")
		}
		generation := trimString(params["rebuild_generation"])
		if generation != "" && !membersmodule.ValidRebuildGeneration(generation) {
			return badRequest("rebuild_generation is invalid")
		}
		if s.transport != nil {
			inviteReq := InviteUserRequest{
				RoomID:          member.RoomID,
				InviterMXID:     s.memberOwnerMXID(),
				InviteeMXID:     member.UserID,
				Reason:          trimString(params["reason"]),
				IsDirect:        isDirect,
				InviteRoomState: inviteRoomState,
			}
			if err := s.transport.InviteUser(sendCtx, inviteReq); err != nil {
				if !isAlreadyJoinedRoomError(err) {
					return transportWriteError(err)
				}
				if apiErr := s.reinviteAlreadyJoinedRoomMember(sendCtx, scope, member, params, inviteReq, err); apiErr != nil {
					return apiErr
				}
			}
		}
		if generation != "" {
			member.RequestID = generation
			if err := markRecoverableOperation(sendCtx, operationPhaseMatrixCommitted, member.RoomID); err != nil {
				return recoverableOperationWriteError(sendCtx, err)
			}
		}
		return nil
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
	membersmodule.SortByJoinOrder(members)
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
	if name != "" && group.Name != name {
		group.Name = name
	}
	if avatarURL := trimString(params["avatar_url"]); avatarURL != "" && group.AvatarURL != avatarURL {
		group.AvatarURL = avatarURL
	}
	if topic := trimString(params["topic"]); topic != "" && group.Topic != topic {
		group.Topic = topic
	}
	if group.MemberCount == 0 {
		group.MemberCount = 1
	}
	// Save also repairs the group conversation. Replaying a join after a
	// partial group/conversation write must therefore not stop at an unchanged
	// group projection.
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
	if err := s.saveChannel(ctx, ch); err != nil {
		return "", err
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
		if ch, ok, lookupErr := s.channelByIDOrRoom(ctx, "", roomID); lookupErr == nil && ok {
			channelID = ch.ChannelID
		}
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
