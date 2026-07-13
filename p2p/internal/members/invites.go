package members

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

func (m *Module) inviteHandler(scope string) actionbase.Handler {
	return func(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
		return m.Invite(ctx, scope, raw)
	}
}

// Invite applies the shared group and channel member-invite workflow.
func (m *Module) Invite(ctx context.Context, scope string, raw map[string]any) (any, *actionbase.Error) {
	if m.config.ResolveTarget == nil || m.config.NewMember == nil || m.config.LookupMember == nil ||
		m.config.SaveMember == nil || m.config.RejectBlocked == nil || m.config.PrepareInvite == nil ||
		m.config.Conversation == nil {
		return nil, actionbase.InternalError(errors.New("member invite dependencies are not configured"))
	}
	roomID, channelID, err := m.config.ResolveTarget(ctx, raw)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if roomID == "" && channelID == "" {
		return nil, actionbase.BadRequest("room_id or channel_id is required")
	}
	users := inviteMemberIDs(actionbase.Params(raw))
	if len(users) == 0 {
		return nil, actionbase.BadRequest("user_id is required")
	}
	if scope == "channel" && roomID == "" && channelID != "" {
		if m.config.LookupChannel == nil {
			return nil, actionbase.InternalError(errors.New("channel lookup is not configured"))
		}
		channel, ok, lookupErr := m.config.LookupChannel(ctx, channelID, "")
		if lookupErr != nil {
			return nil, actionbase.InternalError(lookupErr)
		}
		if ok {
			roomID = channel.RoomID
		}
	}
	sendInvite, actionErr := m.config.PrepareInvite(ctx, scope, roomID, channelID)
	if actionErr != nil {
		return nil, actionErr
	}
	if sendInvite == nil {
		return nil, actionbase.InternalError(errors.New("member invite sender is not configured"))
	}

	members := make([]dirextalkdomain.MemberRecord, 0, len(users))
	for _, userID := range users {
		if actionErr := m.config.RejectBlocked(ctx, "contact", userID); actionErr != nil {
			return nil, actionErr
		}
		member, found, lookupErr := m.config.LookupMember(ctx, roomID, userID)
		if lookupErr != nil {
			return nil, actionbase.InternalError(lookupErr)
		}
		if !found {
			member = m.config.NewMember(roomID, channelID, userID)
		} else if channelID != "" {
			member.ChannelID = channelID
		}
		member.Membership = "invite"
		if scope == "group" {
			member.ChannelID = ""
		}
		applyMemberProfile(&member, actionbase.Params(raw))
		if actionErr := sendInvite(ctx, member, raw, actionbase.Params(raw).Bool("is_direct")); actionErr != nil {
			return nil, actionErr
		}
		if err := m.config.SaveMember(ctx, member); err != nil {
			return nil, actionbase.InternalError(err)
		}
		members = append(members, member)
	}

	result := map[string]any{"status": "ok", "members": members}
	return m.attachOperation(ctx, result, scope+"s.invite", "ok", roomID)
}

// ChannelInviteGrantCreate persists the grant before inviting eligible share-room members.
func (m *Module) ChannelInviteGrantCreate(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	if !m.channelInviteGrantConfigured() {
		return nil, actionbase.InternalError(errors.New("channel invite-grant dependencies are not configured"))
	}
	params := actionbase.Params(raw)
	roomID, channelID, err := m.config.ResolveTarget(ctx, raw)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if roomID == "" && channelID == "" {
		return nil, actionbase.BadRequest("room_id or channel_id is required")
	}
	shareRoomID := params.FirstString("share_room_id", "via_room_id")
	if shareRoomID == "" {
		return nil, actionbase.BadRequest("share_room_id is required")
	}
	channel, ok, err := m.config.LookupChannel(ctx, channelID, roomID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.StatusError(http.StatusNotFound, "channel not found")
	}
	if actionErr := m.config.RequireOwner(ctx, channel.RoomID); actionErr != nil {
		return nil, actionErr
	}
	ownerMXID := m.config.OwnerMXID()
	ownerShareMember, ok, err := m.config.LookupMember(ctx, shareRoomID, ownerMXID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok || !strings.EqualFold(strings.TrimSpace(ownerShareMember.Membership), "join") {
		return nil, actionbase.StatusError(http.StatusForbidden, "owner must be joined to the share room")
	}
	grantID := params.String("grant_id")
	if grantID == "" {
		if m.config.NewGrantID == nil {
			return nil, actionbase.InternalError(errors.New("channel invite-grant ID generator is not configured"))
		}
		grantID = strings.TrimSpace(m.config.NewGrantID())
	}
	grant := dirextalkdomain.ChannelInviteGrant{
		GrantID:     grantID,
		ChannelID:   channel.ChannelID,
		RoomID:      channel.RoomID,
		ShareRoomID: shareRoomID,
		CreatedBy:   ownerMXID,
		CreatedAt:   m.now().UTC().UnixMilli(),
	}
	if err := m.store.UpsertChannelInviteGrant(ctx, grant); err != nil {
		return nil, actionbase.InternalError(err)
	}
	shareMembers, err := m.config.ShareRoomMembers(ctx, shareRoomID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	sendInvite, actionErr := m.config.PrepareInvite(ctx, "channel", channel.RoomID, channel.ChannelID)
	if actionErr != nil {
		return nil, actionErr
	}
	if sendInvite == nil {
		return nil, actionbase.InternalError(errors.New("member invite sender is not configured"))
	}

	invited := make([]dirextalkdomain.MemberRecord, 0, len(shareMembers))
	for _, shareMember := range shareMembers {
		if shareMember.UserID == "" || shareMember.UserID == ownerMXID ||
			!strings.EqualFold(strings.TrimSpace(shareMember.Membership), "join") {
			continue
		}
		existing, found, err := m.config.LookupMember(ctx, channel.RoomID, shareMember.UserID)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		if found && (strings.EqualFold(existing.Membership, "join") || strings.EqualFold(existing.Membership, "invite")) {
			continue
		}
		member := m.config.NewMember(channel.RoomID, channel.ChannelID, shareMember.UserID)
		if found {
			member = existing
			member.ChannelID = channel.ChannelID
		}
		member.Membership = "invite"
		member.Role = fallback(member.Role, "member")
		member.DisplayName = fallback(shareMember.DisplayName, member.DisplayName)
		member.AvatarURL = fallback(shareMember.AvatarURL, member.AvatarURL)
		member.Domain = fallback(shareMember.Domain, member.Domain)
		if actionErr := sendInvite(ctx, member, raw, false); actionErr != nil {
			return nil, actionErr
		}
		if err := m.config.SaveMember(ctx, member); err != nil {
			return nil, actionbase.InternalError(err)
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
		"channel":       channel,
		"members":       invited,
	}, nil
}

func (m *Module) channelInviteGrantConfigured() bool {
	return m.store != nil &&
		m.config.ResolveTarget != nil &&
		m.config.LookupChannel != nil &&
		m.config.RequireOwner != nil &&
		m.config.OwnerMXID != nil &&
		m.config.LookupMember != nil &&
		m.config.NewMember != nil &&
		m.config.SaveMember != nil &&
		m.config.ShareRoomMembers != nil &&
		m.config.PrepareInvite != nil
}

func inviteMemberIDs(params actionbase.Params) []string {
	seen := make(map[string]struct{})
	users := make([]string, 0)
	add := func(userID string) {
		userID = strings.TrimSpace(userID)
		if userID == "" {
			return
		}
		if _, ok := seen[userID]; ok {
			return
		}
		seen[userID] = struct{}{}
		users = append(users, userID)
	}
	for _, key := range []string{"user_id", "user_mxid", "peer_mxid", "mxid"} {
		add(params.String(key))
	}
	for _, key := range []string{"user_ids", "user_mxids", "peer_mxids", "invitees", "invite"} {
		for _, userID := range params.Strings(key) {
			add(userID)
		}
	}
	return users
}

func applyMemberProfile(member *dirextalkdomain.MemberRecord, params actionbase.Params) {
	if displayName := params.String("display_name"); displayName != "" {
		member.DisplayName = displayName
	}
	if avatarURL := params.String("avatar_url"); avatarURL != "" {
		member.AvatarURL = avatarURL
	}
	if domain := params.String("domain"); domain != "" {
		member.Domain = domain
	}
}

func fallback(value, defaultValue string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return defaultValue
}
