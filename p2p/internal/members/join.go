package members

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

func (m *Module) joinHandler(scope string) actionbase.Handler {
	return func(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
		return m.Join(ctx, scope, raw)
	}
}

// Join applies the shared retained group/channel join workflow while Matrix
// writes and projection refresh remain behind the root JoinRetained adapter.
func (m *Module) Join(ctx context.Context, scope string, raw map[string]any) (any, *actionbase.Error) {
	if !m.joinConfigured() {
		return nil, actionbase.InternalError(errors.New("member join dependencies are not configured"))
	}
	params := actionbase.Params(raw)
	roomID, channelID, err := m.config.ResolveTarget(ctx, raw)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if scope == "channel" && roomID == "" && channelID == "" {
		grant, ok, lookupErr := m.lookupChannelInviteGrant(ctx, params)
		if lookupErr != nil {
			return nil, actionbase.InternalError(lookupErr)
		}
		if ok {
			roomID, channelID = grant.RoomID, grant.ChannelID
			raw["room_id"] = roomID
			raw["channel_id"] = channelID
		}
	}
	if roomID == "" && channelID == "" {
		return nil, actionbase.BadRequest("room_id or channel_id is required")
	}
	if scope == "channel" {
		if actionErr := m.completeChannelTarget(ctx, raw, &roomID, &channelID); actionErr != nil {
			return nil, actionErr
		}
	}

	userID := firstMemberID(params)
	if userID == "" {
		userID = m.config.OwnerMXID()
	}
	if scope == "group" {
		if actionErr := m.requireRecordedGroupInvite(ctx, roomID, userID, params); actionErr != nil {
			return nil, actionErr
		}
	}
	if scope == "channel" {
		if actionErr := m.requireChannelInviteGrant(ctx, roomID, channelID, userID, params); actionErr != nil {
			return nil, actionErr
		}
	}

	existing, found, err := m.config.LookupMember(ctx, roomID, userID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if found && removedMembership(existing.Membership) && !removedMemberHasFreshInvite(scope, params) {
		return nil, actionbase.StatusError(http.StatusForbidden, scope+" member was removed")
	}
	member := existing
	if !found {
		member = m.config.NewMember(roomID, channelID, userID)
	}
	member.Membership = "join"
	if scope == "group" {
		member.ChannelID = ""
	}
	applyMemberProfile(&member, params)
	m.config.ApplyLocalProfile(&member)
	if actionErr := m.config.JoinRetained(ctx, scope, &member, raw); actionErr != nil {
		return nil, actionErr
	}

	result := map[string]any{"status": "ok", "room_id": member.RoomID, "member": member}
	if scope == "channel" {
		if m.config.ChannelSnapshot == nil {
			return nil, actionbase.InternalError(errors.New("channel snapshot is not configured"))
		}
		result["channel"] = m.config.ChannelSnapshot(ctx, member.ChannelID)
	}
	return m.attachOperation(ctx, result, scope+"s.join", "ok", member.RoomID)
}

func (m *Module) joinConfigured() bool {
	return m.config.ResolveTarget != nil &&
		m.config.NewMember != nil &&
		m.config.LookupMember != nil &&
		m.config.OwnerMXID != nil &&
		m.config.ApplyLocalProfile != nil &&
		m.config.JoinRetained != nil &&
		m.config.Conversation != nil
}

func (m *Module) completeChannelTarget(ctx context.Context, raw map[string]any, roomID, channelID *string) *actionbase.Error {
	if (*channelID == "" && *roomID != "") || (*roomID == "" && *channelID != "") {
		if m.config.LookupChannel == nil {
			return actionbase.InternalError(errors.New("channel lookup is not configured"))
		}
	}
	if *channelID == "" && *roomID != "" {
		channel, ok, err := m.config.LookupChannel(ctx, "", *roomID)
		if err != nil {
			return actionbase.InternalError(err)
		}
		if ok {
			*channelID = channel.ChannelID
			raw["channel_id"] = *channelID
		}
	}
	if *roomID == "" && *channelID != "" {
		channel, ok, err := m.config.LookupChannel(ctx, *channelID, "")
		if err != nil {
			return actionbase.InternalError(err)
		}
		if ok {
			*roomID = channel.RoomID
			raw["room_id"] = *roomID
		}
	}
	return nil
}

func (m *Module) requireRecordedGroupInvite(ctx context.Context, roomID, userID string, params actionbase.Params) *actionbase.Error {
	if params.String("invite_event_id") == "" && params.String("direct_room_id") == "" {
		return nil
	}
	existing, ok, err := m.config.LookupMember(ctx, roomID, userID)
	if err != nil {
		return actionbase.InternalError(err)
	}
	if !ok || (!strings.EqualFold(strings.TrimSpace(existing.Membership), "invite") &&
		!removedMemberHasFreshGroupInvite(existing.Membership, params)) {
		return actionbase.StatusError(http.StatusForbidden, "group invite is missing or expired")
	}
	return nil
}

func (m *Module) requireChannelInviteGrant(ctx context.Context, roomID, channelID, userID string, params actionbase.Params) *actionbase.Error {
	if params.String("grant_id") == "" && params.String("share_room_id") == "" && params.String("via_room_id") == "" {
		return nil
	}
	grant, ok, err := m.lookupChannelInviteGrant(ctx, params)
	if err != nil {
		return actionbase.InternalError(err)
	}
	if !ok {
		existing, memberOK, memberErr := m.config.LookupMember(ctx, roomID, userID)
		if memberErr != nil {
			return actionbase.InternalError(memberErr)
		}
		if memberOK {
			membership := strings.TrimSpace(existing.Membership)
			if strings.EqualFold(membership, "invite") ||
				((removedMembership(membership) || leftMembership(membership)) && removedMemberHasFreshInvite("channel", params)) {
				return nil
			}
		}
		return actionbase.StatusError(http.StatusForbidden, "channel invite grant is missing or expired")
	}
	if roomID != "" && grant.RoomID != roomID {
		return actionbase.StatusError(http.StatusForbidden, "channel invite grant room mismatch")
	}
	if channelID != "" && grant.ChannelID != channelID {
		return actionbase.StatusError(http.StatusForbidden, "channel invite grant channel mismatch")
	}
	member, ok, err := m.config.LookupMember(ctx, grant.ShareRoomID, userID)
	if err != nil {
		return actionbase.InternalError(err)
	}
	if !ok || !strings.EqualFold(strings.TrimSpace(member.Membership), "join") {
		return actionbase.StatusError(http.StatusForbidden, "user is not joined to the share room")
	}
	return nil
}

func (m *Module) lookupChannelInviteGrant(ctx context.Context, params actionbase.Params) (dirextalkdomain.ChannelInviteGrant, bool, error) {
	if m.store == nil {
		return dirextalkdomain.ChannelInviteGrant{}, false, errors.New("member store is not configured")
	}
	grantID := params.String("grant_id")
	shareRoomID := params.FirstString("share_room_id", "via_room_id")
	roomID, channelID := params.String("room_id"), params.String("channel_id")
	grants, err := m.store.ListChannelInviteGrants(ctx)
	if err != nil {
		return dirextalkdomain.ChannelInviteGrant{}, false, err
	}
	for _, grant := range grants {
		if grantID != "" && grant.GrantID != grantID {
			continue
		}
		if shareRoomID != "" && grant.ShareRoomID != shareRoomID {
			continue
		}
		if roomID != "" && grant.RoomID != roomID {
			continue
		}
		if channelID != "" && grant.ChannelID != channelID {
			continue
		}
		return grant, true, nil
	}
	return dirextalkdomain.ChannelInviteGrant{}, false, nil
}

func removedMemberHasFreshInvite(scope string, params actionbase.Params) bool {
	switch scope {
	case "group":
		return params.String("invite_event_id") != ""
	case "channel":
		return params.String("grant_id") != "" || params.String("share_room_id") != "" || params.String("via_room_id") != ""
	default:
		return false
	}
}

func removedMemberHasFreshGroupInvite(membership string, params actionbase.Params) bool {
	return (removedMembership(membership) || leftMembership(membership)) && params.String("invite_event_id") != ""
}

func removedMembership(membership string) bool {
	switch strings.ToLower(strings.TrimSpace(membership)) {
	case "remove", "removed", "ban", "banned":
		return true
	default:
		return false
	}
}

func leftMembership(membership string) bool {
	switch strings.ToLower(strings.TrimSpace(membership)) {
	case "leave", "left":
		return true
	default:
		return false
	}
}
