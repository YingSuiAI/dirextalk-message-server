package members

import (
	"context"
	"errors"
	"net/http"
	"strings"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

// RoomReactivate records the local owner's retained room invitation. Matrix
// re-invitation remains in the root protocol adapter that calls this action.
func (m *Module) RoomReactivate(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	if m.config.OwnerMXID == nil || m.config.LookupMember == nil || m.config.NewMember == nil ||
		m.config.SaveMember == nil || m.config.ApplyLocalProfile == nil ||
		m.config.SaveRetainedMetadata == nil || m.config.Conversation == nil {
		return nil, actionbase.InternalError(errors.New("room reactivation dependencies are not configured"))
	}
	params := actionbase.Params(raw)
	roomID := params.String("room_id")
	scope := strings.ToLower(params.String("room_type"))
	if scope == "" {
		scope = strings.ToLower(params.String("scope"))
	}
	if roomID == "" || (scope != "group" && scope != "channel") {
		return nil, actionbase.BadRequest("room_id and room_type are required")
	}

	ownerMXID := m.config.OwnerMXID()
	userID := firstMemberID(params)
	if userID == "" {
		userID = ownerMXID
	}
	if userID != ownerMXID {
		return nil, actionbase.StatusError(http.StatusForbidden, "room reactivation user must be local owner")
	}
	channelID := params.String("channel_id")
	member, found, err := m.config.LookupMember(ctx, roomID, userID)
	if err != nil {
		return nil, actionbase.InternalError(err)
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
	applyMemberProfile(&member, params)
	m.config.ApplyLocalProfile(&member)
	if err := m.config.SaveMember(ctx, member); err != nil {
		return nil, actionbase.InternalError(err)
	}
	if actionErr := m.config.SaveRetainedMetadata(ctx, scope, member, raw); actionErr != nil {
		return nil, actionErr
	}

	result := map[string]any{"status": "invite", "room_id": member.RoomID, "member": member}
	if scope == "channel" {
		if m.config.ChannelSnapshot == nil {
			return nil, actionbase.InternalError(errors.New("channel snapshot is not configured"))
		}
		result["channel"] = m.config.ChannelSnapshot(ctx, member.ChannelID)
	}
	return m.attachOperation(ctx, result, scope+"s.reactivate", "invite", member.RoomID)
}
