package members

import (
	"context"
	"errors"
	"strings"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

func (m *Module) mutationHandler(scope, action string, muted bool) actionbase.Handler {
	return func(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
		return m.mutate(ctx, scope, action, muted, raw)
	}
}

func (m *Module) mutate(
	ctx context.Context,
	scope, action string,
	muted bool,
	raw map[string]any,
) (any, *actionbase.Error) {
	if m.config.ResolveTarget == nil || m.config.NewMember == nil || m.config.LookupMember == nil ||
		m.config.SaveMember == nil || m.config.PublishPolicy == nil || m.config.Conversation == nil {
		return nil, actionbase.InternalError(errors.New("member mutation dependencies are not configured"))
	}
	roomID, channelID := m.config.ResolveTarget(raw)
	if roomID == "" && channelID == "" {
		return nil, actionbase.BadRequest("room_id or channel_id is required")
	}
	userID := firstMemberID(actionbase.Params(raw))
	if userID == "" {
		return nil, actionbase.BadRequest("user_id is required")
	}

	member := m.config.NewMember(roomID, channelID, userID)
	existing, ok, err := m.config.LookupMember(ctx, roomID, userID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if ok {
		member = existing
		if channelID != "" {
			member.ChannelID = channelID
		}
	}
	if scope == "group" {
		member.ChannelID = ""
	}
	member.Membership = strings.TrimSpace(member.Membership)
	if member.Membership == "" {
		member.Membership = "join"
	}
	member.Muted = muted
	if err := m.config.SaveMember(ctx, member); err != nil {
		return nil, actionbase.InternalError(err)
	}
	if actionErr := m.config.PublishPolicy(ctx, member); actionErr != nil {
		return nil, actionErr
	}

	result := map[string]any{"status": "ok", "member": member}
	operation, conversation, err := m.config.Conversation.Operation(ctx, action, "ok", member.RoomID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	result["operation"] = operation
	if conversation != nil {
		result["conversation"] = *conversation
	}
	return result, nil
}

func firstMemberID(params actionbase.Params) string {
	if userID := params.FirstString("user_id", "user_mxid", "peer_mxid", "mxid"); userID != "" {
		return userID
	}
	return params.FirstListString("user_ids", "user_mxids", "peer_mxids", "invitees")
}
