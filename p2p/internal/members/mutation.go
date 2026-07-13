package members

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

type lifecycleSpec struct {
	scope         string
	membership    string
	status        string
	forceOwner    bool
	requireInvite bool
	kick          bool
	leave         bool
}

var lifecycleSpecs = map[string]lifecycleSpec{
	actionChannelLeave:      {scope: "channel", membership: "leave", status: "ok", forceOwner: true, leave: true},
	actionChannelRemove:     {scope: "channel", membership: "remove", status: "ok", kick: true},
	actionGroupInviteReject: {scope: "group", membership: "reject", status: "rejected", forceOwner: true, requireInvite: true},
	actionGroupLeave:        {scope: "group", membership: "leave", status: "ok", forceOwner: true, leave: true},
	actionGroupRemove:       {scope: "group", membership: "remove", status: "ok", kick: true},
}

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
	member, _, actionErr := m.loadMember(ctx, scope, raw, func() string {
		return firstMemberID(actionbase.Params(raw))
	})
	if actionErr != nil {
		return nil, actionErr
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

	return m.resultWithOperation(ctx, action, "ok", member)
}

func (m *Module) lifecycleHandler(action string) actionbase.Handler {
	return func(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
		return m.HandleLifecycle(ctx, action, raw)
	}
}

// HandleLifecycle applies the shared leave, remove, and invite-reject workflow.
func (m *Module) HandleLifecycle(ctx context.Context, action string, raw map[string]any) (any, *actionbase.Error) {
	spec, ok := lifecycleSpecs[action]
	if !ok {
		return nil, actionbase.InternalError(errors.New("unknown member lifecycle action"))
	}
	if m.config.SaveMember == nil || m.config.Conversation == nil ||
		(spec.forceOwner && m.config.OwnerMXID == nil) ||
		(spec.kick && (m.config.KickMember == nil || m.config.OwnerMXID == nil)) ||
		(spec.leave && m.config.LeaveMember == nil) {
		return nil, actionbase.InternalError(errors.New("member lifecycle dependencies are not configured"))
	}
	member, found, actionErr := m.loadMember(ctx, spec.scope, raw, func() string {
		if spec.forceOwner {
			return m.config.OwnerMXID()
		}
		return firstMemberID(actionbase.Params(raw))
	})
	if actionErr != nil {
		return nil, actionErr
	}
	if spec.requireInvite && (!found || !strings.EqualFold(strings.TrimSpace(member.Membership), "invite")) {
		return nil, actionbase.StatusError(http.StatusNotFound, spec.scope+" invite not found")
	}
	if (spec.leave || spec.kick) && strings.EqualFold(member.Role, "owner") {
		return nil, actionbase.StatusError(http.StatusConflict, spec.scope+" owner cannot leave; dissolve the "+spec.scope+" instead")
	}

	member.Membership = spec.membership
	params := actionbase.Params(raw)
	if spec.kick {
		if actionErr := m.config.KickMember(ctx, member.RoomID, m.config.OwnerMXID(), member.UserID, params.String("reason")); actionErr != nil {
			return nil, actionErr
		}
	}
	if spec.leave {
		if actionErr := m.config.LeaveMember(ctx, member.RoomID, member.UserID); actionErr != nil {
			return nil, actionErr
		}
	}
	if err := m.config.SaveMember(ctx, member); err != nil {
		return nil, actionbase.InternalError(err)
	}
	return m.resultWithOperation(ctx, action, spec.status, member)
}

func (m *Module) loadMember(
	ctx context.Context,
	scope string,
	raw map[string]any,
	userID func() string,
) (dirextalkdomain.MemberRecord, bool, *actionbase.Error) {
	if m.config.ResolveTarget == nil || m.config.NewMember == nil || m.config.LookupMember == nil {
		return dirextalkdomain.MemberRecord{}, false, actionbase.InternalError(errors.New("member lookup dependencies are not configured"))
	}
	roomID, channelID := m.config.ResolveTarget(raw)
	if roomID == "" && channelID == "" {
		return dirextalkdomain.MemberRecord{}, false, actionbase.BadRequest("room_id or channel_id is required")
	}
	resolvedUserID := userID()
	if resolvedUserID == "" {
		return dirextalkdomain.MemberRecord{}, false, actionbase.BadRequest("user_id is required")
	}
	member := m.config.NewMember(roomID, channelID, resolvedUserID)
	existing, found, err := m.config.LookupMember(ctx, roomID, resolvedUserID)
	if err != nil {
		return dirextalkdomain.MemberRecord{}, false, actionbase.InternalError(err)
	}
	if found {
		member = existing
		if channelID != "" {
			member.ChannelID = channelID
		}
	}
	if scope == "group" {
		member.ChannelID = ""
	}
	return member, found, nil
}

func (m *Module) resultWithOperation(ctx context.Context, action, status string, member dirextalkdomain.MemberRecord) (any, *actionbase.Error) {
	result := map[string]any{"status": status, "member": member}
	operation, conversation, err := m.config.Conversation.Operation(ctx, action, status, member.RoomID)
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
