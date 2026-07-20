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

func (m *Module) joinRequestHandler(action string) actionbase.Handler {
	return func(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
		return m.handleJoinRequest(ctx, action, raw)
	}
}

func (m *Module) handleJoinRequest(ctx context.Context, action string, raw map[string]any) (any, *actionbase.Error) {
	approved := action == actionChannelJoinRequestApprove
	if !approved && action != actionChannelJoinRequestReject {
		return nil, actionbase.InternalError(errors.New("unknown channel join-request action"))
	}
	if m.config.SaveMember == nil || m.config.SaveMemberGeneration == nil || m.config.PublishJoinRequest == nil ||
		m.config.CompleteJoinRequest == nil || m.config.Conversation == nil {
		return nil, actionbase.InternalError(errors.New("channel join-request dependencies are not configured"))
	}
	member, found, actionErr := m.loadMember(ctx, "channel", raw, func() string {
		return firstMemberID(actionbase.Params(raw))
	})
	if actionErr != nil {
		return nil, actionErr
	}
	if !found {
		return nil, actionbase.CodedError(http.StatusNotFound, actionbase.RequestNotFoundCode, "join request not found")
	}
	if !approved {
		joined, err := m.matrixJoined(ctx, member.RoomID, member.UserID)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		if joined {
			settlementCtx, cancel := actionbase.SettlementContext(ctx)
			defer cancel()
			member, actionErr = m.repairMatrixJoinedMember(settlementCtx, member)
			if actionErr != nil {
				return nil, actionErr
			}
			result, completionErr := m.config.CompleteJoinRequest(settlementCtx, true, member, raw)
			if completionErr != nil {
				return nil, completionErr
			}
			if result == nil {
				result = map[string]any{}
			}
			status := actionbase.String(result["status"])
			if status == "" {
				status = "joined"
				result["status"] = status
			}
			return m.attachOperation(settlementCtx, result, action, status, member.RoomID)
		}
	}
	requestID := actionbase.Params(raw).String("request_id")
	expectedRequestID := member.RequestID
	if requestID != "" && member.RequestID != "" && requestID != member.RequestID {
		return m.currentJoinRequestResult(ctx, action, member)
	}
	if member.RequestID == "" && requestID != "" {
		member.RequestID = requestID
	}
	membership := strings.ToLower(strings.TrimSpace(member.Membership))
	if approved && membership == "join" {
		settlementCtx, cancel := actionbase.SettlementContext(ctx)
		defer cancel()
		result, completionErr := m.config.CompleteJoinRequest(settlementCtx, true, member, raw)
		if completionErr != nil {
			return nil, completionErr
		}
		status := actionbase.String(result["status"])
		if status == "" {
			status = "joining"
			result["status"] = status
		}
		return m.attachOperation(settlementCtx, result, action, status, member.RoomID)
	}
	if !approved && membership == "join" {
		joined, err := m.matrixJoined(ctx, member.RoomID, member.UserID)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		status := "joining"
		if joined {
			status = "joined"
		}
		return m.attachOperation(ctx, m.joinRequestResult(ctx, status, member), action, status, member.RoomID)
	}
	if result, status, handled := replayedJoinRequestResult(approved, membership, member); handled {
		if m.config.ChannelSnapshot != nil {
			result["channel"] = m.config.ChannelSnapshot(ctx, member.ChannelID)
		}
		return m.attachOperation(ctx, result, action, status, member.RoomID)
	}
	if !joinRequestMutationAllowed(approved, membership) {
		return nil, actionbase.CodedError(http.StatusGone, actionbase.RequestExpiredCode, "join request expired")
	}

	settlementCtx, cancel := actionbase.SettlementContext(ctx)
	defer cancel()

	stateStatus := "rejected"
	member.Membership = "reject"
	if approved {
		stateStatus = "approved"
		member.Membership = "approved"
	}
	if m.config.SaveMemberGeneration == nil {
		return nil, actionbase.InternalError(errors.New("member state transition persistence is not configured"))
	}
	saved, err := m.config.SaveMemberGeneration(settlementCtx, member, expectedRequestID, membership)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !saved {
		current, ok, lookupErr := m.config.LookupMember(settlementCtx, member.RoomID, member.UserID)
		if lookupErr != nil {
			return nil, actionbase.InternalError(lookupErr)
		}
		if !ok {
			return nil, actionbase.InternalError(errors.New("join request disappeared during state transition"))
		}
		return m.currentJoinRequestResult(settlementCtx, action, current)
	}
	if actionErr := m.config.PublishJoinRequest(settlementCtx, member.RoomID, member.UserID, stateStatus, actionbase.Params(raw).String("reason"), member.RequestID); actionErr != nil {
		return nil, actionErr
	}
	result, actionErr := m.config.CompleteJoinRequest(settlementCtx, approved, member, raw)
	if actionErr != nil {
		return nil, actionErr
	}
	if result == nil {
		result = map[string]any{}
	}
	status := "rejected"
	if approved {
		status = actionbase.String(result["status"])
		if status == "" {
			status = "approved"
		}
	} else {
		if current := actionbase.String(result["status"]); current != "" {
			if strings.EqualFold(current, "reject") {
				result["status"] = status
			} else {
				status = current
			}
		} else {
			result["status"] = status
		}
	}
	return m.attachOperation(settlementCtx, result, action, status, member.RoomID)
}

func (m *Module) currentJoinRequestResult(ctx context.Context, action string, member dirextalkdomain.MemberRecord) (any, *actionbase.Error) {
	status := strings.ToLower(strings.TrimSpace(member.Membership))
	switch status {
	case "reject", "rejected":
		status = "rejected"
	case "join":
		joined, err := m.matrixJoined(ctx, member.RoomID, member.UserID)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		if joined {
			status = "joined"
		} else {
			status = "joining"
		}
	}
	result := m.joinRequestResult(ctx, status, member)
	if status == "join_failed" {
		result["error_code"] = actionbase.MatrixJoinFailedCode
	}
	return m.attachOperation(ctx, result, action, status, member.RoomID)
}

func replayedJoinRequestResult(approved bool, membership string, member dirextalkdomain.MemberRecord) (map[string]any, string, bool) {
	if approved {
		switch membership {
		case "reject", "rejected":
			return map[string]any{"status": "rejected", "member": member}, "rejected", true
		}
		return nil, "", false
	}
	switch membership {
	case "approved", "joining", "join_failed":
		result := map[string]any{"status": membership, "member": member}
		if membership == "joining" {
			result["error_code"] = actionbase.JoinResultUnconfirmedCode
		} else if membership == "join_failed" {
			result["error_code"] = actionbase.MatrixJoinFailedCode
		}
		return result, membership, true
	default:
		return nil, "", false
	}
}

func (m *Module) joinRequestResult(ctx context.Context, status string, member dirextalkdomain.MemberRecord) map[string]any {
	result := map[string]any{"status": status, "member": member}
	if status == "joining" {
		result["error_code"] = actionbase.JoinResultUnconfirmedCode
	}
	if m.config.ChannelSnapshot != nil {
		result["channel"] = m.config.ChannelSnapshot(ctx, member.ChannelID)
	}
	return result
}

func joinRequestMutationAllowed(approved bool, membership string) bool {
	membership = strings.ToLower(strings.TrimSpace(membership))
	if approved {
		switch membership {
		case "pending", "approved", "joining", "join_failed", "join":
			return true
		default:
			return false
		}
	}
	return membership == "pending" || membership == "reject" || membership == "rejected"
}

func (m *Module) matrixJoined(ctx context.Context, roomID, userID string) (bool, error) {
	if m.config.MatrixJoined == nil {
		return false, nil
	}
	return m.config.MatrixJoined(ctx, roomID, userID)
}

func (m *Module) repairMatrixJoinedMember(
	ctx context.Context,
	member dirextalkdomain.MemberRecord,
) (dirextalkdomain.MemberRecord, *actionbase.Error) {
	for attempt := 0; attempt < 2; attempt++ {
		joined := member
		joined.Membership = "join"
		if m.config.SaveMemberGeneration == nil {
			if err := m.config.SaveMember(ctx, joined); err != nil {
				return dirextalkdomain.MemberRecord{}, actionbase.InternalError(err)
			}
			return joined, nil
		}
		saved, err := m.config.SaveMemberGeneration(ctx, joined, member.RequestID, member.Membership)
		if err != nil {
			return dirextalkdomain.MemberRecord{}, actionbase.InternalError(err)
		}
		if saved {
			return joined, nil
		}
		current, found, err := m.config.LookupMember(ctx, member.RoomID, member.UserID)
		if err != nil {
			return dirextalkdomain.MemberRecord{}, actionbase.InternalError(err)
		}
		if !found {
			return dirextalkdomain.MemberRecord{}, actionbase.InternalError(errors.New("matrix joined member disappeared during projection repair"))
		}
		if strings.EqualFold(strings.TrimSpace(current.Membership), "join") {
			return current, nil
		}
		member = current
	}
	return dirextalkdomain.MemberRecord{}, actionbase.InternalError(errors.New("matrix joined member changed during projection repair"))
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
	if spec.requireInvite {
		membership := strings.ToLower(strings.TrimSpace(member.Membership))
		if found {
			joined, err := m.matrixJoined(ctx, member.RoomID, member.UserID)
			if err != nil {
				return nil, actionbase.InternalError(err)
			}
			if joined {
				settlementCtx, cancel := actionbase.SettlementContext(ctx)
				defer cancel()
				member, actionErr = m.repairMatrixJoinedMember(settlementCtx, member)
				if actionErr != nil {
					return nil, actionErr
				}
				return m.resultWithOperation(settlementCtx, action, "joined", member)
			}
		}
		if found && (membership == "reject" || membership == "rejected") {
			return m.resultWithOperation(ctx, action, spec.status, member)
		}
		if !found {
			return nil, actionbase.CodedError(http.StatusNotFound, actionbase.RequestNotFoundCode, spec.scope+" invite not found")
		}
		if membership != "invite" {
			return nil, actionbase.CodedError(http.StatusGone, actionbase.RequestExpiredCode, spec.scope+" invite expired")
		}
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
	roomID, channelID, err := m.config.ResolveTarget(ctx, raw)
	if err != nil {
		return dirextalkdomain.MemberRecord{}, false, actionbase.InternalError(err)
	}
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
	return m.attachOperation(ctx, result, action, status, member.RoomID)
}

func (m *Module) attachOperation(ctx context.Context, result map[string]any, action, status, roomID string) (any, *actionbase.Error) {
	result, err := m.HydrateResult(ctx, result, action, status, roomID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return result, nil
}

// HydrateResult adds the legacy operation and conversation presentation to an
// already-computed member result. It performs no Matrix or product-state
// writes, so durable recovery can safely use it while another worker owns the
// mutation lease.
func (m *Module) HydrateResult(
	ctx context.Context,
	result map[string]any,
	action,
	status,
	roomID string,
) (map[string]any, error) {
	operation, conversation, err := m.config.Conversation.Operation(ctx, action, status, roomID)
	if err != nil {
		return nil, err
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
