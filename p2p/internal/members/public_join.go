package members

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

// ChannelPublicJoinRequest records or immediately completes a public-channel
// join request after giving the remote owner-node adapter first handling.
func (m *Module) ChannelPublicJoinRequest(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	if !m.publicJoinRequestConfigured() {
		return nil, actionbase.InternalError(errors.New("public channel join-request dependencies are not configured"))
	}
	params := actionbase.Params(raw)
	roomID, channelID, err := m.config.ResolveTarget(ctx, raw)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if roomID == "" && channelID == "" {
		return nil, actionbase.BadRequest("room_id or channel_id is required")
	}
	userID := firstMemberID(params)
	if userID == "" {
		userID = m.config.OwnerMXID()
		raw["user_mxid"] = userID
	}
	settlementCtx, cancel := actionbase.SettlementContext(ctx)
	defer cancel()
	if remote, handled, actionErr := m.config.ForwardPublicJoinRequest(settlementCtx, raw); actionErr != nil {
		return nil, actionErr
	} else if handled {
		return remote, nil
	}

	channel, ok, err := m.config.LookupChannel(settlementCtx, channelID, roomID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.StatusError(http.StatusNotFound, "channel not found")
	}
	roomID, channelID = channel.RoomID, channel.ChannelID
	if !strings.EqualFold(channel.Visibility, "public") {
		return nil, actionbase.StatusError(http.StatusForbidden, "channel is private")
	}
	if strings.EqualFold(channel.JoinPolicy, "invite") {
		return nil, actionbase.StatusError(http.StatusForbidden, "channel requires invite")
	}
	if userID == "" {
		return nil, actionbase.BadRequest("user_id is required")
	}
	if actionErr := m.config.RejectBlocked(settlementCtx, "contact", userID); actionErr != nil {
		return nil, actionErr
	}
	existing, found, err := m.config.LookupMember(settlementCtx, roomID, userID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	requestID := params.String("request_id")
	newGeneration := found && requestID != "" && existing.RequestID != requestID &&
		publicJoinGenerationMayRestart(existing.Membership)
	if found && existing.RequestID != "" && requestID != "" && existing.RequestID != requestID && !newGeneration {
		return m.currentPublicJoinState(settlementCtx, existing, channel)
	}
	if requestID == "" && found {
		requestID = existing.RequestID
	}
	if requestID == "" {
		if m.config.NewRequestID == nil {
			return nil, actionbase.InternalError(errors.New("channel join request ID generator is not configured"))
		}
		requestID = strings.TrimSpace(m.config.NewRequestID())
		if requestID == "" {
			return nil, actionbase.InternalError(errors.New("channel join request ID generator returned an empty ID"))
		}
	}
	raw["request_id"] = requestID
	if found && existing.RequestID == "" && !newGeneration {
		if m.config.SaveMemberGeneration == nil {
			return nil, actionbase.InternalError(errors.New("member generation persistence is not configured"))
		}
		bound := existing
		bound.RequestID = requestID
		saved, err := m.config.SaveMemberGeneration(settlementCtx, bound, "", existing.Membership)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		if !saved {
			current, ok, lookupErr := m.config.LookupMember(settlementCtx, roomID, userID)
			if lookupErr != nil {
				return nil, actionbase.InternalError(lookupErr)
			}
			if !ok {
				return nil, actionbase.InternalError(errors.New("channel join generation disappeared while binding request ID"))
			}
			return m.currentPublicJoinState(settlementCtx, current, channel)
		}
		existing = bound
	}
	if found && removedMembership(existing.Membership) {
		if channelID != "" {
			existing.ChannelID = channelID
		}
		if actionErr := m.config.PublishJoinRequest(settlementCtx, roomID, userID, "rejected", params.String("reason"), existing.RequestID); actionErr != nil {
			return nil, actionErr
		}
		return map[string]any{"status": "rejected", "member": existing}, nil
	}
	if found && !newGeneration {
		existingStatus := strings.ToLower(strings.TrimSpace(existing.Membership))
		switch existingStatus {
		case "join", "joined":
			joined, joinedErr := m.matrixJoined(settlementCtx, roomID, userID)
			if joinedErr != nil {
				return nil, actionbase.InternalError(joinedErr)
			}
			if joined {
				return map[string]any{"status": "joined", "room_id": roomID, "member": existing, "channel": channel}, nil
			}
			if strings.EqualFold(channel.JoinPolicy, "open") {
				return m.config.CompleteJoinRequest(settlementCtx, true, existing, raw)
			}
			return m.currentPublicJoinState(settlementCtx, existing, channel)
		case "reject", "rejected":
			return map[string]any{"status": "rejected", "member": existing, "channel": channel}, nil
		case "pending":
			if !strings.EqualFold(channel.JoinPolicy, "open") {
				return map[string]any{"status": "pending", "member": existing, "channel": channel}, nil
			}
		case "approved", "joining", "join_failed":
			if strings.EqualFold(channel.JoinPolicy, "open") {
				return m.config.CompleteJoinRequest(settlementCtx, true, existing, raw)
			}
			return m.currentPublicJoinState(settlementCtx, existing, channel)
		}
	}

	member := existing
	if !found {
		member = m.config.NewMember(roomID, channelID, userID)
	} else if newGeneration {
		member.JoinedAt = m.now().UTC().UnixMilli()
	}
	status := "pending"
	member.Membership = "pending"
	if strings.EqualFold(channel.JoinPolicy, "open") {
		status = "approved"
		member.Membership = "approved"
	}
	member.Role = fallback(member.Role, "member")
	member.RequestID = requestID
	requesterNodeBaseURL := fallback(params.String("requester_node_base_url"), params.String("applicant_node_base_url"))
	if requesterNodeBaseURL != "" && (newGeneration || member.RequesterNodeBaseURL == "") {
		member.RequesterNodeBaseURL = requesterNodeBaseURL
	}
	ApplyMemberProfile(&member, params)
	if newGeneration {
		if m.config.SaveMemberGeneration == nil {
			return nil, actionbase.InternalError(errors.New("member generation persistence is not configured"))
		}
		saved, err := m.config.SaveMemberGeneration(settlementCtx, member, existing.RequestID, existing.Membership)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		if !saved {
			current, ok, lookupErr := m.config.LookupMember(settlementCtx, roomID, userID)
			if lookupErr != nil {
				return nil, actionbase.InternalError(lookupErr)
			}
			if !ok {
				return nil, actionbase.InternalError(errors.New("channel join generation disappeared during restart"))
			}
			return m.currentPublicJoinState(settlementCtx, current, channel)
		}
	} else if err := m.config.SaveMember(settlementCtx, member); err != nil {
		return nil, actionbase.InternalError(err)
	}
	if actionErr := m.config.PublishJoinRequest(settlementCtx, roomID, userID, status, params.String("reason"), member.RequestID); actionErr != nil {
		return nil, actionErr
	}
	if strings.EqualFold(channel.JoinPolicy, "open") {
		result, actionErr := m.config.CompleteJoinRequest(settlementCtx, true, member, raw)
		if actionErr != nil {
			return nil, actionErr
		}
		result["channel"] = m.config.ChannelSnapshot(settlementCtx, channelID)
		return result, nil
	}
	channel.MemberStatus = member.Membership
	channel.Role = dirextalkdomain.NormalizeProductMemberRole(member.Role)
	channel.IsOwned = dirextalkdomain.ProductOwnerRole(channel.Role)
	return map[string]any{"status": status, "member": member, "channel": channel}, nil
}

func publicJoinGenerationMayRestart(membership string) bool {
	switch strings.ToLower(strings.TrimSpace(membership)) {
	case "reject", "rejected", "leave", "left":
		return true
	default:
		return false
	}
}

func (m *Module) publicJoinRequestConfigured() bool {
	return m.config.ResolveTarget != nil &&
		m.config.OwnerMXID != nil &&
		m.config.ForwardPublicJoinRequest != nil &&
		m.config.NewRequestID != nil &&
		m.config.LookupChannel != nil &&
		m.config.RejectBlocked != nil &&
		m.config.LookupMember != nil &&
		m.config.NewMember != nil &&
		m.config.SaveMember != nil &&
		m.config.SaveMemberGeneration != nil &&
		m.config.PublishJoinRequest != nil &&
		m.config.CompleteJoinRequest != nil &&
		m.config.ChannelSnapshot != nil
}

func (m *Module) currentPublicJoinState(ctx context.Context, member dirextalkdomain.MemberRecord, channel dirextalkdomain.Channel) (any, *actionbase.Error) {
	status := strings.ToLower(strings.TrimSpace(member.Membership))
	result := map[string]any{"status": status, "member": member, "channel": channel}
	switch status {
	case "join", "joined":
		joined, err := m.matrixJoined(ctx, member.RoomID, member.UserID)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		if joined {
			result["status"] = "joined"
			result["room_id"] = member.RoomID
			return result, nil
		}
		result["status"] = "joining"
		result["error_code"] = actionbase.JoinResultUnconfirmedCode
	case "reject", "rejected":
		result["status"] = "rejected"
	case "joining":
		result["error_code"] = actionbase.JoinResultUnconfirmedCode
	case "join_failed":
		result["error_code"] = actionbase.MatrixJoinFailedCode
	}
	return result, nil
}

// ChannelPublicJoinResult applies an owner-node decision on the requester node.
func (m *Module) ChannelPublicJoinResult(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	if m.config.ResolveTarget == nil || m.config.OwnerMXID == nil || m.config.LookupMember == nil ||
		m.config.SaveMember == nil || m.config.SaveMemberGeneration == nil || m.config.ApplyLocalProfile == nil ||
		m.config.CompleteJoinRequest == nil || m.config.ChannelSnapshot == nil {
		return nil, actionbase.InternalError(errors.New("public channel join-result dependencies are not configured"))
	}
	params := actionbase.Params(raw)
	roomID, channelID, err := m.config.ResolveTarget(ctx, raw)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if roomID == "" && channelID == "" {
		return nil, actionbase.BadRequest("room_id or channel_id is required")
	}
	ownerMXID := m.config.OwnerMXID()
	userID := firstMemberID(params)
	if userID == "" {
		userID = ownerMXID
	}
	if userID != ownerMXID {
		return nil, actionbase.StatusError(http.StatusForbidden, "join result user must be local owner")
	}
	member, ok, err := m.config.LookupMember(ctx, roomID, userID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.CodedError(http.StatusNotFound, actionbase.RequestNotFoundCode, "join request not found")
	}
	expectedRequestID := member.RequestID
	expectedMembership := member.Membership
	requestID := params.String("request_id")
	if member.RequestID != "" && requestID != "" && member.RequestID != requestID {
		return m.currentPublicJoinState(ctx, member, m.config.ChannelSnapshot(ctx, member.ChannelID))
	}
	if member.RequestID == "" && requestID != "" {
		member.RequestID = requestID
		saved, saveErr := m.config.SaveMemberGeneration(ctx, member, expectedRequestID, expectedMembership)
		if saveErr != nil {
			return nil, actionbase.InternalError(saveErr)
		}
		if !saved {
			current, found, lookupErr := m.config.LookupMember(ctx, roomID, userID)
			if lookupErr != nil {
				return nil, actionbase.InternalError(lookupErr)
			}
			if !found {
				return nil, actionbase.InternalError(errors.New("join request disappeared while binding callback generation"))
			}
			return m.currentPublicJoinState(ctx, current, m.config.ChannelSnapshot(ctx, current.ChannelID))
		}
		expectedRequestID = member.RequestID
		if expectedMembership == "" {
			expectedMembership = member.Membership
		}
	}
	memberStatus := strings.ToLower(strings.TrimSpace(member.Membership))
	switch memberStatus {
	case "pending", "approved", "joining", "join_failed", "invite", "join", "joined", "reject", "rejected":
	default:
		return nil, actionbase.CodedError(http.StatusGone, actionbase.RequestExpiredCode, "join request expired")
	}
	if channelID != "" {
		member.ChannelID = channelID
	}
	m.config.ApplyLocalProfile(&member)
	switch strings.ToLower(params.String("status")) {
	case "rejected":
		joined, joinedErr := m.matrixJoined(ctx, member.RoomID, member.UserID)
		if joinedErr != nil {
			return nil, actionbase.InternalError(joinedErr)
		}
		if joined {
			settlementCtx, cancel := actionbase.SettlementContext(ctx)
			defer cancel()
			member.Membership = "join"
			return m.config.CompleteJoinRequest(settlementCtx, true, member, raw)
		}
		if memberStatus == "reject" || memberStatus == "rejected" {
			return map[string]any{"status": "rejected", "member": member, "channel": m.config.ChannelSnapshot(ctx, member.ChannelID)}, nil
		}
		if memberStatus == "invite" {
			return nil, actionbase.CodedError(http.StatusGone, actionbase.RequestExpiredCode, "join request expired")
		}
		switch memberStatus {
		case "approved", "joining", "join_failed":
			// An owner may have lost the callback ACK while the requester is
			// already resolving the approval. A delayed rejection cannot cancel
			// that in-flight generation; return its current recoverable state.
			return m.currentPublicJoinState(ctx, member, m.config.ChannelSnapshot(ctx, member.ChannelID))
		}
		settlementCtx, cancel := actionbase.SettlementContext(ctx)
		defer cancel()
		member.Membership = "reject"
		saved, saveErr := m.config.SaveMemberGeneration(settlementCtx, member, expectedRequestID, expectedMembership)
		if saveErr != nil {
			return nil, actionbase.InternalError(saveErr)
		}
		if !saved {
			current, found, lookupErr := m.config.LookupMember(settlementCtx, roomID, userID)
			if lookupErr != nil {
				return nil, actionbase.InternalError(lookupErr)
			}
			if !found {
				return nil, actionbase.InternalError(errors.New("join request disappeared during rejected callback settlement"))
			}
			return m.currentPublicJoinState(settlementCtx, current, m.config.ChannelSnapshot(settlementCtx, current.ChannelID))
		}
		if m.config.EmitJoinRequestChanged != nil {
			m.config.EmitJoinRequestChanged(settlementCtx, member, "rejected")
		}
		return map[string]any{"status": "rejected", "member": member, "channel": m.config.ChannelSnapshot(settlementCtx, member.ChannelID)}, nil
	case "approved", "joining", "joined":
		settlementCtx, cancel := actionbase.SettlementContext(ctx)
		defer cancel()
		return m.config.CompleteJoinRequest(settlementCtx, true, member, raw)
	default:
		return nil, actionbase.BadRequest("status must be approved or rejected")
	}
}
