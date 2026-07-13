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
	if remote, handled, actionErr := m.config.ForwardPublicJoinRequest(ctx, raw); actionErr != nil {
		return nil, actionErr
	} else if handled {
		return remote, nil
	}

	channel, ok, err := m.config.LookupChannel(ctx, channelID, roomID)
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
	if actionErr := m.config.RejectBlocked(ctx, "contact", userID); actionErr != nil {
		return nil, actionErr
	}
	existing, found, err := m.config.LookupMember(ctx, roomID, userID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if found && removedMembership(existing.Membership) {
		if channelID != "" {
			existing.ChannelID = channelID
		}
		if actionErr := m.config.PublishJoinRequest(ctx, roomID, userID, "rejected", params.String("reason")); actionErr != nil {
			return nil, actionErr
		}
		return map[string]any{"status": "rejected", "member": existing}, nil
	}

	member := existing
	if !found {
		member = m.config.NewMember(roomID, channelID, userID)
	}
	status := "pending"
	member.Membership = "pending"
	if strings.EqualFold(channel.JoinPolicy, "open") {
		status = "approved"
		member.Membership = "approved"
	}
	member.Role = fallback(member.Role, "member")
	if member.RequesterNodeBaseURL == "" {
		member.RequesterNodeBaseURL = fallback(params.String("requester_node_base_url"), params.String("applicant_node_base_url"))
	}
	applyMemberProfile(&member, params)
	if err := m.config.SaveMember(ctx, member); err != nil {
		return nil, actionbase.InternalError(err)
	}
	if actionErr := m.config.PublishJoinRequest(ctx, roomID, userID, status, params.String("reason")); actionErr != nil {
		return nil, actionErr
	}
	if strings.EqualFold(channel.JoinPolicy, "open") {
		result, actionErr := m.config.CompleteJoinRequest(ctx, true, member, raw)
		if actionErr != nil {
			return nil, actionErr
		}
		result["channel"] = m.config.ChannelSnapshot(ctx, channelID)
		return result, nil
	}
	channel.MemberStatus = member.Membership
	channel.Role = dirextalkdomain.NormalizeProductMemberRole(member.Role)
	channel.IsOwned = dirextalkdomain.ProductOwnerRole(channel.Role)
	return map[string]any{"status": status, "member": member, "channel": channel}, nil
}

func (m *Module) publicJoinRequestConfigured() bool {
	return m.config.ResolveTarget != nil &&
		m.config.OwnerMXID != nil &&
		m.config.ForwardPublicJoinRequest != nil &&
		m.config.LookupChannel != nil &&
		m.config.RejectBlocked != nil &&
		m.config.LookupMember != nil &&
		m.config.NewMember != nil &&
		m.config.SaveMember != nil &&
		m.config.PublishJoinRequest != nil &&
		m.config.CompleteJoinRequest != nil &&
		m.config.ChannelSnapshot != nil
}

// ChannelPublicJoinResult applies an owner-node decision on the requester node.
func (m *Module) ChannelPublicJoinResult(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	if m.config.ResolveTarget == nil || m.config.OwnerMXID == nil || m.config.LookupMember == nil ||
		m.config.SaveMember == nil || m.config.ApplyLocalProfile == nil ||
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
		return nil, actionbase.StatusError(http.StatusNotFound, "join request not found")
	}
	memberStatus := strings.ToLower(strings.TrimSpace(member.Membership))
	switch memberStatus {
	case "pending", "approved", "joining", "join_failed", "invite":
	default:
		return nil, actionbase.StatusError(http.StatusNotFound, "join request not found")
	}
	if channelID != "" {
		member.ChannelID = channelID
	}
	m.config.ApplyLocalProfile(&member)
	switch strings.ToLower(params.String("status")) {
	case "rejected":
		if memberStatus == "invite" {
			return nil, actionbase.StatusError(http.StatusNotFound, "join request not found")
		}
		member.Membership = "reject"
		if err := m.config.SaveMember(ctx, member); err != nil {
			return nil, actionbase.InternalError(err)
		}
		if m.config.EmitJoinRequestChanged != nil {
			m.config.EmitJoinRequestChanged(ctx, member, "rejected")
		}
		return map[string]any{"status": "rejected", "member": member, "channel": m.config.ChannelSnapshot(ctx, member.ChannelID)}, nil
	case "approved", "joining", "joined":
		return m.config.CompleteJoinRequest(ctx, true, member, raw)
	default:
		return nil, actionbase.BadRequest("status must be approved or rejected")
	}
}
