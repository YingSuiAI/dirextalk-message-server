package p2p

import (
	"context"
	"net/http"
	"strings"
	"time"
)

func (s *Service) channelJoinRequest(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID, channelID := s.memberTarget(params)
	if roomID == "" && channelID == "" {
		return nil, badRequest("room_id or channel_id is required")
	}
	userID := firstMemberID(params)
	if userID == "" {
		s.mu.Lock()
		userID = s.ownerMXID
		s.mu.Unlock()
		params["user_mxid"] = userID
	}
	if remote, handled, apiErr := s.remoteChannelJoinRequest(ctx, params); apiErr != nil {
		return nil, apiErr
	} else if handled {
		return remote, nil
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "channel not found")
	}
	roomID = ch.RoomID
	channelID = ch.ChannelID
	if apiErr := s.rejectIfBlocked(ctx, "channel", roomID); apiErr != nil {
		return nil, apiErr
	}
	if !strings.EqualFold(ch.Visibility, "public") {
		return nil, statusError(403, "channel is private")
	}
	if strings.EqualFold(ch.JoinPolicy, "invite") {
		return nil, statusError(403, "channel requires invite")
	}
	if userID == "" {
		return nil, badRequest("user_id is required")
	}
	if apiErr := s.rejectIfBlocked(ctx, "contact", userID); apiErr != nil {
		return nil, apiErr
	}
	existing, ok, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return nil, internalError(err)
	}
	if ok && memberRemoved(existing.Membership) {
		if channelID != "" {
			existing.ChannelID = channelID
		}
		if apiErr := s.publishJoinRequestState(ctx, roomID, userID, "rejected", trimString(params["reason"])); apiErr != nil {
			return nil, apiErr
		}
		return map[string]any{"status": "rejected", "member": existing}, nil
	}
	member := existing
	if !ok {
		member = s.memberRecordFor(roomID, channelID, userID)
	}
	status := "pending"
	member.Membership = "pending"
	if strings.EqualFold(ch.JoinPolicy, "open") {
		status = "approved"
		member.Membership = "approved"
	}
	member.Role = fallbackString(member.Role, "member")
	if member.RequesterNodeBaseURL == "" {
		member.RequesterNodeBaseURL = fallbackString(trimString(params["requester_node_base_url"]), trimString(params["applicant_node_base_url"]))
	}
	applyMemberProfileParams(&member, params)
	if err := s.saveMember(ctx, member); err != nil {
		return nil, internalError(err)
	}
	stateStatus := "pending"
	if strings.EqualFold(ch.JoinPolicy, "open") {
		stateStatus = "approved"
	}
	if apiErr := s.publishJoinRequestState(ctx, roomID, userID, stateStatus, trimString(params["reason"])); apiErr != nil {
		return nil, apiErr
	}
	if strings.EqualFold(ch.JoinPolicy, "open") {
		result, apiErr := s.completeApprovedChannelJoin(ctx, member, params)
		if apiErr != nil {
			return nil, apiErr
		}
		result["channel"] = s.channelSnapshot(ctx, channelID)
		return result, nil
	}
	ch.MemberStatus = member.Membership
	ch.Role = normalizeProductMemberRole(member.Role)
	ch.IsOwned = productOwnerRole(ch.Role)
	return map[string]any{"status": status, "member": member, "channel": ch}, nil
}

func (s *Service) channelJoinResult(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID, channelID := s.memberTarget(params)
	if roomID == "" && channelID == "" {
		return nil, badRequest("room_id or channel_id is required")
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	userID := fallbackString(firstMemberID(params), ownerMXID)
	if userID != ownerMXID {
		return nil, statusError(http.StatusForbidden, "join result user must be local owner")
	}
	member, ok, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "join request not found")
	}
	memberStatus := strings.ToLower(strings.TrimSpace(member.Membership))
	switch memberStatus {
	case "pending", "approved", "joining", "join_failed", "invite":
	default:
		return nil, statusError(404, "join request not found")
	}
	if channelID != "" {
		member.ChannelID = channelID
	}
	s.applyLocalOwnerMemberProfile(&member)
	switch strings.ToLower(trimString(params["status"])) {
	case "rejected":
		if memberStatus == "invite" {
			return nil, statusError(404, "join request not found")
		}
		member.Membership = "reject"
		if err := s.saveMember(ctx, member); err != nil {
			return nil, internalError(err)
		}
		_ = s.appendP2PEvent(ctx, p2pEvent{
			Type:    "channel.join_request.changed",
			RoomID:  member.RoomID,
			Payload: map[string]any{"user_id": member.UserID, "status": "rejected", "channel_id": member.ChannelID},
		})
		return map[string]any{"status": "rejected", "member": member, "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	case "approved", "joining", "joined":
		return s.completeApprovedChannelJoin(ctx, member, params)
	default:
		return nil, badRequest("status must be approved or rejected")
	}
}

func (s *Service) completeApprovedChannelJoin(ctx context.Context, member memberRecord, params map[string]any) (map[string]any, *apiError) {
	if member.UserID == "" {
		return nil, badRequest("user_id is required")
	}
	member.Membership = "joining"
	if err := s.saveMember(ctx, member); err != nil {
		return nil, internalError(err)
	}
	if domainFromMXID(member.UserID) != s.serverName {
		if apiErr := s.ensureRemoteApprovedChannelInvite(ctx, member, params); apiErr != nil {
			return nil, apiErr
		}
		return s.notifyRemoteChannelJoinResult(ctx, member, "approved", params)
	}
	if s.transport == nil {
		member.Membership = "approved"
		if err := s.saveMember(ctx, member); err != nil {
			return nil, internalError(err)
		}
		return map[string]any{"status": "approved", "member": member, "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	}
	joinParams := cloneParams(params)
	joinParams["server_names"] = channelJoinServerNames(params["server_names"], member.RoomID)
	if apiErr := s.joinAndProjectRetainedRoom(ctx, "channel", &member, joinParams); apiErr != nil {
		member.Membership = "join_failed"
		if saveErr := s.saveMember(ctx, member); saveErr != nil {
			return nil, internalError(saveErr)
		}
		return map[string]any{"status": "join_failed", "member": member, "error": apiErr.Error, "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	}
	return map[string]any{"status": "joined", "room_id": member.RoomID, "member": member, "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
}

func (s *Service) ensureRemoteApprovedChannelInvite(ctx context.Context, member memberRecord, params map[string]any) *apiError {
	if s.transport == nil {
		return nil
	}
	if trimString(params["requester_node_base_url"]) == "" &&
		trimString(params["applicant_node_base_url"]) == "" &&
		member.RequesterNodeBaseURL == "" {
		return nil
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	inviteRoomState, apiErr := s.productInviteRoomState(ctx, "channel", member.RoomID, member.ChannelID)
	if apiErr != nil {
		return apiErr
	}
	inviteReq := InviteUserRequest{
		RoomID:          member.RoomID,
		InviterMXID:     ownerMXID,
		InviteeMXID:     member.UserID,
		Reason:          trimString(params["reason"]),
		InviteRoomState: inviteRoomState,
	}
	if err := s.transport.InviteUser(ctx, inviteReq); err != nil {
		if isAlreadyJoinedRoomError(err) {
			return s.kickAndInviteStaleJoinedRoomMember(ctx, member, inviteReq)
		}
		return transportWriteError(err)
	}
	return nil
}

func (s *Service) notifyRemoteChannelJoinResult(ctx context.Context, member memberRecord, status string, params map[string]any) (map[string]any, *apiError) {
	base := trimString(params["requester_node_base_url"])
	if base == "" {
		base = trimString(params["applicant_node_base_url"])
	}
	if base == "" {
		base = member.RequesterNodeBaseURL
	}
	if base == "" {
		switch status {
		case "approved":
			member.Membership = "approved"
		case "rejected":
			member.Membership = "reject"
		default:
			member.Membership = status
		}
		if err := s.saveMember(ctx, member); err != nil {
			return nil, internalError(err)
		}
		resultStatus := member.Membership
		if status == "rejected" {
			resultStatus = "rejected"
		}
		return map[string]any{"status": resultStatus, "member": member, "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	}
	remoteParams := map[string]any{
		"room_id":              member.RoomID,
		"channel_id":           member.ChannelID,
		"user_id":              member.UserID,
		"status":               status,
		"reason":               trimString(params["reason"]),
		"request_id":           trimString(params["request_id"]),
		"server_names":         channelJoinServerNames(params["server_names"], member.RoomID),
		"remote_node_base_url": base,
	}
	const maxAttempts = 8
	var remote map[string]any
	var httpStatus int
	var err error
retryLoop:
	for attempt := 0; attempt < maxAttempts; attempt++ {
		remote = nil
		httpStatus, err = s.remotePublicAction(ctx, domainFromMXID(member.UserID), "channels.public.join_result", remoteParams, &remote)
		remoteStatus := fallbackString(trimString(remote["status"]), status)
		if status != "approved" || err != nil || httpStatus != http.StatusOK || !strings.EqualFold(remoteStatus, "join_failed") || attempt == maxAttempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break retryLoop
		case <-time.After(time.Duration(attempt+1) * 500 * time.Millisecond):
		}
	}
	if err != nil {
		if status == "approved" {
			member.Membership = "join_failed"
		} else {
			member.Membership = "reject"
		}
		if saveErr := s.saveMember(ctx, member); saveErr != nil {
			return nil, internalError(saveErr)
		}
		return map[string]any{"status": member.Membership, "member": member, "error": err.Error(), "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	}
	if httpStatus != http.StatusOK {
		if status == "approved" {
			member.Membership = "join_failed"
		} else {
			member.Membership = "reject"
		}
		if saveErr := s.saveMember(ctx, member); saveErr != nil {
			return nil, internalError(saveErr)
		}
		return map[string]any{"status": member.Membership, "member": member, "error": "target node join result failed", "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	}
	remoteStatus := fallbackString(trimString(remote["status"]), status)
	switch remoteStatus {
	case "joined":
		member.Membership = "join"
	case "rejected":
		member.Membership = "reject"
	default:
		member.Membership = remoteStatus
	}
	if err := s.saveMember(ctx, member); err != nil {
		return nil, internalError(err)
	}
	remote["member"] = member
	remote["channel"] = s.channelSnapshot(ctx, member.ChannelID)
	return remote, nil
}

func (s *Service) publicP2PBaseURL() string {
	base, ok := normalizeRemoteNodeBaseURL(strings.TrimRight(s.homeserver, "/") + "/_p2p")
	if !ok {
		return ""
	}
	return base.String()
}

func cloneParams(params map[string]any) map[string]any {
	if len(params) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(params))
	for key, value := range params {
		out[key] = value
	}
	return out
}
