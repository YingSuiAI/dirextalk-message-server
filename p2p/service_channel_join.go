package p2p

import (
	"context"
	"net/http"
	"strings"
	"time"
)

type channelInviteGrantStore interface {
	UpsertChannelInviteGrant(ctx context.Context, grant channelInviteGrant) error
	ListChannelInviteGrants(ctx context.Context) ([]channelInviteGrant, error)
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
