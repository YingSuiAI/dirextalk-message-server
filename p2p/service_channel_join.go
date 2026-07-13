package p2p

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

type channelInviteGrantStore interface {
	UpsertChannelInviteGrant(ctx context.Context, grant channelInviteGrant) error
	ListChannelInviteGrants(ctx context.Context) ([]channelInviteGrant, error)
}

func (s *Service) completeApprovedChannelJoin(ctx context.Context, member memberRecord, params map[string]any) (map[string]any, *apiError) {
	if member.UserID == "" {
		return nil, badRequest("user_id is required")
	}
	expectedRequestID := member.RequestID
	expectedMembership := member.Membership
	if joined, err := s.matrixMemberJoined(ctx, member.RoomID, member.UserID); err != nil {
		return nil, transportWriteError(err)
	} else if joined {
		if domainFromMXID(member.UserID) == s.serverName {
			return s.completeLocalApprovedChannelJoin(ctx, member, params)
		}
		member.Membership = "join"
		if current, stale, apiErr := s.persistRemoteChannelJoinGeneration(
			ctx, member, expectedRequestID, expectedMembership,
		); apiErr != nil {
			return nil, apiErr
		} else if stale {
			return current, nil
		}
		return map[string]any{"status": "joined", "room_id": member.RoomID, "member": member, "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	}
	member.Membership = "joining"
	if current, stale, apiErr := s.persistRemoteChannelJoinGeneration(
		ctx, member, expectedRequestID, expectedMembership,
	); apiErr != nil {
		return nil, apiErr
	} else if stale {
		return current, nil
	}
	if domainFromMXID(member.UserID) != s.serverName {
		if apiErr := s.ensureRemoteApprovedChannelInvite(ctx, member, params); apiErr != nil {
			if apiErr.Code == actionbase.JoinResultUnconfirmedCode {
				return map[string]any{
					"status": "joining", "member": member, "error": apiErr.Error,
					"error_code": actionbase.JoinResultUnconfirmedCode, "channel": s.channelSnapshot(ctx, member.ChannelID),
				}, nil
			}
			return nil, apiErr
		}
		return s.notifyRemoteChannelJoinResult(ctx, member, "approved", params)
	}
	if s.transport == nil {
		member.Membership = "approved"
		if current, stale, apiErr := s.persistRemoteChannelJoinGeneration(
			ctx, member, expectedRequestID, "joining",
		); apiErr != nil {
			return nil, apiErr
		} else if stale {
			return current, nil
		}
		return map[string]any{"status": "approved", "member": member, "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	}
	joinParams := cloneParams(params)
	joinParams["server_names"] = channelJoinServerNames(params["server_names"], member.RoomID)
	return s.completeLocalApprovedChannelJoin(ctx, member, joinParams)
}

func (s *Service) completeLocalApprovedChannelJoin(ctx context.Context, member memberRecord, params map[string]any) (map[string]any, *apiError) {
	attempt, apiErr := s.joinAndProjectRetainedRoomGeneration(ctx, "channel", &member, params)
	member = attempt.Member
	if attempt.Stale {
		return s.currentChannelJoinResult(ctx, member)
	}
	if apiErr == nil {
		return map[string]any{"status": "joined", "room_id": member.RoomID, "member": member, "channel": s.channelSnapshot(ctx, member.ChannelID)}, nil
	}
	if !attempt.Final {
		return nil, apiErr
	}
	if attempt.Busy || strings.EqualFold(strings.TrimSpace(member.Membership), "joining") {
		return map[string]any{
			"status": "joining", "member": member, "error": apiErr.Error,
			"error_code": fallbackString(apiErr.Code, actionbase.MatrixJoinUnconfirmedCode), "channel": s.channelSnapshot(ctx, member.ChannelID),
		}, nil
	}
	if strings.EqualFold(strings.TrimSpace(member.Membership), "join") ||
		strings.EqualFold(strings.TrimSpace(member.Membership), "joined") {
		return map[string]any{
			"status": "joined", "room_id": member.RoomID, "member": member, "error": apiErr.Error,
			"error_code": actionbase.OperationRecoveryCode, "channel": s.channelSnapshot(ctx, member.ChannelID),
		}, nil
	}
	return map[string]any{
		"status": "join_failed", "member": member, "error": apiErr.Error,
		"error_code": actionbase.MatrixJoinFailedCode, "channel": s.channelSnapshot(ctx, member.ChannelID),
	}, nil
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
	if operation, ok := recoverableOperationSnapshot(ctx); ok && operationHasExternalCommit(operation.Phase) && operation.CurrentRoomID == member.RoomID {
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
		RoomID:              member.RoomID,
		InviterMXID:         ownerMXID,
		InviteeMXID:         member.UserID,
		Reason:              trimString(params["reason"]),
		PublicJoinRequestID: fallbackString(trimString(params["request_id"]), member.RequestID),
		InviteRoomState:     inviteRoomState,
	}
	if err := s.transport.InviteUser(ctx, inviteReq); err != nil {
		if isAlreadyJoinedRoomError(err) {
			// A normal approval retry must never kick a member. A concurrent or
			// already-completed join is accepted only after Matrix confirms it.
			joined, confirmErr := s.matrixMemberJoined(ctx, member.RoomID, member.UserID)
			if confirmErr != nil {
				return transportWriteError(confirmErr)
			}
			if !joined {
				return actionbase.CodedError(http.StatusAccepted, actionbase.JoinResultUnconfirmedCode, "channel invite result is unconfirmed")
			}
			if markErr := markRecoverableOperation(ctx, operationPhaseMatrixCommitted, member.RoomID); markErr != nil {
				return recoverableOperationWriteError(ctx, markErr)
			}
			return nil
		}
		if ambiguousChannelJoinTransportError(err) {
			return actionbase.CodedError(http.StatusAccepted, actionbase.JoinResultUnconfirmedCode, "channel invite result is unconfirmed")
		}
		return transportWriteError(err)
	}
	if err := markRecoverableOperation(ctx, operationPhaseMatrixCommitted, member.RoomID); err != nil {
		return recoverableOperationWriteError(ctx, err)
	}
	return nil
}

func ambiguousChannelJoinTransportError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "timeout") || strings.Contains(message, "timed out") ||
		strings.Contains(message, "deadline exceeded") ||
		strings.Contains(message, "unexpected eof") || message == "eof"
}

func (s *Service) notifyRemoteChannelJoinResult(ctx context.Context, member memberRecord, status string, params map[string]any) (map[string]any, *apiError) {
	expectedMembership := member.Membership
	requestID := fallbackString(trimString(params["request_id"]), member.RequestID)
	if requestID == "" {
		if operation, ok := recoverableOperationSnapshot(ctx); ok {
			requestID = fallbackString(operation.RequestID, operation.OperationID)
		}
	}
	base := strings.TrimSpace(member.RequesterNodeBaseURL)
	if base == "" {
		base = trimString(params["requester_node_base_url"])
	}
	if base == "" {
		base = trimString(params["applicant_node_base_url"])
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
		if current, stale, apiErr := s.persistRemoteChannelJoinGeneration(ctx, member, requestID, expectedMembership); apiErr != nil {
			return nil, apiErr
		} else if stale {
			return current, nil
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
		"request_id":           requestID,
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
	if terminalErr := remoteJoinResultTerminalError(httpStatus, err); terminalErr != nil {
		return nil, terminalErr
	}
	if err != nil {
		if status == "approved" {
			member.Membership = "joining"
		} else {
			member.Membership = "reject"
		}
		if current, stale, apiErr := s.persistRemoteChannelJoinGeneration(ctx, member, requestID, expectedMembership); apiErr != nil {
			return nil, apiErr
		} else if stale {
			return current, nil
		}
		resultStatus := member.Membership
		if status == "rejected" {
			resultStatus = "rejected"
		}
		return map[string]any{
			"status": resultStatus, "member": member, "error": err.Error(),
			"error_code": actionbase.JoinResultUnconfirmedCode, "channel": s.channelSnapshot(ctx, member.ChannelID),
		}, nil
	}
	if httpStatus != http.StatusOK {
		if status == "approved" {
			member.Membership = "joining"
		} else {
			member.Membership = "reject"
		}
		if current, stale, apiErr := s.persistRemoteChannelJoinGeneration(ctx, member, requestID, expectedMembership); apiErr != nil {
			return nil, apiErr
		} else if stale {
			return current, nil
		}
		resultStatus := member.Membership
		if status == "rejected" {
			resultStatus = "rejected"
		}
		return map[string]any{
			"status": resultStatus, "member": member, "error": "target node join result failed",
			"error_code": actionbase.JoinResultUnconfirmedCode, "channel": s.channelSnapshot(ctx, member.ChannelID),
		}, nil
	}
	if err := markRecoverableOperation(ctx, operationPhaseCallbackAcknowledged, member.RoomID); err != nil {
		return nil, recoverableOperationWriteError(ctx, err)
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
	if current, stale, apiErr := s.persistRemoteChannelJoinGeneration(ctx, member, requestID, expectedMembership); apiErr != nil {
		return nil, apiErr
	} else if stale {
		return current, nil
	}
	remote["member"] = member
	remote["channel"] = s.channelSnapshot(ctx, member.ChannelID)
	if strings.EqualFold(remoteStatus, "join_failed") {
		remote["error_code"] = actionbase.MatrixJoinFailedCode
	}
	return remote, nil
}

func (s *Service) persistRemoteChannelJoinGeneration(
	ctx context.Context,
	member memberRecord,
	expectedRequestID string,
	expectedMembership string,
) (map[string]any, bool, *apiError) {
	saved, err := s.saveMemberIfState(ctx, member, expectedRequestID, expectedMembership)
	if err != nil {
		return nil, false, internalError(err)
	}
	if saved {
		return nil, false, nil
	}
	current, found, err := s.lookupMember(ctx, member.RoomID, member.UserID)
	if err != nil {
		return nil, false, internalError(err)
	}
	if !found {
		return nil, false, internalError(errors.New("channel join generation disappeared during callback settlement"))
	}
	result, apiErr := s.currentChannelJoinResult(ctx, current)
	return result, true, apiErr
}

func (s *Service) currentChannelJoinResult(ctx context.Context, current memberRecord) (map[string]any, *apiError) {
	status := strings.ToLower(strings.TrimSpace(current.Membership))
	result := map[string]any{"status": status, "member": current, "channel": s.channelSnapshot(ctx, current.ChannelID)}
	switch status {
	case "join", "joined":
		joined, joinedErr := s.matrixMemberJoined(ctx, current.RoomID, current.UserID)
		if joinedErr != nil {
			return nil, transportWriteError(joinedErr)
		}
		if joined {
			result["status"] = "joined"
			result["room_id"] = current.RoomID
		} else {
			result["status"] = "joining"
			result["error_code"] = actionbase.JoinResultUnconfirmedCode
		}
	case "reject", "rejected":
		result["status"] = "rejected"
	case "joining":
		result["error_code"] = actionbase.JoinResultUnconfirmedCode
	case "join_failed":
		result["error_code"] = actionbase.MatrixJoinFailedCode
	}
	return result, nil
}

func remoteJoinResultTerminalError(status int, err error) *apiError {
	if status < http.StatusBadRequest || status >= http.StatusInternalServerError {
		return nil
	}
	result := &apiError{Status: status, Error: http.StatusText(status)}
	var remoteErr *remotePublicActionError
	if errors.As(err, &remoteErr) {
		result.Error = remoteErr.Message
		result.Code = remoteErr.Code
		result.OperationID = remoteErr.OperationID
		result.CurrentRoomID = remoteErr.CurrentRoomID
	}
	if result.Code == "" {
		switch status {
		case http.StatusNotFound:
			result.Code = actionbase.RequestNotFoundCode
		case http.StatusGone:
			result.Code = actionbase.RequestExpiredCode
		}
	}
	return result
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
