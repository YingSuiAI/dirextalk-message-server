package p2p

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	membersmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/members"
	operationsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/operations"
)

type roomReactivationDecision struct {
	Status            string
	NeedsRebuild      bool
	RebuildGeneration string
}

func (s *Service) notifyRemoteRoomReactivation(ctx context.Context, scope string, member memberRecord, params map[string]any) (roomReactivationDecision, *apiError) {
	rebuildGeneration := trimString(params["rebuild_generation"])
	if !membersmodule.ValidRebuildGeneration(rebuildGeneration) {
		return roomReactivationDecision{}, badRequest("rebuild_generation is invalid")
	}
	if member.UserID == "" || domainFromMXID(member.UserID) == s.serverName {
		return roomReactivationDecision{}, badRequest("retained-room rebuild requires a remote member")
	}
	base := trimString(params["requester_node_base_url"])
	if base == "" {
		base = trimString(params["applicant_node_base_url"])
	}
	if base == "" {
		base = trimString(params["remote_node_base_url"])
	}
	if base == "" {
		base = "https://" + domainFromMXID(member.UserID) + "/_p2p"
	}
	reactivationParams := map[string]any{
		"room_id":              member.RoomID,
		"channel_id":           member.ChannelID,
		"room_type":            scope,
		"user_id":              member.UserID,
		"display_name":         member.DisplayName,
		"avatar_url":           member.AvatarURL,
		"domain":               member.Domain,
		"server_names":         retainedRoomServerNames(params, member.RoomID),
		"remote_node_base_url": base,
		"rebuild_generation":   rebuildGeneration,
	}
	if scope == "group" {
		if group, ok, err := s.groupByRoom(ctx, member.RoomID); err != nil {
			return roomReactivationDecision{}, internalError(err)
		} else if ok {
			reactivationParams["name"] = group.Name
			reactivationParams["topic"] = group.Topic
			reactivationParams["avatar_url"] = fallbackString(trimString(reactivationParams["avatar_url"]), group.AvatarURL)
			reactivationParams["invite_policy"] = group.InvitePolicy
		}
	}
	if scope == "channel" {
		if ch, ok, err := s.channelByIDOrRoom(ctx, member.ChannelID, member.RoomID); err != nil {
			return roomReactivationDecision{}, internalError(err)
		} else if ok {
			reactivationParams["channel_id"] = ch.ChannelID
			reactivationParams["name"] = ch.Name
			reactivationParams["description"] = ch.Description
			reactivationParams["avatar_url"] = fallbackString(trimString(reactivationParams["avatar_url"]), ch.AvatarURL)
			reactivationParams["visibility"] = ch.Visibility
			reactivationParams["join_policy"] = ch.JoinPolicy
			reactivationParams["channel_type"] = ch.ChannelType
			reactivationParams["comments_enabled"] = ch.CommentsEnabled
		}
	}
	var remote struct {
		Status            string `json:"status"`
		NeedsRebuild      *bool  `json:"needs_rebuild"`
		RebuildGeneration string `json:"rebuild_generation"`
	}
	status, err := s.remotePublicAction(ctx, domainFromMXID(member.UserID), "rooms.reactivate", reactivationParams, &remote)
	if err != nil {
		if status != 0 && status != http.StatusBadGateway {
			return roomReactivationDecision{}, statusError(status, err.Error())
		}
		return roomReactivationDecision{}, statusError(http.StatusBadGateway, err.Error())
	}
	if status != http.StatusOK {
		return roomReactivationDecision{}, statusError(status, "target node room reactivation failed")
	}
	remoteStatus := strings.ToLower(strings.TrimSpace(remote.Status))
	if remote.RebuildGeneration != rebuildGeneration || remote.NeedsRebuild == nil {
		return roomReactivationDecision{}, statusError(http.StatusBadGateway, "target node returned an invalid room reactivation generation")
	}
	if (remoteStatus == "joined" && *remote.NeedsRebuild) || (remoteStatus == "invite" && !*remote.NeedsRebuild) ||
		(remoteStatus != "joined" && remoteStatus != "invite") {
		return roomReactivationDecision{}, statusError(http.StatusBadGateway, "target node returned an invalid room reactivation decision")
	}
	return roomReactivationDecision{
		Status: remoteStatus, NeedsRebuild: *remote.NeedsRebuild, RebuildGeneration: remote.RebuildGeneration,
	}, nil
}

func (s *Service) reinviteAlreadyJoinedRoomMember(
	ctx context.Context,
	scope string,
	member *memberRecord,
	params map[string]any,
	inviteReq InviteUserRequest,
	inviteErr error,
) *apiError {
	if member == nil {
		return badRequest("member is required")
	}
	rebuildGeneration := trimString(params["rebuild_generation"])
	if rebuildGeneration == "" {
		joined, err := s.matrixMemberJoined(ctx, member.RoomID, member.UserID)
		if err != nil {
			return transportWriteError(err)
		}
		if !joined {
			return transportWriteError(inviteErr)
		}
		member.Membership = "join"
		return nil
	}
	if !membersmodule.ValidRebuildGeneration(rebuildGeneration) {
		return badRequest("rebuild_generation is invalid")
	}
	joined, err := s.matrixMemberJoined(ctx, member.RoomID, member.UserID)
	if err != nil {
		return transportWriteError(err)
	}
	if !joined {
		return transportWriteError(inviteErr)
	}
	if member.RequestID == rebuildGeneration {
		member.Membership = "join"
		return nil
	}
	decision, apiErr := s.notifyRemoteRoomReactivation(ctx, scope, *member, params)
	if apiErr != nil {
		return apiErr
	}
	if decision.RebuildGeneration != rebuildGeneration {
		return statusError(http.StatusBadGateway, "target node returned an invalid room reactivation generation")
	}
	if !decision.NeedsRebuild {
		member.Membership = "join"
		return nil
	}
	if apiErr := s.kickAndInviteStaleJoinedRoomMember(ctx, *member, inviteReq); apiErr != nil {
		return apiErr
	}
	member.Membership = "invite"
	member.RequestID = rebuildGeneration
	return nil
}

func (s *Service) kickAndInviteStaleJoinedRoomMember(ctx context.Context, member memberRecord, inviteReq InviteUserRequest) *apiError {
	if s.transport == nil {
		return nil
	}
	if err := s.transport.KickUser(ctx, KickUserRequest{
		RoomID:     member.RoomID,
		SenderMXID: inviteReq.InviterMXID,
		TargetMXID: member.UserID,
		Reason:     "reactivate rebuilt member invite",
		Timestamp:  time.Now().UTC(),
	}); err != nil && !isAlreadyLeftRoomError(err) {
		return transportWriteError(err)
	}
	if err := s.transport.InviteUser(ctx, inviteReq); err != nil {
		return transportWriteError(err)
	}
	return nil
}

func (s *Service) saveRetainedRoomInviteMetadata(ctx context.Context, scope string, member memberRecord, params map[string]any) *apiError {
	switch scope {
	case "group":
		if err := s.ensureJoinedGroupRecord(ctx, member, params); err != nil {
			return internalError(err)
		}
	case "channel":
		memberStatus := "invite"
		if strings.EqualFold(strings.TrimSpace(member.Membership), "join") || strings.EqualFold(strings.TrimSpace(member.Membership), "joined") {
			memberStatus = "join"
		}
		ch := channel{
			ChannelID:       fallbackString(member.ChannelID, member.RoomID),
			RoomID:          member.RoomID,
			Name:            fallbackString(trimString(params["name"]), member.RoomID),
			Description:     trimString(params["description"]),
			AvatarURL:       trimString(params["avatar_url"]),
			Visibility:      fallbackString(trimString(params["visibility"]), "private"),
			JoinPolicy:      fallbackString(trimString(params["join_policy"]), "invite"),
			ChannelType:     fallbackString(trimString(params["channel_type"]), "post"),
			CommentsEnabled: boolParam(params["comments_enabled"]),
			MemberStatus:    memberStatus,
			Role:            fallbackString(member.Role, "member"),
		}
		if existing, ok, err := s.channelByIDOrRoom(ctx, ch.ChannelID, ch.RoomID); err != nil {
			return internalError(err)
		} else if ok {
			mergeRefreshedChannel(&ch, existing)
			ch.MemberStatus = memberStatus
		}
		if err := s.saveChannel(ctx, ch); err != nil {
			return internalError(err)
		}
	}
	return nil
}

type retainedRoomJoinAttempt struct {
	Member memberRecord
	Stale  bool
	Busy   bool
	Final  bool
}

func (s *Service) joinAndProjectRetainedRoom(ctx context.Context, scope string, member *memberRecord, params map[string]any) *apiError {
	attempt, apiErr := s.joinAndProjectRetainedRoomGeneration(ctx, scope, member, params)
	if member != nil {
		*member = attempt.Member
	}
	if attempt.Stale {
		if strings.EqualFold(strings.TrimSpace(attempt.Member.Membership), "join") ||
			strings.EqualFold(strings.TrimSpace(attempt.Member.Membership), "joined") {
			return nil
		}
		return codedError(http.StatusConflict, actionbase.MatrixJoinUnconfirmedCode, "Matrix join generation changed during settlement")
	}
	return apiErr
}

func (s *Service) joinAndProjectRetainedRoomGeneration(
	ctx context.Context,
	scope string,
	member *memberRecord,
	params map[string]any,
) (result retainedRoomJoinAttempt, retErr *apiError) {
	if member == nil {
		return result, badRequest("member is required")
	}
	if member.RequestID == "" {
		if operation, ok := recoverableOperationSnapshot(ctx); ok {
			member.RequestID = operation.RequestID
		}
		member.RequestID = fallbackString(member.RequestID, trimString(params["request_id"]))
	}
	if operation, ok := recoverableOperationSnapshot(ctx); ok && operationHasExternalCommit(operation.Phase) && operation.CurrentRoomID != "" {
		member.RoomID = operation.CurrentRoomID
	}
	current, found, err := s.lookupMember(ctx, member.RoomID, member.UserID)
	if err != nil {
		return result, internalError(err)
	}
	if !found {
		// A normal local group/channel join has no prior invite/request projection.
		// Establish its durable generation before any Matrix side effect, but do not
		// prewrite join: Matrix membership remains the authoritative joined fact.
		initial := *member
		initial.Membership = "joining"
		if err := s.saveMember(ctx, initial); err != nil {
			return result, internalError(err)
		}
		current, found, err = s.lookupMember(ctx, member.RoomID, member.UserID)
		if err != nil {
			return result, internalError(err)
		}
		if !found {
			return result, internalError(errors.New("retained-room join generation was not persisted"))
		}
		*member = current
	}
	if member.RequestID != "" && current.RequestID != "" && member.RequestID != current.RequestID {
		*member = current
		return retainedRoomJoinAttempt{Member: current, Stale: true, Final: true}, nil
	}
	member.RequestID = fallbackString(member.RequestID, current.RequestID)
	joinRequestID := member.RequestID
	expectedRequestID := current.RequestID
	expectedMembership := current.Membership

	matrixJoined := false
	if s.transport != nil {
		matrixJoined, err = s.matrixMemberJoined(ctx, member.RoomID, member.UserID)
		if err != nil {
			return result, transportWriteError(err)
		}
	}
	var workflow *operationsmodule.Tracker
	if s.transport != nil && !matrixJoined {
		var acquired bool
		workflow, acquired, err = s.acquireMatrixJoinWorkflow(ctx, *member)
		if err != nil {
			return result, recoverableOperationWriteError(ctx, err)
		}
		if !acquired {
			matrixJoined, err = s.matrixMemberJoined(ctx, member.RoomID, member.UserID)
			if err != nil {
				return result, transportWriteError(err)
			}
			current, found, err = s.lookupMember(ctx, member.RoomID, member.UserID)
			if err != nil {
				return result, internalError(err)
			}
			if !found {
				return result, internalError(errors.New("retained-room join generation disappeared while waiting"))
			}
			*member = current
			if joinRequestID != "" && current.RequestID != "" && joinRequestID != current.RequestID {
				return retainedRoomJoinAttempt{Member: current, Stale: true, Final: true}, nil
			}
			result = retainedRoomJoinAttempt{Member: current, Busy: !matrixJoined, Final: true}
			if !matrixJoined {
				return result, codedError(http.StatusConflict, actionbase.MatrixJoinUnconfirmedCode, "Matrix join is already in progress")
			}
			expectedRequestID = current.RequestID
			expectedMembership = current.Membership
		} else if workflow != nil {
			defer func() {
				writeCtx, cancel := operationWriteContext(ctx)
				err := workflow.Release(writeCtx)
				cancel()
				if err != nil && retErr == nil {
					retErr = recoverableOperationWriteError(ctx, err)
				}
			}()
			current, found, err = s.lookupMember(ctx, member.RoomID, member.UserID)
			if err != nil {
				return result, internalError(err)
			}
			if !found {
				return result, internalError(errors.New("retained-room join generation disappeared after claim"))
			}
			if member.RequestID != "" && current.RequestID != "" && member.RequestID != current.RequestID ||
				!retainedRoomMatrixJoinClaimable(current.Membership) {
				*member = current
				return retainedRoomJoinAttempt{Member: current, Stale: true, Final: true}, nil
			}
			claim := *member
			claim.RequestID = fallbackString(current.RequestID, claim.RequestID)
			claim.Membership = "joining"
			saved, saveErr := s.saveMemberIfState(ctx, claim, current.RequestID, current.Membership)
			if saveErr != nil {
				return result, internalError(saveErr)
			}
			if !saved {
				latest, ok, lookupErr := s.lookupMember(ctx, member.RoomID, member.UserID)
				if lookupErr != nil {
					return result, internalError(lookupErr)
				}
				if !ok {
					return result, internalError(errors.New("retained-room join generation disappeared during claim"))
				}
				*member = latest
				return retainedRoomJoinAttempt{Member: latest, Stale: true, Final: true}, nil
			}
			*member = claim
			expectedRequestID = claim.RequestID
			expectedMembership = claim.Membership
		}
	}

	matrixErr := (*apiError)(nil)
	if !matrixJoined {
		matrixErr = s.commitRetainedRoomMatrixJoin(ctx, member, params)
		if matrixErr != nil {
			matrixJoined, err = s.matrixMemberJoined(ctx, member.RoomID, member.UserID)
			if err != nil {
				return result, transportWriteError(err)
			}
		} else {
			matrixJoined = true
		}
	} else if s.transport != nil {
		if err := markRecoverableOperation(ctx, operationPhaseMatrixCommitted, member.RoomID); err != nil {
			matrixErr = recoverableOperationWriteError(ctx, err)
		}
	}

	if !matrixJoined {
		member.Membership = "join_failed"
		if matrixErr != nil && matrixErr.Code == actionbase.MatrixJoinUnconfirmedCode {
			member.Membership = "joining"
		}
		saved, saveErr := s.saveMemberIfState(ctx, *member, expectedRequestID, expectedMembership)
		if saveErr != nil {
			return result, internalError(saveErr)
		}
		if !saved {
			latest, ok, lookupErr := s.lookupMember(ctx, member.RoomID, member.UserID)
			if lookupErr != nil {
				return result, internalError(lookupErr)
			}
			if !ok {
				return result, internalError(errors.New("retained-room join generation disappeared during failure settlement"))
			}
			*member = latest
			return retainedRoomJoinAttempt{Member: latest, Stale: true, Final: true}, nil
		}
		return retainedRoomJoinAttempt{Member: *member, Final: true}, matrixErr
	}

	member.Membership = "join"
	if !productMembershipJoined(expectedMembership) || member.JoinedAt == 0 {
		member.JoinedAt = time.Now().UTC().UnixMilli()
	}
	saved, saveErr := s.saveMemberIfState(ctx, *member, expectedRequestID, expectedMembership)
	if saveErr != nil {
		return result, internalError(saveErr)
	}
	if !saved {
		latest, ok, lookupErr := s.lookupMember(ctx, member.RoomID, member.UserID)
		if lookupErr != nil {
			return result, internalError(lookupErr)
		}
		if !ok {
			return result, internalError(errors.New("retained-room join generation disappeared during success settlement"))
		}
		*member = latest
		return retainedRoomJoinAttempt{Member: latest, Stale: true, Final: true}, nil
	}
	result = retainedRoomJoinAttempt{Member: *member, Final: true}
	if matrixErr != nil {
		return result, matrixErr
	}
	switch scope {
	case "group":
		if err := s.ensureJoinedGroupRecord(ctx, *member, params); err != nil {
			return result, internalError(err)
		}
	case "channel":
		if refreshedChannelID, err := s.refreshRoomChannel(ctx, member.RoomID); err != nil {
			return result, internalError(err)
		} else if refreshedChannelID != "" && refreshedChannelID != member.ChannelID {
			member.ChannelID = refreshedChannelID
			saved, saveErr = s.saveMemberIfState(ctx, *member, member.RequestID, "join")
			if saveErr != nil {
				return result, internalError(saveErr)
			}
			if !saved {
				latest, ok, lookupErr := s.lookupMember(ctx, member.RoomID, member.UserID)
				if lookupErr != nil {
					return result, internalError(lookupErr)
				}
				if !ok {
					return result, internalError(errors.New("retained-room join generation disappeared during channel refresh"))
				}
				*member = latest
				return retainedRoomJoinAttempt{Member: latest, Stale: true, Final: true}, nil
			}
			result.Member = *member
		}
		if latest, stale, err := s.refreshRoomMembersForGeneration(ctx, member.RoomID, member.ChannelID, *member); err != nil {
			return result, internalError(err)
		} else if stale {
			*member = latest
			return retainedRoomJoinAttempt{Member: latest, Stale: true, Final: true}, nil
		}
		if err := s.backfillJoinedPostChannelContent(ctx, member.RoomID, member.ChannelID); err != nil {
			return result, internalError(err)
		}
	}
	return result, nil
}

func retainedRoomMatrixJoinClaimable(membership string) bool {
	switch strings.ToLower(strings.TrimSpace(membership)) {
	case "invite", "pending", "approved", "joining", "join_failed", "join", "joined":
		return true
	default:
		return false
	}
}

// commitRetainedRoomMatrixJoin performs only the Matrix fact transition. The
// caller owns all product projection writes, which lets recovery paths fence
// those writes with their durable request generation.
func (s *Service) commitRetainedRoomMatrixJoin(ctx context.Context, member *memberRecord, params map[string]any) *apiError {
	if member == nil {
		return badRequest("member is required")
	}
	if operation, ok := recoverableOperationSnapshot(ctx); ok && operationHasExternalCommit(operation.Phase) && operation.CurrentRoomID != "" {
		member.RoomID = operation.CurrentRoomID
	}
	if s.transport != nil {
		joined, err := s.matrixMemberJoined(ctx, member.RoomID, member.UserID)
		if err != nil {
			return transportWriteError(err)
		}
		if !joined {
			result, joinErr := s.joinRoomWithRetry(ctx, JoinRoomRequest{
				RoomIDOrAlias: fallbackString(member.RoomID, member.ChannelID),
				UserMXID:      member.UserID,
				DisplayName:   member.DisplayName,
				AvatarURL:     member.AvatarURL,
				ServerNames:   retainedRoomServerNames(params, member.RoomID),
			}, 10, isRetainedRoomJoinRetryable)
			if joinErr != nil {
				if !isAlreadyJoinedRoomError(joinErr) {
					if ambiguousChannelJoinTransportError(joinErr) || isFederatedJoinInProgress(joinErr) {
						if markErr := markRecoverableOperation(ctx, operationPhaseMatrixUnconfirmed, member.RoomID); markErr != nil {
							return recoverableOperationWriteError(ctx, markErr)
						}
						return codedError(http.StatusConflict, actionbase.MatrixJoinUnconfirmedCode, "Matrix join could not be confirmed locally")
					}
					return transportWriteError(joinErr)
				}
				confirmed, confirmErr := s.matrixMemberJoined(ctx, member.RoomID, member.UserID)
				if confirmErr != nil {
					return transportWriteError(confirmErr)
				}
				if !confirmed {
					return codedError(http.StatusConflict, actionbase.MatrixJoinUnconfirmedCode, "Matrix join could not be confirmed locally")
				}
			}
			if result.RoomID != "" {
				member.RoomID = result.RoomID
			}
		}
		if err := markRecoverableOperation(ctx, operationPhaseMatrixCommitted, member.RoomID); err != nil {
			return recoverableOperationWriteError(ctx, err)
		}
	}
	return nil
}

func (s *Service) acquireMatrixJoinWorkflow(
	ctx context.Context,
	member memberRecord,
) (*operationsmodule.Tracker, bool, error) {
	if s.store == nil {
		return nil, true, nil
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{
		member.RoomID, member.UserID, member.RequestID,
	}, "\x00")))
	now := time.Now().UTC().UnixMilli()
	record := operationsmodule.Record{
		OperationID: "_workflow_matrix_join_" + hex.EncodeToString(digest[:16]),
		Action:      "_workflow.matrix_join",
		Status:      operationStatusRunning,
		Phase:       operationPhasePrepared,
		RoomID:      member.RoomID,
		UserID:      member.UserID,
		RequestID:   member.RequestID,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	owner := "matrix_join_" + randomToken("claim")
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for {
		claimedRecord, claimed, err := s.store.ClaimOperation(waitCtx, record, owner, operationLeaseDurationMillis)
		if err != nil {
			if waitCtx.Err() != nil {
				return nil, false, nil
			}
			return nil, false, err
		}
		if claimed {
			return operationsmodule.NewTracker(s.store, owner, claimedRecord, operationLeaseDurationMillis), true, nil
		}
		select {
		case <-waitCtx.Done():
			return nil, false, nil
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (s *Service) refreshRoomMembersForGeneration(
	ctx context.Context,
	roomID,
	channelID string,
	target memberRecord,
) (memberRecord, bool, error) {
	if s.transport == nil || roomID == "" {
		return memberRecord{}, false, nil
	}
	members, err := s.transport.ListRoomMembers(ctx, roomID)
	if err != nil {
		return memberRecord{}, false, err
	}
	for _, member := range members {
		member.RoomID = roomID
		member.ChannelID = fallbackString(member.ChannelID, channelID)
		member.Membership = fallbackString(member.Membership, "join")
		member.Role = fallbackString(member.Role, "member")
		current, found, lookupErr := s.lookupMember(ctx, member.RoomID, member.UserID)
		if lookupErr != nil {
			return memberRecord{}, false, lookupErr
		}
		if found {
			mergeRefreshedMember(&member, current)
		}
		if member.UserID != target.UserID {
			if found && preserveActivePublicJoinOnRoomInviteRefresh(current, member) {
				// ListRoomMembers includes Matrix invites, but an invite is only an
				// implementation step for an active public-join generation. A join
				// by another member must not downgrade that durable workflow before
				// its callback settles it.
				continue
			}
			if err := s.saveMember(ctx, member); err != nil {
				return memberRecord{}, false, err
			}
			continue
		}
		if !found || current.RequestID != target.RequestID {
			return current, true, nil
		}
		if !strings.EqualFold(strings.TrimSpace(member.Membership), "join") &&
			!strings.EqualFold(strings.TrimSpace(member.Membership), "joined") {
			continue
		}
		member.Membership = "join"
		member.RequestID = target.RequestID
		saved, saveErr := s.saveMemberIfState(ctx, member, target.RequestID, "join")
		if saveErr != nil {
			return memberRecord{}, false, saveErr
		}
		if !saved {
			latest, ok, latestErr := s.lookupMember(ctx, roomID, target.UserID)
			if latestErr != nil {
				return memberRecord{}, false, latestErr
			}
			if !ok {
				return memberRecord{}, false, errors.New("retained-room target disappeared during member refresh")
			}
			return latest, true, nil
		}
	}
	latest, found, err := s.lookupMember(ctx, roomID, target.UserID)
	if err != nil {
		return memberRecord{}, false, err
	}
	if !found {
		return memberRecord{}, false, errors.New("retained-room target disappeared after member refresh")
	}
	if latest.RequestID != target.RequestID {
		return latest, true, nil
	}
	return memberRecord{}, false, nil
}

func preserveActivePublicJoinOnRoomInviteRefresh(existing, refreshed memberRecord) bool {
	if !strings.EqualFold(strings.TrimSpace(refreshed.Membership), "invite") ||
		strings.TrimSpace(existing.RequestID) == "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(existing.Membership)) {
	case "pending", "approved", "joining", "join_failed", "join", "joined", "reject", "rejected":
		return true
	default:
		return false
	}
}

func (s *Service) matrixMemberJoined(ctx context.Context, roomID, userID string) (bool, error) {
	if s.transport == nil || strings.TrimSpace(roomID) == "" || strings.TrimSpace(userID) == "" {
		return false, nil
	}
	if verifier, ok := s.transport.(interface {
		JoinedRoomReady(context.Context, string, string) (bool, error)
	}); ok {
		return verifier.JoinedRoomReady(ctx, roomID, userID)
	}
	members, err := s.transport.ListRoomMembers(ctx, roomID)
	if err != nil {
		return false, err
	}
	for _, member := range members {
		if member.UserID == userID && (strings.EqualFold(strings.TrimSpace(member.Membership), "join") ||
			strings.EqualFold(strings.TrimSpace(member.Membership), "joined")) {
			return true, nil
		}
	}
	return false, nil
}

func (s *Service) joinRoomWithRetry(ctx context.Context, req JoinRoomRequest, maxAttempts int, retryable func(error) bool) (JoinRoomResult, error) {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		result, err := s.transport.JoinRoom(ctx, req)
		if err == nil || retryable == nil || !retryable(err) || attempt == maxAttempts-1 {
			return result, err
		}
		select {
		case <-ctx.Done():
			return JoinRoomResult{}, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 300 * time.Millisecond):
		}
	}
	return JoinRoomResult{}, nil
}

func isRetainedRoomJoinRetryable(err error) bool {
	return isRoomJoinRequiresInvite(err) || isFederatedJoinInProgress(err)
}

func isRoomJoinRequiresInvite(err error) bool {
	if err == nil {
		return false
	}
	if isDirectRoomJoinRequiresInvite(err) {
		return true
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "join rule \"invite\" forbids it")
}

func isFederatedJoinInProgress(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "already a federated join to this room in progress") ||
		strings.Contains(message, "federated join in progress")
}

func retainedRoomServerNames(params map[string]any, roomID string) []string {
	serverNames := stringSliceParam(params["server_names"])
	if len(serverNames) > 0 {
		return serverNames
	}
	if server, ok := roomServerFromMatrixRoomID(roomID); ok && server != "" {
		return []string{server}
	}
	return nil
}
