package p2p

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	contactsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/contacts"
	membersmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/members"
	operationsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/operations"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

const (
	operationLeaseDurationMillis = int64(90_000)

	operationStatusRunning     = "running"
	operationStatusReconciling = "reconciling"
	operationStatusCompleted   = "completed"
	operationStatusFailed      = "failed"

	operationPhasePrepared             = "prepared"
	operationPhaseMatrixUnconfirmed    = "matrix_unconfirmed"
	operationPhaseMatrixCommitted      = "matrix_committed"
	operationPhaseCallbackAcknowledged = "callback_acknowledged"
	operationPhaseCompleted            = "completed"
)

var (
	operationIDPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	publicRequestIDPattern = regexp.MustCompile(`^[\x21-\x7e]{1,512}$`)
)

type recoverableOperationEntry struct {
	mu   sync.Mutex
	refs int
}

type serviceOperationState struct {
	operationEntriesMu sync.Mutex
	operationEntries   map[string]*recoverableOperationEntry
	workflowEntriesMu  sync.Mutex
	workflowEntries    map[string]*recoverableOperationEntry
}

type recoverableOperationContextKey struct{}

func recoverableProductAction(action string) bool {
	switch action {
	case "contacts.request", "contacts.requests.accept", "contacts.requests.reject",
		"groups.join", "groups.invite.reject", "channels.join",
		"channels.join_request.approve", "channels.join_request.reject",
		"channels.public.join_request", "channels.public.join_result":
		return true
	default:
		return false
	}
}

func retainedRoomInviteAction(action string) bool {
	return action == "groups.invite" || action == "channels.invite"
}

func explicitRetainedRoomRebuildAction(action string, params map[string]any) bool {
	return retainedRoomInviteAction(action) && trimString(params["rebuild_generation"]) != ""
}

func validateExplicitRetainedRoomRebuild(action string, params map[string]any) *apiError {
	if !explicitRetainedRoomRebuildAction(action, params) {
		return nil
	}
	if !membersmodule.ValidRebuildGeneration(trimString(params["rebuild_generation"])) {
		return badRequest("rebuild_generation is invalid")
	}
	if len(retainedRoomInviteMemberIDs(params)) != 1 {
		return badRequest("retained-room rebuild requires exactly one user")
	}
	return nil
}

func retainedRoomInviteMemberIDs(raw map[string]any) []string {
	params := actionbase.Params(raw)
	seen := make(map[string]struct{})
	users := make([]string, 0, 1)
	add := func(userID string) {
		userID = strings.TrimSpace(userID)
		if userID == "" {
			return
		}
		if _, found := seen[userID]; found {
			return
		}
		seen[userID] = struct{}{}
		users = append(users, userID)
	}
	for _, key := range []string{"user_id", "user_mxid", "peer_mxid", "mxid"} {
		add(params.String(key))
	}
	for _, key := range []string{"user_ids", "user_mxids", "peer_mxids", "invitees", "invite"} {
		for _, userID := range params.Strings(key) {
			add(userID)
		}
	}
	return users
}

func memberWorkflowProductAction(action string) bool {
	// channels.public.join_request intentionally relies on its operation claim
	// and member-generation CAS instead. A fresh request generation must be
	// able to advance while an older owner decision waits for a remote callback;
	// the old decision's final CAS then returns the new current state.
	switch action {
	case "groups.join", "groups.invite.reject", "channels.join",
		"channels.join_request.approve", "channels.join_request.reject":
		return true
	default:
		return false
	}
}

func durableBusinessWorkflowAction(action string) bool {
	if memberWorkflowProductAction(action) {
		return true
	}
	switch action {
	case "contacts.request", "contacts.requests.accept", "contacts.requests.reject", "channels.public.join_request",
		"groups.invite", "channels.invite":
		return true
	default:
		return false
	}
}

// canonicalRecoverableParams resolves durable workflow identity before the
// operation and member locks are acquired. In particular, legacy channel join
// callers may send only a grant identifier; resolving its retained room here
// prevents two processes using different operation IDs from dispatching the
// same Matrix join concurrently.
func (s *Service) canonicalRecoverableParams(
	ctx context.Context,
	action string,
	params map[string]any,
) (map[string]any, *apiError) {
	if action != "channels.join" || s.store == nil {
		return params, nil
	}
	roomID, channelID, err := s.memberTarget(ctx, params)
	if err != nil {
		return nil, internalError(err)
	}
	if roomID != "" && channelID != "" {
		return params, nil
	}
	grantID := trimString(params["grant_id"])
	shareRoomID := fallbackString(trimString(params["share_room_id"]), trimString(params["via_room_id"]))
	if grantID == "" && shareRoomID == "" {
		return params, nil
	}
	grants, err := s.store.ListChannelInviteGrants(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	for _, grant := range grants {
		if grantID != "" && grant.GrantID != grantID {
			continue
		}
		if shareRoomID != "" && grant.ShareRoomID != shareRoomID {
			continue
		}
		if roomID != "" && grant.RoomID != roomID {
			continue
		}
		if channelID != "" && grant.ChannelID != channelID {
			continue
		}
		canonical := cloneParams(params)
		canonical["room_id"] = grant.RoomID
		canonical["channel_id"] = grant.ChannelID
		return canonical, nil
	}
	return params, nil
}

// preflightRecoverablePublicAction rejects unauthenticated public callbacks
// before they can claim durable operation or workflow rows. The handlers still
// perform the same checks when they execute; this gate only proves that the
// target and request shape are real enough to begin a recoverable operation.
func (s *Service) preflightRecoverablePublicAction(
	ctx context.Context,
	action string,
	params map[string]any,
) (context.Context, *apiError) {
	if action != "channels.public.join_request" && action != "channels.public.join_result" {
		return ctx, nil
	}
	if apiErr := s.validatePublicRecoveryShape(action, params); apiErr != nil {
		return ctx, apiErr
	}
	if s.store == nil {
		return ctx, nil
	}
	{
		record, apiErr := s.operationRecordFor(ctx, action, params)
		if apiErr != nil {
			return ctx, apiErr
		}
		if _, found, err := s.store.LookupOperation(ctx, record.OperationID); err != nil {
			return ctx, operationPersistenceError(record, err)
		} else if found {
			// Existing operations must reach the normal conflict/cache/recovery
			// path without repeating target discovery. In particular, a lost
			// response remains replayable while the remote owner is unavailable.
			return ctx, nil
		}
	}
	if action == "channels.public.join_request" {
		return s.preflightPublicChannelJoinRequest(ctx, params)
	}
	return s.preflightPublicChannelJoinResult(ctx, params)
}

// validatePublicRecoveryShape is intentionally local and side-effect free.
// Existing durable operations may skip channel discovery so response-loss
// replay survives a remote outage, but unauthenticated caller input must still
// pass the same cheap syntax checks on every attempt.
func (s *Service) validatePublicRecoveryShape(action string, params map[string]any) *apiError {
	roomID := trimString(params["room_id"])
	channelID := trimString(params["channel_id"])
	if roomID == "" && channelID == "" {
		return badRequest("room_id or channel_id is required")
	}
	if roomID != "" {
		if _, valid := roomServerFromMatrixRoomID(roomID); !valid {
			return badRequest("valid Matrix room_id is required")
		}
	}
	userID := firstMemberID(params)
	if userID == "" {
		userID = s.OwnerMXID()
	}
	if userID == "" {
		return badRequest("user_id is required")
	}
	if parsed, err := spec.NewUserID(userID, true); err != nil || parsed == nil {
		return badRequest("valid user_id is required")
	}
	if action == "channels.public.join_result" && userID != s.OwnerMXID() {
		return statusError(403, "join result user must be local owner")
	}
	if rawRequestID, provided := params["request_id"]; provided {
		requestID, ok := rawRequestID.(string)
		requestID = strings.TrimSpace(requestID)
		if !ok || requestID != "" && !publicRequestIDPattern.MatchString(requestID) {
			return badRequest("request_id is invalid")
		}
	}
	if action == "channels.public.join_result" {
		switch strings.ToLower(trimString(params["status"])) {
		case "rejected", "approved", "joining", "joined":
		default:
			return badRequest("status must be approved or rejected")
		}
	}
	return nil
}

func (s *Service) preflightPublicChannelJoinRequest(
	ctx context.Context,
	params map[string]any,
) (context.Context, *apiError) {
	roomID, channelID, err := s.memberTarget(ctx, params)
	if err != nil {
		return ctx, internalError(err)
	}
	if roomID == "" && channelID == "" {
		return ctx, badRequest("room_id or channel_id is required")
	}
	userID := firstMemberID(params)
	if userID == "" {
		userID = s.OwnerMXID()
	}
	if userID == "" {
		return ctx, badRequest("user_id is required")
	}
	if parsed, err := spec.NewUserID(userID, true); err != nil || parsed == nil {
		return ctx, badRequest("valid user_id is required")
	}
	if rawRequestID, provided := params["request_id"]; provided {
		requestID, ok := rawRequestID.(string)
		requestID = strings.TrimSpace(requestID)
		if !ok || requestID != "" && !publicRequestIDPattern.MatchString(requestID) {
			return ctx, badRequest("request_id is invalid")
		}
	}
	if roomID != "" {
		roomServer, validRoomID := roomServerFromMatrixRoomID(roomID)
		if !validRoomID {
			return ctx, badRequest("valid Matrix room_id is required")
		}
		if roomServer != s.serverName {
			remoteChannel, found, apiErr := s.remotePublicChannelGet(ctx, channelID, roomID, params)
			if apiErr != nil {
				return ctx, apiErr
			}
			if !found {
				return ctx, statusError(404, "channel not found")
			}
			if strings.EqualFold(remoteChannel.JoinPolicy, "invite") {
				return ctx, statusError(403, "channel requires invite")
			}
			return withPublicJoinPreflightChannel(ctx, remoteChannel), nil
		}
	}
	localChannel, found, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return ctx, internalError(err)
	}
	if !found {
		return ctx, statusError(404, "channel not found")
	}
	if !strings.EqualFold(localChannel.Visibility, "public") {
		return ctx, statusError(403, "channel is private")
	}
	if strings.EqualFold(localChannel.JoinPolicy, "invite") {
		return ctx, statusError(403, "channel requires invite")
	}
	if apiErr := s.rejectIfBlocked(ctx, "contact", userID); apiErr != nil {
		return ctx, apiErr
	}
	return ctx, nil
}

func (s *Service) preflightPublicChannelJoinResult(
	ctx context.Context,
	params map[string]any,
) (context.Context, *apiError) {
	roomID, channelID, err := s.memberTarget(ctx, params)
	if err != nil {
		return ctx, internalError(err)
	}
	if roomID == "" && channelID == "" {
		return ctx, badRequest("room_id or channel_id is required")
	}
	ownerMXID := s.OwnerMXID()
	userID := firstMemberID(params)
	if userID == "" {
		userID = ownerMXID
	}
	if userID != ownerMXID {
		return ctx, statusError(403, "join result user must be local owner")
	}
	member, found, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return ctx, internalError(err)
	}
	if !found {
		return ctx, actionbase.CodedError(404, actionbase.RequestNotFoundCode, "join request not found")
	}
	switch strings.ToLower(strings.TrimSpace(member.Membership)) {
	case "pending", "approved", "joining", "join_failed", "invite", "join", "reject", "rejected":
	default:
		return ctx, actionbase.CodedError(410, actionbase.RequestExpiredCode, "join request expired")
	}
	switch strings.ToLower(trimString(params["status"])) {
	case "rejected", "approved", "joining", "joined":
		return ctx, nil
	default:
		return ctx, badRequest("status must be approved or rejected")
	}
}

func (s *Service) lockMemberWorkflowForAction(ctx context.Context, action string, params map[string]any) (func(), *apiError) {
	if !memberWorkflowProductAction(action) && !explicitRetainedRoomRebuildAction(action, params) {
		return func() {}, nil
	}
	roomID, channelID, err := s.memberTarget(ctx, params)
	if err != nil {
		return nil, internalError(err)
	}
	roomID = fallbackString(roomID, trimString(params["room_id"]))
	channelID = fallbackString(channelID, trimString(params["channel_id"]))
	userID := firstMemberID(params)
	if userID == "" && (action == "groups.join" || action == "groups.invite.reject" ||
		action == "channels.join" || action == "channels.public.join_request") {
		userID = s.OwnerMXID()
	}
	if roomID == "" && channelID == "" || userID == "" {
		return func() {}, nil
	}
	return s.lockMemberWorkflow(roomID + "\x00" + channelID + "\x00" + userID), nil
}

func (s *Service) handleRecoverableOperation(
	ctx context.Context,
	action string,
	params map[string]any,
	handler actionHandler,
) (retResult any, retErr *apiError) {
	record, actionErr := s.operationRecordFor(ctx, action, params)
	if actionErr != nil {
		return nil, actionErr
	}
	if current, handled, mismatchErr := s.publicJoinResultGenerationMismatch(ctx, action, params, record); handled {
		return current, mismatchErr
	}
	workflow, acquired, workflowErr := s.acquireDurableBusinessWorkflow(ctx, record)
	if workflowErr != nil {
		return nil, workflowErr
	}
	if !acquired {
		return s.operationWorkflowBusyResult(ctx, record)
	}
	if workflow != nil {
		defer func() {
			writeCtx, cancel := operationWriteContext(ctx)
			releaseErr := workflow.Release(writeCtx)
			cancel()
			if releaseErr != nil {
				retResult = nil
				retErr = operationPersistenceError(record, releaseErr)
			}
		}()
	}
	if action == "channels.public.join_request" || action == "channels.public.join_result" {
		// The durable workflow serializes public callbacks by member identity.
		// Resolve the generation again after acquiring it so concurrent caller
		// IDs collapse onto the generation that actually won.
		refreshed, refreshErr := s.operationRecordFor(ctx, action, params)
		if refreshErr != nil {
			return nil, refreshErr
		}
		record = refreshed
		if current, handled, mismatchErr := s.publicJoinResultGenerationMismatch(ctx, action, params, record); handled {
			return current, mismatchErr
		}
	}
	release := s.lockRecoverableOperation(record.OperationID)
	defer release()

	stored, found, err := s.store.LookupOperation(ctx, record.OperationID)
	if err != nil {
		return nil, operationPersistenceError(record, err)
	}
	if found {
		if stored.Action != action || operationIdentityConflict(stored, record) {
			return nil, &apiError{
				Status: httpStatusConflict, Error: "operation_id belongs to a different request",
				Code: actionbase.OperationIDConflictCode, OperationID: record.OperationID,
			}
		}
		record = stored
	}
	superseded := false
	if found {
		superseded, err = s.operationGenerationSuperseded(ctx, record)
		if err != nil {
			return nil, operationPersistenceError(record, err)
		}
	}
	if superseded {
		if retainedRoomInviteAction(record.Action) {
			// A joined Matrix member may still represent the retained pre-rebuild
			// room. Until this exact generation completes, it is not rebuild
			// success and must not authorize clients to stop reconciliation.
			result, hydrateErr := s.hydrateMemberRecoveryResult(ctx, record, retainedRoomRebuildReconcilingResult(record))
			if hydrateErr != nil {
				return nil, operationPersistenceError(record, hydrateErr)
			}
			return result, nil
		}
		if record.Action == "channels.public.join_request" {
			current, currentErr := s.supersededOperationCurrentResult(ctx, record)
			if currentErr != nil {
				return nil, operationPersistenceError(record, currentErr)
			}
			return current, nil
		}
		if record.ResultJSON != "" {
			cached, decodeErr := decodeRecoverableOperationResult(action, record.ResultJSON)
			if decodeErr != nil {
				return nil, operationPersistenceError(record, decodeErr)
			}
			return cached, nil
		}
		current, currentErr := s.supersededOperationCurrentResult(ctx, record)
		if currentErr != nil {
			return nil, operationPersistenceError(record, currentErr)
		}
		return current, nil
	}
	resetCompletedContact := false
	if record.Status == operationStatusCompleted && record.ResultJSON != "" {
		cached, decodeErr := decodeRecoverableOperationResult(action, record.ResultJSON)
		if decodeErr != nil {
			return nil, operationPersistenceError(record, decodeErr)
		}
		current, currentErr := s.completedOperationStillCurrent(ctx, record, cached)
		if currentErr != nil {
			return nil, operationPersistenceError(record, currentErr)
		}
		if current {
			return cached, nil
		}
		if action == "contacts.request" {
			currentResult, currentResultErr := s.supersededOperationCurrentResult(ctx, record)
			if currentResultErr != nil {
				return nil, operationPersistenceError(record, currentResultErr)
			}
			if view, ok := currentResult.(contactsmodule.View); ok && dirextalkdomain.ContactDeleted(view.Status) {
				return view, nil
			}
		}
		if strings.HasPrefix(action, "contacts.") {
			resetCompletedContact = true
		}
	}

	claimOwner := "claim_" + randomToken("operation")
	claimedRecord, claimed, err := s.store.ClaimOperation(ctx, record, claimOwner, operationLeaseDurationMillis)
	if err != nil {
		return nil, operationPersistenceError(record, err)
	}
	if claimedRecord.Action != action || operationIdentityConflict(claimedRecord, record) {
		return nil, &apiError{
			Status: httpStatusConflict, Error: "operation_id belongs to a different request",
			Code: actionbase.OperationIDConflictCode, OperationID: record.OperationID,
		}
	}
	if !claimed {
		return s.operationInFlightResult(ctx, claimedRecord)
	}
	record = claimedRecord
	if resetCompletedContact {
		record.Status = operationStatusRunning
		record.Phase = operationPhasePrepared
		record.CurrentRoomID = ""
		record.ResultJSON = ""
		record.ErrorCode = ""
		record.UpdatedAt = time.Now().UTC().UnixMilli()
	}
	tracker := operationsmodule.NewTracker(s.store, claimOwner, record, operationLeaseDurationMillis)
	defer func() {
		writeCtx, cancel := operationWriteContext(ctx)
		releaseErr := tracker.Release(writeCtx)
		cancel()
		if releaseErr != nil {
			retResult = nil
			retErr = operationPersistenceError(tracker.Snapshot(), releaseErr)
		}
	}()
	if workflow != nil {
		writeCtx, cancel := operationWriteContext(ctx)
		renewErr := workflow.Heartbeat(writeCtx)
		cancel()
		if renewErr != nil {
			return nil, operationPersistenceError(record, renewErr)
		}
	}
	settlementCtx, cancelSettlement := actionbase.SettlementContext(ctx)
	defer cancelSettlement()
	trackedCtx := context.WithValue(settlementCtx, recoverableOperationContextKey{}, tracker)
	handlerParams := params
	if action == "channels.public.join_request" || trimString(params["request_id"]) == "" ||
		action == "channels.public.join_result" && record.BaseRequestID == "" {
		handlerParams = cloneParams(params)
		handlerParams["request_id"] = record.RequestID
	}
	result, apiErr := handler(trackedCtx, handlerParams)
	if apiErr != nil {
		snapshot := tracker.Snapshot()
		status := operationStatusFailed
		if operationNeedsRecovery(snapshot.Phase) {
			status = operationStatusReconciling
			if apiErr.Code == "" {
				apiErr.Code = actionbase.OperationRecoveryCode
			}
		}
		writeCtx, cancel := operationWriteContext(ctx)
		updateErr := tracker.Update(writeCtx, status, snapshot.Phase, snapshot.CurrentRoomID, apiErr.Code, "")
		cancel()
		if updateErr != nil {
			return nil, operationPersistenceError(tracker.Snapshot(), updateErr)
		}
		// The outer durable operation owns the recovery key. Never let a
		// federated peer or nested adapter replace it in the public envelope.
		apiErr.OperationID = snapshot.OperationID
		apiErr.CurrentRoomID = fallbackString(apiErr.CurrentRoomID, snapshot.CurrentRoomID)
		return nil, apiErr
	}

	result, status, currentRoomID, resultErrorCode := operationResultMetadata(result, tracker.Snapshot())
	resultJSON, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return nil, operationPersistenceError(tracker.Snapshot(), marshalErr)
	}
	operationStatus := operationStatusCompleted
	phase := operationPhaseCompleted
	if operationResultNeedsReconciliation(status, resultErrorCode) {
		operationStatus = operationStatusReconciling
		phase = tracker.Snapshot().Phase
	}
	writeCtx, cancel := operationWriteContext(ctx)
	err = tracker.Update(writeCtx, operationStatus, phase, currentRoomID, resultErrorCode, string(resultJSON))
	cancel()
	if err != nil {
		return nil, operationPersistenceError(tracker.Snapshot(), err)
	}
	return result, nil
}

// publicJoinResultGenerationMismatch handles a delayed callback before it can
// claim or complete the current generation's durable operation. Public caller
// request IDs remain correlation-only; they never create a parallel operation
// namespace, but a non-empty persisted generation must still fence stale ACKs.
func (s *Service) publicJoinResultGenerationMismatch(
	ctx context.Context,
	action string,
	params map[string]any,
	record operationsmodule.Record,
) (any, bool, *apiError) {
	if action != "channels.public.join_result" || record.BaseRequestID == "" {
		return nil, false, nil
	}
	explicitRequestID := trimString(params["request_id"])
	if explicitRequestID == "" || explicitRequestID == record.RequestID {
		return nil, false, nil
	}
	current, err := s.supersededOperationCurrentResult(ctx, record)
	if err != nil {
		return nil, true, operationPersistenceError(record, err)
	}
	return current, true, nil
}

func (s *Service) operationGenerationSuperseded(ctx context.Context, record operationsmodule.Record) (bool, error) {
	if record.RequestID == "" {
		return false, nil
	}
	if strings.HasPrefix(record.Action, "contacts.") && s.contactsModule != nil {
		contact, found, err := s.contactsModule.LookupByPeer(ctx, record.PeerMXID)
		if err != nil || !found {
			return false, err
		}
		if record.Action == "contacts.request" && dirextalkdomain.ContactDeleted(contact.Status) &&
			record.Status != operationStatusCompleted && contact.RequestID == record.BaseRequestID {
			return false, nil
		}
		return contact.RequestID != "" && contact.RequestID != record.RequestID, nil
	}
	if record.RoomID == "" || record.UserID == "" {
		return false, nil
	}
	member, found, err := s.lookupMember(ctx, record.RoomID, record.UserID)
	if err != nil || !found {
		return false, err
	}
	if record.Action == "channels.public.join_request" && channelJoinGenerationMayRestart(member.Membership) &&
		record.Status != operationStatusCompleted && member.RequestID == record.BaseRequestID {
		// A newly claimed generation may have failed before it could advance a
		// terminal member. The persisted base generation is a cross-instance
		// fence: once another operation advances it, this operation is stale
		// regardless of process clocks.
		return false, nil
	}
	return member.RequestID != "" && member.RequestID != record.RequestID, nil
}

func (s *Service) operationInFlightResult(ctx context.Context, record operationsmodule.Record) (any, *apiError) {
	if retainedRoomInviteAction(record.Action) {
		if record.Status == operationStatusCompleted && record.ResultJSON != "" {
			result, err := decodeRecoverableOperationResult(record.Action, record.ResultJSON)
			if err != nil {
				return nil, operationPersistenceError(record, err)
			}
			current, currentErr := s.completedOperationStillCurrent(ctx, record, result)
			if currentErr != nil {
				return nil, operationPersistenceError(record, currentErr)
			}
			if current {
				return result, nil
			}
		}
		result, err := s.hydrateMemberRecoveryResult(ctx, record, retainedRoomRebuildReconcilingResult(record))
		if err != nil {
			return nil, operationPersistenceError(record, err)
		}
		return result, nil
	}
	if record.ResultJSON != "" {
		result, err := decodeRecoverableOperationResult(record.Action, record.ResultJSON)
		if err != nil {
			return nil, operationPersistenceError(record, err)
		}
		if record.Status != operationStatusCompleted {
			return result, nil
		}
		current, currentErr := s.completedOperationStillCurrent(ctx, record, result)
		if currentErr != nil {
			return nil, operationPersistenceError(record, currentErr)
		}
		if current {
			return result, nil
		}
	}
	current, err := s.supersededOperationCurrentResult(ctx, record)
	if err == nil {
		return current, nil
	}
	currentRoomID := fallbackString(record.CurrentRoomID, record.RoomID)
	if strings.HasPrefix(record.Action, "contacts.") {
		view, hydrateErr := s.hydrateContactRecoveryView(ctx, record, contactsmodule.View{
			PeerMXID: record.PeerMXID, RoomID: currentRoomID, CurrentRoomID: currentRoomID,
			Status: "joining", ErrorCode: actionbase.OperationRecoveryCode, OperationID: record.OperationID,
		})
		if hydrateErr != nil {
			return nil, operationPersistenceError(record, hydrateErr)
		}
		return view, nil
	}
	result := map[string]any{
		"status": "joining", "error_code": actionbase.OperationRecoveryCode,
		"operation_id": record.OperationID,
	}
	if currentRoomID != "" {
		result["current_room_id"] = currentRoomID
		result["room_id"] = currentRoomID
	}
	hydrated, hydrateErr := s.hydrateMemberRecoveryResult(ctx, record, result)
	if hydrateErr != nil {
		return nil, operationPersistenceError(record, hydrateErr)
	}
	return hydrated, nil
}

func (s *Service) acquireDurableBusinessWorkflow(
	ctx context.Context,
	record operationsmodule.Record,
) (*operationsmodule.Tracker, bool, *apiError) {
	if record.Action == "channels.public.join_request" {
		advance, err := s.publicJoinCanAdvanceTerminalGeneration(ctx, record)
		if err != nil {
			return nil, false, operationPersistenceError(record, err)
		}
		if advance {
			// The member-generation CAS is the fence for a fresh request. Do not
			// wait behind the previous terminal decision's remote callback.
			return nil, true, nil
		}
	}
	workflowRecord, ok := durableBusinessWorkflowRecord(record)
	if !ok {
		return nil, true, nil
	}
	owner := "workflow_" + randomToken("claim")
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for {
		claimedRecord, claimed, err := s.store.ClaimOperation(waitCtx, workflowRecord, owner, operationLeaseDurationMillis)
		if err != nil {
			if waitCtx.Err() != nil {
				return nil, false, nil
			}
			return nil, false, operationPersistenceError(record, err)
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

func (s *Service) publicJoinCanAdvanceTerminalGeneration(ctx context.Context, record operationsmodule.Record) (bool, error) {
	if record.RoomID == "" || record.UserID == "" || record.RequestID == "" ||
		record.BaseRequestID == "" || record.RequestID == record.BaseRequestID {
		return false, nil
	}
	member, found, err := s.lookupMember(ctx, record.RoomID, record.UserID)
	if err != nil || !found {
		return false, err
	}
	return member.RequestID == record.BaseRequestID && channelJoinGenerationMayRestart(member.Membership), nil
}

func durableBusinessWorkflowRecord(record operationsmodule.Record) (operationsmodule.Record, bool) {
	if !durableBusinessWorkflowAction(record.Action) {
		return operationsmodule.Record{}, false
	}
	kind := "member"
	identity := []string{record.RoomID, record.UserID}
	workflow := operationsmodule.Record{
		Action: "_workflow.member", Status: operationStatusRunning, Phase: operationPhasePrepared,
		RoomID: record.RoomID, UserID: record.UserID,
	}
	if strings.HasPrefix(record.Action, "contacts.") {
		kind = "contact"
		identity = []string{record.PeerMXID}
		workflow = operationsmodule.Record{
			Action: "_workflow.contact", Status: operationStatusRunning, Phase: operationPhasePrepared,
			PeerMXID: record.PeerMXID,
		}
	}
	for _, part := range identity {
		if strings.TrimSpace(part) == "" {
			return operationsmodule.Record{}, false
		}
	}
	digest := sha256.Sum256([]byte(kind + "\x00" + strings.Join(identity, "\x00")))
	now := time.Now().UTC().UnixMilli()
	// A leading underscore is outside the public operation_id grammar, so a
	// caller cannot pre-claim or poison this internal workflow namespace.
	workflow.OperationID = "_workflow_" + hex.EncodeToString(digest[:16])
	workflow.CreatedAt = now
	workflow.UpdatedAt = now
	return workflow, true
}

func (s *Service) operationWorkflowBusyResult(ctx context.Context, record operationsmodule.Record) (any, *apiError) {
	if retainedRoomInviteAction(record.Action) {
		stored, found, err := s.store.LookupOperation(ctx, record.OperationID)
		if err != nil {
			return nil, operationPersistenceError(record, err)
		}
		if found && stored.Action == record.Action && !operationIdentityConflict(stored, record) &&
			stored.Status == operationStatusCompleted && stored.ResultJSON != "" {
			result, decodeErr := decodeRecoverableOperationResult(stored.Action, stored.ResultJSON)
			if decodeErr != nil {
				return nil, operationPersistenceError(stored, decodeErr)
			}
			current, currentErr := s.completedOperationStillCurrent(ctx, stored, result)
			if currentErr != nil {
				return nil, operationPersistenceError(stored, currentErr)
			}
			if current {
				return result, nil
			}
		}
		result, hydrateErr := s.hydrateMemberRecoveryResult(ctx, record, retainedRoomRebuildReconcilingResult(record))
		if hydrateErr != nil {
			return nil, operationPersistenceError(record, hydrateErr)
		}
		return result, nil
	}
	current, err := s.supersededOperationCurrentResult(ctx, record)
	if err == nil {
		switch value := current.(type) {
		case contactsmodule.View:
			if record.Action != "contacts.request" && !strings.EqualFold(value.Status, "accepted") &&
				!strings.EqualFold(value.Status, "rejected") && !strings.EqualFold(value.Status, "joining") {
				value.Status = "joining"
				value.ErrorCode = actionbase.MatrixJoinUnconfirmedCode
			}
			value.OperationID = record.OperationID
			hydrated, hydrateErr := s.hydrateContactRecoveryView(ctx, record, value)
			if hydrateErr != nil {
				return nil, operationPersistenceError(record, hydrateErr)
			}
			return hydrated, nil
		case map[string]any:
			status := strings.ToLower(trimString(value["status"]))
			if status != "approved" && status != "joined" && status != "rejected" && status != "joining" && status != "join_failed" {
				value["status"] = "joining"
				value["error_code"] = actionbase.JoinResultUnconfirmedCode
			}
			value["operation_id"] = record.OperationID
			hydrated, hydrateErr := s.hydrateMemberRecoveryResult(ctx, record, value)
			if hydrateErr != nil {
				return nil, operationPersistenceError(record, hydrateErr)
			}
			return hydrated, nil
		}
	}
	if strings.HasPrefix(record.Action, "contacts.") {
		errorCode := actionbase.MatrixJoinUnconfirmedCode
		if record.Action == "contacts.request" {
			errorCode = actionbase.OperationRecoveryCode
		}
		view, hydrateErr := s.hydrateContactRecoveryView(ctx, record, contactsmodule.View{
			PeerMXID: record.PeerMXID, RoomID: record.RoomID, CurrentRoomID: record.RoomID,
			Status: "joining", ErrorCode: errorCode, OperationID: record.OperationID,
		})
		if hydrateErr != nil {
			return nil, operationPersistenceError(record, hydrateErr)
		}
		return view, nil
	}
	result := map[string]any{
		"status": "joining", "error_code": actionbase.JoinResultUnconfirmedCode,
		"operation_id": record.OperationID,
	}
	if record.RoomID != "" {
		result["room_id"] = record.RoomID
		result["current_room_id"] = record.RoomID
	}
	hydrated, hydrateErr := s.hydrateMemberRecoveryResult(ctx, record, result)
	if hydrateErr != nil {
		return nil, operationPersistenceError(record, hydrateErr)
	}
	return hydrated, nil
}

func retainedRoomRebuildReconcilingResult(record operationsmodule.Record) map[string]any {
	currentRoomID := fallbackString(record.CurrentRoomID, record.RoomID)
	result := map[string]any{
		"status":       "joining",
		"error_code":   actionbase.OperationRecoveryCode,
		"operation_id": record.OperationID,
	}
	if currentRoomID != "" {
		result["room_id"] = currentRoomID
		result["current_room_id"] = currentRoomID
	}
	return result
}

func (s *Service) hydrateContactRecoveryView(
	ctx context.Context,
	record operationsmodule.Record,
	view contactsmodule.View,
) (contactsmodule.View, error) {
	if s.contactsModule == nil {
		return contactsmodule.View{}, errors.New("contact recovery presentation is not configured")
	}
	hydrated, err := s.contactsModule.HydrateView(ctx, record.Action, contactsmodule.RecordFromView(view))
	if err != nil {
		return contactsmodule.View{}, err
	}
	hydrated.OperationID = fallbackString(view.OperationID, record.OperationID)
	hydrated.CurrentRoomID = fallbackString(
		view.CurrentRoomID,
		fallbackString(view.RoomID, fallbackString(record.CurrentRoomID, record.RoomID)),
	)
	hydrated.ErrorCode = view.ErrorCode
	return hydrated, nil
}

func (s *Service) hydrateMemberRecoveryResult(
	ctx context.Context,
	record operationsmodule.Record,
	result map[string]any,
) (map[string]any, error) {
	if s.membersModule == nil {
		return nil, errors.New("member recovery presentation is not configured")
	}
	status := strings.ToLower(strings.TrimSpace(trimString(result["status"])))
	if status == "" {
		status = "joining"
	}
	roomID := fallbackString(
		trimString(result["current_room_id"]),
		fallbackString(trimString(result["room_id"]), fallbackString(record.CurrentRoomID, record.RoomID)),
	)
	return s.membersModule.HydrateResult(ctx, result, record.Action, status, roomID)
}

func (s *Service) supersededOperationCurrentResult(ctx context.Context, record operationsmodule.Record) (any, error) {
	if strings.HasPrefix(record.Action, "contacts.") && s.contactsModule != nil {
		contact, found, err := s.contactsModule.LookupByPeer(ctx, record.PeerMXID)
		if err != nil {
			return nil, err
		}
		if found {
			view, hydrateErr := s.contactsModule.HydrateView(ctx, record.Action, contact)
			if hydrateErr != nil {
				return nil, hydrateErr
			}
			view.OperationID = record.OperationID
			view.CurrentRoomID = contact.RoomID
			if strings.EqualFold(strings.TrimSpace(contact.Status), "joining") {
				view.ErrorCode = actionbase.MatrixJoinUnconfirmedCode
			}
			return view, nil
		}
	}
	member, found, err := s.lookupMember(ctx, record.RoomID, record.UserID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("operation generation was superseded but current member is missing")
	}
	status := strings.ToLower(strings.TrimSpace(member.Membership))
	result := map[string]any{
		"status": status, "member": member, "operation_id": record.OperationID,
		"current_room_id": member.RoomID,
	}
	switch status {
	case "join":
		joined, joinedErr := s.matrixMemberJoined(ctx, member.RoomID, member.UserID)
		if joinedErr != nil {
			return nil, joinedErr
		}
		if joined {
			result["status"] = "joined"
			result["room_id"] = member.RoomID
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
	if member.ChannelID != "" {
		result["channel"] = s.channelSnapshot(ctx, member.ChannelID)
	}
	return s.hydrateMemberRecoveryResult(ctx, record, result)
}

const httpStatusConflict = 409

func (s *Service) operationRecordFor(ctx context.Context, action string, params map[string]any) (operationsmodule.Record, *apiError) {
	now := time.Now().UTC().UnixMilli()
	providedOperationID := false
	operationID := ""
	if rawOperationID, provided := params["operation_id"]; provided {
		value, ok := rawOperationID.(string)
		operationID = strings.TrimSpace(value)
		if !ok || !operationIDPattern.MatchString(operationID) {
			return operationsmodule.Record{}, actionbase.CodedError(400, actionbase.OperationIDInvalidCode, "operation_id is invalid")
		}
		providedOperationID = true
	}
	roomID, channelID, err := s.memberTarget(ctx, params)
	if err != nil {
		return operationsmodule.Record{}, internalError(err)
	}
	if roomID == "" {
		roomID = trimString(params["room_id"])
	}
	if channelID == "" {
		channelID = trimString(params["channel_id"])
	}
	operationChannelID := channelID
	if roomID != "" {
		// A retained Matrix room uniquely identifies the member workflow.
		// channel_id is a projection that may only become known after a remote
		// lookup, so including it would change the derived operation on replay.
		operationChannelID = ""
	}
	userID := firstMemberID(params)
	if userID == "" && (strings.HasPrefix(action, "channels.") || action == "groups.join" || action == "groups.invite.reject") {
		userID = s.OwnerMXID()
	}
	peerMXID := fallbackString(trimString(params["peer_mxid"]), trimString(params["mxid"]))
	explicitRequestID := trimString(params["request_id"])
	requestID := firstNonEmptyOperationPart(params, "request_id", "invite_event_id", "grant_id", "direct_room_id")
	if explicitRetainedRoomRebuildAction(action, params) {
		requestID = trimString(params["rebuild_generation"])
	}
	if action == "contacts.request" && explicitRequestID == "" && providedOperationID {
		digest := sha256.Sum256([]byte(action + "\x00" + operationID))
		requestID = "request_" + hex.EncodeToString(digest[:16])
	}
	memberGeneration := int64(0)
	baseRequestID := ""
	if strings.HasPrefix(action, "contacts.") && s.contactsModule != nil {
		var contact contactStorageRecord
		var found bool
		var lookupErr error
		if roomID != "" {
			contact, found, lookupErr = s.contactsModule.LookupByRoom(ctx, roomID)
		}
		if !found && peerMXID != "" && lookupErr == nil {
			contact, found, lookupErr = s.contactsModule.LookupByPeer(ctx, peerMXID)
		}
		if lookupErr != nil {
			return operationsmodule.Record{}, internalError(lookupErr)
		}
		if found {
			baseRequestID = contact.RequestID
			if action == "contacts.request" && explicitRequestID == "" && !providedOperationID &&
				dirextalkdomain.ContactDeleted(contact.Status) {
				digest := sha256.Sum256([]byte(strings.Join([]string{
					action, contact.PeerMXID, contact.RequestID, "deleted-next",
				}, "\x00")))
				requestID = "request_" + hex.EncodeToString(digest[:16])
			} else {
				requestID = fallbackString(requestID, contact.RequestID)
			}
			peerMXID = fallbackString(peerMXID, contact.PeerMXID)
			roomID = fallbackString(roomID, contact.RoomID)
		}
	}
	if action == "contacts.request" {
		// The direct room does not exist when a fresh request operation is
		// identified. A response-loss replay may discover the committed room,
		// but that room is recovery output (current_room_id), not new identity.
		roomID = ""
	}
	if roomID != "" && userID != "" {
		if member, ok, lookupErr := s.lookupMember(ctx, roomID, userID); lookupErr != nil {
			return operationsmodule.Record{}, internalError(lookupErr)
		} else if ok {
			baseRequestID = member.RequestID
			if action == "channels.public.join_request" {
				restartable := channelJoinGenerationMayRestart(member.Membership)
				if member.RequestID != "" && !restartable {
					// An active generation is canonical. Unauthenticated caller IDs
					// are validation-only inputs and never select durable identity.
					requestID = member.RequestID
				} else {
					requestID = publicJoinRequestGeneration(roomID, operationChannelID, userID, member.RequestID)
				}
			} else if action == "channels.public.join_result" {
				// A callback can only settle the requester's current generation.
				requestID = fallbackString(
					member.RequestID,
					publicJoinRequestGeneration(roomID, operationChannelID, userID, ""),
				)
			} else if (action == "groups.join" || action == "groups.invite.reject") && member.RequestID != "" {
				// Group cards carry the direct-room message event ID, while the
				// persisted member generation is the authoritative Matrix invite
				// event ID. They are expected to differ.
				requestID = member.RequestID
			} else if requestID == "" {
				requestID = member.RequestID
			}
			memberGeneration = member.JoinedAt
		}
	}
	if action == "channels.public.join_request" && baseRequestID == "" {
		// No persisted member exists yet. Discard caller-selected request IDs
		// and derive the first generation from the server-owned target identity.
		requestID = publicJoinRequestGeneration(roomID, operationChannelID, userID, "")
	}
	if requestID == "" && memberGeneration > 0 {
		requestID = formatOperationGeneration(memberGeneration)
	}
	if requestID == "" {
		digest := sha256.Sum256([]byte(strings.Join([]string{
			roomID, operationChannelID, userID, peerMXID,
		}, "\x00")))
		requestID = "req_" + hex.EncodeToString(digest[:16])
	}
	if action == "channels.public.join_request" || action == "channels.public.join_result" ||
		explicitRetainedRoomRebuildAction(action, params) {
		// Public callbacks use the canonical request generation as their
		// durable key. A caller-selected operation ID is validated above but
		// cannot create an unbounded parallel operation namespace.
		providedOperationID = false
		operationID = ""
	}
	if !providedOperationID {
		digest := sha256.Sum256([]byte(strings.Join([]string{
			action, roomID, operationChannelID, userID, peerMXID, requestID,
		}, "\x00")))
		operationID = "op_" + hex.EncodeToString(digest[:16])
	}
	return operationsmodule.Record{
		OperationID: operationID, Action: action, Status: operationStatusRunning, Phase: operationPhasePrepared,
		RoomID: roomID, UserID: userID, PeerMXID: peerMXID, RequestID: requestID, BaseRequestID: baseRequestID,
		CreatedAt: now, UpdatedAt: now,
	}, nil
}

func publicJoinRequestGeneration(roomID, channelID, userID, currentRequestID string) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		"channels.public.join_request", roomID, channelID, userID, currentRequestID, "next",
	}, "\x00")))
	return "request_" + hex.EncodeToString(digest[:16])
}

func firstNonEmptyOperationPart(params map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := trimString(params[key]); value != "" {
			return value
		}
	}
	return ""
}

func formatOperationGeneration(value int64) string {
	return time.UnixMilli(value).UTC().Format(time.RFC3339Nano)
}

func operationIdentityConflict(stored, incoming operationsmodule.Record) bool {
	return conflictingOperationValue(stored.RoomID, incoming.RoomID) ||
		conflictingOperationValue(stored.UserID, incoming.UserID) ||
		conflictingOperationValue(stored.PeerMXID, incoming.PeerMXID) ||
		conflictingOperationValue(stored.RequestID, incoming.RequestID)
}

func conflictingOperationValue(left, right string) bool {
	if left == "" {
		return right != ""
	}
	return right != "" && left != right
}

func operationPersistenceError(record operationsmodule.Record, err error) *apiError {
	if err == nil {
		err = errors.New("recoverable operation persistence failed")
	}
	return &apiError{
		Status: 500, Error: "internal error: " + err.Error(), Code: actionbase.OperationRecoveryCode,
		OperationID: record.OperationID, CurrentRoomID: fallbackString(record.CurrentRoomID, record.RoomID),
	}
}

func operationWriteContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
}

func operationHasExternalCommit(phase string) bool {
	return phase == operationPhaseMatrixCommitted || phase == operationPhaseCallbackAcknowledged || phase == operationPhaseCompleted
}

func operationNeedsRecovery(phase string) bool {
	return phase == operationPhaseMatrixUnconfirmed || operationHasExternalCommit(phase)
}

func operationResultNeedsReconciliation(status, errorCode string) bool {
	if errorCode == actionbase.MatrixJoinUnconfirmedCode || errorCode == actionbase.JoinResultUnconfirmedCode ||
		errorCode == actionbase.MatrixJoinFailedCode || errorCode == actionbase.OperationRecoveryCode {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "approved", "joining", "join_failed":
		return true
	default:
		return false
	}
}

func operationResultMetadata(result any, record operationsmodule.Record) (any, string, string, string) {
	status := ""
	currentRoomID := record.CurrentRoomID
	errorCode := ""
	switch value := result.(type) {
	case map[string]any:
		status = trimString(value["status"])
		if currentRoomID == "" {
			currentRoomID = fallbackString(trimString(value["current_room_id"]), trimString(value["room_id"]))
		}
		currentRoomID = fallbackString(currentRoomID, record.RoomID)
		errorCode = trimString(value["error_code"])
		value["operation_id"] = record.OperationID
		if currentRoomID != "" {
			value["current_room_id"] = currentRoomID
		}
		result = value
	case contactsmodule.View:
		status = value.Status
		if currentRoomID == "" {
			currentRoomID = fallbackString(value.CurrentRoomID, value.RoomID)
		}
		currentRoomID = fallbackString(currentRoomID, record.RoomID)
		value.OperationID = record.OperationID
		value.CurrentRoomID = currentRoomID
		errorCode = value.ErrorCode
		result = value
	}
	currentRoomID = fallbackString(currentRoomID, record.RoomID)
	return result, status, currentRoomID, errorCode
}

func decodeRecoverableOperationResult(action, resultJSON string) (any, error) {
	if strings.HasPrefix(action, "contacts.") {
		var view contactsmodule.View
		if err := json.Unmarshal([]byte(resultJSON), &view); err != nil {
			return nil, err
		}
		return view, nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(resultJSON), &raw); err != nil {
		return nil, err
	}
	result := make(map[string]any, len(raw))
	for key, value := range raw {
		switch key {
		case "member":
			var member memberRecord
			if err := json.Unmarshal(value, &member); err != nil {
				return nil, err
			}
			result[key] = member
		case "channel":
			var current channel
			if err := json.Unmarshal(value, &current); err != nil {
				return nil, err
			}
			result[key] = current
		case "conversation":
			var conversation conversationView
			if err := json.Unmarshal(value, &conversation); err != nil {
				return nil, err
			}
			result[key] = conversation
		default:
			var decoded any
			decoder := json.NewDecoder(strings.NewReader(string(value)))
			decoder.UseNumber()
			if err := decoder.Decode(&decoded); err != nil {
				return nil, err
			}
			result[key] = decoded
		}
	}
	return result, nil
}

func (s *Service) completedOperationStillCurrent(ctx context.Context, record operationsmodule.Record, result any) (bool, error) {
	status := ""
	currentRoomID := record.CurrentRoomID
	switch value := result.(type) {
	case map[string]any:
		status = trimString(value["status"])
		if currentRoomID == "" {
			currentRoomID = fallbackString(trimString(value["current_room_id"]), trimString(value["room_id"]))
		}
	case contactsmodule.View:
		status = value.Status
		if currentRoomID == "" {
			currentRoomID = fallbackString(value.CurrentRoomID, value.RoomID)
		}
	}
	currentRoomID = fallbackString(currentRoomID, record.RoomID)
	if retainedRoomInviteAction(record.Action) {
		member, found, err := s.lookupMember(ctx, record.RoomID, record.UserID)
		if err != nil || !found || member.RequestID != record.RequestID {
			return false, err
		}
		switch strings.ToLower(strings.TrimSpace(member.Membership)) {
		case "invite":
			if s.transport == nil {
				return true, nil
			}
			joined, joinErr := s.matrixMemberJoined(ctx, record.RoomID, record.UserID)
			return !joined, joinErr
		case "join":
			if s.transport == nil {
				return true, nil
			}
			return s.matrixMemberJoined(ctx, record.RoomID, record.UserID)
		default:
			return false, nil
		}
	}
	if strings.HasPrefix(record.Action, "contacts.") {
		view, ok := result.(contactsmodule.View)
		if !ok || s.contactsModule == nil {
			return false, nil
		}
		// A pending outbound contact request deliberately resends its Matrix
		// invite on replay. Do not let the operation result cache suppress that
		// existing recovery behavior.
		if record.Action == "contacts.request" && strings.EqualFold(strings.TrimSpace(view.Status), "pending_outbound") {
			return false, nil
		}
		contact, found, err := s.contactsModule.LookupByPeer(ctx, view.PeerMXID)
		if err != nil || !found {
			return false, err
		}
		if contact.RoomID != view.RoomID || !strings.EqualFold(strings.TrimSpace(contact.Status), strings.TrimSpace(view.Status)) {
			return false, nil
		}
		if record.Action == "contacts.requests.reject" &&
			(strings.EqualFold(strings.TrimSpace(view.Status), "reject") || strings.EqualFold(strings.TrimSpace(view.Status), "rejected")) &&
			s.transport != nil {
			joined, joinErr := s.matrixMemberJoined(ctx, contact.RoomID, s.OwnerMXID())
			if joinErr != nil || joined {
				return false, joinErr
			}
		}
		if record.Action == "contacts.requests.reject" &&
			strings.EqualFold(strings.TrimSpace(view.Status), "accepted") && s.transport != nil {
			joined, joinErr := s.matrixMemberJoined(ctx, contact.RoomID, s.OwnerMXID())
			if joinErr != nil || !joined {
				return false, joinErr
			}
		}
		if record.Action == "contacts.requests.accept" && strings.EqualFold(strings.TrimSpace(view.Status), "accepted") && s.transport != nil {
			joined, joinErr := s.matrixMemberJoined(ctx, contact.RoomID, s.OwnerMXID())
			if joinErr != nil || !joined {
				return false, joinErr
			}
			if s.conversationModule != nil {
				_, found, conversationErr := s.conversationModule.GetRecord(ctx, "", contact.RoomID)
				if conversationErr != nil || !found {
					return false, conversationErr
				}
			}
			return true, nil
		}
		return true, nil
	}
	if (status == "rejected" || status == "reject") && record.RoomID != "" && record.UserID != "" {
		member, found, err := s.lookupMember(ctx, record.RoomID, record.UserID)
		if err != nil || !found {
			return false, err
		}
		membership := strings.ToLower(strings.TrimSpace(member.Membership))
		if membership != "reject" && membership != "rejected" {
			return false, nil
		}
		if s.transport != nil {
			joined, joinErr := s.matrixMemberJoined(ctx, record.RoomID, record.UserID)
			if joinErr != nil || joined {
				return false, joinErr
			}
		}
		return true, nil
	}
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "joined", "accepted", "pending_inbound", "pending_outbound":
		// These states are backed by a Matrix room fact when transport is
		// available. A stale cached ProductCore projection is never enough.
	case "ok":
		if record.Action == "groups.invite.reject" {
			return true, nil
		}
	default:
		return false, nil
	}
	if s.transport == nil {
		return true, nil
	}
	return s.matrixMemberJoined(ctx, currentRoomID, record.UserID)
}

func (s *Service) lockRecoverableOperation(operationID string) func() {
	s.operationEntriesMu.Lock()
	if s.operationEntries == nil {
		s.operationEntries = make(map[string]*recoverableOperationEntry)
	}
	entry := s.operationEntries[operationID]
	if entry == nil {
		entry = &recoverableOperationEntry{}
		s.operationEntries[operationID] = entry
	}
	entry.refs++
	s.operationEntriesMu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		s.operationEntriesMu.Lock()
		entry.refs--
		if entry.refs == 0 && s.operationEntries[operationID] == entry {
			delete(s.operationEntries, operationID)
		}
		s.operationEntriesMu.Unlock()
	}
}

func (s *Service) lockMemberWorkflow(key string) func() {
	s.workflowEntriesMu.Lock()
	if s.workflowEntries == nil {
		s.workflowEntries = make(map[string]*recoverableOperationEntry)
	}
	entry := s.workflowEntries[key]
	if entry == nil {
		entry = &recoverableOperationEntry{}
		s.workflowEntries[key] = entry
	}
	entry.refs++
	s.workflowEntriesMu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		s.workflowEntriesMu.Lock()
		entry.refs--
		if entry.refs == 0 && s.workflowEntries[key] == entry {
			delete(s.workflowEntries, key)
		}
		s.workflowEntriesMu.Unlock()
	}
}

func operationTracker(ctx context.Context) (*operationsmodule.Tracker, bool) {
	tracker, ok := ctx.Value(recoverableOperationContextKey{}).(*operationsmodule.Tracker)
	return tracker, ok && tracker != nil
}

func recoverableOperationSnapshot(ctx context.Context) (operationsmodule.Record, bool) {
	tracker, ok := operationTracker(ctx)
	if !ok {
		return operationsmodule.Record{}, false
	}
	return tracker.Snapshot(), true
}

func markRecoverableOperation(ctx context.Context, phase, currentRoomID string) error {
	tracker, ok := operationTracker(ctx)
	if !ok {
		return nil
	}
	current := tracker.Snapshot()
	return tracker.Update(ctx, operationStatusRunning, phase, currentRoomID, current.ErrorCode, current.ResultJSON)
}

func recoverableOperationWriteError(ctx context.Context, err error) *apiError {
	if record, ok := recoverableOperationSnapshot(ctx); ok {
		return operationPersistenceError(record, err)
	}
	return internalError(err)
}
