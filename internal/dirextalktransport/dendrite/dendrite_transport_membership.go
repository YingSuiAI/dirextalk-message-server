package dendrite

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func (t *DendriteTransport) InviteUser(ctx context.Context, req InviteUserRequest) error {
	roomID, err := spec.NewRoomID(req.RoomID)
	if err != nil {
		return err
	}
	inviter, err := spec.NewUserID(req.InviterMXID, true)
	if err != nil {
		return err
	}
	if inviter.Domain() != t.serverName {
		return fmt.Errorf("inviter %s is not local to %s", req.InviterMXID, t.serverName)
	}
	invitee, err := spec.NewUserID(req.InviteeMXID, true)
	if err != nil {
		return err
	}
	if err = productpolicy.ValidateClientMembership(ctx, t.productPolicyQuerier(), productpolicy.ClientMembershipRequest{
		RoomID:     req.RoomID,
		SenderMXID: req.InviterMXID,
		TargetMXID: req.InviteeMXID,
		Membership: string(spec.Invite),
	}); err != nil {
		return err
	}
	inviteRoomState, err := inviteStrippedState(req.InviteRoomState, req.InviterMXID)
	if err != nil {
		return err
	}
	return t.rsAPI.PerformInvite(ctx, &roomserverAPI.PerformInviteRequest{
		InviteInput: roomserverAPI.InviteInput{
			RoomID:              *roomID,
			Inviter:             *inviter,
			Invitee:             *invitee,
			Reason:              req.Reason,
			IsDirect:            req.IsDirect,
			PublicJoinRequestID: req.PublicJoinRequestID,
			KeyID:               t.keyID,
			PrivateKey:          t.privateKey,
			EventTime:           time.Now(),
		},
		InviteRoomState: inviteRoomState,
		SendAsServer:    string(t.serverName),
	})
}

func inviteStrippedState(states []RoomStateEvent, senderID string) ([]gomatrixserverlib.InviteStrippedState, error) {
	stripped := make([]gomatrixserverlib.InviteStrippedState, 0, len(states))
	for _, state := range states {
		content, err := json.Marshal(state.Content)
		if err != nil {
			return nil, err
		}
		stateKey := state.StateKey
		raw, err := json.Marshal(map[string]any{
			"type":      state.Type,
			"state_key": stateKey,
			"sender":    senderID,
			"content":   json.RawMessage(content),
		})
		if err != nil {
			return nil, err
		}
		var event gomatrixserverlib.InviteStrippedState
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, err
		}
		stripped = append(stripped, event)
	}
	return stripped, nil
}

func (t *DendriteTransport) JoinRoom(ctx context.Context, req JoinRoomRequest) (JoinRoomResult, error) {
	userID, err := spec.NewUserID(req.UserMXID, true)
	if err != nil {
		return JoinRoomResult{}, err
	}
	if userID.Domain() != t.serverName {
		return JoinRoomResult{}, fmt.Errorf("joining user %s is not local to %s", req.UserMXID, t.serverName)
	}
	var directInvitePolicyErr error
	if _, err = spec.NewRoomID(req.RoomIDOrAlias); err == nil {
		if err = productpolicy.ValidateClientMembership(ctx, t.productPolicyQuerier(), productpolicy.ClientMembershipRequest{
			RoomID:     req.RoomIDOrAlias,
			SenderMXID: req.UserMXID,
			TargetMXID: req.UserMXID,
			Membership: string(spec.Join),
		}); err != nil {
			if !isDirectRoomJoinRequiresInvite(err) {
				return JoinRoomResult{}, err
			}
			if !req.DirectContactReactivation {
				directInvitePolicyErr = err
			}
		}
	}
	serverNames := make([]spec.ServerName, 0, len(req.ServerNames))
	for _, serverName := range req.ServerNames {
		if strings.TrimSpace(serverName) != "" {
			serverNames = append(serverNames, spec.ServerName(strings.TrimSpace(serverName)))
		}
	}
	performJoin := func() (string, spec.ServerName, error) {
		return t.rsAPI.PerformJoin(ctx, &roomserverAPI.PerformJoinRequest{
			RoomIDOrAlias: req.RoomIDOrAlias,
			UserID:        req.UserMXID,
			Content:       joinRoomContent(req),
			ServerNames:   append([]spec.ServerName(nil), serverNames...),
		})
	}
	roomID, joinedVia, err := performJoin()
	if err != nil {
		if directInvitePolicyErr != nil {
			return JoinRoomResult{}, directInvitePolicyErr
		}
		return JoinRoomResult{}, err
	}
	if joinedVia != "" && (joinedVia != t.serverName || len(serverNames) > 0) {
		ready, _, readyErr := t.joinedRoomReadiness(ctx, roomID, req.UserMXID)
		if readyErr != nil {
			return JoinRoomResult{}, fmt.Errorf("confirm joined room %s: %w", roomID, readyErr)
		}
		if !ready {
			// A federation input batch can become visible after this read. A
			// check followed by a destructive purge is not atomic with that input,
			// so it could erase a room which has just finished importing. Preserve
			// the partial state and surface an in-progress result for the durable
			// ProductCore recovery path to reconcile.
			return JoinRoomResult{}, fmt.Errorf("federated join in progress: local room %s is not ready", roomID)
		}
	}
	return JoinRoomResult{RoomID: roomID, JoinedVia: string(joinedVia)}, nil
}

// JoinedRoomReady confirms that a local Matrix join is usable, not merely a
// membership row left behind by a soft-failed federated state import.
func (t *DendriteTransport) JoinedRoomReady(ctx context.Context, roomID, userMXID string) (bool, error) {
	ready, _, err := t.joinedRoomReadiness(ctx, roomID, userMXID)
	return ready, err
}

func (t *DendriteTransport) joinedRoomReadiness(ctx context.Context, roomID, userMXID string) (bool, bool, error) {
	userID, err := spec.NewUserID(userMXID, true)
	if err != nil {
		return false, false, err
	}
	var latest roomserverAPI.QueryLatestEventsAndStateResponse
	if err := t.rsAPI.QueryLatestEventsAndState(ctx, &roomserverAPI.QueryLatestEventsAndStateRequest{
		RoomID: roomID,
		StateToFetch: []gomatrixserverlib.StateKeyTuple{
			{EventType: spec.MRoomCreate, StateKey: ""},
		},
	}, &latest); err != nil {
		return false, false, err
	}
	if !latest.RoomExists || latest.RoomVersion == "" || len(latest.LatestEvents) == 0 {
		return false, latest.RoomExists, nil
	}
	createReady := false
	for _, event := range latest.StateEvents {
		if event != nil && event.Type() == spec.MRoomCreate && event.StateKey() != nil && *event.StateKey() == "" {
			createReady = true
			break
		}
	}
	if !createReady {
		return false, true, nil
	}
	var membership roomserverAPI.QueryMembershipForUserResponse
	if err := t.rsAPI.QueryMembershipForUser(ctx, &roomserverAPI.QueryMembershipForUserRequest{
		RoomID: roomID,
		UserID: *userID,
	}, &membership); err != nil {
		return false, true, err
	}
	return membership.RoomExists && membership.IsInRoom &&
		strings.EqualFold(strings.TrimSpace(membership.Membership), string(spec.Join)), true, nil
}

func joinRoomContent(req JoinRoomRequest) map[string]interface{} {
	content := map[string]interface{}{}
	if displayName := strings.TrimSpace(req.DisplayName); displayName != "" {
		content["displayname"] = displayName
	}
	if avatarURL := strings.TrimSpace(req.AvatarURL); avatarURL != "" {
		content["avatar_url"] = avatarURL
	}
	return content
}

func (t *DendriteTransport) LeaveRoom(ctx context.Context, req LeaveRoomRequest) error {
	leaver, err := spec.NewUserID(req.UserMXID, true)
	if err != nil {
		return err
	}
	if leaver.Domain() != t.serverName {
		return fmt.Errorf("leaving user %s is not local to %s", req.UserMXID, t.serverName)
	}
	if err = productpolicy.ValidateClientMembership(ctx, t.productPolicyQuerier(), productpolicy.ClientMembershipRequest{
		RoomID:     req.RoomID,
		SenderMXID: req.UserMXID,
		TargetMXID: req.UserMXID,
		Membership: string(spec.Leave),
	}); err != nil {
		return err
	}
	res := roomserverAPI.PerformLeaveResponse{}
	if err := t.rsAPI.PerformLeave(ctx, &roomserverAPI.PerformLeaveRequest{
		RoomID: req.RoomID,
		Leaver: *leaver,
	}, &res); err != nil {
		return err
	}
	if res.Code >= 400 {
		return fmt.Errorf("leave room failed: status=%d body=%s", res.Code, jsonString(res.Message))
	}
	return nil
}

func (t *DendriteTransport) KickUser(ctx context.Context, req KickUserRequest) error {
	if req.Timestamp.IsZero() {
		req.Timestamp = time.Now()
	}
	sender, err := spec.NewUserID(req.SenderMXID, true)
	if err != nil {
		return err
	}
	if sender.Domain() != t.serverName {
		return fmt.Errorf("kick sender %s is not local to %s", req.SenderMXID, t.serverName)
	}
	target, err := spec.NewUserID(req.TargetMXID, true)
	if err != nil {
		return err
	}
	if err = productpolicy.ValidateClientMembership(ctx, t.productPolicyQuerier(), productpolicy.ClientMembershipRequest{
		RoomID:     req.RoomID,
		SenderMXID: req.SenderMXID,
		TargetMXID: req.TargetMXID,
		Membership: string(spec.Leave),
	}); err != nil {
		return err
	}
	validRoomID, err := spec.NewRoomID(req.RoomID)
	if err != nil {
		return err
	}
	senderID, err := t.rsAPI.QuerySenderIDForUser(ctx, *validRoomID, *sender)
	if err != nil {
		return err
	}
	if senderID == nil {
		return fmt.Errorf("kick sender %s is not joined to room %s", req.SenderMXID, req.RoomID)
	}
	identity, err := t.rsAPI.SigningIdentityFor(ctx, *validRoomID, *sender)
	if err != nil {
		return err
	}
	stateKey := target.String()
	proto := gomatrixserverlib.ProtoEvent{
		SenderID: string(*senderID),
		RoomID:   req.RoomID,
		Type:     spec.MRoomMember,
		StateKey: &stateKey,
	}
	content := map[string]any{"membership": string(spec.Leave)}
	if strings.TrimSpace(req.Reason) != "" {
		content["reason"] = strings.TrimSpace(req.Reason)
	}
	if err = proto.SetContent(content); err != nil {
		return err
	}
	event, queryRes, err := t.queryAndBuildEvent(ctx, &proto, &identity, req.Timestamp, req.RoomID)
	if err != nil {
		return err
	}
	stateEvents := make([]gomatrixserverlib.PDU, len(queryRes.StateEvents))
	for i := range queryRes.StateEvents {
		stateEvents[i] = queryRes.StateEvents[i].PDU
	}
	provider, err := gomatrixserverlib.NewAuthEvents(gomatrixserverlib.ToPDUs(stateEvents))
	if err != nil {
		return err
	}
	if err = gomatrixserverlib.Allowed(event.PDU, provider, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return t.rsAPI.QueryUserIDForSender(ctx, *validRoomID, senderID)
	}); err != nil {
		return err
	}
	return roomserverAPI.SendEvents(
		ctx,
		t.rsAPI,
		roomserverAPI.KindNew,
		[]*types.HeaderedEvent{{PDU: event.PDU}},
		sender.Domain(),
		sender.Domain(),
		sender.Domain(),
		nil,
		false,
	)
}
