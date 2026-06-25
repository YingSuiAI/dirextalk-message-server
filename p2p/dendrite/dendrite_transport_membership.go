package dendrite

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/productpolicy"
	roomserverAPI "github.com/YingSuiAI/direxio-message-server/roomserver/api"
	"github.com/YingSuiAI/direxio-message-server/roomserver/types"
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
			RoomID:     *roomID,
			Inviter:    *inviter,
			Invitee:    *invitee,
			Reason:     req.Reason,
			IsDirect:   req.IsDirect,
			KeyID:      t.keyID,
			PrivateKey: t.privateKey,
			EventTime:  time.Now(),
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
			directInvitePolicyErr = err
		}
	}
	serverNames := make([]spec.ServerName, 0, len(req.ServerNames))
	for _, serverName := range req.ServerNames {
		if strings.TrimSpace(serverName) != "" {
			serverNames = append(serverNames, spec.ServerName(strings.TrimSpace(serverName)))
		}
	}
	roomID, joinedVia, err := t.rsAPI.PerformJoin(ctx, &roomserverAPI.PerformJoinRequest{
		RoomIDOrAlias: req.RoomIDOrAlias,
		UserID:        req.UserMXID,
		Content:       joinRoomContent(req),
		ServerNames:   serverNames,
	})
	if err != nil {
		if directInvitePolicyErr != nil {
			return JoinRoomResult{}, directInvitePolicyErr
		}
		return JoinRoomResult{}, err
	}
	return JoinRoomResult{RoomID: roomID, JoinedVia: string(joinedVia)}, nil
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
