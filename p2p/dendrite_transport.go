package p2p

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/eventutil"
	"github.com/YingSuiAI/direxio-message-server/internal/productpolicy"
	roomserverAPI "github.com/YingSuiAI/direxio-message-server/roomserver/api"
	"github.com/YingSuiAI/direxio-message-server/roomserver/types"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
)

type DendriteTransport struct {
	serverName spec.ServerName
	keyID      gomatrixserverlib.KeyID
	privateKey ed25519.PrivateKey
	rsAPI      roomserverAPI.ClientRoomserverAPI
}

type productPolicyRoomserver struct {
	roomserverAPI.ClientRoomserverAPI
}

func (q productPolicyRoomserver) InvitePending(ctx context.Context, roomID spec.RoomID, senderID spec.SenderID) (bool, error) {
	invites, ok := q.ClientRoomserverAPI.(interface {
		InvitePending(context.Context, spec.RoomID, spec.SenderID) (bool, error)
	})
	if !ok {
		return false, nil
	}
	return invites.InvitePending(ctx, roomID, senderID)
}

func NewDendriteTransport(serverName spec.ServerName, keyID gomatrixserverlib.KeyID, privateKey ed25519.PrivateKey, rsAPI roomserverAPI.ClientRoomserverAPI) *DendriteTransport {
	return &DendriteTransport{
		serverName: serverName,
		keyID:      keyID,
		privateKey: privateKey,
		rsAPI:      rsAPI,
	}
}

func (t *DendriteTransport) productPolicyQuerier() productPolicyRoomserver {
	return productPolicyRoomserver{ClientRoomserverAPI: t.rsAPI}
}

func (t *DendriteTransport) CreateRoom(ctx context.Context, req CreateRoomRequest) (CreateRoomResult, error) {
	userID, err := spec.NewUserID(req.CreatorMXID, true)
	if err != nil {
		return CreateRoomResult{}, err
	}
	if userID.Domain() != t.serverName {
		return CreateRoomResult{}, fmt.Errorf("creator %s is not local to %s", req.CreatorMXID, t.serverName)
	}
	roomID, err := spec.NewRoomID(fmt.Sprintf("!%s:%s", util.RandomString(16), userID.Domain()))
	if err != nil {
		return CreateRoomResult{}, err
	}
	initialState := make([]gomatrixserverlib.FledglingEvent, 0, len(req.InitialState))
	for _, state := range req.InitialState {
		initialState = append(initialState, gomatrixserverlib.FledglingEvent{
			Type:     state.Type,
			StateKey: state.StateKey,
			Content:  state.Content,
		})
	}
	creatorDisplayName := strings.TrimSpace(req.CreatorDisplayName)
	if creatorDisplayName == "" {
		creatorDisplayName = localpart(req.CreatorMXID)
	}
	creationContent := map[string]any{}
	for key, value := range req.CreationContent {
		creationContent[key] = value
	}
	if roomType := strings.TrimSpace(req.RoomType); roomType != "" {
		creationContent["type"] = roomType
	}
	var creationContentJSON json.RawMessage
	if len(creationContent) > 0 {
		raw, err := json.Marshal(creationContent)
		if err != nil {
			return CreateRoomResult{}, err
		}
		creationContentJSON = raw
	}
	createReq := roomserverAPI.PerformCreateRoomRequest{
		InvitedUsers:    req.InviteMXIDs,
		RoomName:        req.Name,
		Visibility:      matrixVisibility(req.Visibility),
		Topic:           req.Topic,
		StatePreset:     matrixPreset(req.Visibility, req.IsDirect),
		CreationContent: creationContentJSON,
		InitialState:    initialState,
		RoomVersion:     t.rsAPI.DefaultRoomVersion(),
		IsDirect:        req.IsDirect,
		UserDisplayName: creatorDisplayName,
		UserAvatarURL:   strings.TrimSpace(req.CreatorAvatarURL),
		KeyID:           t.keyID,
		PrivateKey:      t.privateKey,
		EventTime:       time.Now(),
	}
	_, createRes := t.rsAPI.PerformCreateRoom(ctx, *userID, *roomID, &createReq)
	if createRes != nil {
		return CreateRoomResult{}, fmt.Errorf("create room failed: status=%d body=%s", createRes.Code, jsonString(createRes.JSON))
	}
	return CreateRoomResult{RoomID: roomID.String()}, nil
}

func (t *DendriteTransport) SendMessage(ctx context.Context, req SendMessageRequest) (SendMessageResult, error) {
	if req.Timestamp.IsZero() {
		req.Timestamp = time.Now()
	}
	fullUserID, err := spec.NewUserID(req.SenderMXID, true)
	if err != nil {
		return SendMessageResult{}, err
	}
	validRoomID, err := spec.NewRoomID(req.RoomID)
	if err != nil {
		return SendMessageResult{}, err
	}
	senderID, err := t.rsAPI.QuerySenderIDForUser(ctx, *validRoomID, *fullUserID)
	if err != nil {
		return SendMessageResult{}, err
	}
	if senderID == nil {
		return SendMessageResult{}, fmt.Errorf("sender %s is not joined to room %s", req.SenderMXID, req.RoomID)
	}
	eventType := strings.TrimSpace(req.EventType)
	if eventType == "" {
		eventType = "m.room.message"
	}
	content := req.Content
	if content == nil {
		content = map[string]any{}
	}
	if eventType == "m.room.message" {
		if _, ok := content["msgtype"]; !ok {
			content["msgtype"] = matrixMessageType(req.MessageType, false)
		}
	}
	if err = productpolicy.ValidateClientEvent(ctx, t.productPolicyQuerier(), productpolicy.ClientEventRequest{
		RoomID:     req.RoomID,
		SenderMXID: req.SenderMXID,
		EventType:  eventType,
		Content:    content,
	}); err != nil {
		return SendMessageResult{}, err
	}
	identity, err := t.rsAPI.SigningIdentityFor(ctx, *validRoomID, *fullUserID)
	if err != nil {
		return SendMessageResult{}, err
	}
	proto := gomatrixserverlib.ProtoEvent{
		SenderID: string(*senderID),
		RoomID:   req.RoomID,
		Type:     eventType,
	}
	if err = proto.SetContent(content); err != nil {
		return SendMessageResult{}, err
	}
	event, queryRes, err := t.queryAndBuildEvent(ctx, &proto, &identity, req.Timestamp, req.RoomID)
	if err != nil {
		return SendMessageResult{}, err
	}
	stateEvents := make([]gomatrixserverlib.PDU, len(queryRes.StateEvents))
	for i := range queryRes.StateEvents {
		stateEvents[i] = queryRes.StateEvents[i].PDU
	}
	provider, err := gomatrixserverlib.NewAuthEvents(gomatrixserverlib.ToPDUs(stateEvents))
	if err != nil {
		return SendMessageResult{}, err
	}
	if err = gomatrixserverlib.Allowed(event.PDU, provider, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return t.rsAPI.QueryUserIDForSender(ctx, *validRoomID, senderID)
	}); err != nil {
		return SendMessageResult{}, err
	}
	if err = roomserverAPI.SendEvents(
		ctx,
		t.rsAPI,
		roomserverAPI.KindNew,
		[]*types.HeaderedEvent{{PDU: event.PDU}},
		fullUserID.Domain(),
		fullUserID.Domain(),
		fullUserID.Domain(),
		nil,
		false,
	); err != nil {
		return SendMessageResult{}, err
	}
	return SendMessageResult{EventID: event.EventID(), OriginServerTS: int64(event.OriginServerTS())}, nil
}

func (t *DendriteTransport) SendStateEvent(ctx context.Context, req SendStateEventRequest) error {
	if req.Timestamp.IsZero() {
		req.Timestamp = time.Now()
	}
	sender, err := spec.NewUserID(req.SenderMXID, true)
	if err != nil {
		return err
	}
	if sender.Domain() != t.serverName {
		return fmt.Errorf("state sender %s is not local to %s", req.SenderMXID, t.serverName)
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
		return fmt.Errorf("state sender %s is not joined to room %s", req.SenderMXID, req.RoomID)
	}
	identity, err := t.rsAPI.SigningIdentityFor(ctx, *validRoomID, *sender)
	if err != nil {
		return err
	}
	stateKey := req.Event.StateKey
	proto := gomatrixserverlib.ProtoEvent{
		SenderID: string(*senderID),
		RoomID:   req.RoomID,
		Type:     req.Event.Type,
		StateKey: &stateKey,
	}
	if err = proto.SetContent(req.Event.Content); err != nil {
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
	if !isDirexioPolicyStateEvent(req.Event.Type) {
		if err = gomatrixserverlib.Allowed(event.PDU, provider, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return t.rsAPI.QueryUserIDForSender(ctx, *validRoomID, senderID)
		}); err != nil {
			return err
		}
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

func isDirexioPolicyStateEvent(eventType string) bool {
	switch eventType {
	case DirexioJoinRequestEventType, DirexioMemberPolicyEventType:
		return true
	default:
		return false
	}
}

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

func (t *DendriteTransport) GetRoomChannel(ctx context.Context, roomID string) (channel, bool, error) {
	var res roomserverAPI.QueryCurrentStateResponse
	if err := t.rsAPI.QueryCurrentState(ctx, &roomserverAPI.QueryCurrentStateRequest{
		RoomID: roomID,
		StateTuples: []gomatrixserverlib.StateKeyTuple{
			{EventType: DirexioRoomProfileEventType, StateKey: ""},
		},
	}, &res); err != nil {
		return channel{}, false, err
	}
	event := res.StateEvents[gomatrixserverlib.StateKeyTuple{EventType: DirexioRoomProfileEventType, StateKey: ""}]
	if event != nil {
		content := map[string]any{}
		if err := json.Unmarshal(event.Content(), &content); err != nil {
			return channel{}, false, err
		}
		if trimString(content["room_type"]) != "" && trimString(content["room_type"]) != DirexioRoomTypeChannel {
			return channel{}, false, nil
		}
		channelID := trimString(content["channel_id"])
		if channelID == "" {
			channelID = roomID
		}
		return channel{
			ChannelID:        channelID,
			RoomID:           roomID,
			Name:             fallbackString(trimString(content["name"]), channelID),
			Description:      trimString(content["description"]),
			AvatarURL:        trimString(content["avatar_url"]),
			Visibility:       fallbackString(trimString(content["visibility"]), "private"),
			JoinPolicy:       fallbackString(trimString(content["join_policy"]), "invite"),
			ChannelType:      fallbackString(trimString(content["channel_type"]), "chat"),
			CommentsEnabled:  boolParam(content["comments_enabled"]),
			MemberCount:      1,
			PendingJoinCount: 0,
		}, true, nil
	}
	return channel{}, false, nil
}

func (t *DendriteTransport) ListRoomMembers(ctx context.Context, roomID string) ([]memberRecord, error) {
	var res roomserverAPI.QueryCurrentStateResponse
	if err := t.rsAPI.QueryCurrentState(ctx, &roomserverAPI.QueryCurrentStateRequest{
		RoomID:         roomID,
		AllowWildcards: true,
		StateTuples: []gomatrixserverlib.StateKeyTuple{
			{EventType: spec.MRoomMember, StateKey: "*"},
		},
	}, &res); err != nil {
		return nil, err
	}
	members := make([]memberRecord, 0, len(res.StateEvents))
	for tuple, event := range res.StateEvents {
		if event == nil {
			continue
		}
		userID := tuple.StateKey
		if userID == "" && event.StateKey() != nil {
			userID = *event.StateKey()
		}
		if userID == "" {
			userID = string(event.SenderID())
		}
		content := map[string]any{}
		if err := json.Unmarshal(event.Content(), &content); err != nil {
			return nil, err
		}
		members = append(members, memberRecord{
			RoomID:      roomID,
			UserID:      userID,
			DisplayName: trimString(content["displayname"]),
			AvatarURL:   trimString(content["avatar_url"]),
			Domain:      domainFromMXID(userID),
			Membership:  fallbackString(trimString(content["membership"]), "join"),
			Role:        fallbackString(trimString(content["role"]), "member"),
			Muted:       boolParam(content["muted"]),
		})
	}
	return members, nil
}

func (t *DendriteTransport) UpdateMemberProfile(ctx context.Context, req UpdateMemberProfileRequest) error {
	if req.Timestamp.IsZero() {
		req.Timestamp = time.Now()
	}
	fullUserID, err := spec.NewUserID(req.UserMXID, true)
	if err != nil {
		return err
	}
	if fullUserID.Domain() != t.serverName {
		return fmt.Errorf("profile user %s is not local to %s", req.UserMXID, t.serverName)
	}
	validRoomID, err := spec.NewRoomID(req.RoomID)
	if err != nil {
		return err
	}
	senderID, err := t.rsAPI.QuerySenderIDForUser(ctx, *validRoomID, *fullUserID)
	if err != nil {
		return err
	}
	if senderID == nil {
		return fmt.Errorf("sender %s is not joined to room %s", req.UserMXID, req.RoomID)
	}
	identity, err := t.rsAPI.SigningIdentityFor(ctx, *validRoomID, *fullUserID)
	if err != nil {
		return err
	}
	stateKey := req.UserMXID
	proto := gomatrixserverlib.ProtoEvent{
		SenderID: string(*senderID),
		RoomID:   req.RoomID,
		Type:     spec.MRoomMember,
		StateKey: &stateKey,
	}
	content := map[string]any{"membership": "join"}
	if strings.TrimSpace(req.DisplayName) != "" {
		content["displayname"] = strings.TrimSpace(req.DisplayName)
	}
	if strings.TrimSpace(req.AvatarURL) != "" {
		content["avatar_url"] = strings.TrimSpace(req.AvatarURL)
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
		fullUserID.Domain(),
		fullUserID.Domain(),
		fullUserID.Domain(),
		nil,
		false,
	)
}

func (t *DendriteTransport) RedactEvent(ctx context.Context, req RedactEventRequest) (RedactEventResult, error) {
	if req.Timestamp.IsZero() {
		req.Timestamp = time.Now()
	}
	fullUserID, err := spec.NewUserID(req.SenderMXID, true)
	if err != nil {
		return RedactEventResult{}, err
	}
	if fullUserID.Domain() != t.serverName {
		return RedactEventResult{}, fmt.Errorf("redaction sender %s is not local to %s", req.SenderMXID, t.serverName)
	}
	validRoomID, err := spec.NewRoomID(req.RoomID)
	if err != nil {
		return RedactEventResult{}, err
	}
	senderID, err := t.rsAPI.QuerySenderIDForUser(ctx, *validRoomID, *fullUserID)
	if err != nil {
		return RedactEventResult{}, err
	}
	if senderID == nil {
		return RedactEventResult{}, fmt.Errorf("sender %s is not joined to room %s", req.SenderMXID, req.RoomID)
	}
	if err = productpolicy.ValidateClientRedaction(ctx, t.productPolicyQuerier(), productpolicy.ClientRedactionRequest{
		RoomID:        req.RoomID,
		SenderMXID:    req.SenderMXID,
		TargetEventID: req.EventID,
	}); err != nil {
		return RedactEventResult{}, err
	}
	identity, err := t.rsAPI.SigningIdentityFor(ctx, *validRoomID, *fullUserID)
	if err != nil {
		return RedactEventResult{}, err
	}
	proto := gomatrixserverlib.ProtoEvent{
		SenderID: string(*senderID),
		RoomID:   req.RoomID,
		Type:     spec.MRoomRedaction,
		Redacts:  req.EventID,
	}
	content := map[string]any{"redacts": req.EventID}
	if strings.TrimSpace(req.Reason) != "" {
		content["reason"] = strings.TrimSpace(req.Reason)
	}
	if err = proto.SetContent(content); err != nil {
		return RedactEventResult{}, err
	}
	event, _, err := t.queryAndBuildEvent(ctx, &proto, &identity, req.Timestamp, req.RoomID)
	if err != nil {
		return RedactEventResult{}, err
	}
	if err = roomserverAPI.SendEvents(
		ctx,
		t.rsAPI,
		roomserverAPI.KindNew,
		[]*types.HeaderedEvent{event},
		fullUserID.Domain(),
		fullUserID.Domain(),
		fullUserID.Domain(),
		nil,
		false,
	); err != nil {
		return RedactEventResult{}, err
	}
	return RedactEventResult{EventID: event.EventID()}, nil
}

func (t *DendriteTransport) queryAndBuildEvent(
	ctx context.Context,
	proto *gomatrixserverlib.ProtoEvent,
	identity *fclient.SigningIdentity,
	eventTime time.Time,
	roomID string,
) (*types.HeaderedEvent, roomserverAPI.QueryLatestEventsAndStateResponse, error) {
	var queryRes roomserverAPI.QueryLatestEventsAndStateResponse
	event, err := eventutil.QueryAndBuildEvent(ctx, proto, identity, eventTime, t.rsAPI, &queryRes)
	if err == nil || !queryRes.RoomExists || queryRes.RoomVersion != "" {
		return event, queryRes, err
	}
	fillMissingRoomVersion(ctx, roomID, &queryRes, t.rsAPI.DefaultRoomVersion(), t.rsAPI.QueryRoomVersionForRoom)
	eventsNeeded, neededErr := gomatrixserverlib.StateNeededForProtoEvent(proto)
	if neededErr != nil {
		return nil, queryRes, neededErr
	}
	event, err = eventutil.BuildEvent(ctx, proto, identity, eventTime, &eventsNeeded, &queryRes)
	return event, queryRes, err
}

func fillMissingRoomVersion(
	ctx context.Context,
	roomID string,
	queryRes *roomserverAPI.QueryLatestEventsAndStateResponse,
	defaultVersion gomatrixserverlib.RoomVersion,
	lookup func(context.Context, string) (gomatrixserverlib.RoomVersion, error),
) {
	if queryRes == nil || queryRes.RoomVersion != "" {
		return
	}
	if lookup != nil {
		if roomVersion, err := lookup(ctx, roomID); err == nil && roomVersion != "" {
			queryRes.RoomVersion = roomVersion
			return
		}
	}
	queryRes.RoomVersion = defaultVersion
}

func matrixVisibility(value string) string {
	if strings.TrimSpace(value) == "public" {
		return "public"
	}
	return "private"
}

func matrixPreset(visibility string, direct bool) string {
	if direct {
		return spec.PresetTrustedPrivateChat
	}
	if matrixVisibility(visibility) == "public" {
		return spec.PresetPublicChat
	}
	return spec.PresetPrivateChat
}

func localpart(mxid string) string {
	if strings.HasPrefix(mxid, "@") && strings.Contains(mxid, ":") {
		return strings.TrimPrefix(mxid[:strings.LastIndex(mxid, ":")], "@")
	}
	return mxid
}

func jsonString(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return http.StatusText(http.StatusInternalServerError)
	}
	return string(raw)
}
