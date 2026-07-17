package dendrite

import (
	"context"
	"crypto/ed25519"
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

func (t *DendriteTransport) ReadRoomCreator(ctx context.Context, roomID string) (string, error) {
	validRoomID, err := spec.NewRoomID(strings.TrimSpace(roomID))
	if err != nil {
		return "", err
	}
	tuple := gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomCreate, StateKey: ""}
	var res roomserverAPI.QueryCurrentStateResponse
	if err := t.rsAPI.QueryCurrentState(ctx, &roomserverAPI.QueryCurrentStateRequest{
		RoomID: validRoomID.String(), StateTuples: []gomatrixserverlib.StateKeyTuple{tuple},
	}, &res); err != nil {
		return "", err
	}
	event := res.StateEvents[tuple]
	if event == nil || event.Type() != spec.MRoomCreate || event.StateKey() == nil || *event.StateKey() != "" {
		return "", nil
	}

	senderID := event.SenderID()
	if strings.TrimSpace(string(senderID)) == "" {
		return "", nil
	}
	creator, err := t.rsAPI.QueryUserIDForSender(ctx, *validRoomID, senderID)
	if err != nil {
		return "", err
	}
	if creator != nil {
		return creator.String(), nil
	}
	return validRoomCreatorMXID(string(senderID)), nil
}

func validRoomCreatorMXID(value string) string {
	userID, err := spec.NewUserID(strings.TrimSpace(value), true)
	if err != nil || userID == nil {
		return ""
	}
	return userID.String()
}

func (t *DendriteTransport) GetRoomChannel(ctx context.Context, roomID string) (channel, bool, error) {
	var res roomserverAPI.QueryCurrentStateResponse
	if err := t.rsAPI.QueryCurrentState(ctx, &roomserverAPI.QueryCurrentStateRequest{
		RoomID: roomID,
		StateTuples: []gomatrixserverlib.StateKeyTuple{
			{EventType: DirextalkRoomProfileEventType, StateKey: ""},
		},
	}, &res); err != nil {
		return channel{}, false, err
	}
	event := res.StateEvents[gomatrixserverlib.StateKeyTuple{EventType: DirextalkRoomProfileEventType, StateKey: ""}]
	if event != nil {
		content := map[string]any{}
		if err := json.Unmarshal(event.Content(), &content); err != nil {
			return channel{}, false, err
		}
		if trimString(content["room_type"]) != DirextalkRoomTypeChannel {
			return channel{}, false, nil
		}
		channelID := trimString(content["channel_id"])
		if channelID == "" || channelID == roomID || strings.HasPrefix(channelID, "!") {
			return channel{}, false, nil
		}
		return channel{
			ChannelID:        channelID,
			RoomID:           roomID,
			Name:             fallbackString(trimString(content["name"]), channelID),
			Description:      trimString(content["description"]),
			AvatarURL:        trimString(content["avatar_url"]),
			Visibility:       fallbackString(trimString(content["visibility"]), "private"),
			JoinPolicy:       fallbackString(trimString(content["join_policy"]), "invite"),
			ChannelType:      fallbackString(trimString(content["channel_type"]), "post"),
			CommentsEnabled:  boolParam(content["comments_enabled"]),
			MemberCount:      1,
			PendingJoinCount: 0,
		}, true, nil
	}
	return channel{}, false, nil
}

func (t *DendriteTransport) ListRoomMembers(ctx context.Context, roomID string) ([]memberRecord, error) {
	validRoomID, err := spec.NewRoomID(strings.TrimSpace(roomID))
	if err != nil {
		return nil, err
	}
	var res roomserverAPI.QueryCurrentStateResponse
	if err := t.rsAPI.QueryCurrentState(ctx, &roomserverAPI.QueryCurrentStateRequest{
		RoomID:         validRoomID.String(),
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
		if fullMXID := validRoomCreatorMXID(userID); fullMXID != "" {
			userID = fullMXID
		} else {
			senderID := spec.SenderID(strings.TrimSpace(userID))
			raw, rawErr := senderID.RawBytes()
			if rawErr != nil || len(raw) != ed25519.PublicKeySize {
				continue
			}
			resolved, resolveErr := t.rsAPI.QueryUserIDForSender(ctx, *validRoomID, senderID)
			if resolveErr != nil {
				return nil, resolveErr
			}
			if resolved == nil {
				continue
			}
			userID = resolved.String()
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
