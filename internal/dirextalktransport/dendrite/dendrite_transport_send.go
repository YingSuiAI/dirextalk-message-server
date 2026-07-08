package dendrite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

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
	if !isDirextalkPolicyStateEvent(req.Event.Type) {
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

func isDirextalkPolicyStateEvent(eventType string) bool {
	switch eventType {
	case DirextalkJoinRequestEventType, DirextalkMemberPolicyEventType:
		return true
	default:
		return false
	}
}
