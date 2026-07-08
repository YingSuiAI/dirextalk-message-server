package dendrite

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/matrix-org/gomatrixserverlib"
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
