package dendrite

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	roomserverTypes "github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
)

type DendriteTransport struct {
	serverName                  spec.ServerName
	keyID                       gomatrixserverlib.KeyID
	privateKey                  ed25519.PrivateKey
	rsAPI                       roomserverAPI.ClientRoomserverAPI
	blockedDirectMessageChecker func(context.Context, string, string) (bool, error)
}

const idempotentCreateOperationContentKey = "io.dirextalk.create_operation"

var errIdempotentRoomCreatorNotJoined = errors.New("creator is not joined")

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

// SetBlockedDirectMessageChecker configures the ProductCore block lookup used
// when sending messages to Dirextalk direct rooms.
func (t *DendriteTransport) SetBlockedDirectMessageChecker(checker func(context.Context, string, string) (bool, error)) {
	t.blockedDirectMessageChecker = checker
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
	hasIdempotencyKey := strings.TrimSpace(req.IdempotencyKey) != ""
	roomLocalpart := util.RandomString(16)
	if hasIdempotencyKey {
		digest := idempotentCreateOperationDigest(req.IdempotencyKey)
		roomLocalpart = "p2p_" + hex.EncodeToString(digest[:16])
	}
	roomID, err := spec.NewRoomID(fmt.Sprintf("!%s:%s", roomLocalpart, userID.Domain()))
	if err != nil {
		return CreateRoomResult{}, err
	}
	rebuiltCreateOnlyRoom := false
	if hasIdempotencyKey {
		known, knownErr := t.isKnownRoom(ctx, *roomID)
		if knownErr != nil {
			return CreateRoomResult{}, knownErr
		}
		if known {
			result := CreateRoomResult{RoomID: roomID.String()}
			reconcileErr := t.reconcileIdempotentRoom(ctx, req, *roomID)
			if reconcileErr == nil {
				return result, nil
			}
			if !errors.Is(reconcileErr, errIdempotentRoomCreatorNotJoined) {
				return result, reconcileErr
			}
			recoverer, ok := t.rsAPI.(roomserverAPI.CreateOnlyRoomRecoveryAPI)
			if !ok {
				return result, fmt.Errorf("roomserver does not support safe create-only recovery: %w", reconcileErr)
			}
			purged, purgeErr := recoverer.PerformAdminPurgeRoomIfCreateOnly(ctx, &roomserverAPI.PerformAdminPurgeCreateOnlyRoomRequest{
				RoomID:                  roomID.String(),
				CreatorMXID:             req.CreatorMXID,
				CreateEventContentKey:   idempotentCreateOperationContentKey,
				CreateEventContentValue: idempotentCreateOperationFingerprint(req.IdempotencyKey),
			})
			if purgeErr != nil {
				return result, fmt.Errorf("conditionally purge incomplete idempotent room %s: %w", roomID.String(), purgeErr)
			}
			if !purged {
				// The serialized roomserver check may have observed input that
				// landed after our first membership read. Reconcile once more
				// before reporting the room as incomplete.
				if currentErr := t.reconcileIdempotentRoom(ctx, req, *roomID); currentErr == nil {
					return result, nil
				}
				return result, reconcileErr
			}
			rebuiltCreateOnlyRoom = true
		}
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
	if hasIdempotencyKey {
		creationContent[idempotentCreateOperationContentKey] = idempotentCreateOperationFingerprint(req.IdempotencyKey)
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
		if hasIdempotencyKey {
			known, knownErr := t.isKnownRoom(ctx, *roomID)
			if knownErr == nil && known {
				result := CreateRoomResult{RoomID: roomID.String()}
				if err := t.reconcileIdempotentRoom(ctx, req, *roomID); err != nil {
					return result, err
				}
				return result, nil
			}
		}
		return CreateRoomResult{}, fmt.Errorf("create room failed: status=%d body=%s", createRes.Code, jsonString(createRes.JSON))
	}
	result := CreateRoomResult{RoomID: roomID.String()}
	if rebuiltCreateOnlyRoom {
		if err := t.reconcileIdempotentRoom(ctx, req, *roomID); err != nil {
			return result, fmt.Errorf("confirm rebuilt idempotent room %s: %w", roomID.String(), err)
		}
	}
	return result, nil
}

func (t *DendriteTransport) reconcileIdempotentRoom(ctx context.Context, req CreateRoomRequest, roomID spec.RoomID) error {
	creatorJoined, err := t.waitForRoomMembership(ctx, roomID.String(), req.CreatorMXID, func(membership string, inRoom bool) bool {
		return inRoom && strings.EqualFold(membership, string(spec.Join))
	})
	if err != nil {
		return err
	}
	if !creatorJoined {
		return fmt.Errorf("idempotent room %s is incomplete: %w", roomID.String(), errIdempotentRoomCreatorNotJoined)
	}

	if len(req.InitialState) > 0 {
		tuples := make([]gomatrixserverlib.StateKeyTuple, 0, len(req.InitialState))
		for _, state := range req.InitialState {
			tuples = append(tuples, gomatrixserverlib.StateKeyTuple{EventType: state.Type, StateKey: state.StateKey})
		}
		current, queryErr := t.queryStateTuples(ctx, roomID.String(), tuples)
		if queryErr != nil {
			return queryErr
		}
		for index, tuple := range tuples {
			if current[tuple] != nil {
				continue
			}
			if sendErr := t.SendStateEvent(ctx, SendStateEventRequest{
				RoomID: roomID.String(), SenderMXID: req.CreatorMXID, Event: req.InitialState[index],
			}); sendErr != nil {
				return fmt.Errorf("repair idempotent room %s state %s/%s: %w", roomID.String(), tuple.EventType, tuple.StateKey, sendErr)
			}
		}
		current, queryErr = t.queryStateTuples(ctx, roomID.String(), tuples)
		if queryErr != nil {
			return queryErr
		}
		for _, tuple := range tuples {
			if current[tuple] == nil {
				return fmt.Errorf("idempotent room %s is incomplete: state %s/%s is missing", roomID.String(), tuple.EventType, tuple.StateKey)
			}
		}
	}

	for _, inviteMXID := range req.InviteMXIDs {
		inviteReady, queryErr := t.waitForRoomMembership(ctx, roomID.String(), inviteMXID, inviteOrJoined)
		if queryErr != nil {
			return queryErr
		}
		if inviteReady {
			continue
		}
		if inviteErr := t.repairIdempotentRoomInvite(ctx, roomID, req, inviteMXID); inviteErr != nil {
			return fmt.Errorf("repair idempotent room %s invite for %s: %w", roomID.String(), inviteMXID, inviteErr)
		}
		inviteReady, queryErr = t.waitForRoomMembership(ctx, roomID.String(), inviteMXID, inviteOrJoined)
		if queryErr != nil {
			return queryErr
		}
		if !inviteReady {
			return fmt.Errorf("idempotent room %s is incomplete: invite for %s is unconfirmed", roomID.String(), inviteMXID)
		}
	}
	return nil
}

func idempotentCreateOperationDigest(idempotencyKey string) [sha256.Size]byte {
	return sha256.Sum256([]byte(idempotencyKey))
}

func idempotentCreateOperationFingerprint(idempotencyKey string) string {
	digest := idempotentCreateOperationDigest(idempotencyKey)
	return hex.EncodeToString(digest[:])
}

func (t *DendriteTransport) repairIdempotentRoomInvite(
	ctx context.Context,
	roomID spec.RoomID,
	req CreateRoomRequest,
	inviteMXID string,
) error {
	inviter, err := spec.NewUserID(req.CreatorMXID, true)
	if err != nil {
		return err
	}
	invitee, err := spec.NewUserID(inviteMXID, true)
	if err != nil {
		return err
	}
	inviteRoomState, err := inviteStrippedState(req.InitialState, req.CreatorMXID)
	if err != nil {
		return err
	}
	return t.rsAPI.PerformInvite(ctx, &roomserverAPI.PerformInviteRequest{
		InviteInput: roomserverAPI.InviteInput{
			RoomID: roomID, Inviter: *inviter, Invitee: *invitee, IsDirect: req.IsDirect,
			KeyID: t.keyID, PrivateKey: t.privateKey, EventTime: time.Now(),
		},
		InviteRoomState: inviteRoomState,
		SendAsServer:    string(t.serverName),
	})
}

func inviteOrJoined(membership string, _ bool) bool {
	return strings.EqualFold(membership, string(spec.Invite)) || strings.EqualFold(membership, string(spec.Join))
}

func (t *DendriteTransport) waitForRoomMembership(
	ctx context.Context,
	roomID, userMXID string,
	accepted func(membership string, inRoom bool) bool,
) (bool, error) {
	userID, err := spec.NewUserID(userMXID, true)
	if err != nil {
		return false, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		var response roomserverAPI.QueryMembershipForUserResponse
		if err := t.rsAPI.QueryMembershipForUser(ctx, &roomserverAPI.QueryMembershipForUserRequest{
			RoomID: roomID, UserID: *userID,
		}, &response); err != nil {
			return false, err
		}
		if response.RoomExists && accepted(response.Membership, response.IsInRoom) {
			return true, nil
		}
		if attempt == 4 {
			break
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return false, nil
}

func (t *DendriteTransport) queryStateTuples(
	ctx context.Context,
	roomID string,
	tuples []gomatrixserverlib.StateKeyTuple,
) (map[gomatrixserverlib.StateKeyTuple]*roomserverTypes.HeaderedEvent, error) {
	var response roomserverAPI.QueryCurrentStateResponse
	if err := t.rsAPI.QueryCurrentState(ctx, &roomserverAPI.QueryCurrentStateRequest{
		RoomID: roomID, StateTuples: tuples,
	}, &response); err != nil {
		return nil, err
	}
	return response.StateEvents, nil
}

func (t *DendriteTransport) isKnownRoom(ctx context.Context, roomID spec.RoomID) (bool, error) {
	querier, ok := t.rsAPI.(interface {
		IsKnownRoom(context.Context, spec.RoomID) (bool, error)
	})
	if !ok {
		return false, errors.New("roomserver does not support idempotent room lookup")
	}
	return querier.IsKnownRoom(ctx, roomID)
}
