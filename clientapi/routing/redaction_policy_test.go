package routing

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	roomserverAPI "github.com/YingSuiAI/direxio-message-server/roomserver/api"
	"github.com/YingSuiAI/direxio-message-server/roomserver/types"
	"github.com/YingSuiAI/direxio-message-server/setup/config"
	dendritetest "github.com/YingSuiAI/direxio-message-server/test"
	userapi "github.com/YingSuiAI/direxio-message-server/userapi/api"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func TestSendRedactionAppliesDirexioProductPolicy(t *testing.T) {
	owner := dendritetest.NewUser(t)
	member := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	room.CreateAndInsert(t, member, spec.MRoomMember, map[string]any{"membership": spec.Join}, dendritetest.WithStateKey(member.ID))
	room.CreateAndInsert(t, owner, spec.MRoomPowerLevels, map[string]any{
		"users":          map[string]int{owner.ID: 100},
		"users_default":  0,
		"events":         map[string]int{},
		"events_default": 0,
		"state_default":  50,
		"ban":            50,
		"kick":           50,
		"redact":         0,
		"invite":         0,
	}, dendritetest.WithStateKey(""))
	room.CreateAndInsert(t, owner, "io.direxio.room.profile", map[string]any{
		"room_type":        "io.direxio.room.channel",
		"comments_enabled": true,
	}, dendritetest.WithStateKey(""))
	target := room.CreateAndInsert(t, owner, "m.room.message", map[string]any{"msgtype": "m.text", "body": "owned"})
	rsAPI := &redactionPolicyRoomserver{
		roomID: room.ID,
		state:  room.CurrentState(),
		events: map[string]*types.HeaderedEvent{
			target.EventID(): target,
		},
	}
	req, err := http.NewRequest("POST", "https://domain", io.NopCloser(strings.NewReader(`{}`)))
	if err != nil {
		t.Fatal(err)
	}

	resp := SendRedaction(req, &userapi.Device{UserID: member.ID}, room.ID, target.EventID(), &config.ClientAPI{}, rsAPI, nil, nil)

	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected Direxio product policy 403, got %d with %#v", resp.Code, resp.JSON)
	}
	if rsAPI.signingIdentityCalled {
		t.Fatalf("expected product policy to reject before signing redaction event")
	}
}

type redactionPolicyRoomserver struct {
	roomserverAPI.ClientRoomserverAPI
	roomID                string
	state                 []*types.HeaderedEvent
	events                map[string]*types.HeaderedEvent
	signingIdentityCalled bool
}

func (r *redactionPolicyRoomserver) QueryCurrentState(ctx context.Context, req *roomserverAPI.QueryCurrentStateRequest, res *roomserverAPI.QueryCurrentStateResponse) error {
	res.StateEvents = map[gomatrixserverlib.StateKeyTuple]*types.HeaderedEvent{}
	for _, tuple := range req.StateTuples {
		for _, event := range r.state {
			if event.Type() == tuple.EventType && event.StateKey() != nil && *event.StateKey() == tuple.StateKey {
				res.StateEvents[tuple] = event
			}
		}
	}
	return nil
}

func (r *redactionPolicyRoomserver) QueryMembershipForUser(ctx context.Context, req *roomserverAPI.QueryMembershipForUserRequest, res *roomserverAPI.QueryMembershipForUserResponse) error {
	res.RoomExists = true
	for _, event := range r.state {
		if event.Type() == spec.MRoomMember && event.StateKey() != nil && *event.StateKey() == req.UserID.String() {
			res.HasBeenInRoom = true
			res.IsInRoom = true
			res.Membership = string(spec.Join)
			return nil
		}
	}
	return nil
}

func (r *redactionPolicyRoomserver) QueryEventsByID(ctx context.Context, req *roomserverAPI.QueryEventsByIDRequest, res *roomserverAPI.QueryEventsByIDResponse) error {
	for _, eventID := range req.EventIDs {
		if event := r.events[eventID]; event != nil {
			res.Events = append(res.Events, event)
		}
	}
	return nil
}

func (r *redactionPolicyRoomserver) QuerySenderIDForUser(ctx context.Context, roomID spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	if roomID.String() != r.roomID {
		return nil, fmt.Errorf("unknown room %s", roomID.String())
	}
	senderID := spec.SenderIDFromUserID(userID)
	return &senderID, nil
}

func (r *redactionPolicyRoomserver) SigningIdentityFor(ctx context.Context, roomID spec.RoomID, sender spec.UserID) (fclient.SigningIdentity, error) {
	r.signingIdentityCalled = true
	return fclient.SigningIdentity{PrivateKey: ed25519.NewKeyFromSeed(make([]byte, 32))}, fmt.Errorf("policy was not applied before signing")
}
