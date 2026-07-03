package p2p

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"strings"
	"testing"

	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
	dendritetest "github.com/YingSuiAI/dirextalk-message-server/test"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
)

func TestDendriteTransportSendMessageAppliesProductPolicy(t *testing.T) {
	owner := dendritetest.NewUser(t)
	member := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	room.CreateAndInsert(t, member, spec.MRoomMember, map[string]any{"membership": spec.Join}, dendritetest.WithStateKey(member.ID))
	room.CreateAndInsert(t, owner, DirextalkRoomProfileEventType, map[string]any{
		"room_type":        DirextalkRoomTypeChannel,
		"comments_enabled": false,
	}, dendritetest.WithStateKey(""))

	rsAPI := &policyTransportRoomserver{
		roomID: room.ID,
		state:  room.CurrentState(),
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

	_, err := transport.SendMessage(context.Background(), SendMessageRequest{
		SenderMXID: member.ID,
		RoomID:     room.ID,
		EventType:  "m.room.message",
		Content:    map[string]any{"msgtype": "m.text", "body": "blocked", "p2p_kind": "channel_comment"},
	})

	if err == nil || !strings.Contains(err.Error(), "channel comments are disabled") {
		t.Fatalf("expected product policy error, got %v", err)
	}
	if rsAPI.signingIdentityCalled {
		t.Fatalf("expected product policy to reject before building and signing the message event")
	}
}

func TestDendriteTransportRedactEventAppliesProductPolicy(t *testing.T) {
	owner := dendritetest.NewUser(t)
	member := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	room.CreateAndInsert(t, member, spec.MRoomMember, map[string]any{"membership": spec.Join}, dendritetest.WithStateKey(member.ID))
	room.CreateAndInsert(t, owner, DirextalkRoomProfileEventType, map[string]any{
		"room_type":        DirextalkRoomTypeChannel,
		"comments_enabled": true,
	}, dendritetest.WithStateKey(""))
	target := room.CreateAndInsert(t, owner, "m.room.message", map[string]any{"msgtype": "m.text", "body": "owned"})

	rsAPI := &policyTransportRoomserver{
		roomID: room.ID,
		state:  room.CurrentState(),
		events: map[string]*types.HeaderedEvent{
			target.EventID(): target,
		},
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

	_, err := transport.RedactEvent(context.Background(), RedactEventRequest{
		SenderMXID: member.ID,
		RoomID:     room.ID,
		EventID:    target.EventID(),
	})

	if err == nil || !strings.Contains(err.Error(), "redact another sender") {
		t.Fatalf("expected product policy redaction error, got %v", err)
	}
	if rsAPI.signingIdentityCalled {
		t.Fatalf("expected product policy to reject before building and signing the redaction event")
	}
}

func TestDendriteTransportInviteUserAppliesProductPolicy(t *testing.T) {
	owner := dendritetest.NewUser(t)
	member := dendritetest.NewUser(t)
	invitee := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	room.CreateAndInsert(t, member, spec.MRoomMember, map[string]any{"membership": spec.Join}, dendritetest.WithStateKey(member.ID))
	room.CreateAndInsert(t, owner, DirextalkRoomProfileEventType, map[string]any{
		"room_type":     DirextalkRoomTypeGroup,
		"invite_policy": "owner",
	}, dendritetest.WithStateKey(""))
	rsAPI := &policyTransportRoomserver{
		roomID: room.ID,
		state:  room.CurrentState(),
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

	err := transport.InviteUser(context.Background(), InviteUserRequest{
		RoomID:      room.ID,
		InviterMXID: member.ID,
		InviteeMXID: invitee.ID,
	})

	if err == nil || !strings.Contains(err.Error(), "invite members") {
		t.Fatalf("expected product policy invite error, got %v", err)
	}
	if rsAPI.inviteCalled {
		t.Fatalf("expected product policy to reject before PerformInvite")
	}
}

func TestDendriteTransportJoinRoomAppliesProductPolicy(t *testing.T) {
	owner := dendritetest.NewUser(t)
	requester := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	room.CreateAndInsert(t, owner, DirextalkRoomProfileEventType, map[string]any{
		"room_type":   DirextalkRoomTypeChannel,
		"join_policy": "approval",
	}, dendritetest.WithStateKey(""))
	rsAPI := &policyTransportRoomserver{
		roomID: room.ID,
		state:  room.CurrentState(),
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

	_, err := transport.JoinRoom(context.Background(), JoinRoomRequest{
		RoomIDOrAlias: room.ID,
		UserMXID:      requester.ID,
	})

	if err == nil || !strings.Contains(err.Error(), "approved join request") {
		t.Fatalf("expected product policy join error, got %v", err)
	}
	if rsAPI.joinCalled {
		t.Fatalf("expected product policy to reject before PerformJoin")
	}
}

func TestDendriteTransportJoinRoomLetsRoomserverAcceptPendingDirectInvite(t *testing.T) {
	owner := dendritetest.NewUser(t)
	requester := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	room.CreateAndInsert(t, owner, DirextalkRoomProfileEventType, map[string]any{
		"room_type":      DirextalkRoomTypeDirect,
		"requester_mxid": requester.ID,
		"target_mxid":    owner.ID,
	}, dendritetest.WithStateKey(""))
	rsAPI := &policyTransportRoomserver{
		roomID:    room.ID,
		state:     room.CurrentState(),
		allowJoin: true,
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

	joined, err := transport.JoinRoom(context.Background(), JoinRoomRequest{
		RoomIDOrAlias: room.ID,
		UserMXID:      requester.ID,
	})

	if err != nil {
		t.Fatalf("expected roomserver pending direct invite to decide join, got %v", err)
	}
	if joined.RoomID != room.ID || !rsAPI.joinCalled {
		t.Fatalf("expected PerformJoin to accept the pending direct invite, got joined=%#v joinCalled=%v", joined, rsAPI.joinCalled)
	}
}

func TestDendriteTransportJoinRoomCarriesProfileContent(t *testing.T) {
	owner := dendritetest.NewUser(t)
	requester := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	rsAPI := &policyTransportRoomserver{
		roomID:    room.ID,
		state:     room.CurrentState(),
		allowJoin: true,
	}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

	_, err := transport.JoinRoom(context.Background(), JoinRoomRequest{
		RoomIDOrAlias: room.ID,
		UserMXID:      requester.ID,
		DisplayName:   "Alice Device",
		AvatarURL:     "mxc://example.com/alice",
	})

	if err != nil {
		t.Fatalf("expected join to succeed, got %v", err)
	}
	if rsAPI.joinContent["displayname"] != "Alice Device" || rsAPI.joinContent["avatar_url"] != "mxc://example.com/alice" {
		t.Fatalf("expected profile content on PerformJoin, got %#v", rsAPI.joinContent)
	}
}

func TestDendriteTransportCreateRoomCarriesCreatorProfile(t *testing.T) {
	rsAPI := &policyTransportRoomserver{}
	transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

	for _, roomType := range []string{DirextalkRoomTypeDirect, DirextalkRoomTypeGroup, DirextalkRoomTypeChannel} {
		t.Run(roomType, func(t *testing.T) {
			rsAPI.createRoomRequest = nil
			_, err := transport.CreateRoom(context.Background(), CreateRoomRequest{
				CreatorMXID:        "@owner:test",
				CreatorDisplayName: "Owner",
				CreatorAvatarURL:   "mxc://test/owner-avatar",
				Name:               "Product Room",
				Visibility:         "private",
				RoomType:           roomType,
				IsDirect:           roomType == DirextalkRoomTypeDirect,
				InitialState: []RoomStateEvent{{
					Type:     DirextalkRoomProfileEventType,
					StateKey: "",
					Content: map[string]any{
						"room_type": roomType,
					},
				}},
			})
			if err != nil {
				t.Fatalf("expected create room to succeed, got %v", err)
			}
			if rsAPI.createRoomRequest == nil {
				t.Fatalf("expected PerformCreateRoom request")
			}
			if rsAPI.createRoomRequest.UserDisplayName != "Owner" || rsAPI.createRoomRequest.UserAvatarURL != "mxc://test/owner-avatar" {
				t.Fatalf("expected creator profile on PerformCreateRoom, got %#v", rsAPI.createRoomRequest)
			}
		})
	}
}

func TestDendriteTransportGetRoomChannelRequiresChannelRoomType(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile map[string]any
		wantOK  bool
	}{
		{
			name:    "empty profile is not a channel",
			profile: map[string]any{},
		},
		{
			name: "group profile is not a channel",
			profile: map[string]any{
				"room_type": DirextalkRoomTypeGroup,
				"name":      "Group with A, B",
			},
		},
		{
			name: "channel profile without product id is not a channel",
			profile: map[string]any{
				"room_type": DirextalkRoomTypeChannel,
				"name":      "Posts",
			},
		},
		{
			name: "channel profile with Matrix room id as product id is not a channel",
			profile: map[string]any{
				"room_type":  DirextalkRoomTypeChannel,
				"channel_id": "!posts:test",
				"name":       "Posts",
			},
		},
		{
			name: "channel profile is a channel",
			profile: map[string]any{
				"room_type":  DirextalkRoomTypeChannel,
				"channel_id": "ch",
				"name":       "Posts",
			},
			wantOK: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := dendritetest.NewUser(t)
			room := dendritetest.NewRoom(t, owner)
			room.CreateAndInsert(t, owner, DirextalkRoomProfileEventType, tc.profile, dendritetest.WithStateKey(""))
			rsAPI := &policyTransportRoomserver{
				roomID: room.ID,
				state:  room.CurrentState(),
			}
			transport := NewDendriteTransport(spec.ServerName("test"), gomatrixserverlib.KeyID("ed25519:test"), ed25519.NewKeyFromSeed(make([]byte, 32)), rsAPI)

			ch, ok, err := transport.GetRoomChannel(context.Background(), room.ID)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if ok != tc.wantOK {
				t.Fatalf("expected ok=%v, got ok=%v channel=%#v", tc.wantOK, ok, ch)
			}
			if tc.wantOK && ch.ChannelID != "ch" {
				t.Fatalf("expected channel id ch, got %#v", ch)
			}
		})
	}
}

type policyTransportRoomserver struct {
	roomserverAPI.ClientRoomserverAPI
	roomID                string
	state                 []*types.HeaderedEvent
	events                map[string]*types.HeaderedEvent
	signingIdentityCalled bool
	inviteCalled          bool
	joinCalled            bool
	allowJoin             bool
	joinContent           map[string]interface{}
	createRoomRequest     *roomserverAPI.PerformCreateRoomRequest
}

func (r *policyTransportRoomserver) QueryCurrentState(ctx context.Context, req *roomserverAPI.QueryCurrentStateRequest, res *roomserverAPI.QueryCurrentStateResponse) error {
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

func (r *policyTransportRoomserver) QueryMembershipForUser(ctx context.Context, req *roomserverAPI.QueryMembershipForUserRequest, res *roomserverAPI.QueryMembershipForUserResponse) error {
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

func (r *policyTransportRoomserver) QuerySenderIDForUser(ctx context.Context, roomID spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	if roomID.String() != r.roomID {
		return nil, fmt.Errorf("unknown room %s", roomID.String())
	}
	senderID := spec.SenderIDFromUserID(userID)
	return &senderID, nil
}

func (r *policyTransportRoomserver) QueryEventsByID(ctx context.Context, req *roomserverAPI.QueryEventsByIDRequest, res *roomserverAPI.QueryEventsByIDResponse) error {
	for _, eventID := range req.EventIDs {
		if event := r.events[eventID]; event != nil {
			res.Events = append(res.Events, event)
		}
	}
	return nil
}

func (r *policyTransportRoomserver) SigningIdentityFor(ctx context.Context, roomID spec.RoomID, sender spec.UserID) (fclient.SigningIdentity, error) {
	r.signingIdentityCalled = true
	return fclient.SigningIdentity{}, fmt.Errorf("policy was not applied before signing")
}

func (r *policyTransportRoomserver) PerformInvite(ctx context.Context, req *roomserverAPI.PerformInviteRequest) error {
	r.inviteCalled = true
	return fmt.Errorf("policy was not applied before invite")
}

func (r *policyTransportRoomserver) PerformJoin(ctx context.Context, req *roomserverAPI.PerformJoinRequest) (string, spec.ServerName, error) {
	r.joinCalled = true
	r.joinContent = req.Content
	if r.allowJoin {
		return req.RoomIDOrAlias, spec.ServerName("test"), nil
	}
	return "", "", fmt.Errorf("policy was not applied before join")
}

func (r *policyTransportRoomserver) DefaultRoomVersion() gomatrixserverlib.RoomVersion {
	return gomatrixserverlib.RoomVersionV10
}

func (r *policyTransportRoomserver) PerformCreateRoom(ctx context.Context, userID spec.UserID, roomID spec.RoomID, createRequest *roomserverAPI.PerformCreateRoomRequest) (string, *util.JSONResponse) {
	r.createRoomRequest = createRequest
	return roomID.String(), nil
}
