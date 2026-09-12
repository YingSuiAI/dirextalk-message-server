package productpolicy

import (
	"context"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func TestGroupYingCannotBeInvitedThroughOrdinaryMatrixMembership(t *testing.T) {
	room, owner := "!group:example.test", "@owner:example.test"
	q := stateQuerier{state: map[gomatrixserverlib.StateKeyTuple]*types.HeaderedEvent{
		{EventType: spec.MRoomCreate, StateKey: ""}:    stateEvent(t, room, owner, spec.MRoomCreate, "", map[string]any{"type": DirextalkRoomTypeGroup}),
		{EventType: spec.MRoomMember, StateKey: owner}: stateEvent(t, room, owner, spec.MRoomMember, owner, map[string]any{"membership": "join"}),
	}}
	for _, target := range []string{"@ying:example.test", "@ying:foreign.test"} {
		if err := ValidateClientMembership(context.Background(), q, ClientMembershipRequest{RoomID: room, SenderMXID: owner, TargetMXID: target, Membership: "invite"}); err == nil {
			t.Fatal("ordinary invitation bypassed Native owner workflow")
		}
	}
	if err := ValidateClientMembership(context.Background(), q, ClientMembershipRequest{RoomID: room, SenderMXID: owner, TargetMXID: "@ying:example.test", Membership: "invite", GroupAgentControl: true}); err != nil {
		t.Fatalf("trusted invite rejected: %v", err)
	}
}

func TestGroupYingMessageRequiresRealSenderAndCurrentBinding(t *testing.T) {
	room, owner, agent := "!group:example.test", "@owner:example.test", "@ying:example.test"
	q := stateQuerier{state: map[gomatrixserverlib.StateKeyTuple]*types.HeaderedEvent{}}
	q.state[gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomCreate, StateKey: ""}] = stateEvent(t, room, owner, spec.MRoomCreate, "", map[string]any{"type": DirextalkRoomTypeGroup})
	for _, user := range []string{owner, agent, "@member:example.test"} {
		q.state[gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomMember, StateKey: user}] = stateEvent(t, room, user, spec.MRoomMember, user, map[string]any{"membership": "join"})
	}
	tuple := gomatrixserverlib.StateKeyTuple{EventType: "io.dirextalk.group_agent", StateKey: ""}
	binding := map[string]any{"room_id": room, "enabled": true, "owner_mxid": owner, "agent_mxid": agent, "revision": 1}
	q.state[tuple] = stateEvent(t, room, owner, tuple.EventType, "", binding)
	content := map[string]any{"msgtype": "m.text", "body": "reply", "msg_type": "group_agent_reply", "io.dirextalk.group_agent": map[string]any{"owner_mxid": owner, "agent_mxid": agent, "binding_revision": 1, "request_id": "request"}}
	req := ClientEventRequest{RoomID: room, SenderMXID: agent, EventType: "m.room.message", Content: content}
	if err := ValidateClientEvent(context.Background(), q, req); err != nil {
		t.Fatalf("valid Ying reply=%v", err)
	}
	req.SenderMXID = "@member:example.test"
	if err := ValidateClientEvent(context.Background(), q, req); err == nil {
		t.Fatal("member impersonated Ying metadata")
	}
	req.SenderMXID = agent
	binding["enabled"] = false
	q.state[tuple] = stateEvent(t, room, owner, tuple.EventType, "", binding)
	if err := ValidateClientEvent(context.Background(), q, req); err == nil {
		t.Fatal("disabled binding published late reply")
	}
}
