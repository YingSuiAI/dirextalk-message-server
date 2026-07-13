package contacts

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

func TestAcceptPendingInboundJoinsAndBuildsAcceptedSnapshot(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:remote.example", RoomID: "!old:remote.example", Status: "pending_inbound",
		Remark: "request", DisplayNameOverride: true,
	}
	harness := newRequestHarness(existing)
	harness.joinOutcomes = []DirectRoomJoinOutcome{{Kind: DirectRoomJoinSucceeded, RoomID: " !joined:remote.example "}}
	harness.conversation.operation = map[string]any{"action": "contacts.request", "status": "accepted"}
	harness.conversation.operationView = &dirextalkdomain.ConversationView{MatrixRoomID: " !joined:remote.example ", Kind: dirextalkdomain.ConversationKindDirect}

	result, apiErr := harness.module.AcceptPendingInbound(context.Background(), existing, map[string]any{
		"display_name": " Alice ", "avatar_url": " mxc://remote.example/alice ", "domain": " remote.example ",
		"server_names": []string{" remote.example ", "", "remote.example"},
	})
	view, ok := result.(View)
	if apiErr != nil || !ok {
		t.Fatalf("accept pending = (%#v, %#v)", result, apiErr)
	}
	want := existing
	want.RoomID = " !joined:remote.example "
	want.DisplayName = "Alice"
	want.AvatarURL = "mxc://remote.example/alice"
	want.Domain = "remote.example"
	want.Status = "accepted"
	want.Remark = ""
	if got := RecordFromView(view); !reflect.DeepEqual(got, want) {
		t.Fatalf("accepted contact = %#v, want %#v", got, want)
	}
	wantJoin := DirectRoomJoinRequest{
		RoomID: existing.RoomID, Profile: harness.profile, ServerNames: []string{"remote.example"}, Mode: DirectRoomJoinNormal,
	}
	if !reflect.DeepEqual(harness.joins, []DirectRoomJoinRequest{wantJoin}) {
		t.Fatalf("join requests = %#v, want %#v", harness.joins, wantJoin)
	}
	wantCalls := []string{
		"join:!old:remote.example",
		"list",
		"list-conversations",
		"upsert: !joined:remote.example ",
		"delete-conversation:direct:!old:remote.example",
		"delete-conversation:group: !joined:remote.example ",
		"save-conversation:direct: !joined:remote.example ",
		"operation:contacts.request:accepted: !joined:remote.example ",
	}
	if got := harness.log.snapshot(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("accept calls = %#v, want %#v", got, wantCalls)
	}
}

func TestAcceptPendingInboundPreservesStoredFieldsWithoutJoin(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:remote.example", RoomID: "!old:remote.example", DisplayName: "Stored",
		AvatarURL: "mxc://remote.example/stored", Domain: "stored.example", Status: "pending_inbound", Remark: "request",
	}
	harness := newRequestHarness(existing)
	harness.module.joinRoom = nil

	result, apiErr := harness.module.AcceptPendingInbound(context.Background(), existing, map[string]any{
		"display_name": "Request", "avatar_url": "mxc://remote.example/request", "domain": "request.example",
	})
	view, ok := result.(View)
	if apiErr != nil || !ok {
		t.Fatalf("accept pending = (%#v, %#v)", result, apiErr)
	}
	want := existing
	want.Status = "accepted"
	want.Remark = ""
	if got := RecordFromView(view); !reflect.DeepEqual(got, want) || len(harness.joins) != 0 {
		t.Fatalf("accepted contact = %#v, joins=%#v, want %#v", got, harness.joins, want)
	}
}

func TestAcceptPendingInboundReactivatesWithSameProfileAndServers(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!old:remote.example", Status: "pending_inbound"}
	harness := newRequestHarness(existing)
	harness.joinOutcomes = []DirectRoomJoinOutcome{
		{Kind: DirectRoomJoinInviteRequired},
		{Kind: DirectRoomJoinSucceeded, RoomID: existing.RoomID},
	}

	result, apiErr := harness.module.AcceptPendingInbound(context.Background(), existing, map[string]any{
		"remote_node_base_url": " https://remote.example/_p2p ",
		"server_names":         []string{" remote.example "},
		"display_name":         "Alice",
		"reason":               "retry",
	})
	view, ok := result.(View)
	if apiErr != nil || !ok || view.Status != "accepted" {
		t.Fatalf("reactivated pending = (%#v, %#v)", result, apiErr)
	}
	if len(harness.joins) != 2 || harness.joins[0].Mode != DirectRoomJoinNormal || harness.joins[1].Mode != DirectRoomJoinReactivation ||
		!reflect.DeepEqual(harness.joins[0].Profile, harness.joins[1].Profile) || !reflect.DeepEqual(harness.joins[0].ServerNames, harness.joins[1].ServerNames) {
		t.Fatalf("join requests = %#v", harness.joins)
	}
	wantPeer := PeerReactivationRequest{
		Contact: existing, RequesterMXID: harness.profile.MXID, RemoteNodeBaseURL: "https://remote.example/_p2p",
		DisplayName: "Alice", Remark: "retry",
	}
	if !reflect.DeepEqual(harness.reactivations, []PeerReactivationRequest{wantPeer}) {
		t.Fatalf("peer requests = %#v, want %#v", harness.reactivations, wantPeer)
	}
}

func TestAcceptPendingInboundMapsPeerOutcomesToExistingRoomApproval(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!old:remote.example", Status: "pending_inbound"}
	tests := []struct {
		name        string
		peer        PeerReactivationResult
		wantInvites int
	}{
		{name: "peer pending keeps old room without invite", peer: PeerReactivationResult{PendingInbound: true}},
		{name: "peer missing keeps old room with invite", peer: PeerReactivationResult{NotRetained: true}, wantInvites: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness(existing)
			harness.joinOutcomes = []DirectRoomJoinOutcome{{Kind: DirectRoomJoinInviteRequired}}
			harness.reactivationResult = tt.peer

			result, apiErr := harness.module.AcceptPendingInbound(context.Background(), existing, map[string]any{
				"remote_node_base_url": "https://remote.example/_p2p", "domain": "remote.example",
			})
			view, ok := result.(View)
			if apiErr != nil || !ok || view.Status != "pending_outbound" || view.RoomID != existing.RoomID {
				t.Fatalf("peer outcome = (%#v, %#v)", result, apiErr)
			}
			if len(harness.joins) != 1 || len(harness.invites) != tt.wantInvites {
				t.Fatalf("joins=%#v invites=%#v", harness.joins, harness.invites)
			}
		})
	}
}

func TestAcceptPendingInboundFailureBoundaries(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!old:remote.example", Status: "pending_inbound"}
	tests := []struct {
		name          string
		joins         []DirectRoomJoinOutcome
		peerErr       *actionbase.Error
		upsertErr     error
		operationErr  error
		wantPersisted bool
		wantOperation bool
	}{
		{name: "normal join failure", joins: []DirectRoomJoinOutcome{{Kind: DirectRoomJoinFailed, Failure: actionbase.StatusError(http.StatusForbidden, "join denied")}}},
		{name: "peer failure", joins: []DirectRoomJoinOutcome{{Kind: DirectRoomJoinInviteRequired}}, peerErr: actionbase.StatusError(http.StatusForbidden, "peer denied")},
		{name: "reactivation unavailable", joins: []DirectRoomJoinOutcome{{Kind: DirectRoomJoinInviteRequired}, {Kind: DirectRoomJoinRetainedUnavailable, Failure: actionbase.StatusError(http.StatusForbidden, "room unavailable")}}},
		{name: "unknown join outcome", joins: []DirectRoomJoinOutcome{{Kind: DirectRoomJoinUnknown}}},
		{name: "save failure", joins: []DirectRoomJoinOutcome{{Kind: DirectRoomJoinSucceeded, RoomID: existing.RoomID}}, upsertErr: errors.New("save failed")},
		{name: "operation failure", joins: []DirectRoomJoinOutcome{{Kind: DirectRoomJoinSucceeded, RoomID: existing.RoomID}}, operationErr: errors.New("operation failed"), wantPersisted: true, wantOperation: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness(existing)
			harness.joinOutcomes = tt.joins
			harness.reactivationErr = tt.peerErr
			harness.store.upsertErr = tt.upsertErr
			harness.conversation.operationErr = tt.operationErr

			result, apiErr := harness.module.AcceptPendingInbound(context.Background(), existing, nil)
			if result != nil || apiErr == nil {
				t.Fatalf("pending failure = (%#v, %#v)", result, apiErr)
			}
			if got := len(harness.store.upserts()); (got > 0) != tt.wantPersisted {
				t.Fatalf("persisted=%v want=%v", got > 0, tt.wantPersisted)
			}
			operationCalled := false
			for _, call := range harness.log.snapshot() {
				if len(call) >= len("operation:") && call[:len("operation:")] == "operation:" {
					operationCalled = true
				}
			}
			if operationCalled != tt.wantOperation {
				t.Fatalf("operation called=%v want=%v calls=%#v", operationCalled, tt.wantOperation, harness.log.snapshot())
			}
		})
	}
}
