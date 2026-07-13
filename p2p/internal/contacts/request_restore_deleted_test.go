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

func TestRestoreDeletedWithoutTransportUpdatesSnapshot(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:remote.example", RoomID: "!old:remote.example", DisplayName: "Old",
		DisplayNameOverride: true, AvatarURL: "mxc://remote.example/old", Status: "deleted", Remark: "deleted",
	}
	harness := newRequestHarness(existing)
	harness.module.joinRoom = nil

	result, apiErr := harness.module.RestoreDeleted(context.Background(), existing, map[string]any{
		"remote_node_base_url": "https://remote.example/_p2p",
		"display_name":         " New Alice ",
		"avatar_url":           " mxc://remote.example/new ",
	}, "fallback.example")
	view, ok := result.(View)
	if apiErr != nil || !ok {
		t.Fatalf("restore deleted = (%#v, %#v)", result, apiErr)
	}
	want := existing
	want.DisplayName = "New Alice"
	want.AvatarURL = "mxc://remote.example/new"
	want.Domain = "fallback.example"
	want.Status = "accepted"
	want.Remark = ""
	if got := RecordFromView(view); !reflect.DeepEqual(got, want) || len(harness.reactivations) != 0 || len(harness.joins) != 0 {
		t.Fatalf("restored contact = %#v, want %#v; peer=%#v joins=%#v", got, want, harness.reactivations, harness.joins)
	}
}

func TestRestoreDeletedEligibilityPreservesLocalPeerAndExactRoomSemantics(t *testing.T) {
	tests := []struct {
		name            string
		contact         dirextalkdomain.ContactRecord
		raw             map[string]any
		wantJoins       int
		wantPeerProbes  int
		wantFinalRoomID string
	}{
		{
			name: "explicit URL for local peer stays reactive",
			contact: dirextalkdomain.ContactRecord{
				PeerMXID: "@alice:example.com", RoomID: "!local:example.com", Status: "deleted",
			},
			raw:             map[string]any{"remote_node_base_url": "https://unused.example/_p2p"},
			wantJoins:       1,
			wantFinalRoomID: "!local:example.com",
		},
		{
			name: "empty room skips Matrix and peer work",
			contact: dirextalkdomain.ContactRecord{
				PeerMXID: "@alice:remote.example", RoomID: "", Status: "deleted",
			},
			raw:             map[string]any{"remote_node_base_url": "https://remote.example/_p2p"},
			wantFinalRoomID: "",
		},
		{
			name: "whitespace room is still joined",
			contact: dirextalkdomain.ContactRecord{
				PeerMXID: "@alice:remote.example", RoomID: "  ", Status: "deleted",
			},
			wantJoins:       1,
			wantFinalRoomID: "  ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness(tt.contact)
			harness.joinOutcomes = []DirectRoomJoinOutcome{{Kind: DirectRoomJoinSucceeded, RoomID: tt.contact.RoomID}}
			result, apiErr := harness.module.RestoreDeleted(context.Background(), tt.contact, tt.raw, "fallback.example")
			view, ok := result.(View)
			if apiErr != nil || !ok || view.RoomID != tt.wantFinalRoomID || len(harness.joins) != tt.wantJoins || len(harness.reactivations) != tt.wantPeerProbes {
				t.Fatalf("restore = (%#v, %#v), joins=%#v peer=%#v", result, apiErr, harness.joins, harness.reactivations)
			}
		})
	}
}

func TestRestoreDeletedProactivePeerOutcomes(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!old:remote.example", DisplayName: "Alice", Status: "deleted"}
	for _, tt := range []struct {
		name string
		peer PeerReactivationResult
	}{
		{name: "not retained creates replacement", peer: PeerReactivationResult{NotRetained: true}},
		{name: "peer pending creates replacement", peer: PeerReactivationResult{PendingInbound: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness(existing)
			harness.reactivationResult = tt.peer
			result, apiErr := harness.module.RestoreDeleted(context.Background(), existing, map[string]any{
				"remote_node_base_url": "https://remote.example/_p2p",
			}, "fallback.example")
			view, ok := result.(View)
			if apiErr != nil || !ok || view.Status != "pending_outbound" || view.RoomID != "!created:example.com" || len(harness.creates) != 1 || len(harness.joins) != 0 {
				t.Fatalf("restore deleted = (%#v, %#v), creates=%#v joins=%#v", result, apiErr, harness.creates, harness.joins)
			}
		})
	}
}

func TestRestoreDeletedProactiveJoinUsesStoredRoomAndFallbackServer(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!stored:remote.example", Status: "deleted"}
	harness := newRequestHarness(existing)
	harness.reactivationResult = PeerReactivationResult{RoomID: "!ignored:other.example"}
	harness.joinOutcomes = []DirectRoomJoinOutcome{{Kind: DirectRoomJoinSucceeded, RoomID: " !joined:remote.example "}}

	result, apiErr := harness.module.RestoreDeleted(context.Background(), existing, map[string]any{
		"remote_node_base_url": "https://remote.example/_p2p",
		"display_name":         " Alice ",
		"avatar_url":           " mxc://remote.example/alice ",
		"domain":               " remote.example ",
		"remark":               " retained friend ",
		"server_names":         []any{" explicit.remote.example ", "explicit.remote.example"},
	}, "fallback.example")
	view, ok := result.(View)
	if apiErr != nil || !ok || view.Status != "accepted" || view.RoomID != " !joined:remote.example " {
		t.Fatalf("restore deleted = (%#v, %#v)", result, apiErr)
	}
	wantJoin := DirectRoomJoinRequest{
		RoomID: existing.RoomID, Profile: harness.profile, ServerNames: []string{"explicit.remote.example"},
		Mode: DirectRoomJoinReactivation, UseRoomServerFallback: true,
	}
	if !reflect.DeepEqual(harness.joins, []DirectRoomJoinRequest{wantJoin}) {
		t.Fatalf("join requests = %#v, want %#v", harness.joins, wantJoin)
	}
	wantPeer := PeerReactivationRequest{
		Contact: existing, RequesterMXID: harness.profile.MXID,
		RemoteNodeBaseURL: "https://remote.example/_p2p", DisplayName: "Alice",
		AvatarURL: "mxc://remote.example/alice", Domain: "remote.example", Remark: "retained friend",
	}
	if !reflect.DeepEqual(harness.reactivations, []PeerReactivationRequest{wantPeer}) {
		t.Fatalf("peer requests = %#v, want %#v", harness.reactivations, wantPeer)
	}
}

func TestRestoreDeletedProactiveFailuresDoNotFallback(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!old:remote.example", Status: "deleted"}
	tests := []struct {
		name    string
		peerErr *actionbase.Error
		join    DirectRoomJoinOutcome
	}{
		{name: "peer failure", peerErr: actionbase.StatusError(http.StatusForbidden, "peer denied")},
		{name: "unavailable join", join: DirectRoomJoinOutcome{Kind: DirectRoomJoinRetainedUnavailable, Failure: actionbase.StatusError(http.StatusForbidden, "unavailable")}},
		{name: "ordinary join failure", join: DirectRoomJoinOutcome{Kind: DirectRoomJoinFailed, Failure: actionbase.StatusError(http.StatusForbidden, "join denied")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness(existing)
			harness.reactivationErr = tt.peerErr
			if tt.join.Kind != DirectRoomJoinUnknown {
				harness.joinOutcomes = []DirectRoomJoinOutcome{tt.join}
			}
			result, apiErr := harness.module.RestoreDeleted(context.Background(), existing, map[string]any{
				"remote_node_base_url": "https://remote.example/_p2p",
			}, "fallback.example")
			if result != nil || apiErr == nil || len(harness.creates) != 0 || len(harness.store.upserts()) != 0 {
				t.Fatalf("restore failure = (%#v, %#v), creates=%#v upserts=%#v", result, apiErr, harness.creates, harness.store.upserts())
			}
		})
	}
}

func TestRestoreDeletedReactivePeerOutcomesStayDistinct(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!old:remote.example", Status: "deleted"}
	tests := []struct {
		name        string
		peer        PeerReactivationResult
		wantStatus  string
		wantRoomID  string
		wantInvites int
		wantCreates int
	}{
		{name: "pending keeps old room without invite", peer: PeerReactivationResult{PendingInbound: true}, wantStatus: "pending_outbound", wantRoomID: existing.RoomID},
		{name: "not retained creates replacement", peer: PeerReactivationResult{NotRetained: true}, wantStatus: "pending_outbound", wantRoomID: "!created:example.com", wantCreates: 1},
		{name: "retained rejoins old room", wantStatus: "accepted", wantRoomID: "!reactivated:remote.example"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness(existing)
			harness.joinOutcomes = []DirectRoomJoinOutcome{{Kind: DirectRoomJoinInviteRequired}}
			if tt.wantStatus == "accepted" {
				harness.joinOutcomes = append(harness.joinOutcomes, DirectRoomJoinOutcome{Kind: DirectRoomJoinSucceeded, RoomID: tt.wantRoomID})
			}
			harness.reactivationResult = tt.peer

			result, apiErr := harness.module.RestoreDeleted(context.Background(), existing, nil, "fallback.example")
			view, ok := result.(View)
			if apiErr != nil || !ok || view.Status != tt.wantStatus || view.RoomID != tt.wantRoomID || len(harness.invites) != tt.wantInvites || len(harness.creates) != tt.wantCreates {
				t.Fatalf("restore = (%#v, %#v), invites=%#v creates=%#v", result, apiErr, harness.invites, harness.creates)
			}
			if tt.peer.PendingInbound && len(harness.joins) != 1 {
				t.Fatalf("pending peer should not rejoin: %#v", harness.joins)
			}
		})
	}
}

func TestRestoreDeletedReactiveNotRetainedDoesNotReuseOldRoomInvite(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!old:remote.example", Status: "deleted"}
	harness := newRequestHarness(existing)
	harness.joinOutcomes = []DirectRoomJoinOutcome{{Kind: DirectRoomJoinInviteRequired}}
	harness.reactivationResult = PeerReactivationResult{NotRetained: true}

	result, apiErr := harness.module.RestoreDeleted(context.Background(), existing, map[string]any{
		"display_name": "Alice",
	}, "fallback.example")
	view, ok := result.(View)
	if apiErr != nil || !ok || view.RoomID != "!created:example.com" || len(harness.invites) != 0 || len(harness.creates) != 1 {
		t.Fatalf("restore = (%#v, %#v), invites=%#v creates=%#v", result, apiErr, harness.invites, harness.creates)
	}
}

func TestRestoreDeletedPersistenceFailuresReturnNil(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!old:remote.example", Status: "deleted"}
	for _, tt := range []struct {
		name         string
		upsertErr    error
		operationErr error
	}{
		{name: "save failure", upsertErr: errors.New("save failed")},
		{name: "operation failure", operationErr: errors.New("operation failed")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness(existing)
			harness.module.joinRoom = nil
			harness.store.upsertErr = tt.upsertErr
			harness.conversation.operationErr = tt.operationErr
			result, apiErr := harness.module.RestoreDeleted(context.Background(), existing, nil, "fallback.example")
			if result != nil || apiErr == nil {
				t.Fatalf("restore failure = (%#v, %#v)", result, apiErr)
			}
		})
	}
}
