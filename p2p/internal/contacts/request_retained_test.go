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

func TestRestoreRetainedPeerEligibility(t *testing.T) {
	tests := []struct {
		name   string
		domain string
		raw    map[string]any
	}{
		{name: "missing URL", domain: "remote.example"},
		{name: "missing domain", raw: map[string]any{"remote_node_base_url": "https://remote.example/_p2p"}},
		{name: "local domain", domain: "example.com", raw: map[string]any{"remote_node_base_url": "https://remote.example/_p2p"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness()
			result, restored, apiErr := harness.module.RestoreRetainedPeer(context.Background(), "@alice:remote.example", tt.raw, tt.domain)
			if !reflect.DeepEqual(result, View{}) || restored || apiErr != nil || len(harness.reactivations) != 0 || len(harness.store.upserts()) != 0 {
				t.Fatalf("restore = (%#v, %v, %#v), reactivations=%#v", result, restored, apiErr, harness.reactivations)
			}
		})
	}
}

func TestRestoreRetainedPeerFallsBackToFreshRequestOutcomes(t *testing.T) {
	tests := []struct {
		name    string
		result  PeerReactivationResult
		peerErr *actionbase.Error
		wantErr string
	}{
		{name: "not retained", result: PeerReactivationResult{NotRetained: true}},
		{name: "peer pending", result: PeerReactivationResult{PendingInbound: true}},
		{name: "missing room", result: PeerReactivationResult{RoomID: "  "}},
		{name: "peer error", peerErr: actionbase.StatusError(http.StatusForbidden, "peer denied"), wantErr: "peer denied"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness()
			harness.reactivationResult = tt.result
			harness.reactivationErr = tt.peerErr
			result, restored, apiErr := harness.module.RestoreRetainedPeer(context.Background(), "@alice:remote.example", map[string]any{
				"remote_node_base_url": "https://remote.example/_p2p",
			}, "remote.example")
			if !reflect.DeepEqual(result, View{}) || restored || len(harness.store.upserts()) != 0 || len(harness.joins) != 0 {
				t.Fatalf("restore = (%#v, %v, %#v), joins=%#v", result, restored, apiErr, harness.joins)
			}
			if tt.wantErr == "" {
				if apiErr != nil {
					t.Fatal(apiErr)
				}
			} else if apiErr == nil || apiErr.Error != tt.wantErr {
				t.Fatalf("restore error = %#v, want %q", apiErr, tt.wantErr)
			}
		})
	}
}

func TestRestoreRetainedPeerPersistsAcceptedWithoutTransport(t *testing.T) {
	harness := newRequestHarness()
	harness.module.joinRoom = nil
	harness.reactivationResult = PeerReactivationResult{RoomID: "!retained:remote.example"}

	result, restored, apiErr := harness.module.RestoreRetainedPeer(context.Background(), "@alice:remote.example", map[string]any{
		"remote_node_base_url": "https://remote.example/_p2p",
		"display_name":         " Alice ",
		"avatar_url":           " mxc://remote.example/alice ",
		"reason":               "request remark is not persisted",
	}, "remote.example")
	want := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:remote.example", RoomID: "!retained:remote.example", DisplayName: "Alice",
		AvatarURL: "mxc://remote.example/alice", Domain: "remote.example", Status: "accepted",
	}
	if apiErr != nil || !restored || !reflect.DeepEqual(RecordFromView(result), want) || len(harness.joins) != 0 {
		t.Fatalf("restore = (%#v, %v, %#v), want %#v joins=%#v", result, restored, apiErr, want, harness.joins)
	}
	if len(harness.reactivations) != 1 || harness.reactivations[0].Contact != (dirextalkdomain.ContactRecord{PeerMXID: want.PeerMXID, Domain: want.Domain}) ||
		harness.reactivations[0].Remark != "request remark is not persisted" {
		t.Fatalf("peer request = %#v", harness.reactivations)
	}
}

func TestRestoreRetainedPeerJoinsWithRoomServerFallback(t *testing.T) {
	harness := newRequestHarness()
	harness.reactivationResult = PeerReactivationResult{RoomID: "!retained:remote.example"}
	harness.joinOutcomes = []DirectRoomJoinOutcome{{Kind: DirectRoomJoinSucceeded, RoomID: " !joined:remote.example "}}

	result, restored, apiErr := harness.module.RestoreRetainedPeer(context.Background(), "@alice:remote.example", map[string]any{
		"remote_node_base_url": "https://remote.example/_p2p",
	}, "remote.example")
	if apiErr != nil || !restored || result.RoomID != " !joined:remote.example " {
		t.Fatalf("restore = (%#v, %v, %#v)", result, restored, apiErr)
	}
	wantJoin := DirectRoomJoinRequest{
		RoomID: "!retained:remote.example", Profile: harness.profile, Mode: DirectRoomJoinReactivation, UseRoomServerFallback: true,
	}
	if !reflect.DeepEqual(harness.joins, []DirectRoomJoinRequest{wantJoin}) {
		t.Fatalf("join requests = %#v, want %#v", harness.joins, wantJoin)
	}
}

func TestRestoreRetainedPeerMapsJoinOutcomes(t *testing.T) {
	tests := []struct {
		name            string
		join            DirectRoomJoinOutcome
		createErr       *actionbase.Error
		wantRestored    bool
		wantStatus      string
		wantError       string
		wantCreateCalls int
	}{
		{name: "unavailable creates replacement", join: DirectRoomJoinOutcome{Kind: DirectRoomJoinRetainedUnavailable, Failure: actionbase.StatusError(http.StatusForbidden, "unavailable")}, wantRestored: true, wantStatus: "pending_outbound", wantCreateCalls: 1},
		{name: "unavailable replacement failure", join: DirectRoomJoinOutcome{Kind: DirectRoomJoinRetainedUnavailable, Failure: actionbase.StatusError(http.StatusForbidden, "unavailable")}, createErr: actionbase.StatusError(http.StatusForbidden, "create denied"), wantError: "create denied", wantCreateCalls: 1},
		{name: "ordinary join failure", join: DirectRoomJoinOutcome{Kind: DirectRoomJoinFailed, Failure: actionbase.StatusError(http.StatusForbidden, "join denied")}, wantError: "join denied"},
		{name: "unknown join outcome", join: DirectRoomJoinOutcome{Kind: DirectRoomJoinUnknown}, wantError: "internal error: direct room join returned outcome 0 without failure"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness()
			harness.reactivationResult = PeerReactivationResult{RoomID: "!retained:remote.example"}
			harness.joinOutcomes = []DirectRoomJoinOutcome{tt.join}
			harness.createErr = tt.createErr

			result, restored, apiErr := harness.module.RestoreRetainedPeer(context.Background(), "@alice:remote.example", map[string]any{
				"remote_node_base_url": "https://remote.example/_p2p",
			}, "remote.example")
			if restored != tt.wantRestored || result.Status != tt.wantStatus || len(harness.creates) != tt.wantCreateCalls {
				t.Fatalf("restore = (%#v, %v, %#v), creates=%#v", result, restored, apiErr, harness.creates)
			}
			if tt.wantError == "" {
				if apiErr != nil {
					t.Fatal(apiErr)
				}
			} else if apiErr == nil || apiErr.Error != tt.wantError {
				t.Fatalf("restore error = %#v, want %q", apiErr, tt.wantError)
			}
		})
	}
}

func TestRestoreRetainedPeerPersistenceFailuresAreNotRestored(t *testing.T) {
	for _, tt := range []struct {
		name         string
		upsertErr    error
		operationErr error
	}{
		{name: "save failure", upsertErr: errors.New("save failed")},
		{name: "operation failure", operationErr: errors.New("operation failed")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness()
			harness.reactivationResult = PeerReactivationResult{RoomID: "!retained:remote.example"}
			harness.joinOutcomes = []DirectRoomJoinOutcome{{Kind: DirectRoomJoinSucceeded, RoomID: "!retained:remote.example"}}
			harness.store.upsertErr = tt.upsertErr
			harness.conversation.operationErr = tt.operationErr

			result, restored, apiErr := harness.module.RestoreRetainedPeer(context.Background(), "@alice:remote.example", map[string]any{
				"remote_node_base_url": "https://remote.example/_p2p",
			}, "remote.example")
			if !reflect.DeepEqual(result, View{}) || restored || apiErr == nil {
				t.Fatalf("restore failure = (%#v, %v, %#v)", result, restored, apiErr)
			}
		})
	}
}
