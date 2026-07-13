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

func TestRequestValidatesAndBlocksBeforeContactLookup(t *testing.T) {
	tests := []struct {
		name       string
		raw        map[string]any
		blocked    bool
		blockErr   error
		wantStatus int
		wantError  string
		wantChecks int
	}{
		{name: "missing peer", wantStatus: http.StatusBadRequest, wantError: "mxid is required"},
		{name: "self", raw: map[string]any{"mxid": " @owner:example.com "}, wantStatus: http.StatusBadRequest, wantError: "mxid must be a remote peer"},
		{name: "blocked", raw: map[string]any{"mxid": " @alice:remote.example "}, blocked: true, wantStatus: http.StatusForbidden, wantError: "already blocked", wantChecks: 1},
		{name: "block lookup failure", raw: map[string]any{"mxid": "@alice:remote.example"}, blockErr: errors.New("read blocks"), wantStatus: http.StatusInternalServerError, wantError: "internal error: read blocks", wantChecks: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness()
			harness.blocked = tt.blocked
			harness.blockErr = tt.blockErr

			result, actionErr := harness.module.Request(context.Background(), tt.raw)
			if result != nil || actionErr == nil || actionErr.Status != tt.wantStatus || actionErr.Error != tt.wantError {
				t.Fatalf("request = (%#v, %#v), want nil status=%d error=%q", result, actionErr, tt.wantStatus, tt.wantError)
			}
			if len(harness.blockChecks) != tt.wantChecks || len(harness.log.snapshot()) != 0 || len(harness.creates) != 0 || len(harness.reactivations) != 0 {
				t.Fatalf("short-circuit checks=%#v log=%#v creates=%#v peers=%#v", harness.blockChecks, harness.log.snapshot(), harness.creates, harness.reactivations)
			}
		})
	}
}

func TestRequestLookupFailurePreventsWorkflowSideEffects(t *testing.T) {
	harness := newRequestHarness()
	harness.store.listErr = errors.New("read contacts")

	result, actionErr := harness.module.Request(context.Background(), map[string]any{"mxid": "@alice:remote.example"})
	if result != nil || actionErr == nil || actionErr.Status != http.StatusInternalServerError || actionErr.Error != "internal error: read contacts" {
		t.Fatalf("request = (%#v, %#v)", result, actionErr)
	}
	if !reflect.DeepEqual(harness.blockChecks, []string{"@alice:remote.example"}) || len(harness.creates) != 0 || len(harness.reactivations) != 0 || len(harness.joins) != 0 || len(harness.store.upserts()) != 0 {
		t.Fatalf("lookup failure leaked side effects: checks=%#v creates=%#v peers=%#v joins=%#v upserts=%#v", harness.blockChecks, harness.creates, harness.reactivations, harness.joins, harness.store.upserts())
	}
}

func TestRequestDispatchesExistingContactStatuses(t *testing.T) {
	tests := []struct {
		name        string
		status      string
		wantStatus  string
		wantUpserts int
		wantInvites int
	}{
		{name: "deleted", status: " Deleted ", wantStatus: "accepted", wantUpserts: 1},
		{name: "pending inbound", status: " Pending_Inbound ", wantStatus: "accepted", wantUpserts: 1},
		{name: "pending outbound", status: " Pending_Outbound ", wantStatus: " Pending_Outbound ", wantUpserts: 1, wantInvites: 1},
		{name: "accepted", status: " accepted ", wantStatus: " accepted "},
		{name: "unknown", status: "future_status", wantStatus: "future_status"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existing := dirextalkdomain.ContactRecord{
				PeerMXID: "@alice:remote.example", RoomID: "!direct:remote.example",
				Domain: "stored.example", Status: tt.status,
			}
			harness := newRequestHarness(existing)
			harness.module.joinRoom = nil

			result, actionErr := harness.module.Request(context.Background(), map[string]any{
				"mxid": "@alice:remote.example",
			})
			view, ok := result.(View)
			if actionErr != nil || !ok || view.Status != tt.wantStatus || len(harness.store.upserts()) != tt.wantUpserts || len(harness.invites) != tt.wantInvites {
				t.Fatalf("request = (%#v, %#v), upserts=%#v invites=%#v", result, actionErr, harness.store.upserts(), harness.invites)
			}
			if !reflect.DeepEqual(harness.blockChecks, []string{existing.PeerMXID}) {
				t.Fatalf("block checks = %#v", harness.blockChecks)
			}
		})
	}
}

func TestRequestDerivesFreshContactDomain(t *testing.T) {
	tests := []struct {
		name       string
		peerMXID   string
		domain     any
		wantDomain string
	}{
		{name: "peer domain", peerMXID: "@alice:remote.example", wantDomain: "remote.example"},
		{name: "peer domain with port", peerMXID: "@alice:remote.example:8448", wantDomain: "remote.example:8448"},
		{name: "explicit domain", peerMXID: "@alice:remote.example", domain: " explicit.example ", wantDomain: "explicit.example"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness()
			result, actionErr := harness.module.Request(context.Background(), map[string]any{
				"mxid": tt.peerMXID, "domain": tt.domain,
			})
			view, ok := result.(View)
			if actionErr != nil || !ok || view.Domain != tt.wantDomain || view.PeerMXID != tt.peerMXID {
				t.Fatalf("request = (%#v, %#v), want domain=%q", result, actionErr, tt.wantDomain)
			}
		})
	}
}

func TestRequestNoLocalContactProbesBeforeFreshFallback(t *testing.T) {
	tests := []struct {
		name        string
		peer        PeerReactivationResult
		peerErr     *actionbase.Error
		wantNil     bool
		wantStatus  string
		wantRoomID  string
		wantCreates int
	}{
		{name: "peer failure", peerErr: actionbase.StatusError(http.StatusForbidden, "peer denied"), wantNil: true},
		{name: "not retained creates fresh", peer: PeerReactivationResult{NotRetained: true}, wantStatus: "pending_outbound", wantRoomID: "!created:example.com", wantCreates: 1},
		{name: "retained restores", peer: PeerReactivationResult{RoomID: "!retained:remote.example"}, wantStatus: "accepted", wantRoomID: "!retained:remote.example"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness()
			harness.module.joinRoom = nil
			harness.reactivationResult = tt.peer
			harness.reactivationErr = tt.peerErr

			result, actionErr := harness.module.Request(context.Background(), map[string]any{
				"mxid": "@alice:remote.example", "remote_node_base_url": "https://remote.example/_p2p",
			})
			if tt.wantNil {
				if result != nil || actionErr != tt.peerErr {
					t.Fatalf("request = (%#v, %#v), want nil/%#v", result, actionErr, tt.peerErr)
				}
			} else {
				view, ok := result.(View)
				if actionErr != nil || !ok || view.Status != tt.wantStatus || view.RoomID != tt.wantRoomID {
					t.Fatalf("request = (%#v, %#v)", result, actionErr)
				}
			}
			if len(harness.reactivations) != 1 || len(harness.creates) != tt.wantCreates {
				t.Fatalf("peer/create calls = %#v / %#v", harness.reactivations, harness.creates)
			}
			if tt.wantCreates == 1 {
				calls := harness.log.snapshot()
				if len(calls) < 4 || calls[1] != "reactivate:@alice:remote.example" || calls[2] != "new-room-id" || calls[3] != "create:@alice:remote.example" {
					t.Fatalf("probe/fresh order = %#v", calls)
				}
			}
		})
	}
}

func TestRequestPreservesLeafErrorResultShapes(t *testing.T) {
	t.Run("fresh create failure boxes zero view", func(t *testing.T) {
		harness := newRequestHarness()
		harness.createErr = actionbase.StatusError(http.StatusForbidden, "create denied")
		result, actionErr := harness.module.Request(context.Background(), map[string]any{"mxid": "@alice:remote.example"})
		view, ok := result.(View)
		if !ok || !reflect.DeepEqual(view, View{}) || actionErr != harness.createErr {
			t.Fatalf("request = (%#v, %#v)", result, actionErr)
		}
	})

	t.Run("pending resend failure boxes zero view", func(t *testing.T) {
		existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!old:remote.example", Status: "pending_outbound"}
		harness := newRequestHarness(existing)
		harness.inviteErr = actionbase.StatusError(http.StatusForbidden, "invite denied")
		result, actionErr := harness.module.Request(context.Background(), map[string]any{"mxid": existing.PeerMXID})
		view, ok := result.(View)
		if !ok || !reflect.DeepEqual(view, View{}) || actionErr != harness.inviteErr {
			t.Fatalf("request = (%#v, %#v)", result, actionErr)
		}
	})
}
