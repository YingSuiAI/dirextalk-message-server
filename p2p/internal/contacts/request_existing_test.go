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

func TestResolveExistingRequestSkipsIneligiblePeerProbe(t *testing.T) {
	tests := []struct {
		name    string
		contact dirextalkdomain.ContactRecord
		raw     map[string]any
	}{
		{
			name:    "accepted without explicit URL",
			contact: dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!direct:example.com", Status: "accepted"},
		},
		{
			name:    "accepted local peer",
			contact: dirextalkdomain.ContactRecord{PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "accepted"},
			raw:     map[string]any{"remote_node_base_url": "https://remote.example/_p2p"},
		},
		{
			name:    "unknown status",
			contact: dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!direct:example.com", Status: " future_status "},
			raw:     map[string]any{"remote_node_base_url": "https://remote.example/_p2p"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness(tt.contact)
			result, apiErr := harness.module.ResolveExistingRequest(context.Background(), tt.contact, tt.raw, "fallback.example")
			view, ok := result.(View)
			if apiErr != nil || !ok || !reflect.DeepEqual(RecordFromView(view), tt.contact) {
				t.Fatalf("resolve = (%#v, %#v)", result, apiErr)
			}
			if len(harness.reactivations) != 0 || len(harness.store.upserts()) != 0 {
				t.Fatalf("ineligible resolve side effects: reactivations=%#v upserts=%#v", harness.reactivations, harness.store.upserts())
			}
		})
	}
}

func TestResolveExistingRequestProbesRemoteAcceptedContact(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:remote.example", RoomID: "!direct:example.com", DisplayName: "Stored Alice",
		Domain: "stored.example", Status: "accepted", Remark: "stored remark",
	}
	harness := newRequestHarness(existing)
	harness.reactivationResult = PeerReactivationResult{RoomID: "!ignored:remote.example"}

	result, apiErr := harness.module.ResolveExistingRequest(context.Background(), existing, map[string]any{
		"remote_node_base_url": " https://remote.example/_p2p ",
		"display_name":         " Request Alice ",
		"avatar_url":           " mxc://remote.example/request ",
		"domain":               " request.example ",
		"reason":               " reconnect ",
	}, "fallback.example")
	view, ok := result.(View)
	if apiErr != nil || !ok || !reflect.DeepEqual(RecordFromView(view), existing) {
		t.Fatalf("resolve = (%#v, %#v)", result, apiErr)
	}
	wantRequest := PeerReactivationRequest{
		Contact: existing, RequesterMXID: "@owner:example.com", RemoteNodeBaseURL: "https://remote.example/_p2p",
		DisplayName: "Request Alice", AvatarURL: "mxc://remote.example/request", Domain: "request.example", Remark: "reconnect",
	}
	if !reflect.DeepEqual(harness.reactivations, []PeerReactivationRequest{wantRequest}) {
		t.Fatalf("reactivation requests = %#v, want %#v", harness.reactivations, wantRequest)
	}
	if len(harness.store.upserts()) != 0 || len(harness.creates) != 0 {
		t.Fatalf("successful retained probe changed contact: upserts=%#v creates=%#v", harness.store.upserts(), harness.creates)
	}
	wantCalls := []string{
		"reactivate:@alice:remote.example",
		"operation:contacts.request:accepted:!direct:example.com",
	}
	if got := harness.log.snapshot(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("resolve calls = %#v, want %#v", got, wantCalls)
	}
}

func TestResolveExistingRequestHandlesPeerOutcomes(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!old:example.com", DisplayName: "Alice", Status: "accepted"}
	tests := []struct {
		name               string
		result             PeerReactivationResult
		reactivationErr    *actionbase.Error
		createErr          *actionbase.Error
		operationErr       error
		wantStatus         int
		wantError          string
		wantReplacement    bool
		wantZeroViewResult bool
		wantOperationCalls int
	}{
		{name: "peer pending creates replacement", result: PeerReactivationResult{PendingInbound: true}, wantReplacement: true, wantOperationCalls: 1},
		{name: "peer missing retained room creates replacement", result: PeerReactivationResult{NotRetained: true}, wantReplacement: true, wantOperationCalls: 1},
		{name: "replacement failure preserves zero view result", result: PeerReactivationResult{NotRetained: true}, createErr: actionbase.StatusError(http.StatusForbidden, "create denied"), wantStatus: http.StatusForbidden, wantError: "create denied", wantReplacement: true, wantZeroViewResult: true},
		{name: "peer error stops resolution", reactivationErr: actionbase.StatusError(http.StatusForbidden, "peer denied"), wantStatus: http.StatusForbidden, wantError: "peer denied"},
		{name: "operation failure follows successful probe", operationErr: errors.New("operation failed"), wantStatus: http.StatusInternalServerError, wantError: "internal error: operation failed", wantOperationCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness(existing)
			harness.reactivationResult = tt.result
			harness.reactivationErr = tt.reactivationErr
			harness.createErr = tt.createErr
			harness.conversation.operationErr = tt.operationErr

			result, apiErr := harness.module.ResolveExistingRequest(context.Background(), existing, map[string]any{
				"remote_node_base_url": "https://remote.example/_p2p",
			}, "fallback.example")
			if tt.wantStatus == 0 {
				view, ok := result.(View)
				if apiErr != nil || !ok || !tt.wantReplacement || view.Status != "pending_outbound" || view.RoomID != "!created:example.com" {
					t.Fatalf("resolve = (%#v, %#v)", result, apiErr)
				}
			} else if apiErr == nil || apiErr.Status != tt.wantStatus || apiErr.Error != tt.wantError {
				t.Fatalf("resolve error = %#v, want status=%d error=%q", apiErr, tt.wantStatus, tt.wantError)
			}
			if tt.wantStatus != 0 && !tt.wantReplacement && result != nil {
				t.Fatalf("resolve error result = %#v, want nil", result)
			}
			if tt.wantZeroViewResult {
				view, ok := result.(View)
				if !ok || !reflect.DeepEqual(view, View{}) {
					t.Fatalf("replacement error result = %#v, want zero View", result)
				}
			}
			if got := len(harness.creates); (got > 0) != tt.wantReplacement {
				t.Fatalf("replacement create count = %d, want replacement=%v", got, tt.wantReplacement)
			}
			operationCalls := 0
			for _, call := range harness.log.snapshot() {
				if len(call) >= len("operation:") && call[:len("operation:")] == "operation:" {
					operationCalls++
				}
			}
			if operationCalls != tt.wantOperationCalls {
				t.Fatalf("operation calls = %d, want %d; calls=%#v", operationCalls, tt.wantOperationCalls, harness.log.snapshot())
			}
		})
	}
}

func TestRequestPeerApprovalUpdatesExistingRoomAndControlsInvite(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:remote.example", RoomID: "!old:example.com", DisplayName: "Old Alice",
		AvatarURL: "mxc://example.com/old", Status: "accepted", Remark: "old",
	}
	for _, sendInvite := range []bool{false, true} {
		t.Run(map[bool]string{false: "without invite", true: "with invite"}[sendInvite], func(t *testing.T) {
			harness := newRequestHarness(existing)
			view, apiErr := harness.module.RequestPeerApproval(context.Background(), existing, map[string]any{
				"display_name": " New Alice ", "avatar_url": " mxc://example.com/new ", "reason": " retry ",
			}, "fallback.example", sendInvite)
			if apiErr != nil {
				t.Fatal(apiErr)
			}
			want := existing
			want.DisplayName = "New Alice"
			want.AvatarURL = "mxc://example.com/new"
			want.Domain = "fallback.example"
			want.Status = "pending_outbound"
			want.Remark = "retry"
			if got := RecordFromView(view); !reflect.DeepEqual(got, want) {
				t.Fatalf("approval contact = %#v, want %#v", got, want)
			}
			if got := len(harness.invites); got != map[bool]int{false: 0, true: 1}[sendInvite] {
				t.Fatalf("invite count = %d", got)
			}
		})
	}
}

func TestRequestPeerApprovalCreatesFreshRequestForBlankRoom(t *testing.T) {
	for _, roomID := range []string{"", "  "} {
		t.Run("room="+roomID, func(t *testing.T) {
			existing := dirextalkdomain.ContactRecord{
				PeerMXID: "@alice:remote.example", RoomID: roomID, DisplayName: "must not inherit", Domain: "stored.example", Status: "deleted", Remark: "stored",
			}
			harness := newRequestHarness(existing)
			view, apiErr := harness.module.RequestPeerApproval(context.Background(), existing, map[string]any{
				"display_name": " Request Alice ", "reason": " request remark ",
			}, "fallback.example", true)
			if apiErr != nil {
				t.Fatal(apiErr)
			}
			want := dirextalkdomain.ContactRecord{
				PeerMXID: existing.PeerMXID, RoomID: "!created:example.com", DisplayName: "Request Alice",
				Domain: "fallback.example", Status: "pending_outbound", Remark: "request remark",
			}
			if got := RecordFromView(view); !reflect.DeepEqual(got, want) {
				t.Fatalf("fresh approval contact = %#v, want %#v", got, want)
			}
			if len(harness.invites) != 0 {
				t.Fatalf("blank-room approval invited old room: %#v", harness.invites)
			}
		})
	}
}

func TestRequestPeerApprovalFailureBoundaries(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!old:example.com", Status: "accepted"}
	tests := []struct {
		name          string
		inviteErr     *actionbase.Error
		upsertErr     error
		operationErr  error
		wantPersisted bool
		wantOperation bool
	}{
		{name: "invite failure", inviteErr: actionbase.StatusError(http.StatusForbidden, "invite denied")},
		{name: "save failure", upsertErr: errors.New("save failed")},
		{name: "operation failure", operationErr: errors.New("operation failed"), wantPersisted: true, wantOperation: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness(existing)
			harness.inviteErr = tt.inviteErr
			harness.store.upsertErr = tt.upsertErr
			harness.conversation.operationErr = tt.operationErr

			_, apiErr := harness.module.RequestPeerApproval(context.Background(), existing, nil, "fallback.example", true)
			if apiErr == nil {
				t.Fatal("expected approval failure")
			}
			if got := len(harness.store.upserts()); (got > 0) != tt.wantPersisted {
				t.Fatalf("persisted = %v, want %v", got > 0, tt.wantPersisted)
			}
			operationCalled := false
			for _, call := range harness.log.snapshot() {
				if len(call) >= len("operation:") && call[:len("operation:")] == "operation:" {
					operationCalled = true
				}
			}
			if operationCalled != tt.wantOperation {
				t.Fatalf("operation called = %v, want %v; calls=%#v", operationCalled, tt.wantOperation, harness.log.snapshot())
			}
		})
	}
}
