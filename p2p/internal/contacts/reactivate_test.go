package contacts

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

type reactivationCall struct {
	profile       LocalProfileSnapshot
	roomID        string
	requesterMXID string
}

type reactivationHarness struct {
	*saveHarness
	profile      LocalProfileSnapshot
	profileCalls int
	invites      []reactivationCall
	inviteErr    *actionbase.Error
}

func newReactivationHarness(records []dirextalkdomain.ContactRecord) *reactivationHarness {
	harness := &reactivationHarness{saveHarness: newSaveHarness(records)}
	harness.module = New(harness.store, harness.conversation, Config{
		DeleteGroup: func(_ context.Context, roomID string) error {
			harness.log.add("delete-group:" + roomID)
			return harness.deleteGroupErr
		},
		LocalProfile: func() LocalProfileSnapshot {
			harness.profileCalls++
			return harness.profile
		},
		ReactivateDirectRoom: func(_ context.Context, profile LocalProfileSnapshot, roomID, requesterMXID string) *actionbase.Error {
			harness.log.add("invite:" + roomID + ":" + requesterMXID)
			harness.invites = append(harness.invites, reactivationCall{
				profile: profile, roomID: roomID, requesterMXID: requesterMXID,
			})
			return harness.inviteErr
		},
	})
	return harness
}

func TestReactivateValidatesRequesterAndRetainedContact(t *testing.T) {
	accepted := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "accepted",
	}
	wantReadErr := errors.New("read failed")
	tests := []struct {
		name        string
		records     []dirextalkdomain.ContactRecord
		params      map[string]any
		profileMXID string
		listErr     error
		wantStatus  int
		wantError   string
		wantProfile bool
	}{
		{name: "requester required", wantStatus: http.StatusBadRequest, wantError: "requester_mxid is required"},
		{name: "self requester", records: []dirextalkdomain.ContactRecord{accepted}, params: map[string]any{"requester_mxid": accepted.PeerMXID}, profileMXID: accepted.PeerMXID, wantStatus: http.StatusBadRequest, wantError: "requester_mxid must be a remote peer", wantProfile: true},
		{name: "read error", records: []dirextalkdomain.ContactRecord{accepted}, params: map[string]any{"requester_mxid": accepted.PeerMXID}, listErr: wantReadErr, wantStatus: http.StatusInternalServerError, wantError: "internal error: read failed", wantProfile: true},
		{name: "peer missing", params: map[string]any{"requester_mxid": accepted.PeerMXID}, wantStatus: http.StatusNotFound, wantError: "retained contact not found", wantProfile: true},
		{name: "room mismatch", records: []dirextalkdomain.ContactRecord{accepted}, params: map[string]any{"requester_mxid": accepted.PeerMXID, "room_id": "!other:example.com"}, wantStatus: http.StatusNotFound, wantError: "retained contact not found", wantProfile: true},
		{name: "roomless nonaccepted", records: []dirextalkdomain.ContactRecord{{PeerMXID: accepted.PeerMXID, RoomID: accepted.RoomID, Status: "pending_outbound"}}, params: map[string]any{"requester_mxid": accepted.PeerMXID}, wantStatus: http.StatusNotFound, wantError: "retained contact not found", wantProfile: true},
		{name: "deleted retained room", records: []dirextalkdomain.ContactRecord{{PeerMXID: accepted.PeerMXID, RoomID: accepted.RoomID, Status: " DeLeTeD "}}, params: map[string]any{"requester_mxid": accepted.PeerMXID, "room_id": accepted.RoomID}, wantStatus: http.StatusNotFound, wantError: "retained contact not found", wantProfile: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newReactivationHarness(tt.records)
			harness.profile = LocalProfileSnapshot{MXID: tt.profileMXID}
			harness.store.listErr = tt.listErr
			result, apiErr := harness.module.Handlers()[actionReactivate](context.Background(), tt.params)
			if result != nil || apiErr == nil || apiErr.Status != tt.wantStatus || apiErr.Error != tt.wantError {
				t.Fatalf("contacts.reactivate = (%#v, %#v), want status=%d error=%q", result, apiErr, tt.wantStatus, tt.wantError)
			}
			if got := harness.profileCalls > 0; got != tt.wantProfile {
				t.Fatalf("LocalProfile called = %t, want %t", got, tt.wantProfile)
			}
			if len(harness.invites) != 0 || len(harness.store.upserts()) != 0 {
				t.Fatalf("failed reactivation calls = invites %#v upserts %#v", harness.invites, harness.store.upserts())
			}
		})
	}
}

func TestReactivateRecordsPendingInboundWithoutTrustingCallerProfile(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", AvatarURL: "mxc://example.com/alice",
		Status: "pending_outbound", Remark: "friend", DisplayNameOverride: true,
	}
	harness := newReactivationHarness([]dirextalkdomain.ContactRecord{existing})
	harness.profile = LocalProfileSnapshot{MXID: "@owner:remote.example", DisplayName: "Owner", AvatarURL: "mxc://remote.example/owner"}
	operation := map[string]any{"action": actionReactivate, "status": "pending_inbound", "room_id": existing.RoomID}
	conversation := &dirextalkdomain.ConversationView{ConversationID: "conv", MatrixRoomID: existing.RoomID}
	harness.conversation.operation = operation
	harness.conversation.operationView = conversation

	result, apiErr := harness.module.Handlers()[actionReactivate](context.Background(), map[string]any{
		"room_id": existing.RoomID, "requester_mxid": existing.PeerMXID,
		"display_name": "Spoofed", "avatar_url": "mxc://evil/avatar", "domain": "evil.example",
	})
	if apiErr != nil {
		t.Fatalf("contacts.reactivate error = %#v", apiErr)
	}
	response, ok := result.(map[string]any)
	if !ok || response["status"] != "pending_inbound" || response["room_id"] != existing.RoomID || !reflect.DeepEqual(response["operation"], operation) {
		t.Fatalf("contacts.reactivate result = %T %#v", result, result)
	}
	if got, ok := response["conversation"].(dirextalkdomain.ConversationView); !ok || got.ConversationID != conversation.ConversationID {
		t.Fatalf("conversation result = %T %#v", response["conversation"], response["conversation"])
	}
	want := existing
	want.DisplayName = "alice"
	want.Domain = "example.com"
	want.Status = "pending_inbound"
	if got := harness.store.upserts(); !reflect.DeepEqual(got, []dirextalkdomain.ContactRecord{want}) {
		t.Fatalf("upserted contacts = %#v, want %#v", got, want)
	}
	if len(harness.invites) != 0 {
		t.Fatalf("pending reactivation invited room: %#v", harness.invites)
	}
}

func TestReactivateAcceptedContactInvitesStoredOrExplicitRoom(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", DisplayName: "Alice", Status: "accepted", Remark: "friend",
	}
	tests := []struct {
		name   string
		params map[string]any
	}{
		{name: "stored room", params: map[string]any{"requester_mxid": existing.PeerMXID}},
		{name: "matching explicit room", params: map[string]any{"requester_mxid": existing.PeerMXID, "room_id": existing.RoomID}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newReactivationHarness([]dirextalkdomain.ContactRecord{existing})
			harness.profile = LocalProfileSnapshot{MXID: "@owner:remote.example", DisplayName: "Owner", AvatarURL: "mxc://remote.example/owner"}
			operation := map[string]any{"action": actionReactivate, "status": "invited", "room_id": existing.RoomID}
			harness.conversation.operation = operation
			result, apiErr := harness.module.Handlers()[actionReactivate](context.Background(), tt.params)
			if apiErr != nil {
				t.Fatalf("contacts.reactivate error = %#v", apiErr)
			}
			response, ok := result.(map[string]any)
			if !ok || response["status"] != "invited" || response["room_id"] != existing.RoomID || !reflect.DeepEqual(response["operation"], operation) {
				t.Fatalf("contacts.reactivate result = %T %#v", result, result)
			}
			wantCall := reactivationCall{profile: harness.profile, roomID: existing.RoomID, requesterMXID: existing.PeerMXID}
			if !reflect.DeepEqual(harness.invites, []reactivationCall{wantCall}) {
				t.Fatalf("reactivation invites = %#v, want %#v", harness.invites, wantCall)
			}
			if len(harness.store.upserts()) != 0 {
				t.Fatalf("accepted reactivation wrote contact: %#v", harness.store.upserts())
			}
		})
	}
}

func TestReactivateAcceptedContactWithoutInviterStillReturnsInvited(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "accepted",
	}
	harness := newSaveHarness([]dirextalkdomain.ContactRecord{existing})
	result, apiErr := harness.module.Handlers()[actionReactivate](context.Background(), map[string]any{
		"requester_mxid": existing.PeerMXID,
	})
	if apiErr != nil {
		t.Fatalf("contacts.reactivate error = %#v", apiErr)
	}
	if response, ok := result.(map[string]any); !ok || response["status"] != "invited" || response["room_id"] != existing.RoomID {
		t.Fatalf("contacts.reactivate result = %T %#v", result, result)
	}
	if len(harness.store.upserts()) != 0 {
		t.Fatalf("accepted reactivation without inviter wrote contact: %#v", harness.store.upserts())
	}
}

func TestReactivateAcceptedContactCompletesWhilePeerLockIsHeld(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "accepted",
	}
	harness := newReactivationHarness([]dirextalkdomain.ContactRecord{existing})
	harness.profile = LocalProfileSnapshot{MXID: "@owner:remote.example"}

	lockHeld := make(chan struct{})
	releaseLock := make(chan struct{})
	lockDone := make(chan struct{})
	go func() {
		harness.module.SerializePeer(existing.PeerMXID, func() {
			close(lockHeld)
			<-releaseLock
		})
		close(lockDone)
	}()
	<-lockHeld

	type outcome struct {
		result any
		err    *actionbase.Error
	}
	resultCh := make(chan outcome, 1)
	go func() {
		result, apiErr := harness.module.Handlers()[actionReactivate](context.Background(), map[string]any{
			"requester_mxid": existing.PeerMXID,
		})
		resultCh <- outcome{result: result, err: apiErr}
	}()

	select {
	case got := <-resultCh:
		if got.err != nil {
			close(releaseLock)
			<-lockDone
			t.Fatalf("contacts.reactivate error = %#v", got.err)
		}
		if response, ok := got.result.(map[string]any); !ok || response["status"] != "invited" {
			close(releaseLock)
			<-lockDone
			t.Fatalf("contacts.reactivate result = %T %#v", got.result, got.result)
		}
	case <-time.After(2 * time.Second):
		close(releaseLock)
		<-lockDone
		t.Fatal("contacts.reactivate waited for the local peer lock and risks a distributed lock cycle")
	}
	close(releaseLock)
	<-lockDone
}

func TestReactivateMapsInviteSaveAndOperationErrors(t *testing.T) {
	pending := dirextalkdomain.ContactRecord{PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "pending_outbound"}
	accepted := pending
	accepted.Status = "accepted"
	wantErr := errors.New("reactivate failed")
	tests := []struct {
		name          string
		record        dirextalkdomain.ContactRecord
		configure     func(*reactivationHarness)
		wantStatus    int
		wantError     string
		wantInvite    bool
		wantCommitted bool
		wantOperation bool
	}{
		{name: "invite", record: accepted, configure: func(h *reactivationHarness) {
			h.inviteErr = actionbase.CodedError(http.StatusForbidden, "invite_denied", "invite denied")
		}, wantStatus: http.StatusForbidden, wantError: "invite denied", wantInvite: true},
		{name: "pending save", record: pending, configure: func(h *reactivationHarness) { h.store.upsertErr = wantErr }, wantStatus: http.StatusInternalServerError, wantError: "internal error: reactivate failed"},
		{name: "pending save after contact commit", record: pending, configure: func(h *reactivationHarness) { h.conversation.saveErr = wantErr }, wantStatus: http.StatusInternalServerError, wantError: "internal error: reactivate failed", wantCommitted: true},
		{name: "pending operation", record: pending, configure: func(h *reactivationHarness) { h.conversation.operationErr = wantErr }, wantStatus: http.StatusInternalServerError, wantError: "internal error: reactivate failed", wantCommitted: true, wantOperation: true},
		{name: "accepted operation", record: accepted, configure: func(h *reactivationHarness) { h.conversation.operationErr = wantErr }, wantStatus: http.StatusInternalServerError, wantError: "internal error: reactivate failed", wantInvite: true, wantOperation: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newReactivationHarness([]dirextalkdomain.ContactRecord{tt.record})
			harness.profile = LocalProfileSnapshot{MXID: "@owner:remote.example"}
			tt.configure(harness)
			result, apiErr := harness.module.Handlers()[actionReactivate](context.Background(), map[string]any{
				"room_id": tt.record.RoomID, "requester_mxid": tt.record.PeerMXID,
			})
			if result != nil || apiErr == nil || apiErr.Status != tt.wantStatus || apiErr.Error != tt.wantError {
				t.Fatalf("contacts.reactivate failure = (%#v, %#v)", result, apiErr)
			}
			if tt.name == "invite" && apiErr.Code != "invite_denied" {
				t.Fatalf("contacts.reactivate code = %q, want invite_denied", apiErr.Code)
			}
			if got := len(harness.invites) > 0; got != tt.wantInvite {
				t.Fatalf("ReactivateDirectRoom called = %t, want %t", got, tt.wantInvite)
			}
			if got := len(harness.store.upserts()) == 1; got != tt.wantCommitted {
				t.Fatalf("successful contact commit = %t, want %t", got, tt.wantCommitted)
			}
			operationCalled := false
			for _, call := range harness.log.snapshot() {
				if call == "operation:contacts.reactivate:"+tt.record.Status+":"+tt.record.RoomID ||
					call == "operation:contacts.reactivate:pending_inbound:"+tt.record.RoomID ||
					call == "operation:contacts.reactivate:invited:"+tt.record.RoomID {
					operationCalled = true
				}
			}
			if operationCalled != tt.wantOperation {
				t.Fatalf("Operation called = %t, want %t; calls=%v", operationCalled, tt.wantOperation, harness.log.snapshot())
			}
		})
	}
}
