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

type deleteHarness struct {
	*saveHarness
	leaves   []string
	leaveErr *actionbase.Error
}

func newDeleteHarness(records []dirextalkdomain.ContactRecord) *deleteHarness {
	harness := &deleteHarness{saveHarness: newSaveHarness(records)}
	harness.module = New(harness.store, harness.conversation, Config{
		DeleteGroup: func(_ context.Context, roomID string) error {
			harness.log.add("delete-group:" + roomID)
			return harness.deleteGroupErr
		},
		LeaveRoom: func(_ context.Context, roomID string) *actionbase.Error {
			harness.log.add("leave:" + roomID)
			harness.leaves = append(harness.leaves, roomID)
			return harness.leaveErr
		},
	})
	return harness
}

func TestDeleteLeavesRoomAndPersistsDeletedSnapshot(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", DisplayName: "Alice",
		AvatarURL: "mxc://example.com/alice", Domain: "example.com", Status: "accepted", Remark: "friend",
	}
	harness := newDeleteHarness([]dirextalkdomain.ContactRecord{existing})
	operation := map[string]any{"action": actionDelete, "status": "deleted", "room_id": existing.RoomID}
	conversation := &dirextalkdomain.ConversationView{ConversationID: "conv", MatrixRoomID: existing.RoomID}
	harness.conversation.operation = operation
	harness.conversation.operationView = conversation

	result, apiErr := harness.module.Handlers()[actionDelete](context.Background(), map[string]any{
		"room_id": existing.RoomID, "peer_mxid": "@spoofed:example.com",
	})
	if apiErr != nil {
		t.Fatalf("contacts.delete error = %#v", apiErr)
	}
	response, ok := result.(map[string]any)
	if !ok || response["status"] != "ok" || !reflect.DeepEqual(response["operation"], operation) {
		t.Fatalf("contacts.delete result = %T %#v", result, result)
	}
	if got, ok := response["conversation"].(dirextalkdomain.ConversationView); !ok || got.ConversationID != conversation.ConversationID {
		t.Fatalf("contacts.delete conversation = %T %#v", response["conversation"], response["conversation"])
	}
	want := existing
	want.Status = "deleted"
	if got := harness.store.upserts(); !reflect.DeepEqual(got, []dirextalkdomain.ContactRecord{want}) {
		t.Fatalf("upserted contacts = %#v, want %#v", got, want)
	}
	if !reflect.DeepEqual(harness.leaves, []string{existing.RoomID}) {
		t.Fatalf("left rooms = %v, want [%s]", harness.leaves, existing.RoomID)
	}
	calls := harness.log.snapshot()
	leaveIndex, upsertIndex := -1, -1
	for index, call := range calls {
		switch call {
		case "leave:" + existing.RoomID:
			leaveIndex = index
		case "upsert:" + existing.RoomID:
			upsertIndex = index
		}
	}
	if leaveIndex < 0 || upsertIndex < 0 || leaveIndex >= upsertIndex {
		t.Fatalf("leave/save order = %v", calls)
	}
}

func TestDeletePreservesMissingAndIdempotentCompatibility(t *testing.T) {
	tests := []struct {
		name      string
		existing  []dirextalkdomain.ContactRecord
		params    map[string]any
		want      dirextalkdomain.ContactRecord
		wantLeave bool
	}{
		{
			name: "missing room snapshot still attempts leave",
			params: map[string]any{
				"room_id": " !missing:example.com ", "mxid": " @alice:example.com ", "display_name": " Alice ",
				"avatar_url": " mxc://example.com/alice ", "domain": " example.com ", "reason": " cleanup ",
			},
			want: dirextalkdomain.ContactRecord{
				PeerMXID: "@alice:example.com", RoomID: "!missing:example.com", DisplayName: "Alice",
				AvatarURL: "mxc://example.com/alice", Domain: "example.com", Status: "deleted", Remark: "cleanup",
			},
			wantLeave: true,
		},
		{
			name: "already deleted skips leave and fills missing profile",
			existing: []dirextalkdomain.ContactRecord{{
				PeerMXID: "@bob:example.com", RoomID: "!deleted:example.com", Status: " DeLeTeD ", Remark: "old",
			}},
			params: map[string]any{
				"room_id": "!deleted:example.com", "display_name": " Bob ", "avatar_url": " mxc://example.com/bob ", "domain": " example.com ",
			},
			want: dirextalkdomain.ContactRecord{
				PeerMXID: "@bob:example.com", RoomID: "!deleted:example.com", DisplayName: "Bob",
				AvatarURL: "mxc://example.com/bob", Domain: "example.com", Status: "deleted", Remark: "old",
			},
		},
		{
			name:   "empty identity remains compatible",
			params: map[string]any{},
			want:   dirextalkdomain.ContactRecord{Status: "deleted"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newDeleteHarness(tt.existing)
			result, apiErr := harness.module.Handlers()[actionDelete](context.Background(), tt.params)
			if apiErr != nil {
				t.Fatalf("contacts.delete error = %#v", apiErr)
			}
			if response, ok := result.(map[string]any); !ok || response["status"] != "ok" {
				t.Fatalf("contacts.delete result = %T %#v", result, result)
			}
			if got := harness.store.upserts(); !reflect.DeepEqual(got, []dirextalkdomain.ContactRecord{tt.want}) {
				t.Fatalf("upserted contacts = %#v, want %#v", got, tt.want)
			}
			if got := len(harness.leaves) > 0; got != tt.wantLeave {
				t.Fatalf("LeaveRoom called = %t, want %t; rooms=%v", got, tt.wantLeave, harness.leaves)
			}
		})
	}
}

func TestDeleteMissingContactPreservesRemarkAliasesAndPrecedence(t *testing.T) {
	for _, key := range []string{"remark", "request_message", "message", "reason"} {
		t.Run(key, func(t *testing.T) {
			harness := newDeleteHarness(nil)
			_, apiErr := harness.module.Handlers()[actionDelete](context.Background(), map[string]any{
				"peer_mxid": "@alice:example.com", key: " hello ",
			})
			if apiErr != nil {
				t.Fatalf("contacts.delete error = %#v", apiErr)
			}
			if got := harness.store.upserts(); len(got) != 1 || got[0].Remark != "hello" {
				t.Fatalf("%s remark upsert = %#v", key, got)
			}
		})
	}

	harness := newDeleteHarness(nil)
	_, apiErr := harness.module.Handlers()[actionDelete](context.Background(), map[string]any{
		"peer_mxid": "@alice:example.com", "remark": "first", "request_message": "second", "message": "third", "reason": "fourth",
	})
	if apiErr != nil {
		t.Fatalf("contacts.delete error = %#v", apiErr)
	}
	if got := harness.store.upserts(); len(got) != 1 || got[0].Remark != "first" {
		t.Fatalf("remark precedence upsert = %#v", got)
	}
}

func TestDeleteWithoutLeavePortStillPersists(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "accepted",
	}
	harness := newSaveHarness([]dirextalkdomain.ContactRecord{existing})
	result, apiErr := harness.module.Handlers()[actionDelete](context.Background(), map[string]any{"room_id": existing.RoomID})
	if apiErr != nil {
		t.Fatalf("contacts.delete error = %#v", apiErr)
	}
	if response, ok := result.(map[string]any); !ok || response["status"] != "ok" {
		t.Fatalf("contacts.delete result = %T %#v", result, result)
	}
	if got := harness.store.upserts(); len(got) != 1 || got[0].Status != "deleted" {
		t.Fatalf("upserted contacts = %#v", got)
	}
}

func TestDeleteRereadsStatusUnderPeerLockBeforeLeavingRoom(t *testing.T) {
	active := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "accepted",
	}
	deleted := active
	deleted.Status = "deleted"
	store := &scriptedUpdateStore{results: []updateListResult{
		{records: []dirextalkdomain.ContactRecord{active}},
		{records: []dirextalkdomain.ContactRecord{deleted}},
	}}
	log := &operationLog{}
	conversation := &saveConversationPort{
		log: log, deleteErrs: make(map[string]error), operation: map[string]any{"action": actionDelete},
	}
	leaves := make(chan string, 1)
	module := New(store, conversation, Config{
		LeaveRoom: func(_ context.Context, roomID string) *actionbase.Error {
			leaves <- roomID
			return nil
		},
	})

	lockHeld := make(chan struct{})
	releaseLock := make(chan struct{})
	lockDone := make(chan struct{})
	released := false
	go func() {
		module.SerializePeer(active.PeerMXID, func() {
			close(lockHeld)
			<-releaseLock
		})
		close(lockDone)
	}()
	<-lockHeld
	defer func() {
		if !released {
			close(releaseLock)
			<-lockDone
		}
	}()

	type deleteResult struct {
		result any
		err    *actionbase.Error
	}
	resultCh := make(chan deleteResult, 1)
	go func() {
		result, apiErr := module.Handlers()[actionDelete](context.Background(), map[string]any{"room_id": active.RoomID})
		resultCh <- deleteResult{result: result, err: apiErr}
	}()

	waitForPeerMutationRefCount(t, module, active.PeerMXID, 2)
	listCalls, upserts := store.snapshot()
	if listCalls != 1 || len(upserts) != 0 {
		t.Fatalf("while peer lock held: ListContacts=%d UpsertContact=%#v", listCalls, upserts)
	}
	select {
	case roomID := <-leaves:
		t.Fatalf("left %q before locked reread", roomID)
	default:
	}

	close(releaseLock)
	released = true
	<-lockDone
	got := <-resultCh
	if got.err != nil {
		t.Fatalf("contacts.delete error = %#v", got.err)
	}
	if response, ok := got.result.(map[string]any); !ok || response["status"] != "ok" {
		t.Fatalf("contacts.delete result = %T %#v", got.result, got.result)
	}
	select {
	case roomID := <-leaves:
		t.Fatalf("left %q after locked reread observed deleted status", roomID)
	default:
	}
}

func TestDeleteMapsLeaveSaveAndOperationErrors(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "accepted",
	}
	wantErr := errors.New("delete failed")
	tests := []struct {
		name          string
		configure     func(*deleteHarness)
		wantStatus    int
		wantError     string
		wantLeave     bool
		wantCommitted bool
		wantOperation bool
	}{
		{
			name: "read", configure: func(h *deleteHarness) { h.store.listErr = wantErr },
			wantStatus: http.StatusInternalServerError, wantError: "internal error: delete failed",
		},
		{
			name: "leave", configure: func(h *deleteHarness) {
				h.leaveErr = actionbase.CodedError(http.StatusForbidden, "leave_denied", "leave denied")
			},
			wantStatus: http.StatusForbidden, wantError: "leave denied", wantLeave: true,
		},
		{
			name: "save", configure: func(h *deleteHarness) { h.store.upsertErr = wantErr },
			wantStatus: http.StatusInternalServerError, wantError: "internal error: delete failed", wantLeave: true,
		},
		{
			name: "save after contact commit", configure: func(h *deleteHarness) { h.conversation.saveErr = wantErr },
			wantStatus: http.StatusInternalServerError, wantError: "internal error: delete failed", wantLeave: true, wantCommitted: true,
		},
		{
			name: "operation", configure: func(h *deleteHarness) { h.conversation.operationErr = wantErr },
			wantStatus: http.StatusInternalServerError, wantError: "internal error: delete failed", wantLeave: true, wantCommitted: true, wantOperation: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newDeleteHarness([]dirextalkdomain.ContactRecord{existing})
			tt.configure(harness)
			result, apiErr := harness.module.Handlers()[actionDelete](context.Background(), map[string]any{"room_id": existing.RoomID})
			if result != nil || apiErr == nil || apiErr.Status != tt.wantStatus || apiErr.Error != tt.wantError {
				t.Fatalf("contacts.delete failure = (%#v, %#v), want status=%d error=%q", result, apiErr, tt.wantStatus, tt.wantError)
			}
			if tt.name == "leave" && apiErr.Code != "leave_denied" {
				t.Fatalf("contacts.delete code = %q, want leave_denied", apiErr.Code)
			}
			if got := len(harness.leaves) > 0; got != tt.wantLeave {
				t.Fatalf("LeaveRoom called = %t, want %t", got, tt.wantLeave)
			}
			if got := len(harness.store.upserts()) == 1; got != tt.wantCommitted {
				t.Fatalf("successful contact commit = %t, want %t", got, tt.wantCommitted)
			}
			operationCalled := false
			for _, call := range harness.log.snapshot() {
				if call == "operation:contacts.delete:deleted:!direct:example.com" {
					operationCalled = true
				}
			}
			if operationCalled != tt.wantOperation {
				t.Fatalf("Operation called = %t, want %t", operationCalled, tt.wantOperation)
			}
		})
	}
}
