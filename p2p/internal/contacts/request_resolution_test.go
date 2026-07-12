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

func TestRequestDeleteAcceptedIsNoOpWithOperation(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", DisplayName: "Alice", Status: "accepted",
	}
	harness := newSaveHarness([]dirextalkdomain.ContactRecord{existing})
	operation := map[string]any{"action": actionRequestDelete, "status": "accepted", "room_id": existing.RoomID}
	conversation := &dirextalkdomain.ConversationView{ConversationID: "conv", MatrixRoomID: existing.RoomID}
	harness.conversation.operation = operation
	harness.conversation.operationView = conversation

	result, apiErr := harness.module.Handlers()[actionRequestDelete](context.Background(), map[string]any{
		"room_id": existing.RoomID, "peer_mxid": "@spoofed:example.com",
	})
	if apiErr != nil {
		t.Fatalf("contacts.requests.delete error = %#v", apiErr)
	}
	response, ok := result.(map[string]any)
	if !ok || response["status"] != "ok" || !reflect.DeepEqual(response["operation"], operation) {
		t.Fatalf("contacts.requests.delete result = %#v", result)
	}
	if got, ok := response["conversation"].(dirextalkdomain.ConversationView); !ok || got.ConversationID != conversation.ConversationID {
		t.Fatalf("conversation result = %T %#v", response["conversation"], response["conversation"])
	}
	if upserts := harness.store.upserts(); len(upserts) != 0 {
		t.Fatalf("accepted request delete wrote contact: %#v", upserts)
	}
	if got := harness.log.snapshot(); !reflect.DeepEqual(got, []string{
		"list", "list", "operation:contacts.requests.delete:accepted:!direct:example.com",
	}) {
		t.Fatalf("accepted request delete calls = %v", got)
	}
}

func TestRequestDeletePersistsDeletedSnapshot(t *testing.T) {
	tests := []struct {
		name     string
		existing []dirextalkdomain.ContactRecord
		params   map[string]any
		want     dirextalkdomain.ContactRecord
	}{
		{
			name: "pending contact fills missing profile fields",
			existing: []dirextalkdomain.ContactRecord{{
				PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "pending_inbound", Remark: "original",
			}},
			params: map[string]any{
				"room_id": "!direct:example.com", "display_name": " Alice ", "avatar_url": " mxc://example.com/alice ", "domain": " example.com ",
			},
			want: dirextalkdomain.ContactRecord{
				PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", DisplayName: "Alice",
				AvatarURL: "mxc://example.com/alice", Domain: "example.com", Status: "deleted", Remark: "original",
			},
		},
		{
			name: "missing contact preserves parameter aliases",
			params: map[string]any{
				"room_id": "!missing:example.com", "mxid": " @bob:example.com ", "display_name": " Bob ",
				"avatar_url": " mxc://example.com/bob ", "domain": " example.com ", "request_message": " hello ",
			},
			want: dirextalkdomain.ContactRecord{
				PeerMXID: "@bob:example.com", RoomID: "!missing:example.com", DisplayName: "Bob",
				AvatarURL: "mxc://example.com/bob", Domain: "example.com", Status: "deleted", Remark: "hello",
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
			harness := newSaveHarness(tt.existing)
			harness.conversation.operation = map[string]any{"action": actionRequestDelete}
			result, apiErr := harness.module.Handlers()[actionRequestDelete](context.Background(), tt.params)
			if apiErr != nil {
				t.Fatalf("contacts.requests.delete error = %#v", apiErr)
			}
			response, ok := result.(map[string]any)
			if !ok || response["status"] != "ok" {
				t.Fatalf("contacts.requests.delete result = %#v", result)
			}
			if got := harness.store.upserts(); !reflect.DeepEqual(got, []dirextalkdomain.ContactRecord{tt.want}) {
				t.Fatalf("upserted contacts = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestRequestDeletePreservesRemarkAliasesAndPrecedence(t *testing.T) {
	for _, key := range []string{"remark", "request_message", "message", "reason"} {
		t.Run(key, func(t *testing.T) {
			harness := newSaveHarness(nil)
			_, apiErr := harness.module.Handlers()[actionRequestDelete](context.Background(), map[string]any{
				"room_id": "!missing:example.com", "peer_mxid": "@alice:example.com", key: " hello ",
			})
			if apiErr != nil {
				t.Fatalf("contacts.requests.delete error = %#v", apiErr)
			}
			if got := harness.store.upserts(); len(got) != 1 || got[0].Remark != "hello" {
				t.Fatalf("%s remark upsert = %#v", key, got)
			}
		})
	}

	harness := newSaveHarness(nil)
	_, apiErr := harness.module.Handlers()[actionRequestDelete](context.Background(), map[string]any{
		"room_id": "!missing:example.com", "peer_mxid": "@alice:example.com",
		"remark": "first", "request_message": "second", "message": "third", "reason": "fourth",
	})
	if apiErr != nil {
		t.Fatalf("contacts.requests.delete error = %#v", apiErr)
	}
	if got := harness.store.upserts(); len(got) != 1 || got[0].Remark != "first" {
		t.Fatalf("remark precedence upsert = %#v", got)
	}
}

func TestRequestDeleteRereadsAndWritesOnlyAfterPeerLockAcquisition(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "pending_inbound",
	}
	store := &scriptedUpdateStore{results: []updateListResult{{records: []dirextalkdomain.ContactRecord{existing}}}}
	log := &operationLog{}
	conversation := &saveConversationPort{log: log, deleteErrs: make(map[string]error), operation: map[string]any{"action": actionRequestDelete}}
	module := New(store, conversation, Config{})

	lockHeld := make(chan struct{})
	releaseLock := make(chan struct{})
	lockDone := make(chan struct{})
	released := false
	go func() {
		module.SerializePeer(existing.PeerMXID, func() {
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
		result, apiErr := module.Handlers()[actionRequestDelete](context.Background(), map[string]any{"room_id": existing.RoomID})
		resultCh <- deleteResult{result: result, err: apiErr}
	}()

	waitForPeerMutationRefCount(t, module, existing.PeerMXID, 2)
	listCalls, upserts := store.snapshot()
	if listCalls != 1 || len(upserts) != 0 || len(log.snapshot()) != 0 {
		t.Fatalf("while peer lock held: ListContacts=%d UpsertContact=%#v conversation=%v", listCalls, upserts, log.snapshot())
	}

	close(releaseLock)
	released = true
	<-lockDone
	got := <-resultCh
	if got.err != nil {
		t.Fatalf("contacts.requests.delete error = %#v", got.err)
	}
	response, ok := got.result.(map[string]any)
	if !ok || response["status"] != "ok" {
		t.Fatalf("contacts.requests.delete result = %#v", got.result)
	}
	listCalls, upserts = store.snapshot()
	if listCalls != 3 || len(upserts) != 1 || upserts[0].Status != "deleted" {
		t.Fatalf("after peer lock release: ListContacts=%d UpsertContact=%#v", listCalls, upserts)
	}
}

func TestRequestDeleteMapsReadSaveAndOperationErrors(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "pending_inbound",
	}
	wantErr := errors.New("request delete failed")
	tests := []struct {
		name              string
		configure         func(*saveHarness)
		wantCommitted     bool
		wantOperationCall bool
	}{
		{name: "read", configure: func(h *saveHarness) { h.store.listErr = wantErr }},
		{name: "save", configure: func(h *saveHarness) { h.store.upsertErr = wantErr }},
		{name: "operation", configure: func(h *saveHarness) { h.conversation.operationErr = wantErr }, wantCommitted: true, wantOperationCall: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newSaveHarness([]dirextalkdomain.ContactRecord{existing})
			tt.configure(harness)
			result, apiErr := harness.module.Handlers()[actionRequestDelete](context.Background(), map[string]any{"room_id": existing.RoomID})
			if result != nil || apiErr == nil || apiErr.Status != http.StatusInternalServerError || apiErr.Error != "internal error: request delete failed" {
				t.Fatalf("contacts.requests.delete failure = (%#v, %#v)", result, apiErr)
			}
			if committed := len(harness.store.upserts()) == 1; committed != tt.wantCommitted {
				t.Fatalf("successful contact commit = %t, want %t", committed, tt.wantCommitted)
			}
			operationCalled := false
			for _, call := range harness.log.snapshot() {
				if call == "operation:contacts.requests.delete:deleted:!direct:example.com" {
					operationCalled = true
				}
			}
			if operationCalled != tt.wantOperationCall {
				t.Fatalf("Operation called = %t, want %t", operationCalled, tt.wantOperationCall)
			}
		})
	}
}
