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

type acceptCall struct {
	contact     dirextalkdomain.ContactRecord
	serverNames []string
}

type acceptHarness struct {
	*saveHarness
	finalRoomID string
	acceptErr   *actionbase.Error
	acceptCalls []acceptCall
}

func newAcceptHarness(records []dirextalkdomain.ContactRecord) *acceptHarness {
	harness := &acceptHarness{saveHarness: newSaveHarness(records)}
	harness.module = New(harness.store, harness.conversation, Config{
		DeleteGroup: func(_ context.Context, roomID string) error {
			harness.log.add("delete-group:" + roomID)
			return harness.deleteGroupErr
		},
		AcceptDirectRoom: func(_ context.Context, contact dirextalkdomain.ContactRecord, serverNames []string) (string, *actionbase.Error) {
			harness.log.add("accept-room:" + contact.RoomID)
			harness.acceptCalls = append(harness.acceptCalls, acceptCall{
				contact: contact, serverNames: append([]string(nil), serverNames...),
			})
			return harness.finalRoomID, harness.acceptErr
		},
	})
	return harness
}

func TestAcceptAlreadyAcceptedIsNoOpWithContactView(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", DisplayName: "Alice",
		DisplayNameOverride: true, Status: "accepted", Remark: "friend",
	}
	harness := newAcceptHarness([]dirextalkdomain.ContactRecord{existing})
	operation := map[string]any{"action": actionRequestAccept, "status": "accepted", "room_id": existing.RoomID}
	conversation := &dirextalkdomain.ConversationView{ConversationID: "conv", MatrixRoomID: existing.RoomID}
	harness.conversation.operation = operation
	harness.conversation.operationView = conversation

	result, apiErr := harness.module.Handlers()[actionRequestAccept](context.Background(), map[string]any{
		"room_id": existing.RoomID, "peer_mxid": "@spoofed:example.com", "display_name": "Spoofed",
	})
	if apiErr != nil {
		t.Fatalf("contacts.requests.accept error = %#v", apiErr)
	}
	view, ok := result.(View)
	if !ok || RecordFromView(view) != existing || !reflect.DeepEqual(view.Operation, operation) || view.Conversation != conversation {
		t.Fatalf("accepted no-op result = %T %#v", result, result)
	}
	if len(harness.acceptCalls) != 0 || len(harness.store.upserts()) != 0 {
		t.Fatalf("accepted no-op calls = accept %#v upserts %#v", harness.acceptCalls, harness.store.upserts())
	}
}

func TestAcceptPersistsCompatibleSnapshotAndAdapterResult(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!old:example.com", DisplayName: "Alice",
		DisplayNameOverride: true, AvatarURL: "mxc://example.com/old", Domain: "old.example",
		Status: "pending_inbound", Remark: "please accept",
	}
	harness := newAcceptHarness([]dirextalkdomain.ContactRecord{existing})
	harness.finalRoomID = "!replacement:example.com"
	operation := map[string]any{"action": actionRequestAccept, "status": "accepted", "room_id": harness.finalRoomID}
	harness.conversation.operation = operation

	result, apiErr := harness.module.Handlers()[actionRequestAccept](context.Background(), map[string]any{
		"room_id": existing.RoomID, "peer_mxid": " @spoofed:example.com ", "display_name": "Wrong",
		"avatar_url": " mxc://example.com/new ", "domain": " new.example ",
		"server_names": []any{" remote.example ", "backup.example", "remote.example", ""},
	})
	if apiErr != nil {
		t.Fatalf("contacts.requests.accept error = %#v", apiErr)
	}
	want := dirextalkdomain.ContactRecord{
		PeerMXID: "@spoofed:example.com", RoomID: harness.finalRoomID, DisplayName: existing.DisplayName,
		AvatarURL: "mxc://example.com/new", Domain: "new.example", Status: "accepted",
	}
	view, ok := result.(View)
	if !ok || RecordFromView(view) != want || !reflect.DeepEqual(view.Operation, operation) {
		t.Fatalf("accepted result = %T %#v, want %#v", result, result, want)
	}
	if got := harness.store.upserts(); !reflect.DeepEqual(got, []dirextalkdomain.ContactRecord{want}) {
		t.Fatalf("upserted contacts = %#v, want %#v", got, want)
	}
	if len(harness.acceptCalls) != 1 || harness.acceptCalls[0].contact != existing ||
		!reflect.DeepEqual(harness.acceptCalls[0].serverNames, []string{"remote.example", "backup.example"}) {
		t.Fatalf("accept adapter calls = %#v", harness.acceptCalls)
	}
	if view.DisplayNameOverride {
		t.Fatal("legacy accept must reset display_name_override on the rebuilt snapshot")
	}
	calls := harness.log.snapshot()
	acceptIndex, upsertIndex, operationIndex := -1, -1, -1
	for index, call := range calls {
		switch call {
		case "accept-room:" + existing.RoomID:
			acceptIndex = index
		case "upsert:" + harness.finalRoomID:
			upsertIndex = index
		case "operation:contacts.requests.accept:accepted:" + harness.finalRoomID:
			operationIndex = index
		}
	}
	if acceptIndex < 0 || upsertIndex < 0 || operationIndex < 0 || acceptIndex >= upsertIndex || upsertIndex >= operationIndex {
		t.Fatalf("accept/save/operation order = %v", calls)
	}
}

func TestAcceptUsesExplicitEmptyFinalRoomFromAdapter(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!old:example.com", Status: "pending_inbound",
	}
	harness := newAcceptHarness([]dirextalkdomain.ContactRecord{existing})
	result, apiErr := harness.module.Handlers()[actionRequestAccept](context.Background(), map[string]any{"room_id": existing.RoomID})
	if apiErr != nil {
		t.Fatalf("contacts.requests.accept error = %#v", apiErr)
	}
	view, ok := result.(View)
	if !ok || view.RoomID != "" || view.Status != "accepted" {
		t.Fatalf("accepted empty final room = %T %#v", result, result)
	}
	if got := harness.store.upserts(); len(got) != 1 || got[0].RoomID != "" {
		t.Fatalf("upserted contacts = %#v, want explicit empty room", got)
	}
}

func TestAcceptWithoutAdapterPreservesFallbacksAndLegacyStatuses(t *testing.T) {
	tests := []struct {
		name     string
		existing []dirextalkdomain.ContactRecord
		params   map[string]any
		want     dirextalkdomain.ContactRecord
	}{
		{
			name: "rejected contact can be accepted without transport",
			existing: []dirextalkdomain.ContactRecord{{
				PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", DisplayName: "Alice",
				AvatarURL: "mxc://example.com/alice", Domain: "example.com", Status: "rejected", Remark: "old",
			}},
			params: map[string]any{"room_id": "!direct:example.com"},
			want: dirextalkdomain.ContactRecord{
				PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", DisplayName: "Alice",
				AvatarURL: "mxc://example.com/alice", Domain: "example.com", Status: "accepted",
			},
		},
		{
			name:   "empty room and identity remain compatible",
			params: map[string]any{},
			want:   dirextalkdomain.ContactRecord{Status: "accepted"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newSaveHarness(tt.existing)
			result, apiErr := harness.module.Handlers()[actionRequestAccept](context.Background(), tt.params)
			if apiErr != nil {
				t.Fatalf("contacts.requests.accept error = %#v", apiErr)
			}
			view, ok := result.(View)
			if !ok || RecordFromView(view) != tt.want {
				t.Fatalf("accepted result = %T %#v, want %#v", result, result, tt.want)
			}
			if got := harness.store.upserts(); !reflect.DeepEqual(got, []dirextalkdomain.ContactRecord{tt.want}) {
				t.Fatalf("upserted contacts = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestAcceptRereadsStatusUnderPeerLockBeforeJoining(t *testing.T) {
	pending := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "pending_inbound",
	}
	accepted := pending
	accepted.Status = "accepted"
	store := &scriptedUpdateStore{results: []updateListResult{
		{records: []dirextalkdomain.ContactRecord{pending}},
		{records: []dirextalkdomain.ContactRecord{accepted}},
	}}
	log := &operationLog{}
	conversation := &saveConversationPort{
		log: log, deleteErrs: make(map[string]error), operation: map[string]any{"action": actionRequestAccept},
	}
	acceptCalls := make(chan struct{}, 1)
	module := New(store, conversation, Config{
		AcceptDirectRoom: func(context.Context, dirextalkdomain.ContactRecord, []string) (string, *actionbase.Error) {
			acceptCalls <- struct{}{}
			return pending.RoomID, nil
		},
	})

	lockHeld := make(chan struct{})
	releaseLock := make(chan struct{})
	lockDone := make(chan struct{})
	released := false
	go func() {
		module.SerializePeer(pending.PeerMXID, func() {
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

	type acceptResult struct {
		result any
		err    *actionbase.Error
	}
	resultCh := make(chan acceptResult, 1)
	go func() {
		result, apiErr := module.Handlers()[actionRequestAccept](context.Background(), map[string]any{"room_id": pending.RoomID})
		resultCh <- acceptResult{result: result, err: apiErr}
	}()

	waitForPeerMutationRefCount(t, module, pending.PeerMXID, 2)
	listCalls, upserts := store.snapshot()
	if listCalls != 1 || len(upserts) != 0 {
		t.Fatalf("while peer lock held: ListContacts=%d UpsertContact=%#v", listCalls, upserts)
	}
	select {
	case <-acceptCalls:
		t.Fatal("accept adapter called before locked reread")
	default:
	}

	close(releaseLock)
	released = true
	<-lockDone
	got := <-resultCh
	if got.err != nil {
		t.Fatalf("contacts.requests.accept error = %#v", got.err)
	}
	view, ok := got.result.(View)
	if !ok || RecordFromView(view) != accepted {
		t.Fatalf("accepted result = %T %#v", got.result, got.result)
	}
	select {
	case <-acceptCalls:
		t.Fatal("accept adapter called after locked reread observed accepted status")
	default:
	}
	_, upserts = store.snapshot()
	if len(upserts) != 0 {
		t.Fatalf("accepted no-op upserts = %#v", upserts)
	}
}

func TestAcceptMapsLockedRereadFailureWithoutJoining(t *testing.T) {
	pending := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "pending_inbound",
	}
	wantErr := errors.New("locked read failed")
	tests := []struct {
		name       string
		lockedRead updateListResult
		wantStatus int
		wantError  string
	}{
		{name: "error", lockedRead: updateListResult{err: wantErr}, wantStatus: http.StatusInternalServerError, wantError: "internal error: locked read failed"},
		{name: "not found", lockedRead: updateListResult{}, wantStatus: http.StatusNotFound, wantError: "contact request not found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &scriptedUpdateStore{results: []updateListResult{{records: []dirextalkdomain.ContactRecord{pending}}, tt.lockedRead}}
			log := &operationLog{}
			conversation := &saveConversationPort{log: log, deleteErrs: make(map[string]error)}
			acceptCalls := 0
			module := New(store, conversation, Config{
				AcceptDirectRoom: func(context.Context, dirextalkdomain.ContactRecord, []string) (string, *actionbase.Error) {
					acceptCalls++
					return pending.RoomID, nil
				},
			})

			result, apiErr := module.Handlers()[actionRequestAccept](context.Background(), map[string]any{"room_id": pending.RoomID})
			if result != nil || apiErr == nil || apiErr.Status != tt.wantStatus || apiErr.Error != tt.wantError {
				t.Fatalf("contacts.requests.accept locked failure = (%#v, %#v)", result, apiErr)
			}
			_, upserts := store.snapshot()
			if acceptCalls != 0 || len(upserts) != 0 || len(log.snapshot()) != 0 {
				t.Fatalf("failed locked read calls = accept %d upserts %#v conversation %v", acceptCalls, upserts, log.snapshot())
			}
		})
	}
}
