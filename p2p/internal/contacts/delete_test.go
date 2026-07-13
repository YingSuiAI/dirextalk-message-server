package contacts

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"sync"
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

type updateListResult struct {
	records []dirextalkdomain.ContactRecord
	err     error
}

type scriptedUpdateStore struct {
	mu        sync.Mutex
	results   []updateListResult
	listCalls int
	upserts   []dirextalkdomain.ContactRecord
}

func (s *scriptedUpdateStore) ListContacts(context.Context) ([]dirextalkdomain.ContactRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	index := s.listCalls
	s.listCalls++
	if len(s.results) == 0 {
		return nil, nil
	}
	if index >= len(s.results) {
		index = len(s.results) - 1
	}
	result := s.results[index]
	return append([]dirextalkdomain.ContactRecord(nil), result.records...), result.err
}

func (s *scriptedUpdateStore) UpsertContact(_ context.Context, contact dirextalkdomain.ContactRecord) error {
	s.mu.Lock()
	s.upserts = append(s.upserts, contact)
	s.mu.Unlock()
	return nil
}

func (s *scriptedUpdateStore) snapshot() (int, []dirextalkdomain.ContactRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listCalls, append([]dirextalkdomain.ContactRecord(nil), s.upserts...)
}

func TestUpdateHandlerValidatesAndUsesLockedReread(t *testing.T) {
	accepted := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", DisplayName: "Alice", Status: "accepted",
	}
	wantReadErr := errors.New("locked reread failed")
	tests := []struct {
		name       string
		params     map[string]any
		results    []updateListResult
		wantStatus int
		wantError  string
	}{
		{name: "room required", params: map[string]any{"display_name": "Alice"}, wantStatus: http.StatusBadRequest, wantError: "room_id is required"},
		{name: "display name required", params: map[string]any{"room_id": accepted.RoomID, "display_name": "  "}, wantStatus: http.StatusBadRequest, wantError: "display_name is required"},
		{name: "initial read error", params: map[string]any{"room_id": accepted.RoomID, "display_name": "Alice"}, results: []updateListResult{{err: wantReadErr}}, wantStatus: http.StatusInternalServerError, wantError: "internal error: locked reread failed"},
		{name: "initial not found", params: map[string]any{"room_id": accepted.RoomID, "display_name": "Alice"}, results: []updateListResult{{}}, wantStatus: http.StatusNotFound, wantError: "contact not found"},
		{name: "locked read error", params: map[string]any{"room_id": accepted.RoomID, "display_name": "Alice"}, results: []updateListResult{{records: []dirextalkdomain.ContactRecord{accepted}}, {err: wantReadErr}}, wantStatus: http.StatusInternalServerError, wantError: "internal error: locked reread failed"},
		{name: "locked not found", params: map[string]any{"room_id": accepted.RoomID, "display_name": "Alice"}, results: []updateListResult{{records: []dirextalkdomain.ContactRecord{accepted}}, {}}, wantStatus: http.StatusNotFound, wantError: "contact not found"},
		{name: "locked status changed", params: map[string]any{"room_id": accepted.RoomID, "display_name": "Alice"}, results: []updateListResult{{records: []dirextalkdomain.ContactRecord{accepted}}, {records: []dirextalkdomain.ContactRecord{{PeerMXID: accepted.PeerMXID, RoomID: accepted.RoomID, Status: "pending_inbound"}}}}, wantStatus: http.StatusForbidden, wantError: "contact is not accepted"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &scriptedUpdateStore{results: tt.results}
			result, apiErr := New(store, nil, Config{}).Handlers()[actionUpdate](context.Background(), tt.params)
			if result != nil || apiErr == nil || apiErr.Status != tt.wantStatus || apiErr.Error != tt.wantError {
				t.Fatalf("contacts.update = (%#v, %#v), want status=%d error=%q", result, apiErr, tt.wantStatus, tt.wantError)
			}
			if len(store.upserts) != 0 {
				t.Fatalf("failed update wrote contacts: %#v", store.upserts)
			}
		})
	}
}

func TestUpdateHandlerPersistsSnapshotAndAttachesConversation(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", DisplayName: "Alice",
		AvatarURL: "mxc://example.com/old", Domain: "old.example", Status: "accepted", Remark: "friend",
	}
	tests := []struct {
		name       string
		params     map[string]any
		wantAvatar string
		wantDomain string
	}{
		{name: "empty optional fields preserve snapshot", params: map[string]any{"room_id": existing.RoomID, "display_name": "  Alice Local  ", "avatar_url": " ", "domain": " "}, wantAvatar: existing.AvatarURL, wantDomain: existing.Domain},
		{name: "nonempty optional fields replace snapshot", params: map[string]any{"room_id": existing.RoomID, "display_name": " Alice Local ", "avatar_url": " mxc://example.com/new ", "domain": " new.example "}, wantAvatar: "mxc://example.com/new", wantDomain: "new.example"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newSaveHarness([]dirextalkdomain.ContactRecord{existing})
			operation := map[string]any{"action": actionUpdate, "status": "accepted", "room_id": existing.RoomID}
			conversation := &dirextalkdomain.ConversationView{ConversationID: "conv", MatrixRoomID: existing.RoomID}
			harness.conversation.operation = operation
			harness.conversation.operationView = conversation

			result, apiErr := harness.module.Handlers()[actionUpdate](context.Background(), tt.params)
			if apiErr != nil {
				t.Fatalf("contacts.update error = %#v", apiErr)
			}
			view, ok := result.(View)
			if !ok {
				t.Fatalf("contacts.update result = %T %#v", result, result)
			}
			want := existing
			want.DisplayName = "Alice Local"
			want.DisplayNameOverride = true
			want.AvatarURL = tt.wantAvatar
			want.Domain = tt.wantDomain
			if got := RecordFromView(view); got != want {
				t.Fatalf("updated record = %#v, want %#v", got, want)
			}
			if !reflect.DeepEqual(view.Operation, operation) || view.Conversation != conversation {
				t.Fatalf("presentation = operation %#v conversation %#v", view.Operation, view.Conversation)
			}
			upserts := harness.store.upserts()
			if !reflect.DeepEqual(upserts, []dirextalkdomain.ContactRecord{want}) {
				t.Fatalf("upserted contacts = %#v, want %#v", upserts, want)
			}
		})
	}
}

func TestUpdateHandlerRereadsAndWritesOnlyAfterPeerLockAcquisition(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", DisplayName: "Alice", Status: "accepted",
	}
	store := &scriptedUpdateStore{results: []updateListResult{{records: []dirextalkdomain.ContactRecord{existing}}}}
	log := &operationLog{}
	conversation := &saveConversationPort{
		log:        log,
		deleteErrs: make(map[string]error),
		operation:  map[string]any{"action": actionUpdate},
	}
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

	type updateResult struct {
		result any
		err    *actionbase.Error
	}
	resultCh := make(chan updateResult, 1)
	go func() {
		result, apiErr := module.Handlers()[actionUpdate](context.Background(), map[string]any{
			"room_id": existing.RoomID, "display_name": "Alice Local",
		})
		resultCh <- updateResult{result: result, err: apiErr}
	}()

	waitForPeerMutationRefCount(t, module, existing.PeerMXID, 2)
	listCalls, upserts := store.snapshot()
	if listCalls != 1 || len(upserts) != 0 {
		t.Fatalf("while peer lock held: ListContacts=%d UpsertContact=%#v, want one key lookup and no write", listCalls, upserts)
	}
	if calls := log.snapshot(); len(calls) != 0 {
		t.Fatalf("while peer lock held: conversation calls=%v, want none", calls)
	}

	close(releaseLock)
	released = true
	<-lockDone
	got := <-resultCh
	if got.err != nil {
		t.Fatalf("contacts.update error = %#v", got.err)
	}
	if _, ok := got.result.(View); !ok {
		t.Fatalf("contacts.update result = %T %#v", got.result, got.result)
	}
	listCalls, upserts = store.snapshot()
	if listCalls != 3 || len(upserts) != 1 || upserts[0].DisplayName != "Alice Local" {
		t.Fatalf("after peer lock release: ListContacts=%d UpsertContact=%#v", listCalls, upserts)
	}
}
