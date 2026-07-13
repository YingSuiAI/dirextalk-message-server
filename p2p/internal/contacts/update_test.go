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
