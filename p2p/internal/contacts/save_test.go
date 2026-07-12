package contacts

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

type operationLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *operationLog) add(call string) {
	l.mu.Lock()
	l.calls = append(l.calls, call)
	l.mu.Unlock()
}

func (l *operationLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.calls...)
}

type saveStore struct {
	mu sync.Mutex

	log       *operationLog
	records   []dirextalkdomain.ContactRecord
	upserted  []dirextalkdomain.ContactRecord
	listErr   error
	upsertErr error
}

func (s *saveStore) ListContacts(context.Context) ([]dirextalkdomain.ContactRecord, error) {
	s.log.add("list")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listErr != nil {
		return nil, s.listErr
	}
	return append([]dirextalkdomain.ContactRecord(nil), s.records...), nil
}

func (s *saveStore) UpsertContact(_ context.Context, contact dirextalkdomain.ContactRecord) error {
	s.log.add("upsert:" + contact.RoomID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.upsertErr != nil {
		return s.upsertErr
	}
	s.upserted = append(s.upserted, contact)
	if contact.PeerMXID != "" {
		kept := s.records[:0]
		for _, existing := range s.records {
			if existing.PeerMXID == contact.PeerMXID && existing.RoomID != contact.RoomID {
				continue
			}
			kept = append(kept, existing)
		}
		s.records = kept
	}
	for index := range s.records {
		if s.records[index].RoomID == contact.RoomID {
			s.records[index] = contact
			return nil
		}
	}
	s.records = append(s.records, contact)
	return nil
}

func (s *saveStore) upserts() []dirextalkdomain.ContactRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]dirextalkdomain.ContactRecord(nil), s.upserted...)
}

type saveConversationPort struct {
	mu sync.Mutex

	log           *operationLog
	deleteErrs    map[string]error
	listErr       error
	saveErr       error
	operation     map[string]any
	operationView *dirextalkdomain.ConversationView
	operationErr  error
	saved         []dirextalkdomain.ConversationRecord
	records       []dirextalkdomain.ConversationRecord
}

func conversationDeleteKey(roomID string, kind dirextalkdomain.ConversationKind) string {
	return string(kind) + "|" + roomID
}

func (p *saveConversationPort) ListRecords(context.Context) ([]dirextalkdomain.ConversationRecord, error) {
	p.log.add("list-conversations")
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.listErr != nil {
		return nil, p.listErr
	}
	return append([]dirextalkdomain.ConversationRecord(nil), p.records...), nil
}

func (p *saveConversationPort) Operation(_ context.Context, action, status, roomID string) (map[string]any, *dirextalkdomain.ConversationView, error) {
	p.log.add(fmt.Sprintf("operation:%s:%s:%s", action, status, roomID))
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.operation, p.operationView, p.operationErr
}

func (p *saveConversationPort) DeleteKindByRoom(_ context.Context, roomID string, kind dirextalkdomain.ConversationKind) error {
	p.log.add(fmt.Sprintf("delete-conversation:%s:%s", kind, roomID))
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.deleteErrs[conversationDeleteKey(roomID, kind)]; err != nil {
		return err
	}
	kept := p.records[:0]
	for _, record := range p.records {
		if record.MatrixRoomID == roomID && record.Kind == kind {
			continue
		}
		kept = append(kept, record)
	}
	p.records = kept
	return nil
}

func (p *saveConversationPort) Save(_ context.Context, record dirextalkdomain.ConversationRecord) error {
	p.log.add(fmt.Sprintf("save-conversation:%s:%s", record.Kind, record.MatrixRoomID))
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.saveErr != nil {
		return p.saveErr
	}
	p.saved = append(p.saved, record)
	for index := range p.records {
		if p.records[index].MatrixRoomID == record.MatrixRoomID && p.records[index].Kind == record.Kind {
			p.records[index] = record
			return nil
		}
	}
	p.records = append(p.records, record)
	return nil
}

func (p *saveConversationPort) savedRecords() []dirextalkdomain.ConversationRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]dirextalkdomain.ConversationRecord(nil), p.saved...)
}

func (p *saveConversationPort) recordsSnapshot() []dirextalkdomain.ConversationRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]dirextalkdomain.ConversationRecord(nil), p.records...)
}

type saveHarness struct {
	module         *Module
	store          *saveStore
	conversation   *saveConversationPort
	log            *operationLog
	deleteGroupErr error
}

func newSaveHarness(records []dirextalkdomain.ContactRecord) *saveHarness {
	harness := &saveHarness{log: &operationLog{}}
	harness.store = &saveStore{log: harness.log, records: append([]dirextalkdomain.ContactRecord(nil), records...)}
	harness.conversation = &saveConversationPort{log: harness.log, deleteErrs: make(map[string]error)}
	harness.module = harness.newModule()
	return harness
}

func (harness *saveHarness) newModule() *Module {
	return New(harness.store, harness.conversation, Config{
		DeleteGroup: func(_ context.Context, roomID string) error {
			harness.log.add("delete-group:" + roomID)
			return harness.deleteGroupErr
		},
	})
}

func TestSaveReconcilesRestartedPeerStateInOrder(t *testing.T) {
	contact := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!new:example.com", DisplayName: "Alice", Status: "accepted",
	}
	harness := newSaveHarness([]dirextalkdomain.ContactRecord{
		{PeerMXID: contact.PeerMXID, RoomID: "!old-a:example.com", Status: "pending_outbound"},
		{PeerMXID: "@other:example.com", RoomID: "!other:example.com", Status: "accepted"},
		{PeerMXID: contact.PeerMXID, RoomID: "", Status: "rejected"},
		{PeerMXID: contact.PeerMXID, RoomID: "!old-b:example.com", Status: "accepted"},
		{PeerMXID: contact.PeerMXID, RoomID: contact.RoomID, Status: "pending_inbound"},
	})
	harness.conversation.records = []dirextalkdomain.ConversationRecord{
		{MatrixRoomID: "!old-a:example.com", Kind: dirextalkdomain.ConversationKindDirect, PeerMXID: contact.PeerMXID},
		{MatrixRoomID: "!old-c:example.com", Kind: dirextalkdomain.ConversationKindDirect, PeerMXID: contact.PeerMXID},
		{MatrixRoomID: contact.RoomID, Kind: dirextalkdomain.ConversationKindDirect, PeerMXID: contact.PeerMXID},
		{MatrixRoomID: "!other-direct:example.com", Kind: dirextalkdomain.ConversationKindDirect, PeerMXID: "@other:example.com"},
		{MatrixRoomID: "!old-group:example.com", Kind: dirextalkdomain.ConversationKindGroup, PeerMXID: contact.PeerMXID},
	}

	if err := harness.module.Save(context.Background(), contact); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	wantCalls := []string{
		"list",
		"list-conversations",
		"upsert:!new:example.com",
		"delete-conversation:direct:!old-a:example.com",
		"delete-conversation:direct:!old-b:example.com",
		"delete-conversation:direct:!old-c:example.com",
		"delete-group:!new:example.com",
		"delete-conversation:group:!new:example.com",
		"save-conversation:direct:!new:example.com",
	}
	if got := harness.log.snapshot(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("operation order = %v, want %v", got, wantCalls)
	}
	if got := harness.store.upserts(); !reflect.DeepEqual(got, []dirextalkdomain.ContactRecord{contact}) {
		t.Fatalf("upserted contacts = %#v, want %#v", got, contact)
	}
	wantConversation := dirextalkdomain.ConversationFromContact(contact)
	if got := harness.conversation.savedRecords(); !reflect.DeepEqual(got, []dirextalkdomain.ConversationRecord{wantConversation}) {
		t.Fatalf("saved conversations = %#v, want %#v", got, wantConversation)
	}
}

func TestSaveRetryFromNewModuleRecoversGhostDirectConversationAfterPartialCommit(t *testing.T) {
	wantErr := errors.New("delete old direct conversation")
	oldRoomID := "!old:example.com"
	contact := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!new:example.com", DisplayName: "Alice", Status: "accepted",
	}
	harness := newSaveHarness([]dirextalkdomain.ContactRecord{{
		PeerMXID: contact.PeerMXID, RoomID: oldRoomID, DisplayName: "Alice", Status: "accepted",
	}})
	harness.conversation.records = []dirextalkdomain.ConversationRecord{{
		MatrixRoomID: oldRoomID, Kind: dirextalkdomain.ConversationKindDirect, PeerMXID: contact.PeerMXID,
	}}
	deleteKey := conversationDeleteKey(oldRoomID, dirextalkdomain.ConversationKindDirect)
	harness.conversation.deleteErrs[deleteKey] = wantErr

	if err := harness.module.Save(context.Background(), contact); !errors.Is(err, wantErr) {
		t.Fatalf("first Save() error = %v, want %v", err, wantErr)
	}
	if got := harness.store.upserts(); !reflect.DeepEqual(got, []dirextalkdomain.ContactRecord{contact}) {
		t.Fatalf("first Save did not commit contact Upsert: %#v", got)
	}
	delete(harness.conversation.deleteErrs, deleteKey)
	harness.module = harness.newModule()
	if err := harness.module.Save(context.Background(), contact); err != nil {
		t.Fatalf("retry Save() error = %v", err)
	}

	wantCalls := []string{
		"list", "list-conversations", "upsert:!new:example.com", "delete-conversation:direct:!old:example.com",
		"list", "list-conversations", "upsert:!new:example.com", "delete-conversation:direct:!old:example.com",
		"delete-group:!new:example.com", "delete-conversation:group:!new:example.com", "save-conversation:direct:!new:example.com",
	}
	if got := harness.log.snapshot(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("operation order = %v, want %v", got, wantCalls)
	}
	wantConversations := []dirextalkdomain.ConversationRecord{dirextalkdomain.ConversationFromContact(contact)}
	if got := harness.conversation.recordsSnapshot(); !reflect.DeepEqual(got, wantConversations) {
		t.Fatalf("durable conversations after retry = %#v, want %#v", got, wantConversations)
	}
}

func TestSaveStopsAtFailureAndPreservesEarlierCommits(t *testing.T) {
	wantErr := errors.New("stage failed")
	contact := dirextalkdomain.ContactRecord{PeerMXID: "@alice:example.com", RoomID: "!new:example.com", Status: "accepted"}
	old := dirextalkdomain.ContactRecord{PeerMXID: contact.PeerMXID, RoomID: "!old:example.com", Status: "pending_outbound"}
	tests := []struct {
		name         string
		configure    func(*saveHarness)
		wantCalls    []string
		wantUpserted bool
	}{
		{
			name:      "list",
			configure: func(h *saveHarness) { h.store.listErr = wantErr },
			wantCalls: []string{"list"},
		},
		{
			name:      "conversation list",
			configure: func(h *saveHarness) { h.conversation.listErr = wantErr },
			wantCalls: []string{"list", "list-conversations"},
		},
		{
			name:      "upsert",
			configure: func(h *saveHarness) { h.store.upsertErr = wantErr },
			wantCalls: []string{"list", "list-conversations", "upsert:!new:example.com"},
		},
		{
			name: "old direct conversation deletion",
			configure: func(h *saveHarness) {
				h.conversation.deleteErrs[conversationDeleteKey(old.RoomID, dirextalkdomain.ConversationKindDirect)] = wantErr
			},
			wantCalls:    []string{"list", "list-conversations", "upsert:!new:example.com", "delete-conversation:direct:!old:example.com"},
			wantUpserted: true,
		},
		{
			name:      "group deletion",
			configure: func(h *saveHarness) { h.deleteGroupErr = wantErr },
			wantCalls: []string{
				"list", "list-conversations", "upsert:!new:example.com", "delete-conversation:direct:!old:example.com", "delete-group:!new:example.com",
			},
			wantUpserted: true,
		},
		{
			name: "group conversation deletion",
			configure: func(h *saveHarness) {
				h.conversation.deleteErrs[conversationDeleteKey(contact.RoomID, dirextalkdomain.ConversationKindGroup)] = wantErr
			},
			wantCalls: []string{
				"list", "list-conversations", "upsert:!new:example.com", "delete-conversation:direct:!old:example.com", "delete-group:!new:example.com",
				"delete-conversation:group:!new:example.com",
			},
			wantUpserted: true,
		},
		{
			name:      "conversation save",
			configure: func(h *saveHarness) { h.conversation.saveErr = wantErr },
			wantCalls: []string{
				"list", "list-conversations", "upsert:!new:example.com", "delete-conversation:direct:!old:example.com", "delete-group:!new:example.com",
				"delete-conversation:group:!new:example.com", "save-conversation:direct:!new:example.com",
			},
			wantUpserted: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newSaveHarness([]dirextalkdomain.ContactRecord{old})
			tt.configure(harness)
			if err := harness.module.Save(context.Background(), contact); !errors.Is(err, wantErr) {
				t.Fatalf("Save() error = %v, want %v", err, wantErr)
			}
			if got := harness.log.snapshot(); !reflect.DeepEqual(got, tt.wantCalls) {
				t.Fatalf("operation order = %v, want %v", got, tt.wantCalls)
			}
			if got := len(harness.store.upserts()) > 0; got != tt.wantUpserted {
				t.Fatalf("successful UpsertContact = %t, want %t", got, tt.wantUpserted)
			}
		})
	}
}

func TestSavePreservesEmptyRoomAndPeerCompatibility(t *testing.T) {
	tests := []struct {
		name      string
		contact   dirextalkdomain.ContactRecord
		existing  []dirextalkdomain.ContactRecord
		wantCalls []string
	}{
		{
			name:     "empty peer does not replace another empty-peer room",
			contact:  dirextalkdomain.ContactRecord{RoomID: "!current:example.com", Status: "accepted"},
			existing: []dirextalkdomain.ContactRecord{{RoomID: "!old:example.com", Status: "accepted"}},
			wantCalls: []string{
				"list", "list-conversations", "upsert:!current:example.com", "delete-group:!current:example.com",
				"delete-conversation:group:!current:example.com", "save-conversation:direct:!current:example.com",
			},
		},
		{
			name:     "empty room still replaces old peer room and saves direct conversation",
			contact:  dirextalkdomain.ContactRecord{PeerMXID: "@alice:example.com", Status: "accepted"},
			existing: []dirextalkdomain.ContactRecord{{PeerMXID: "@alice:example.com", RoomID: "!old:example.com", Status: "accepted"}},
			wantCalls: []string{
				"list", "list-conversations", "upsert:", "delete-conversation:direct:!old:example.com", "save-conversation:direct:",
			},
		},
		{
			name:      "empty room and peer still persist both projections",
			contact:   dirextalkdomain.ContactRecord{DisplayName: "legacy", Status: "accepted"},
			wantCalls: []string{"list", "list-conversations", "upsert:", "save-conversation:direct:"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newSaveHarness(tt.existing)
			if err := harness.module.Save(context.Background(), tt.contact); err != nil {
				t.Fatalf("Save() error = %v", err)
			}
			if got := harness.log.snapshot(); !reflect.DeepEqual(got, tt.wantCalls) {
				t.Fatalf("operation order = %v, want %v", got, tt.wantCalls)
			}
		})
	}
}

type blockingSaveStore struct {
	mu sync.Mutex

	listCalls     int
	firstEntered  chan struct{}
	secondEntered chan struct{}
	releaseFirst  chan struct{}
}

func (s *blockingSaveStore) ListContacts(context.Context) ([]dirextalkdomain.ContactRecord, error) {
	s.mu.Lock()
	s.listCalls++
	call := s.listCalls
	if call == 1 {
		close(s.firstEntered)
	}
	if call == 2 {
		close(s.secondEntered)
	}
	s.mu.Unlock()
	if call == 1 {
		<-s.releaseFirst
	}
	return nil, nil
}

func (*blockingSaveStore) UpsertContact(context.Context, dirextalkdomain.ContactRecord) error {
	return nil
}

type noOpConversationPort struct{}

func (noOpConversationPort) ListRecords(context.Context) ([]dirextalkdomain.ConversationRecord, error) {
	return nil, nil
}

func (noOpConversationPort) Save(context.Context, dirextalkdomain.ConversationRecord) error {
	return nil
}

func (noOpConversationPort) DeleteKindByRoom(context.Context, string, dirextalkdomain.ConversationKind) error {
	return nil
}

func (noOpConversationPort) Operation(context.Context, string, string, string) (map[string]any, *dirextalkdomain.ConversationView, error) {
	return nil, nil, nil
}

func TestSaveSerializesItsPersistenceOrchestration(t *testing.T) {
	store := &blockingSaveStore{
		firstEntered:  make(chan struct{}),
		secondEntered: make(chan struct{}),
		releaseFirst:  make(chan struct{}),
	}
	module := New(store, noOpConversationPort{}, Config{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- module.Save(context.Background(), dirextalkdomain.ContactRecord{PeerMXID: "@first:example.com"})
	}()
	select {
	case <-store.firstEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("first Save did not enter ListContacts")
	}

	secondStarted := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondDone <- module.Save(context.Background(), dirextalkdomain.ContactRecord{PeerMXID: "@second:example.com"})
	}()
	<-secondStarted
	select {
	case <-store.secondEntered:
		t.Fatal("second Save entered persistence orchestration before first Save completed")
	case <-time.After(200 * time.Millisecond):
	}
	close(store.releaseFirst)

	for name, result := range map[string]<-chan error{"first": firstDone, "second": secondDone} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s Save error = %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s Save", name)
		}
	}
	select {
	case <-store.secondEntered:
	default:
		t.Fatal("second Save never entered persistence orchestration")
	}
}
