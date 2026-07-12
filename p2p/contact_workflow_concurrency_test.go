package p2p

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

type concurrentContactCreateTransport struct {
	recordingTransport

	mu            sync.Mutex
	createCalls   int
	createEntered chan int
	releaseCreate chan struct{}
}

func newConcurrentContactCreateTransport() *concurrentContactCreateTransport {
	return &concurrentContactCreateTransport{
		createEntered: make(chan int, 2),
		releaseCreate: make(chan struct{}),
	}
}

func (t *concurrentContactCreateTransport) CreateRoom(_ context.Context, _ CreateRoomRequest) (CreateRoomResult, error) {
	t.mu.Lock()
	t.createCalls++
	call := t.createCalls
	t.mu.Unlock()
	t.createEntered <- call
	<-t.releaseCreate
	return CreateRoomResult{RoomID: fmt.Sprintf("!concurrent-%d:example.com", call)}, nil
}

func (t *concurrentContactCreateTransport) calls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.createCalls
}

func TestConcurrentContactRequestsCreateOneDirectRoom(t *testing.T) {
	transport := newConcurrentContactCreateTransport()
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	type result struct {
		contact contactRecord
		apiErr  *apiError
	}
	const peerMXID = "@alice:remote.example"
	start := make(chan struct{})
	ready := make(chan struct{}, 2)
	results := make(chan result, 2)
	for range 2 {
		go func() {
			ready <- struct{}{}
			<-start
			value, requestErr := service.Handle(context.Background(), "contacts.request", map[string]any{
				"mxid":         peerMXID,
				"display_name": "Alice",
			})
			contact, _ := value.(contactRecord)
			results <- result{contact: contact, apiErr: requestErr}
		}()
	}
	<-ready
	<-ready
	close(start)

	select {
	case <-transport.createEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("first contacts.request did not reach Matrix room creation")
	}

	secondCreateEntered := false
	select {
	case <-transport.createEntered:
		secondCreateEntered = true
	case <-time.After(250 * time.Millisecond):
	}
	close(transport.releaseCreate)

	first := <-results
	second := <-results
	if first.apiErr != nil || second.apiErr != nil {
		t.Fatalf("concurrent contacts.request errors = (%#v, %#v)", first.apiErr, second.apiErr)
	}
	if secondCreateEntered || transport.calls() != 1 {
		t.Fatalf("concurrent contacts.request created %d Matrix rooms, want 1", transport.calls())
	}
	if first.contact.RoomID == "" || second.contact.RoomID != first.contact.RoomID {
		t.Fatalf("concurrent contacts = (%#v, %#v), want the same direct room", first.contact, second.contact)
	}
}

type projectorSnapshotContextKey struct{}

type blockingProjectorContactStore struct {
	Store

	mu               sync.Mutex
	snapshotCaptured bool
	snapshotRead     chan struct{}
	releaseSnapshot  chan struct{}
}

func (s *blockingProjectorContactStore) ListContacts(ctx context.Context) ([]contactStorageRecord, error) {
	contacts, err := s.Store.ListContacts(ctx)
	if ctx.Value(projectorSnapshotContextKey{}) != true {
		return contacts, err
	}
	s.mu.Lock()
	if s.snapshotCaptured {
		s.mu.Unlock()
		return contacts, err
	}
	s.snapshotCaptured = true
	close(s.snapshotRead)
	s.mu.Unlock()
	<-s.releaseSnapshot
	return contacts, err
}

func TestProjectorSnapshotCannotOverwriteCompletedContactReplacement(t *testing.T) {
	baseStore := p2pstorage.NewMemoryStore()
	store := &blockingProjectorContactStore{
		Store:           baseStore,
		snapshotRead:    make(chan struct{}),
		releaseSnapshot: make(chan struct{}),
	}
	service, err := NewServiceWithStore(context.Background(), Config{ServerName: "example.com"}, store)
	if err != nil {
		t.Fatal(err)
	}

	const (
		peerMXID      = "@alice:remote.example"
		oldRoomID     = "!old:example.com"
		replacementID = "!replacement:example.com"
	)
	if err := service.saveContact(context.Background(), contactRecord{
		PeerMXID: peerMXID, RoomID: oldRoomID, DisplayName: "Old", Status: "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	peerLockHeld := make(chan struct{})
	releasePeerLock := make(chan struct{})
	peerLockDone := make(chan struct{})
	go func() {
		service.contactsModule.SerializePeer(peerMXID, func() {
			close(peerLockHeld)
			<-releasePeerLock
		})
		close(peerLockDone)
	}()
	<-peerLockHeld

	projectorDone := make(chan error, 1)
	projectorCtx := context.WithValue(context.Background(), projectorSnapshotContextKey{}, true)
	go func() {
		projectorDone <- service.projectDirectContactMember(projectorCtx, memberRecord{
			RoomID: oldRoomID, UserID: peerMXID, DisplayName: "Stale projector snapshot",
		}, map[string]any{})
	}()

	select {
	case <-store.snapshotRead:
	case <-time.After(250 * time.Millisecond):
		// The fixed path is queued on the peer boundary before it can read Store.
	}
	if err := service.saveContact(context.Background(), contactRecord{
		PeerMXID: peerMXID, RoomID: replacementID, DisplayName: "Replacement", Status: "accepted",
	}); err != nil {
		t.Fatal(err)
	}
	close(store.releaseSnapshot)
	close(releasePeerLock)
	<-peerLockDone
	if err := <-projectorDone; err != nil {
		t.Fatal(err)
	}

	contact, ok, err := service.lookupContactByPeer(context.Background(), peerMXID)
	if err != nil || !ok {
		t.Fatalf("replacement lookup = (%#v, %t, %v)", contact, ok, err)
	}
	if contact.RoomID != replacementID {
		t.Fatalf("projector restored stale contact room %q, want %q", contact.RoomID, replacementID)
	}
}
