package contacts

import (
	"context"
	"reflect"
	"testing"
	"time"

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

func TestRequestRejectAcceptedIsNoOpWithContactView(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", DisplayName: "Alice", Status: "accepted", Remark: "friend",
	}
	harness := newSaveHarness([]dirextalkdomain.ContactRecord{existing})
	operation := map[string]any{"action": actionRequestReject, "status": "accepted", "room_id": existing.RoomID}
	conversation := &dirextalkdomain.ConversationView{ConversationID: "conv", MatrixRoomID: existing.RoomID}
	harness.conversation.operation = operation
	harness.conversation.operationView = conversation

	result, apiErr := harness.module.Handlers()[actionRequestReject](context.Background(), map[string]any{
		"room_id": existing.RoomID, "peer_mxid": "@spoofed:example.com", "display_name": "Spoofed",
	})
	if apiErr != nil {
		t.Fatalf("contacts.requests.reject error = %#v", apiErr)
	}
	view, ok := result.(View)
	if !ok || RecordFromView(view) != existing || !reflect.DeepEqual(view.Operation, operation) || view.Conversation != conversation {
		t.Fatalf("accepted reject result = %T %#v", result, result)
	}
	if upserts := harness.store.upserts(); len(upserts) != 0 {
		t.Fatalf("accepted reject wrote contact: %#v", upserts)
	}
}

func TestRequestRejectPersistsCompatibleRejectedSnapshot(t *testing.T) {
	tests := []struct {
		name     string
		existing []dirextalkdomain.ContactRecord
		params   map[string]any
		want     dirextalkdomain.ContactRecord
	}{
		{
			name: "existing profile wins except explicit peer avatar and domain",
			existing: []dirextalkdomain.ContactRecord{{
				PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", DisplayName: "Alice",
				AvatarURL: "mxc://example.com/old", Domain: "old.example", Status: "pending_inbound", Remark: "request",
			}},
			params: map[string]any{
				"room_id": "!direct:example.com", "peer_mxid": " @spoofed:example.com ", "display_name": "Spoofed",
				"avatar_url": " mxc://example.com/new ", "domain": " new.example ",
			},
			want: dirextalkdomain.ContactRecord{
				PeerMXID: "@spoofed:example.com", RoomID: "!direct:example.com", DisplayName: "Alice",
				AvatarURL: "mxc://example.com/new", Domain: "new.example", Status: "rejected", Remark: "request",
			},
		},
		{
			name: "empty parameters fall back to existing profile",
			existing: []dirextalkdomain.ContactRecord{{
				PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", DisplayName: "Alice",
				AvatarURL: "mxc://example.com/old", Domain: "old.example", Status: "pending_outbound", Remark: "request",
			}},
			params: map[string]any{"room_id": "!direct:example.com"},
			want: dirextalkdomain.ContactRecord{
				PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", DisplayName: "Alice",
				AvatarURL: "mxc://example.com/old", Domain: "old.example", Status: "rejected", Remark: "request",
			},
		},
		{
			name: "empty room and identity remain compatible",
			params: map[string]any{
				"display_name": " Unknown ", "avatar_url": " mxc://example.com/unknown ", "domain": " example.com ",
			},
			want: dirextalkdomain.ContactRecord{
				DisplayName: "Unknown", AvatarURL: "mxc://example.com/unknown", Domain: "example.com", Status: "rejected",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newSaveHarness(tt.existing)
			harness.conversation.operation = map[string]any{"action": actionRequestReject}
			result, apiErr := harness.module.Handlers()[actionRequestReject](context.Background(), tt.params)
			if apiErr != nil {
				t.Fatalf("contacts.requests.reject error = %#v", apiErr)
			}
			view, ok := result.(View)
			if !ok || RecordFromView(view) != tt.want {
				t.Fatalf("rejected result = %T %#v, want %#v", result, result, tt.want)
			}
			if got := harness.store.upserts(); !reflect.DeepEqual(got, []dirextalkdomain.ContactRecord{tt.want}) {
				t.Fatalf("upserted contacts = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestSerializePeerSerializesTrimmedPeerKey(t *testing.T) {
	module := &Module{}
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		module.SerializePeer("  @alice:example.com  ", func() {
			close(firstEntered)
			<-releaseFirst
		})
	}()
	waitForPeerMutationSignal(t, firstEntered, "first peer mutation to enter")

	secondCalling := make(chan struct{})
	secondEntered := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		close(secondCalling)
		module.SerializePeer("@alice:example.com", func() {
			close(secondEntered)
		})
	}()
	waitForPeerMutationSignal(t, secondCalling, "second peer mutation to start")
	waitForPeerMutationRefCount(t, module, "@alice:example.com", 2)
	select {
	case <-secondEntered:
		t.Fatal("same peer mutation entered before the first mutation released")
	default:
	}

	close(releaseFirst)
	waitForPeerMutationSignal(t, firstDone, "first peer mutation to finish")
	waitForPeerMutationSignal(t, secondEntered, "second peer mutation to enter after release")
	waitForPeerMutationSignal(t, secondDone, "second peer mutation to finish")
}

func TestSerializePeerAllowsDifferentPeersConcurrently(t *testing.T) {
	module := &Module{}
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		module.SerializePeer("@alice:example.com", func() {
			close(firstEntered)
			<-releaseFirst
		})
	}()
	waitForPeerMutationSignal(t, firstEntered, "first peer mutation to enter")

	secondEntered := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		module.SerializePeer("@bob:example.com", func() {
			close(secondEntered)
		})
	}()
	waitForPeerMutationSignal(t, secondEntered, "different peer mutation to enter concurrently")

	close(releaseFirst)
	waitForPeerMutationSignal(t, firstDone, "first peer mutation to finish")
	waitForPeerMutationSignal(t, secondDone, "different peer mutation to finish")
}

func TestSerializePeerRemovesReleasedEntry(t *testing.T) {
	module := &Module{}

	module.SerializePeer("  @alice:example.com  ", func() {})

	module.peerMutationsMu.Lock()
	entryCount := len(module.peerMutations)
	module.peerMutationsMu.Unlock()
	if entryCount != 0 {
		t.Fatalf("released peer mutation entries = %d, want 0", entryCount)
	}
}

func TestSerializePeerUnlocksAndCleansUpAfterPanic(t *testing.T) {
	module := &Module{}
	panicValue := &struct{}{}
	var heldEntry *peerMutationEntry
	var recovered any

	func() {
		defer func() {
			recovered = recover()
		}()
		module.SerializePeer("@alice:example.com", func() {
			module.peerMutationsMu.Lock()
			heldEntry = module.peerMutations["@alice:example.com"]
			module.peerMutationsMu.Unlock()
			panic(panicValue)
		})
	}()

	if recovered != panicValue {
		t.Fatalf("recovered panic = %v, want original panic value", recovered)
	}
	if heldEntry == nil {
		t.Fatal("peer mutation entry was not observable while callback held its lock")
	}
	if !heldEntry.mu.TryLock() {
		t.Fatal("peer mutation lock remained held after callback panic")
	}
	heldEntry.mu.Unlock()

	module.peerMutationsMu.Lock()
	entryCount := len(module.peerMutations)
	module.peerMutationsMu.Unlock()
	if entryCount != 0 {
		t.Fatalf("peer mutation entries after panic = %d, want 0", entryCount)
	}
}

func waitForPeerMutationRefCount(t *testing.T, module *Module, key string, want int) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		module.peerMutationsMu.Lock()
		entry := module.peerMutations[key]
		got := 0
		if entry != nil {
			got = entry.refs
		}
		module.peerMutationsMu.Unlock()
		if got == want {
			return
		}

		select {
		case <-deadline.C:
			t.Fatalf("peer mutation ref count for %q = %d, want %d", key, got, want)
		case <-ticker.C:
		}
	}
}

func waitForPeerMutationSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}
