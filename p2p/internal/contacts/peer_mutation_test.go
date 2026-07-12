package contacts

import (
	"testing"
	"time"
)

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
