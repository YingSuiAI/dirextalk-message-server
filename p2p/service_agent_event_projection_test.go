package p2p

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	agentevents "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/agentevents"
)

func TestAgentEventProjectionDrainsBeforeAccountResetAndCannotResurrectState(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	store := &blockingAgentEventStore{started: make(chan struct{}), release: make(chan struct{})}
	guard := accountScopedAgentEventStore{service: service, store: store}
	commitDone := make(chan error, 1)
	go func() {
		_, err := guard.Commit(context.Background(), agentevents.CommitRequest{})
		commitDone <- err
	}()

	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("Agent projection commit did not start")
	}
	resetDone := make(chan struct{})
	go func() {
		service.clearAccountStateInMemory()
		close(resetDone)
	}()
	select {
	case <-resetDone:
		t.Fatal("account reset passed an in-flight Agent projection commit")
	case <-time.After(25 * time.Millisecond):
	}

	close(store.release)
	if err := <-commitDone; err != nil {
		t.Fatalf("in-flight commit error = %v", err)
	}
	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("account reset did not resume after Agent projection drained")
	}
	if _, err := guard.Commit(context.Background(), agentevents.CommitRequest{}); !errors.Is(err, agentevents.ErrProjectionStopped) {
		t.Fatalf("post-deprovision commit error = %v", err)
	}
	if got := store.commitCount(); got != 1 {
		t.Fatalf("projection store commit count = %d, want 1", got)
	}
}

type blockingAgentEventStore struct {
	mu      sync.Mutex
	commits int
	started chan struct{}
	release chan struct{}
}

func (store *blockingAgentEventStore) Cursor(context.Context, agentevents.Source) (int64, error) {
	return 0, nil
}

func (store *blockingAgentEventStore) Commit(context.Context, agentevents.CommitRequest) (agentevents.CommitResult, error) {
	store.mu.Lock()
	store.commits++
	store.mu.Unlock()
	close(store.started)
	<-store.release
	return agentevents.CommitResult{}, nil
}

func (store *blockingAgentEventStore) commitCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.commits
}
