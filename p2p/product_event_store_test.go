package p2p

import (
	"context"
	"testing"
)

func TestAppendP2PEventDoesNotNotifyWaitersForDuplicateInsert(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	ctx := context.Background()
	if err := service.appendP2PEvent(ctx, p2pEvent{Seq: 1, Type: "first", DedupeKey: "duplicate"}); err != nil {
		t.Fatalf("append first: %v", err)
	}

	waiter := service.p2pEventWaiter()
	if err := service.appendP2PEvent(ctx, p2pEvent{Seq: 2, Type: "second", DedupeKey: "duplicate"}); err != nil {
		t.Fatalf("append duplicate: %v", err)
	}
	select {
	case <-waiter:
		t.Fatal("duplicate insert notified event waiters")
	default:
	}
}
