package events

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

type pruneErrorStore struct {
	*p2pstorage.MemoryStore
	err error
}

func (s *pruneErrorStore) PruneEventsToMaxRows(context.Context, int64) (int64, error) {
	return 0, s.err
}

type listCaptureStore struct {
	*p2pstorage.MemoryStore
	since int64
	limit int
}

func (s *listCaptureStore) ListEvents(ctx context.Context, since int64, limit int) ([]dirextalkdomain.Event, error) {
	s.since = since
	s.limit = limit
	return s.MemoryStore.ListEvents(ctx, since, limit)
}

func TestAppendDoesNotNotifyWaitersForDuplicateInsert(t *testing.T) {
	module := New(p2pstorage.NewMemoryStore(), Config{Now: func() time.Time { return time.Unix(0, 100).UTC() }})
	ctx := context.Background()
	if err := module.Append(ctx, dirextalkdomain.Event{Seq: 1, Type: "first", DedupeKey: "duplicate"}); err != nil {
		t.Fatalf("append first: %v", err)
	}

	waiter := module.Waiter()
	if err := module.Append(ctx, dirextalkdomain.Event{Seq: 2, Type: "second", DedupeKey: "duplicate"}); err != nil {
		t.Fatalf("append duplicate: %v", err)
	}
	select {
	case <-waiter:
		t.Fatal("duplicate insert notified event waiters")
	default:
	}
}

func TestModuleCoreEventSemantics(t *testing.T) {
	ctx := context.Background()
	fixedNow := time.Unix(0, 1000).UTC()

	t.Run("explicit and same-nanosecond sequences stay monotonic across reset", func(t *testing.T) {
		store := p2pstorage.NewMemoryStore()
		module := New(store, Config{Now: func() time.Time { return fixedNow }})
		for _, event := range []dirextalkdomain.Event{
			{Seq: 900, Type: "explicit"},
			{Type: "same-nanosecond-one"},
			{Type: "same-nanosecond-two"},
		} {
			if err := module.Append(ctx, event); err != nil {
				t.Fatalf("append %q: %v", event.Type, err)
			}
		}
		events, err := module.List(ctx, 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		wantSeqs := []int64{900, 1000, 1001}
		for index, want := range wantSeqs {
			if len(events) != len(wantSeqs) || events[index].Seq != want {
				t.Fatalf("event sequences = %#v, want %v", events, wantSeqs)
			}
		}

		store.ResetAccountState()
		module.ResetSequence()
		if err := module.Append(ctx, dirextalkdomain.Event{Type: "after-reset"}); err != nil {
			t.Fatalf("append after reset: %v", err)
		}
		events, err = module.List(ctx, 0, 10)
		if err != nil || len(events) != 1 || events[0].Seq != fixedNow.UnixNano() {
			t.Fatalf("events after reset = %#v, err=%v", events, err)
		}
	})

	t.Run("prune failure leaves waiters asleep", func(t *testing.T) {
		wantErr := errors.New("prune failed")
		store := &pruneErrorStore{MemoryStore: p2pstorage.NewMemoryStore(), err: wantErr}
		module := New(store, Config{
			RetentionMaxRows:      1,
			RetentionPruneOnWrite: true,
			Now:                   func() time.Time { return fixedNow },
		})
		waiter := module.Waiter()
		if err := module.Append(ctx, dirextalkdomain.Event{Type: "inserted"}); !errors.Is(err, wantErr) {
			t.Fatalf("Append() error = %v, want %v", err, wantErr)
		}
		select {
		case <-waiter:
			t.Fatal("prune failure notified event waiters")
		default:
		}
	})

	t.Run("list normalization and cursor bounds stay at the module boundary", func(t *testing.T) {
		store := &listCaptureStore{MemoryStore: p2pstorage.NewMemoryStore()}
		for _, seq := range []int64{10, 20, 30} {
			if inserted, err := store.InsertEvent(ctx, dirextalkdomain.Event{Seq: seq, Type: "seed"}); err != nil || !inserted {
				t.Fatalf("seed seq %d: inserted=%t err=%v", seq, inserted, err)
			}
		}
		if _, err := store.PruneEventsBefore(ctx, 20); err != nil {
			t.Fatal(err)
		}
		module := New(store, Config{})
		events, err := module.List(ctx, 20, 0)
		if err != nil || store.since != 20 || store.limit != 100 || len(events) != 1 || events[0].Seq != 30 {
			t.Fatalf("List() = %#v, err=%v, forwarded=(%d,%d)", events, err, store.since, store.limit)
		}
		status, err := module.CursorStatus(ctx, 15)
		if err != nil || !status.Expired || status.Since != 15 || status.Bounds != (dirextalkdomain.EventBounds{MinSeq: 20, MaxSeq: 30, Count: 2}) {
			t.Fatalf("CursorStatus() = %#v, err=%v", status, err)
		}
	})
}
