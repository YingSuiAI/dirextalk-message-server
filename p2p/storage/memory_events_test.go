package storage

import (
	"context"
	"testing"
)

type memoryEventStructPayload struct {
	Labels map[string]string
}

func TestMemoryStoreEventsDedupeCopiesBoundsAndPrune(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryStore()
	event := p2pEvent{Seq: 20, Type: "first", DedupeKey: "dedupe", Payload: map[string]any{"nested": map[string]any{"value": "original"}}}
	inserted, err := store.InsertEvent(ctx, event)
	if err != nil || !inserted {
		t.Fatalf("InsertEvent first = (%v, %v), want (true, nil)", inserted, err)
	}
	event.Payload["nested"].(map[string]any)["value"] = "mutated"

	inserted, err = store.InsertEvent(ctx, p2pEvent{Seq: 21, Type: "duplicate", DedupeKey: "dedupe"})
	if err != nil || inserted {
		t.Fatalf("InsertEvent duplicate dedupe = (%v, %v), want (false, nil)", inserted, err)
	}
	inserted, err = store.InsertEvent(ctx, p2pEvent{Seq: 20, Type: "duplicate-seq"})
	if err != nil || inserted {
		t.Fatalf("InsertEvent duplicate seq = (%v, %v), want (false, nil)", inserted, err)
	}
	for _, candidate := range []p2pEvent{{Seq: 10, Type: "second"}, {Seq: 30, Type: "third"}} {
		if inserted, err := store.InsertEvent(ctx, candidate); err != nil || !inserted {
			t.Fatalf("InsertEvent(%d) = (%v, %v)", candidate.Seq, inserted, err)
		}
	}

	events, err := store.ListEvents(ctx, 0, 100)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(events) != 3 || events[0].Seq != 20 || events[1].Seq != 10 || events[2].Seq != 30 {
		t.Fatalf("events did not preserve append order: %#v", events)
	}
	if got := events[0].Payload["nested"].(map[string]any)["value"]; got != "original" {
		t.Fatalf("stored event payload aliased input: %v", got)
	}
	events[0].Payload["nested"].(map[string]any)["value"] = "returned mutation"
	events, _ = store.ListEvents(ctx, 0, 100)
	if got := events[0].Payload["nested"].(map[string]any)["value"]; got != "original" {
		t.Fatalf("ListEvents returned aliased payload: %v", got)
	}

	bounds, err := store.EventBounds(ctx)
	if err != nil || bounds.MinSeq != 10 || bounds.MaxSeq != 30 || bounds.Count != 3 {
		t.Fatalf("EventBounds = (%#v, %v), want min=10 max=30 count=3", bounds, err)
	}
	deleted, err := store.PruneEventsToMaxRows(ctx, 2)
	if err != nil || deleted != 1 {
		t.Fatalf("PruneEventsToMaxRows = (%d, %v), want (1, nil)", deleted, err)
	}
	events, _ = store.ListEvents(ctx, 0, 100)
	if len(events) != 2 || events[0].Seq != 20 || events[1].Seq != 30 {
		t.Fatalf("events after prune = %#v, want seq 20,30", events)
	}

	// Pruning removes the dedupe index with the event, allowing a future event
	// with the same key just as deleting the durable row does.
	deleted, err = store.PruneEventsBefore(ctx, 25)
	if err != nil || deleted != 1 {
		t.Fatalf("PruneEventsBefore = (%d, %v), want (1, nil)", deleted, err)
	}
	inserted, err = store.InsertEvent(ctx, p2pEvent{Seq: 40, Type: "reuse", DedupeKey: "dedupe"})
	if err != nil || !inserted {
		t.Fatalf("InsertEvent after dedupe row prune = (%v, %v), want (true, nil)", inserted, err)
	}
}

func TestMemoryStoreEventsDeepCopyTypedPayloads(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	structured := memoryEventStructPayload{Labels: map[string]string{"value": "original"}}
	inserted, err := store.InsertEvent(context.Background(), p2pEvent{
		Seq:     1,
		Payload: map[string]any{"structured": structured},
	})
	if err != nil || !inserted {
		t.Fatalf("InsertEvent = (%v, %v)", inserted, err)
	}
	structured.Labels["value"] = "mutated"

	events, err := store.ListEvents(context.Background(), 0, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("ListEvents = (%#v, %v)", events, err)
	}
	if got := events[0].Payload["structured"].(memoryEventStructPayload).Labels["value"]; got != "original" {
		t.Fatalf("stored event struct payload aliased input: %v", got)
	}
	events[0].Payload["structured"].(memoryEventStructPayload).Labels["value"] = "returned mutation"
	events, _ = store.ListEvents(context.Background(), 0, 10)
	if got := events[0].Payload["structured"].(memoryEventStructPayload).Labels["value"]; got != "original" {
		t.Fatalf("ListEvents returned aliased struct payload: %v", got)
	}
}
