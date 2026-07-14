package operations

import (
	"context"
	"testing"
)

type trackerStore struct {
	record Record
}

func (s *trackerStore) LookupOperation(context.Context, string) (Record, bool, error) {
	return s.record, true, nil
}

func (s *trackerStore) UpsertOperation(context.Context, Record) error { return nil }

func (s *trackerStore) ClaimOperation(context.Context, Record, string, int64) (Record, bool, error) {
	return s.record, true, nil
}

func (s *trackerStore) CompareAndSwapOperation(_ context.Context, record Record, expectedRevision int64, owner string, leaseDurationMillis int64) (Record, bool, error) {
	if expectedRevision != s.record.Revision || owner != s.record.LeaseOwner {
		return Record{}, false, nil
	}
	record.Revision = expectedRevision + 1
	if leaseDurationMillis == 0 {
		record.LeaseOwner = ""
		record.LeaseUntil = 0
	} else {
		record.LeaseOwner = owner
		record.LeaseUntil = 123
	}
	s.record = record
	return record, true, nil
}

func TestTrackerPreservesCASHeartbeatAndReleaseSemantics(t *testing.T) {
	store := &trackerStore{record: Record{
		OperationID: "op-1", Status: "running", Phase: "prepared", CurrentRoomID: "!old:example.com",
		Revision: 4, LeaseOwner: "worker", LeaseUntil: 99,
	}}
	tracker := NewTracker(store, "worker", store.record, 90_000)

	if err := tracker.Update(context.Background(), "", "matrix_committed", "!new:example.com", "M_TEST", `{"ok":true}`); err != nil {
		t.Fatalf("update: %v", err)
	}
	updated := tracker.Snapshot()
	if updated.Status != "running" || updated.Phase != "matrix_committed" || updated.CurrentRoomID != "!new:example.com" || updated.ErrorCode != "M_TEST" || updated.ResultJSON != `{"ok":true}` || updated.Revision != 5 {
		t.Fatalf("unexpected update snapshot: %#v", updated)
	}

	if err := tracker.Heartbeat(context.Background()); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if got := tracker.Snapshot(); got.Revision != 6 || got.LeaseOwner != "worker" || got.LeaseUntil != 123 {
		t.Fatalf("heartbeat changed lease or revision unexpectedly: %#v", got)
	}

	if err := tracker.Release(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := tracker.Snapshot(); got.LeaseOwner != "" || got.LeaseUntil != 0 || got.Revision != 7 {
		t.Fatalf("release did not clear held lease: %#v", got)
	}
}

func TestTrackerRejectsLostLeaseWithoutChangingSnapshot(t *testing.T) {
	store := &trackerStore{record: Record{OperationID: "op-2", Status: "running", Phase: "prepared", Revision: 7, LeaseOwner: "worker"}}
	tracker := NewTracker(store, "worker", store.record, 90_000)
	store.record.Revision++
	store.record.LeaseOwner = "other-worker"

	err := tracker.Update(context.Background(), "reconciling", "", "", "", "")
	if err == nil || err.Error() != "recoverable operation lease or revision was lost" {
		t.Fatalf("lost lease update error = %v", err)
	}
	if got := tracker.Snapshot(); got.Revision != 7 || got.LeaseOwner != "worker" || got.Status != "running" {
		t.Fatalf("failed update changed local snapshot: %#v", got)
	}

	err = tracker.Release(context.Background())
	if err == nil || err.Error() != "recoverable operation lease or revision was lost while releasing" {
		t.Fatalf("lost lease release error = %v", err)
	}
}
