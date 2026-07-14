package operations

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Tracker serializes one claimant's durable operation lease updates. It owns
// only Store CAS mechanics; callers retain workflow, context, and error-policy
// decisions.
type Tracker struct {
	mu                  sync.Mutex
	store               Store
	owner               string
	record              Record
	leaseDurationMillis int64
}

func NewTracker(store Store, owner string, record Record, leaseDurationMillis int64) *Tracker {
	return &Tracker{
		store:               store,
		owner:               owner,
		record:              record,
		leaseDurationMillis: leaseDurationMillis,
	}
}

func (t *Tracker) Snapshot() Record {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.record
}

func (t *Tracker) Update(ctx context.Context, status, phase, currentRoomID, errorCode, resultJSON string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	record := t.record
	if status != "" {
		record.Status = status
	}
	if phase != "" {
		record.Phase = phase
	}
	if currentRoomID != "" {
		record.CurrentRoomID = currentRoomID
	}
	record.ErrorCode = errorCode
	record.ResultJSON = resultJSON
	record.UpdatedAt = time.Now().UTC().UnixMilli()
	updated, ok, err := t.store.CompareAndSwapOperation(ctx, record, t.record.Revision, t.owner, t.leaseDurationMillis)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("recoverable operation lease or revision was lost")
	}
	t.record = updated
	return nil
}

func (t *Tracker) Heartbeat(ctx context.Context) error {
	current := t.Snapshot()
	return t.Update(ctx, current.Status, current.Phase, current.CurrentRoomID, current.ErrorCode, current.ResultJSON)
}

func (t *Tracker) Release(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.record.LeaseOwner == "" {
		return nil
	}
	record := t.record
	record.LeaseOwner = ""
	record.LeaseUntil = 0
	record.UpdatedAt = time.Now().UTC().UnixMilli()
	updated, ok, err := t.store.CompareAndSwapOperation(ctx, record, t.record.Revision, t.owner, 0)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("recoverable operation lease or revision was lost while releasing")
	}
	t.record = updated
	return nil
}
