package storage

import (
	"context"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/operations"
)

func (s *MemoryStore) LookupOperation(_ context.Context, operationID string) (operations.Record, bool, error) {
	s.mu.RLock()
	record, ok := s.operations[operationID]
	s.mu.RUnlock()
	return record, ok, nil
}

func (s *MemoryStore) UpsertOperation(_ context.Context, record operations.Record) error {
	s.mu.Lock()
	s.operations[record.OperationID] = record
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) ClaimOperation(
	_ context.Context,
	record operations.Record,
	owner string,
	leaseDurationMillis int64,
) (operations.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	leaseOwner := owner
	leaseUntil := int64(0)
	if owner != "" && leaseDurationMillis > 0 {
		leaseUntil = now + leaseDurationMillis
	} else {
		leaseOwner = ""
	}
	current, found := s.operations[record.OperationID]
	if !found {
		record.Revision = 1
		record.LeaseOwner = leaseOwner
		record.LeaseUntil = leaseUntil
		s.operations[record.OperationID] = record
		return record, true, nil
	}
	if current.Action != record.Action ||
		(record.RoomID != "" && current.RoomID != record.RoomID) ||
		(record.UserID != "" && current.UserID != record.UserID) ||
		(record.PeerMXID != "" && current.PeerMXID != record.PeerMXID) ||
		(record.RequestID != "" && current.RequestID != record.RequestID) {
		return current, false, nil
	}
	if current.LeaseOwner != "" && current.LeaseOwner != owner && current.LeaseUntil > now {
		return current, false, nil
	}
	current.Revision++
	current.LeaseOwner = leaseOwner
	current.LeaseUntil = leaseUntil
	s.operations[current.OperationID] = current
	return current, true, nil
}

func (s *MemoryStore) CompareAndSwapOperation(
	_ context.Context,
	record operations.Record,
	expectedRevision int64,
	owner string,
	leaseDurationMillis int64,
) (operations.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found := s.operations[record.OperationID]
	if !found || current.Revision != expectedRevision || current.LeaseOwner != owner || current.LeaseUntil <= time.Now().UnixMilli() {
		return operations.Record{}, false, nil
	}
	record.Revision = current.Revision + 1
	if record.LeaseOwner != "" && leaseDurationMillis > 0 {
		record.LeaseUntil = time.Now().UnixMilli() + leaseDurationMillis
	} else {
		record.LeaseOwner = ""
		record.LeaseUntil = 0
	}
	s.operations[record.OperationID] = record
	return record, true, nil
}
