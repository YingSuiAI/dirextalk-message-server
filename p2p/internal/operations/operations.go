// Package operations defines durable records for recoverable ProductCore operations.
package operations

import "context"

// Record is the durable recovery state for one idempotent operation.
type Record struct {
	OperationID   string
	Action        string
	Status        string
	Phase         string
	RoomID        string
	CurrentRoomID string
	UserID        string
	PeerMXID      string
	RequestID     string
	BaseRequestID string
	ResultJSON    string
	ErrorCode     string
	Revision      int64
	LeaseOwner    string
	LeaseUntil    int64 // Absolute Unix milliseconds assigned by the store clock.
	CreatedAt     int64
	UpdatedAt     int64
}

// Store persists and retrieves recoverable operations by their stable ID.
type Store interface {
	LookupOperation(ctx context.Context, operationID string) (Record, bool, error)
	UpsertOperation(ctx context.Context, record Record) error
	ClaimOperation(ctx context.Context, record Record, owner string, leaseDurationMillis int64) (Record, bool, error)
	CompareAndSwapOperation(ctx context.Context, record Record, expectedRevision int64, owner string, leaseDurationMillis int64) (Record, bool, error)
}
