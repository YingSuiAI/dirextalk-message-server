package legacygateway

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

const (
	InvocationEventType = "io.dirextalk.agent.invoke.v1"
	ResultEventType     = "io.dirextalk.agent.result.v1"
	ErrorEventType      = "io.dirextalk.agent.error.v1"

	MaxInvocationContentBytes   = 16 * 1024
	MaxMatrixIDBytes            = 255
	MaxIdempotencyKeyBytes      = 256
	MaxRequiredCapabilities     = 64
	MaxCapabilityNameBytes      = 128
	MaxJSONSafeUint             = uint64(1<<53 - 1)
	MaxAgentGatewayMessageBytes = 64 * 1024
)

var (
	ErrInvocationNotFound          = errors.New("legacy gateway invocation not found")
	ErrInvocationConflict          = errors.New("legacy gateway invocation conflicts with stored source")
	ErrInvalidInvocationTransition = errors.New("legacy gateway invocation state transition is invalid")
)

type DispatchMode string

const (
	DispatchSingle   DispatchMode = "single"
	DispatchFailover DispatchMode = "failover"
)

type RoutingState string

const (
	RoutingQueued            RoutingState = "queued"
	RoutingOffered           RoutingState = "offered"
	RoutingLeased            RoutingState = "leased"
	RoutingReconcileRequired RoutingState = "reconcile_required"
	RoutingExpired           RoutingState = "expired"
)

// Invocation is the validated Matrix invocation content. The opaque
// idempotency key is intentionally replaced by its tenant/room-bound digest.
type Invocation struct {
	RequestID            string
	InstallationID       string
	PreferredConnectorID string
	RequiredCapabilities []string
	DispatchMode         DispatchMode
	GrantVersion         uint64
	MatrixInputEventID   string
	IdempotencyDigest    [32]byte
}

type CreateRunRequest struct {
	RequestID            string
	IdempotencyDigest    [32]byte
	InstallationID       string
	ConversationID       string
	RequestEventID       string
	PreferredConnectorID string
	RequiredCapabilities []string
	DispatchMode         DispatchMode
	GrantVersion         uint64
}

type CreateRunReceipt struct {
	RequestID    string
	RunID        string
	Inserted     bool
	RoutingState RoutingState
}

type Ingress interface {
	CreateRun(context.Context, CreateRunRequest) (CreateRunReceipt, error)
}

type ReservationStatus string

const (
	ReservationInserted ReservationStatus = "inserted"
	ReservationReplay   ReservationStatus = "replay"
	ReservationConflict ReservationStatus = "conflict"
)

type InvocationState string

const (
	InvocationPending  InvocationState = "pending"
	InvocationAccepted InvocationState = "accepted"
	InvocationRejected InvocationState = "rejected"
)

// InvocationCandidate contains only durable, retry-safe values. In particular,
// it never contains the raw idempotency key.
type InvocationCandidate struct {
	MatrixRoomID         string
	RequestID            string
	MatrixInvokeEventID  string
	MatrixInputEventID   string
	TenantID             string
	InstallationID       string
	ConversationID       string
	RequestEventID       string
	SourceDigest         [32]byte
	IdempotencyDigest    [32]byte
	RequestDigest        [32]byte
	PreferredConnectorID string
	RequiredCapabilities []string
	DispatchMode         DispatchMode
	GrantVersion         uint64
	CreatedAt            time.Time
}

type InvocationRecord struct {
	InvocationCandidate
	State        InvocationState
	RunID        string
	RoutingState RoutingState
	Inserted     bool
	ErrorCode    string
	UpdatedAt    time.Time
}

type Reservation struct {
	Status ReservationStatus
	Record InvocationRecord
}

// Store owns the atomic reservation and terminal admission state. SourceDigest
// fences completion so a conflicting replay cannot update the first record.
type Store interface {
	ReserveInvocation(context.Context, InvocationCandidate) (Reservation, error)
	MarkAccepted(context.Context, string, string, [32]byte, CreateRunReceipt, time.Time) (InvocationRecord, error)
	MarkRejected(context.Context, string, string, [32]byte, string, time.Time) (InvocationRecord, error)
}

// ResultEventContent and ErrorEventContent only reserve the documented Matrix
// envelopes. MC4 does not infer Run completion or fabricate evidence.
type ResultEventContent struct {
	RequestID         string          `json:"request_id"`
	RunID             string          `json:"run_id"`
	InstallationID    string          `json:"installation_id"`
	ConnectorID       string          `json:"connector_id"`
	Outcome           string          `json:"outcome"`
	ReplyToEventID    string          `json:"reply_to_event_id"`
	ResultReference   json.RawMessage `json:"result_reference"`
	EvidenceReference json.RawMessage `json:"evidence_reference"`
}

type ErrorEventContent struct {
	RequestID      string `json:"request_id"`
	InstallationID string `json:"installation_id"`
	Code           string `json:"code"`
	ReplyToEventID string `json:"reply_to_event_id"`
}
