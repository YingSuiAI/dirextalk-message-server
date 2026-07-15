// Package store owns the durable command replay fence used by the standalone
// Connection Stack. It stores only signed-command identity, safe public result
// JSON, counters, and issued quote records; it never stores credentials.
package store

import (
	"context"
	"errors"
)

type Record struct {
	ConnectionID       string
	CommandID          string
	RequestSHA256      string
	ExpectedGeneration int64
	NodeCounter        int64
	Action             string
	ResultJSON         []byte
}

func (r Record) SameIdentity(other Record) bool {
	return r.ConnectionID == other.ConnectionID && r.CommandID == other.CommandID &&
		r.RequestSHA256 == other.RequestSHA256 && r.ExpectedGeneration == other.ExpectedGeneration &&
		r.NodeCounter == other.NodeCounter && r.Action == other.Action
}

type IssuedQuote struct {
	ConnectionID  string
	QuoteID       string
	PlanDigest    string
	CommandID     string
	RequestSHA256 string
	ValidUntil    string
	QuoteJSON     []byte
}

type Repository interface {
	Lookup(ctx context.Context, connectionID, commandID string) (Record, bool, error)
	Commit(ctx context.Context, record Record, quote *IssuedQuote) (stored Record, created bool, err error)
}

type DeploymentReservation struct {
	ConnectionID       string
	DeploymentID       string
	CommandID          string
	RequestSHA256      string
	ExpectedGeneration int64
	NodeCounter        int64
	ApprovalID         string
	ChallengeID        string
	SignerKeyID        string
	QuoteID            string
	ClientToken        string
	BootstrapSessionID string
	WorkerSession      WorkerSession
	SpecJSON           []byte
	ResultJSON         []byte
	State              string
}

func (r DeploymentReservation) SameIdentity(other DeploymentReservation) bool {
	return r.ConnectionID == other.ConnectionID && r.DeploymentID == other.DeploymentID && r.CommandID == other.CommandID && r.RequestSHA256 == other.RequestSHA256 && r.ExpectedGeneration == other.ExpectedGeneration && r.NodeCounter == other.NodeCounter && r.ApprovalID == other.ApprovalID && r.ChallengeID == other.ChallengeID && r.SignerKeyID == other.SignerKeyID && r.QuoteID == other.QuoteID && r.ClientToken == other.ClientToken && r.BootstrapSessionID == other.BootstrapSessionID && sameWorkerSession(r.WorkerSession, other.WorkerSession) && string(r.SpecJSON) == string(other.SpecJSON)
}

func sameWorkerSession(left, right WorkerSession) bool {
	return left.BootstrapSessionID == right.BootstrapSessionID && left.ConnectionID == right.ConnectionID && left.DeploymentID == right.DeploymentID && left.RequestSHA256 == right.RequestSHA256 && left.WorkerImageDigest == right.WorkerImageDigest && left.ArtifactManifestDigest == right.ArtifactManifestDigest && left.BootstrapEndpoint == right.BootstrapEndpoint && left.ExpectedAMIID == right.ExpectedAMIID && left.ExpectedInstanceType == right.ExpectedInstanceType && left.ExpectedArchitecture == right.ExpectedArchitecture && left.ExpectedVPCID == right.ExpectedVPCID && left.ExpectedSubnetID == right.ExpectedSubnetID && left.ExpectedAvailabilityZone == right.ExpectedAvailabilityZone && left.ExpectedSecurityGroupID == right.ExpectedSecurityGroupID && left.ExpectedInstanceID == right.ExpectedInstanceID && left.State == right.State && left.ExpiresAt == right.ExpiresAt && left.LeaseEpoch == right.LeaseEpoch && left.LeaseExpiresAt == right.LeaseExpiresAt && left.TokenSHA256 == right.TokenSHA256 && left.LastSequence == right.LastSequence && left.LastEventAt == right.LastEventAt && left.LastEventSHA256 == right.LastEventSHA256
}

type WorkerSession struct {
	BootstrapSessionID       string
	ConnectionID             string
	DeploymentID             string
	RequestSHA256            string
	WorkerImageDigest        string
	ArtifactManifestDigest   string
	BootstrapEndpoint        string
	ExpectedAMIID            string
	ExpectedInstanceType     string
	ExpectedArchitecture     string
	ExpectedVPCID            string
	ExpectedSubnetID         string
	ExpectedAvailabilityZone string
	ExpectedSecurityGroupID  string
	ExpectedInstanceID       string
	State                    string
	ExpiresAt                string
	LeaseEpoch               int64
	LeaseExpiresAt           string
	TokenSHA256              string
	LastSequence             int64
	LastEventAt              string
	LastEventSHA256          string
}

type WorkerSessionClaim struct {
	Session        WorkerSession
	TokenSHA256    string
	Now            string
	LeaseExpiresAt string
}

type WorkerSessionRepository interface {
	LookupWorkerSession(ctx context.Context, bootstrapSessionID string) (WorkerSession, bool, error)
	ActivateWorkerSession(ctx context.Context, claim WorkerSessionClaim) (WorkerSession, error)
}

type WorkerSessionEventRepository interface {
	RecordWorkerSessionEvent(ctx context.Context, event WorkerSessionEvent) (stored WorkerSession, idempotent bool, err error)
}

type WorkerSessionEvent struct {
	ConnectionID       string
	DeploymentID       string
	BootstrapSessionID string
	ExpectedInstanceID string
	LeaseEpoch         int64
	Sequence           int64
	TokenSHA256        string
	EventSHA256        string
	OccurredAt         string
	Now                string
}

type DeploymentRepository interface {
	Repository
	WorkerSessionRepository
	LookupIssuedQuote(ctx context.Context, connectionID, quoteID string) (IssuedQuote, bool, error)
	LookupDeployment(ctx context.Context, connectionID, deploymentID string) (DeploymentReservation, bool, error)
	ReserveDeployment(ctx context.Context, reservation DeploymentReservation) (stored DeploymentReservation, created bool, err error)
	FinalizeDeployment(ctx context.Context, reservation DeploymentReservation, receipt Record) (stored Record, created bool, err error)
}

// DeploymentDestroyReservation consumes a device approval before the first
// provider mutation. The exact command may then resume after expiry or a
// response loss, while a different command cannot widen the resource set.
type DeploymentDestroyReservation struct {
	ConnectionID       string
	DeploymentID       string
	ServiceID          string
	CommandID          string
	RequestSHA256      string
	ExpectedGeneration int64
	NodeCounter        int64
	ApprovalID         string
	ChallengeID        string
	SignerKeyID        string
	RequestJSON        []byte
	ResultJSON         []byte
	State              string
}

func (reservation DeploymentDestroyReservation) SameIdentity(other DeploymentDestroyReservation) bool {
	return reservation.ConnectionID == other.ConnectionID && reservation.DeploymentID == other.DeploymentID && reservation.ServiceID == other.ServiceID && reservation.CommandID == other.CommandID && reservation.RequestSHA256 == other.RequestSHA256 && reservation.ExpectedGeneration == other.ExpectedGeneration && reservation.NodeCounter == other.NodeCounter && reservation.ApprovalID == other.ApprovalID && reservation.ChallengeID == other.ChallengeID && reservation.SignerKeyID == other.SignerKeyID && string(reservation.RequestJSON) == string(other.RequestJSON)
}

type DeploymentDestroyRepository interface {
	Repository
	LookupDeployment(ctx context.Context, connectionID, deploymentID string) (DeploymentReservation, bool, error)
	LookupDeploymentDestroy(ctx context.Context, connectionID, deploymentID string) (DeploymentDestroyReservation, bool, error)
	ReserveDeploymentDestroy(ctx context.Context, reservation DeploymentDestroyReservation) (stored DeploymentDestroyReservation, created bool, err error)
	FinalizeDeploymentDestroy(ctx context.Context, reservation DeploymentDestroyReservation, receipt Record) (stored Record, created bool, err error)
}

// ServiceBackupReservation consumes one exact device approval before any EBS
// snapshot is requested. Replays keep the same command identity so provider
// mutation identity cannot drift after a timeout or process restart.
type ServiceBackupReservation struct {
	ConnectionID       string
	BackupID           string
	ServiceID          string
	DeploymentID       string
	CommandID          string
	RequestSHA256      string
	ExpectedGeneration int64
	NodeCounter        int64
	ApprovalID         string
	ChallengeID        string
	SignerKeyID        string
	RequestJSON        []byte
	ResultJSON         []byte
	State              string
}

func (reservation ServiceBackupReservation) SameIdentity(other ServiceBackupReservation) bool {
	return reservation.ConnectionID == other.ConnectionID && reservation.BackupID == other.BackupID && reservation.ServiceID == other.ServiceID && reservation.DeploymentID == other.DeploymentID && reservation.CommandID == other.CommandID && reservation.RequestSHA256 == other.RequestSHA256 && reservation.ExpectedGeneration == other.ExpectedGeneration && reservation.NodeCounter == other.NodeCounter && reservation.ApprovalID == other.ApprovalID && reservation.ChallengeID == other.ChallengeID && reservation.SignerKeyID == other.SignerKeyID && string(reservation.RequestJSON) == string(other.RequestJSON)
}

type ServiceBackupRepository interface {
	Repository
	LookupDeployment(ctx context.Context, connectionID, deploymentID string) (DeploymentReservation, bool, error)
	LookupServiceBackup(ctx context.Context, connectionID, backupID string) (ServiceBackupReservation, bool, error)
	ReserveServiceBackup(ctx context.Context, reservation ServiceBackupReservation) (stored ServiceBackupReservation, created bool, err error)
	FinalizeServiceBackup(ctx context.Context, reservation ServiceBackupReservation, receipt Record) (stored Record, created bool, err error)
}

// ServiceRestoreReservation consumes the exact device approval before the
// first replacement volume is created. Its request remains immutable while
// AWS-side convergence or fallback resumes after timeouts and restarts.
type ServiceRestoreReservation struct {
	ConnectionID       string
	RestoreID          string
	ServiceID          string
	DeploymentID       string
	BackupID           string
	CommandID          string
	RequestSHA256      string
	ExpectedGeneration int64
	NodeCounter        int64
	ApprovalID         string
	ChallengeID        string
	SignerKeyID        string
	RequestJSON        []byte
	ResultJSON         []byte
	State              string
}

func (reservation ServiceRestoreReservation) SameIdentity(other ServiceRestoreReservation) bool {
	return reservation.ConnectionID == other.ConnectionID && reservation.RestoreID == other.RestoreID &&
		reservation.ServiceID == other.ServiceID && reservation.DeploymentID == other.DeploymentID &&
		reservation.BackupID == other.BackupID && reservation.CommandID == other.CommandID &&
		reservation.RequestSHA256 == other.RequestSHA256 && reservation.ExpectedGeneration == other.ExpectedGeneration &&
		reservation.NodeCounter == other.NodeCounter && reservation.ApprovalID == other.ApprovalID &&
		reservation.ChallengeID == other.ChallengeID && reservation.SignerKeyID == other.SignerKeyID
}

type ServiceRestoreRepository interface {
	Repository
	LookupDeployment(ctx context.Context, connectionID, deploymentID string) (DeploymentReservation, bool, error)
	LookupServiceBackup(ctx context.Context, connectionID, backupID string) (ServiceBackupReservation, bool, error)
	LookupServiceRestore(ctx context.Context, connectionID, restoreID string) (ServiceRestoreReservation, bool, error)
	ReserveServiceRestore(ctx context.Context, reservation ServiceRestoreReservation) (stored ServiceRestoreReservation, created bool, err error)
	FinalizeServiceRestore(ctx context.Context, reservation ServiceRestoreReservation, receipt Record) (stored Record, created bool, err error)
}

type Error struct{ Code string }

func (e *Error) Error() string {
	if e == nil || e.Code == "" {
		return "connection stack store error"
	}
	return "connection stack store error: " + e.Code
}

func NewError(code string) error { return &Error{Code: code} }

func Code(err error) string {
	var target *Error
	if errors.As(err, &target) && target.Code != "" {
		return target.Code
	}
	return "connection_stack_store_unavailable"
}
