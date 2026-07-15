package cloud

import (
	"context"
	"errors"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

var (
	ErrJobCancelInvalid        = errors.New("cloud job cancel is invalid")
	ErrJobCancelConflict       = errors.New("cloud job cancel conflicts with current state")
	ErrJobCancelNotCancellable = errors.New("cloud job is not cancellable")
	ErrJobCancelExpired        = errors.New("cloud job cancel approval has expired")
	ErrJobCancelSignature      = errors.New("cloud job cancel approval signature is invalid")
)

type JobCancelStore interface {
	PrepareCloudJobCancel(context.Context, PrepareJobCancelRequest) (PrepareJobCancelResult, error)
	ApproveCloudJobCancel(context.Context, ApproveJobCancelRequest) (ApproveJobCancelResult, error)
}

type PrepareJobCancelRequest struct {
	OwnerMXID        string
	JobID            string
	ExpectedRevision int64
	IdempotencyHash  string
	RequestDigest    string
	ApprovalID       string
	ChallengeID      string
	CreatedAt        int64
	ExpiresAt        int64
}

type JobCancelConfirmation struct {
	Job        Job                                `json:"job"`
	Deployment Deployment                         `json:"deployment"`
	Approval   cloudcontracts.JobCancelApprovalV1 `json:"approval"`
}

type PrepareJobCancelResult struct {
	Confirmation JobCancelConfirmation
	Created      bool
}

type ApproveJobCancelRequest struct {
	OwnerMXID         string
	JobID             string
	ExpectedRevision  int64
	IdempotencyHash   string
	RequestDigest     string
	Approval          cloudcontracts.JobCancelApprovalV1
	JobEventID        string
	DeploymentEventID string
	CreatedAt         int64
}

type ApproveJobCancelResult struct {
	Job        Job
	Deployment Deployment
	Created    bool
}
