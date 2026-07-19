package cloud

import (
	"context"
	"errors"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

var (
	ErrDeploymentDestroyInvalid   = errors.New("cloud deployment destroy is invalid")
	ErrDeploymentDestroyConflict  = errors.New("cloud deployment destroy conflicts with current state")
	ErrDeploymentDestroyExpired   = errors.New("cloud deployment destroy approval has expired")
	ErrDeploymentDestroySignature = errors.New("cloud deployment destroy approval signature is invalid")
)

type DeploymentDestroyStore interface {
	PrepareCloudDeploymentDestroy(context.Context, PrepareDeploymentDestroyRequest) (PrepareDeploymentDestroyResult, error)
	ApproveCloudDeploymentDestroy(context.Context, ApproveDeploymentDestroyRequest) (ApproveDeploymentDestroyResult, error)
}

type PrepareDeploymentDestroyRequest struct {
	OwnerMXID        string
	DeploymentID     string
	ExpectedRevision int64
	IdempotencyHash  string
	RequestDigest    string
	ApprovalID       string
	ChallengeID      string
	CreatedAt        int64
	ExpiresAt        int64
}

type DeploymentDestroyConfirmation struct {
	Deployment Deployment                                 `json:"deployment"`
	Approval   cloudcontracts.DeploymentDestroyApprovalV1 `json:"approval"`
}

type PrepareDeploymentDestroyResult struct {
	Confirmation DeploymentDestroyConfirmation
	Created      bool
}

type ApproveDeploymentDestroyRequest struct {
	OwnerMXID         string
	DeploymentID      string
	ExpectedRevision  int64
	IdempotencyHash   string
	RequestDigest     string
	Approval          cloudcontracts.DeploymentDestroyApprovalV1
	JobID             string
	OutboxID          string
	DeploymentEventID string
	JobEventID        string
	CreatedAt         int64
}

type ApproveDeploymentDestroyResult struct {
	Deployment Deployment
	Job        Job
	Created    bool
}
