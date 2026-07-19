package cloud

import (
	"context"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

// ServiceDestroyConfirmationStore is the device-approval boundary for one
// exact tracked resource set. Memory-only stores intentionally do not
// implement it, so a facade without durable atomicity fails closed.
type ServiceDestroyConfirmationStore interface {
	PrepareCloudServiceDestroy(context.Context, PrepareServiceDestroyRequest) (PrepareServiceDestroyResult, error)
	ApproveCloudServiceDestroy(context.Context, ApproveServiceDestroyRequest) (ApproveServiceDestroyResult, error)
}

type PrepareServiceDestroyRequest struct {
	OwnerMXID        string
	ServiceID        string
	ExpectedRevision int64
	IdempotencyHash  string
	RequestDigest    string
	ApprovalID       string
	ChallengeID      string
	ExpiresAt        int64
	CreatedAt        int64
}

type ServiceDestroyConfirmation struct {
	Service    Service                                 `json:"service"`
	Deployment Deployment                              `json:"deployment"`
	Approval   cloudcontracts.ServiceDestroyApprovalV1 `json:"approval"`
}

type PrepareServiceDestroyResult struct {
	Confirmation ServiceDestroyConfirmation
	Created      bool
}

type ApproveServiceDestroyRequest struct {
	OwnerMXID         string
	ServiceID         string
	ExpectedRevision  int64
	IdempotencyHash   string
	Approval          cloudcontracts.ServiceDestroyApprovalV1
	JobID             string
	OutboxID          string
	ServiceEventID    string
	DeploymentEventID string
	JobEventID        string
	CreatedAt         int64
}

type ApproveServiceDestroyResult struct {
	Service    Service
	Deployment Deployment
	Job        Job
	Created    bool
}
