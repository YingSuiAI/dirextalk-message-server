package cloud

import (
	"context"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

// ServiceOperationConfirmationStore is the durable device-approval boundary
// for one exact lifecycle action compiled into the installed managed Recipe.
type ServiceOperationConfirmationStore interface {
	PrepareCloudServiceOperation(context.Context, PrepareServiceOperationRequest) (PrepareServiceOperationResult, error)
	ApproveCloudServiceOperation(context.Context, ApproveServiceOperationRequest) (ApproveServiceOperationResult, error)
}

type PrepareServiceOperationRequest struct {
	OwnerMXID        string
	ServiceID        string
	ExpectedRevision int64
	Operation        cloudcontracts.ServiceOperation
	IdempotencyHash  string
	RequestDigest    string
	ApprovalID       string
	ChallengeID      string
	ExpiresAt        int64
	CreatedAt        int64
}

type ServiceOperationConfirmation struct {
	Service    Service                                   `json:"service"`
	Deployment Deployment                                `json:"deployment"`
	Approval   cloudcontracts.ServiceOperationApprovalV1 `json:"approval"`
}

type PrepareServiceOperationResult struct {
	Confirmation ServiceOperationConfirmation
	Created      bool
}

type ApproveServiceOperationRequest struct {
	OwnerMXID        string
	ServiceID        string
	ExpectedRevision int64
	IdempotencyHash  string
	Approval         cloudcontracts.ServiceOperationApprovalV1
	OperationID      string
	JobID            string
	OutboxID         string
	JobEventID       string
	CreatedAt        int64
}

type ApproveServiceOperationResult struct {
	Service   Service
	Operation cloudcontracts.ServiceOperation
	Job       Job
	Created   bool
}
