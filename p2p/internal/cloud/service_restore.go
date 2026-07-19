package cloud

import (
	"context"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

type ServiceRestorePlanStore interface {
	CreateCloudServiceRestorePlan(context.Context, CreateServiceRestorePlanRequest) (CreateServiceRestorePlanResult, error)
}

type ServiceRestoreConfirmationStore interface {
	PrepareCloudServiceRestore(context.Context, PrepareServiceRestoreRequest) (PrepareServiceRestoreResult, error)
	ApproveCloudServiceRestore(context.Context, ApproveServiceRestoreRequest) (ApproveServiceRestoreResult, error)
}

type CreateServiceRestorePlanRequest struct {
	OwnerMXID, ServiceID, BackupID             string
	ExpectedRevision                           int64
	IdempotencyHash, RequestDigest             string
	RestorePlanID, JobID, OutboxID, JobEventID string
	CreatedAt                                  int64
}

type ServiceRestorePlan struct {
	RestorePlanID string `json:"restore_plan_id"`
	ServiceID     string `json:"service_id"`
	DeploymentID  string `json:"deployment_id"`
	BackupID      string `json:"backup_id"`
	Status        string `json:"status"`
	Revision      int64  `json:"revision"`
	CreatedAt     int64  `json:"created_at"`
	UpdatedAt     int64  `json:"updated_at"`
}

type CreateServiceRestorePlanResult struct {
	Plan    ServiceRestorePlan
	Job     Job
	Created bool
}

type PrepareServiceRestoreRequest struct {
	OwnerMXID, ServiceID, RestorePlanID                     string
	ExpectedRevision                                        int64
	IdempotencyHash, RequestDigest, ApprovalID, ChallengeID string
	CreatedAt, ExpiresAt                                    int64
}
type ServiceRestoreConfirmation struct {
	Service    Service                                 `json:"service"`
	Deployment Deployment                              `json:"deployment"`
	Plan       ServiceRestorePlan                      `json:"restore_plan"`
	Approval   cloudcontracts.ServiceRestoreApprovalV1 `json:"approval"`
}
type PrepareServiceRestoreResult struct {
	Confirmation ServiceRestoreConfirmation
	Created      bool
}
type ApproveServiceRestoreRequest struct {
	OwnerMXID, ServiceID, RestorePlanID string
	ExpectedRevision                    int64
	IdempotencyHash                     string
	Approval                            cloudcontracts.ServiceRestoreApprovalV1
	JobID, OutboxID, JobEventID         string
	CreatedAt                           int64
}
type ServiceRestore struct {
	RestoreID            string   `json:"restore_id"`
	RestorePlanID        string   `json:"restore_plan_id"`
	ServiceID            string   `json:"service_id"`
	DeploymentID         string   `json:"deployment_id"`
	BackupID             string   `json:"backup_id"`
	Status               string   `json:"status"`
	OriginalVolumeIDs    []string `json:"original_volume_ids,omitempty"`
	ReplacementVolumeIDs []string `json:"replacement_volume_ids,omitempty"`
	Revision             int64    `json:"revision"`
	CreatedAt            int64    `json:"created_at"`
	UpdatedAt            int64    `json:"updated_at"`
}
type ApproveServiceRestoreResult struct {
	Service Service
	Restore ServiceRestore
	Job     Job
	Created bool
}
