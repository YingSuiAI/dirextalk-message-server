package cloud

import (
	"context"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

type ServiceBackupConfirmationStore interface {
	PrepareCloudServiceBackup(context.Context, PrepareServiceBackupRequest) (PrepareServiceBackupResult, error)
	ApproveCloudServiceBackup(context.Context, ApproveServiceBackupRequest) (ApproveServiceBackupResult, error)
}

type PrepareServiceBackupRequest struct {
	OwnerMXID        string
	ServiceID        string
	ExpectedRevision int64
	IdempotencyHash  string
	RequestDigest    string
	BackupID         string
	ApprovalID       string
	ChallengeID      string
	ExpiresAt        int64
	CreatedAt        int64
}

type ServiceBackupConfirmation struct {
	Service    Service                                `json:"service"`
	Deployment Deployment                             `json:"deployment"`
	Approval   cloudcontracts.ServiceBackupApprovalV1 `json:"approval"`
}

type PrepareServiceBackupResult struct {
	Confirmation ServiceBackupConfirmation
	Created      bool
}

type ApproveServiceBackupRequest struct {
	OwnerMXID        string
	ServiceID        string
	ExpectedRevision int64
	IdempotencyHash  string
	Approval         cloudcontracts.ServiceBackupApprovalV1
	JobID            string
	OutboxID         string
	JobEventID       string
	CreatedAt        int64
}

type ServiceBackup struct {
	BackupID        string   `json:"backup_id"`
	ServiceID       string   `json:"service_id"`
	DeploymentID    string   `json:"deployment_id"`
	Status          string   `json:"status"`
	RetentionPolicy string   `json:"retention_policy"`
	ImageID         string   `json:"image_id,omitempty"`
	SnapshotIDs     []string `json:"snapshot_ids,omitempty"`
	Revision        int64    `json:"revision"`
	CreatedAt       int64    `json:"created_at"`
	UpdatedAt       int64    `json:"updated_at"`
}

type ApproveServiceBackupResult struct {
	Service Service
	Backup  ServiceBackup
	Job     Job
	Created bool
}
