package cloud

import (
	"context"
	"errors"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const OutboxKindDeploymentPairingResumeRequested = "cloud.deployment.pairing_resume.requested"

var (
	ErrPairingResumeConflict  = errors.New("cloud pairing resume conflicts with the current deployment")
	ErrPairingResumeInvalid   = errors.New("cloud pairing resume is invalid")
	ErrPairingResumeExpired   = errors.New("cloud pairing resume approval has expired")
	ErrPairingResumeSignature = errors.New("cloud pairing resume approval signature is invalid")
)

type PairingResumeStore interface {
	PrepareCloudPairingResume(context.Context, PreparePairingResumeRequest) (PreparePairingResumeResult, error)
	ApproveCloudPairingResume(context.Context, ApprovePairingResumeRequest) (ApprovePairingResumeResult, error)
}

type PreparePairingResumeRequest struct {
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

type PairingResumeConfirmation struct {
	Deployment Deployment                             `json:"deployment"`
	Job        Job                                    `json:"job"`
	Approval   cloudcontracts.PairingResumeApprovalV1 `json:"approval"`
}

type PreparePairingResumeResult struct {
	Confirmation PairingResumeConfirmation
	Created      bool
}

type ApprovePairingResumeRequest struct {
	OwnerMXID         string
	DeploymentID      string
	ExpectedRevision  int64
	IdempotencyHash   string
	RequestDigest     string
	Approval          cloudcontracts.PairingResumeApprovalV1
	OutboxID          string
	DeploymentEventID string
	JobEventID        string
	CreatedAt         int64
}

type ApprovePairingResumeResult struct {
	Deployment Deployment
	Job        Job
	Created    bool
}
