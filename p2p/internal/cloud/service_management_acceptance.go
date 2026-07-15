package cloud

import (
	"context"
	"errors"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

type ServiceManagementAcceptanceStore interface {
	PrepareCloudServiceManagementAcceptance(context.Context, PrepareServiceManagementAcceptanceRequest) (PrepareServiceManagementAcceptanceResult, error)
	ApproveCloudServiceManagementAcceptance(context.Context, ApproveServiceManagementAcceptanceRequest) (ApproveServiceManagementAcceptanceResult, error)
}

type PrepareServiceManagementAcceptanceRequest struct {
	OwnerMXID        string
	ServiceID        string
	ExpectedRevision int64
	IdempotencyHash  string
	RequestDigest    string
	AcceptanceID     string
	ApprovalID       string
	ChallengeID      string
	ServiceEventID   string
	CreatedAt        int64
	ExpiresAt        int64
}

type ServiceManagementAcceptanceConfirmation struct {
	Service  Service                                              `json:"service"`
	Recipe   Recipe                                               `json:"recipe"`
	Approval cloudcontracts.ServiceManagementAcceptanceApprovalV1 `json:"approval"`
}

type PrepareServiceManagementAcceptanceResult struct {
	Confirmation   ServiceManagementAcceptanceConfirmation `json:"confirmation"`
	Created        bool                                    `json:"created"`
	ServiceChanged bool                                    `json:"-"`
}

type ApproveServiceManagementAcceptanceRequest struct {
	OwnerMXID        string
	ServiceID        string
	ExpectedRevision int64
	IdempotencyHash  string
	Approval         cloudcontracts.ServiceManagementAcceptanceApprovalV1
	ServiceEventID   string
	CreatedAt        int64
}

type ServiceManagementAcceptance struct {
	AcceptanceID string `json:"acceptance_id"`
	ServiceID    string `json:"service_id"`
	RecipeID     string `json:"recipe_id"`
	Status       string `json:"status"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

type ApproveServiceManagementAcceptanceResult struct {
	Service    Service                     `json:"service"`
	Recipe     Recipe                      `json:"recipe"`
	Acceptance ServiceManagementAcceptance `json:"acceptance"`
	Created    bool                        `json:"created"`
}

var (
	ErrServiceManagementAcceptanceInvalid   = errors.New("service management acceptance is invalid")
	ErrServiceManagementAcceptanceConflict  = errors.New("service management acceptance conflicts with current state")
	ErrServiceManagementAcceptanceExpired   = errors.New("service management acceptance has expired")
	ErrServiceManagementAcceptanceSignature = errors.New("service management acceptance signature is invalid")
)
