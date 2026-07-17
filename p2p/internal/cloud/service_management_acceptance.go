package cloud

import (
	"context"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

// AgentCloudManagedAcceptanceClient is the protocol-neutral compatibility
// boundary for Agent-owned management acceptance. Message Server may use its
// legacy Service projection only to resolve the Deployment id; Agent must
// revalidate every authoritative Service, Recipe, runtime and cloud binding.
type AgentCloudManagedAcceptanceClient interface {
	CreateCloudManagedAcceptanceChallenge(context.Context, AgentCloudManagedAcceptanceChallengeRequest) (AgentCloudManagedAcceptanceChallenge, error)
	ApproveCloudManagedAcceptance(context.Context, AgentCloudManagedAcceptanceApproveRequest) (AgentCloudManagedAcceptanceOperation, error)
	GetCloudManagedAcceptanceOperation(context.Context, AgentCloudManagedAcceptanceOperationRequest) (AgentCloudManagedAcceptanceOperation, bool, error)
}

type AgentCloudManagedAcceptanceChallengeRequest struct {
	IdempotencyKey             string
	ServiceID                  string
	DeploymentID               string
	SignerKeyID                string
	ExpectedDeploymentRevision int64
}

type AgentCloudManagedAcceptanceChallenge struct {
	OperationID  string
	OwnerID      string
	ScopeDigest  string
	Revision     int64
	Confirmation ServiceManagementAcceptanceConfirmation
}

type AgentCloudManagedAcceptanceApproveRequest struct {
	IdempotencyKey            string
	OperationID               string
	ServiceID                 string
	DeploymentID              string
	ExpectedServiceRevision   int64
	ExpectedOperationRevision int64
	ExpectedScopeDigest       string
	Approval                  cloudcontracts.ServiceManagementAcceptanceApprovalV1
	ApprovalSignature         AgentCloudApprovalSignature
}

type AgentCloudManagedAcceptanceOperationRequest struct {
	OperationID string
}

type AgentCloudManagedAcceptanceOperation struct {
	OperationID  string
	OwnerID      string
	ApprovalID   string
	DeploymentID string
	ScopeDigest  string
	Status       string
	Revision     int64
	Service      Service
	Recipe       Recipe
	Acceptance   ServiceManagementAcceptance
}

type ManagedAcceptanceCompatibilityReader interface {
	GetCloudManagedAcceptanceCompatibility(context.Context, string, string) (ManagedAcceptanceCompatibility, bool, error)
}

type ManagedAcceptanceCompatibility struct {
	DeploymentID       string
	DeploymentRevision int64
	SignerKeyID        string
}

type ServiceManagementAcceptanceConfirmation struct {
	Service  Service                                              `json:"service"`
	Recipe   Recipe                                               `json:"recipe"`
	Approval cloudcontracts.ServiceManagementAcceptanceApprovalV1 `json:"approval"`
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
