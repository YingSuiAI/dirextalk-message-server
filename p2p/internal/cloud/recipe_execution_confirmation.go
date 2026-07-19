package cloud

import (
	"context"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

// RecipeExecutionConfirmationStore is a separate durable approval boundary for
// a compiled Recipe execution. It deliberately does not reuse the spend-bound
// PlanConfirmationStore: an approved Plan cannot by itself start a root-capable
// execution on a Worker.
//
// Memory-only stores intentionally do not implement this interface. They can
// project Cloud state, but cannot claim that a device-authorized execution and
// its private install intent were atomically persisted.
type RecipeExecutionConfirmationStore interface {
	PrepareCloudRecipeExecutionConfirmation(context.Context, PrepareRecipeExecutionConfirmationRequest) (PrepareRecipeExecutionConfirmationResult, error)
	ApproveCloudRecipeExecution(context.Context, ApproveRecipeExecutionRequest) (ApproveRecipeExecutionResult, error)
}

// TrustedRecipeExecutionManifestStore is an internal Orchestrator-only ingress
// for a compiler-validated sealed manifest. It is intentionally separate from
// Store and RecipeExecutionConfirmationStore so no ProductCore action, native
// Agent tool, MCP tool, or client request can submit a manifest, artifact
// digest, command, URL, or secret slot.
type TrustedRecipeExecutionManifestStore interface {
	RegisterTrustedCloudRecipeExecutionManifest(context.Context, RegisterTrustedRecipeExecutionManifestRequest) (RegisterTrustedRecipeExecutionManifestResult, error)
}

// RecipeExecution is the de-secreted durable summary returned to the owner.
// It has no command, artifact bytes, URL, host path, provider receipt, or
// secret value/reference beyond the sealed manifest digest.
type RecipeExecution struct {
	ExecutionID                   string `json:"execution_id"`
	DeploymentID                  string `json:"deployment_id"`
	PlanID                        string `json:"plan_id"`
	RecipeExecutionManifestDigest string `json:"recipe_execution_manifest_digest"`
	Status                        string `json:"status"`
	Revision                      int64  `json:"revision"`
}

// RegisterTrustedRecipeExecutionManifestRequest is accepted only on the
// internal trusted compiler path. Manifest identity is its idempotency key:
// the same execution ID and digest replay, while a different digest or a
// second execution for the same deployment conflicts.
type RegisterTrustedRecipeExecutionManifestRequest struct {
	Manifest     cloudcontracts.RecipeExecutionManifestV1
	RegisteredAt int64
}

type RegisterTrustedRecipeExecutionManifestResult struct {
	Execution RecipeExecution
	Created   bool
}

// PrepareRecipeExecutionConfirmationRequest contains only the target
// Deployment revision and device-approval metadata. The trusted manifest is
// looked up inside the transaction; it is never supplied by the client.
type PrepareRecipeExecutionConfirmationRequest struct {
	OwnerMXID        string
	DeploymentID     string
	ExpectedRevision int64
	IdempotencyHash  string
	RequestDigest    string
	ApprovalID       string
	ChallengeID      string
	ExpiresAt        int64
	CreatedAt        int64
}

// RecipeExecutionConfirmation is the owner-visible short-lived signing
// challenge. It contains only the execution summary and the signed scope; it
// never contains a command, artifact content, secret value, or AWS control
// material.
type RecipeExecutionConfirmation struct {
	Execution RecipeExecution                          `json:"execution"`
	Approval  cloudcontracts.RecipeExecutionApprovalV1 `json:"approval"`
}

type PrepareRecipeExecutionConfirmationResult struct {
	Confirmation RecipeExecutionConfirmation
	Created      bool
}

// ApproveRecipeExecutionRequest contains the exact device-signed approval
// challenge. The Store resolves the trusted execution from that sealed scope
// and creates the fixed private outbox payload itself; callers cannot choose
// arbitrary outbox JSON.
type ApproveRecipeExecutionRequest struct {
	OwnerMXID        string
	DeploymentID     string
	ExpectedRevision int64
	IdempotencyHash  string
	Approval         cloudcontracts.RecipeExecutionApprovalV1
	Job              Job
	OutboxID         string
	JobEventID       string
	CreatedAt        int64
}

type ApproveRecipeExecutionResult struct {
	Execution RecipeExecution
	Job       Job
	Created   bool
}
