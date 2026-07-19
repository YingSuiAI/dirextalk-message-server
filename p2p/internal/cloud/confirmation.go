package cloud

import (
	"context"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

// PlanConfirmationStore is deliberately narrower than Store because plan
// confirmation is a high-risk durable transition. The ProductCore facade uses
// it only when its backing store can atomically bind the selected quote,
// persisted PlanV1, device approval challenge, Deployment, Job, events, and
// private provision outbox.
//
// Memory-only stores intentionally do not implement this interface. They may
// still serve read-only Cloud projections, but cannot claim that a spend-bound
// approval or deployment request has been durably recorded.
type PlanConfirmationStore interface {
	PrepareCloudPlanConfirmation(context.Context, PreparePlanConfirmationRequest) (PreparePlanConfirmationResult, error)
	ApproveCloudPlan(context.Context, ApproveCloudPlanRequest) (ApproveCloudPlanResult, error)
}

// PreparePlanConfirmationRequest selects one already quoted tier. The network,
// secret and integration scopes are intentionally fixed to the first-release
// safe defaults here: no public ingress, no secret delivery and no integration.
// Later scoped plans must be separate revisioned transitions rather than extra
// mutable parameters on this spend approval.
type PreparePlanConfirmationRequest struct {
	OwnerMXID        string
	PlanID           string
	ExpectedRevision int64
	QuoteID          string
	CandidateTier    string
	IdempotencyHash  string
	RequestDigest    string
	ApprovalID       string
	ChallengeID      string
	ExpiresAt        int64
	CreatedAt        int64
}

// PlanConfirmation is the owner-visible signature challenge. It contains no
// AWS endpoint, resource ID, secret value or Worker enrollment material.
type PlanConfirmation struct {
	Plan     Plan                      `json:"plan"`
	Approval cloudcontracts.ApprovalV1 `json:"approval"`
}

type PreparePlanConfirmationResult struct {
	Confirmation PlanConfirmation
	EventID      string
	Created      bool
}

// ApproveCloudPlanRequest includes the exact Flutter-signed ApprovalV1. Its
// authenticity is verified only in the durable store after it locks the
// current plan, quote and registered device public key.
type ApproveCloudPlanRequest struct {
	OwnerMXID         string
	PlanID            string
	ExpectedRevision  int64
	IdempotencyHash   string
	Approval          cloudcontracts.ApprovalV1
	Deployment        Deployment
	Job               Job
	Outbox            OutboxEntry
	PlanEventID       string
	DeploymentEventID string
	JobEventID        string
	CreatedAt         int64
}

type ApproveCloudPlanResult struct {
	Plan       Plan
	Deployment Deployment
	Job        Job
	Created    bool
}
