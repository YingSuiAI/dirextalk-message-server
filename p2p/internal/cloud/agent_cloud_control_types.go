package cloud

import (
	"context"
	"errors"
	"time"
)

var (
	ErrAgentCloudControlInvalid         = errors.New("agent cloud control request is invalid")
	ErrAgentCloudControlConflict        = errors.New("agent cloud control request conflicts with current state")
	ErrAgentCloudControlRejected        = errors.New("agent cloud control request was rejected")
	ErrAgentCloudControlUnavailable     = errors.New("agent cloud control service is unavailable")
	ErrAgentCloudControlInvalidResponse = errors.New("agent cloud control returned an invalid response")
	ErrAgentCloudConnectionNotFound     = errors.New("agent cloud connection was not found")
)

const (
	AgentCloudPlanStatusApproved                = "approved"
	AgentCloudGoalExecutionQueued               = "queued"
	AgentCloudGoalOutcomePending                = "pending"
	AgentCloudGoalRetentionEphemeralAutoDestroy = "ephemeral_auto_destroy"
	AgentCloudGoalPlanningResearchQueued        = "research_queued"
)

// AgentCloudControlClient is intentionally narrower than the Agent gRPC
// service. It can neither register approval devices, retrieve secrets, nor
// choose arbitrary provider resources for destruction.
type AgentCloudControlClient interface {
	CreateAgentCloudGoal(context.Context, AgentCloudGoalCreateRequest) (AgentCloudGoalResult, error)
	ListAgentCloudPlans(context.Context) ([]AgentCloudPlan, error)
	ListAgentCloudConnections(context.Context) ([]AgentCloudConnection, error)
	GetAgentCloudPlan(context.Context, AgentCloudPlanRequest) (AgentCloudPlan, bool, error)
	CreateAgentCloudApprovalChallenge(context.Context, AgentCloudChallengeRequest) (AgentCloudChallenge, error)
	ApproveAgentCloudPlan(context.Context, AgentCloudApproveRequest) (AgentCloudPlan, error)
	EstablishAgentAWSConnection(context.Context, AgentCloudEstablishRequest) (AgentCloudConnection, error)
	GetAgentCloudConnection(context.Context, AgentCloudConnectionRequest) (AgentCloudConnection, bool, error)
	CreateAgentCloudDeploymentDestroyChallenge(context.Context, AgentCloudDeploymentDestroyChallengeRequest) (AgentCloudDeploymentDestroyChallenge, error)
	ApproveAgentCloudDeploymentDestroy(context.Context, AgentCloudDeploymentDestroyApproveRequest) (AgentCloudDeploymentDestroyResult, error)
	GetAgentCloudDestroyOperation(context.Context, AgentCloudDestroyOperationRequest) (AgentCloudDestroyOperation, bool, error)
}

// AgentCloudFoundationClient is an optional, protocol-neutral capability for
// the independent Agent-owned Foundation lifecycle. Message Server only
// forwards owner-bound approval data and reads public operation state; it has
// no bootstrap-secret or provider capability on this surface.
type AgentCloudFoundationClient interface {
	CreateAgentAWSFoundationChallenge(context.Context, AgentCloudFoundationChallengeRequest) (AgentCloudFoundationChallenge, error)
	ApproveAgentAWSFoundation(context.Context, AgentCloudFoundationApproveRequest) (AgentCloudFoundationOperation, error)
	GetAgentAWSFoundationOperation(context.Context, AgentCloudFoundationOperationRequest) (AgentCloudFoundationOperation, bool, error)
}

type AgentCloudGoalCreateRequest struct {
	IdempotencyKey, ConnectionID, Goal, RecipeID, RetentionPolicy string
}

type AgentCloudGoalTask struct {
	TaskID, OwnerID, Goal, ExecutionStatus, OutcomeStatus, RetentionPolicy string
	CurrentStepID, ApprovedPlanID                                          string
	Revision                                                               int64
	CreatedAt, UpdatedAt                                                   time.Time
}

type AgentCloudGoalPlanning struct {
	TaskID, OwnerID, ConnectionID, RecipeID, State, RelatedPlanID string
}

type AgentCloudGoalResult struct {
	Task     AgentCloudGoalTask
	Planning AgentCloudGoalPlanning
}

// AgentCloudPlanningClient is an optional, protocol-neutral seam for the
// independent Agent's quote and plan commands. Keeping it separate from
// AgentCloudControlClient avoids widening the established approval/deployment
// surface for callers which only need status reads.
type AgentCloudPlanningClient interface {
	CreateAgentCloudQuote(context.Context, AgentCloudQuoteCreateRequest) (AgentCloudQuote, error)
	GetAgentCloudQuote(context.Context, AgentCloudQuoteRequest) (AgentCloudQuote, bool, error)
	CreateAgentCloudPlan(context.Context, AgentCloudPlanCreateRequest) (AgentCloudPlan, error)
	GetAgentCloudPlan(context.Context, AgentCloudPlanRequest) (AgentCloudPlan, bool, error)
}

type AgentCloudPlanRequest struct{ PlanID string }
type AgentCloudConnectionRequest struct{ ConnectionID string }

type AgentCloudQuoteRequest struct{ QuoteID string }

type AgentCloudQuoteCreateRequest struct {
	IdempotencyKey          string
	Scopes                  []AgentCloudQuoteScope
	Usage                   AgentCloudUsageEstimate
	SpotQualification       *AgentCloudSpotQualification
	BootstrapSessionID      string
	ExpectedSessionRevision int64
}

type AgentCloudPlanCreateRequest struct {
	IdempotencyKey, QuoteID, CandidateProfile string
	CurrentScope                              AgentCloudQuoteScope
}

type AgentCloudQuoteScope struct {
	ConnectionID     string
	Recipe           AgentCloudRecipeBinding
	Resource         AgentCloudResourceScope
	Network          AgentCloudNetworkScope
	SecretScope      []AgentCloudSecretScope
	IntegrationScope []AgentCloudIntegrationScope
	Retention        AgentCloudRetentionScope
}

type AgentCloudUsageEstimate struct {
	RuntimeHoursPerMonth, PublicIPv4Hours, EntryHours uint32
	LogIngestMiB, LogStoredMiBMonths                  uint64
	SnapshotGiBMonths, InternetEgressMiB              uint64
}

type AgentCloudSpotQualification struct {
	EvidenceID, RecipeDigest, CheckpointName, ResumeAction string
	MaxRetries                                             uint32
	CheckpointVerifiedAt, InterruptionTestedAt             time.Time
}

type AgentCloudCostItem struct {
	Category, Description, SourceID                                        string
	HourlyEstimateMicros, MonthlyEstimateMicros, MaximumLaunchAmountMicros uint64
}

type AgentCloudQuotaEvidence struct {
	ServiceCode, QuotaCode               string
	LimitUnits, UsedUnits, RequiredUnits uint64
}

type AgentCloudQuoteCandidate struct {
	CandidateProfile                            string
	Scope                                       AgentCloudQuoteScope
	ScopeDigest                                 string
	OfferedAvailabilityZones                    []string
	Quotas                                      []AgentCloudQuotaEvidence
	CostItems                                   []AgentCloudCostItem
	HourlyEstimateMicros, MonthlyEstimateMicros uint64
	MaximumLaunchAmountMicros                   uint64
}

type AgentCloudQuote struct {
	QuoteID, Currency, Digest string
	QuotedAt, ValidUntil      time.Time
	Candidates                []AgentCloudQuoteCandidate
	Usage                     AgentCloudUsageEstimate
	Assumptions, Exclusions   []string
	SpotQualification         *AgentCloudSpotQualification
}

type AgentCloudPlan struct {
	PlanID, OwnerID, ConnectionID          string
	Recipe                                 AgentCloudRecipeBinding
	QuoteID, QuoteDigest, QuoteScopeDigest string
	CandidateProfile                       string
	QuoteValidUntil                        time.Time
	Resource                               AgentCloudResourceScope
	Network                                AgentCloudNetworkScope
	SecretScope                            []AgentCloudSecretScope
	IntegrationScope                       []AgentCloudIntegrationScope
	Retention                              AgentCloudRetentionScope
	Status, PlanHash                       string
	Revision                               int64
}

type AgentCloudRecipeBinding struct{ RecipeID, Digest, Maturity string }

type AgentCloudResourceScope struct {
	CandidateProfile      string
	Region                string
	AvailabilityZones     []string
	InstanceType          string
	InstanceCount         uint32
	Architecture          string
	VCPU                  uint32
	MemoryMiB             uint64
	GPUType               string
	GPUCount              uint32
	GPUMemoryMiB          uint64
	DiskGiB               uint64
	VolumeType            string
	VolumeIOPS            uint32
	VolumeThroughputMiBPS uint32
	VolumeEncrypted       bool
	PurchaseOption        string
	WorkerImageID         string
	WorkerImageDigest     string
	VolumeScopes          []AgentCloudVolumeScope
}

// AgentCloudVolumeScope is an approval- and quote-bound data volume. It is
// deliberately separate from DiskGiB, which describes only the Worker root
// disk and cannot satisfy a persistent Recipe volume slot.
type AgentCloudVolumeScope struct {
	SlotID          string
	SizeGiB         uint32
	VolumeType      string
	IOPS            uint32
	ThroughputMiBPS uint32
	Encrypted       bool
	KMSKeyID        string
	DeviceName      string
	MountPath       string
	ReadOnly        bool
	Persistent      bool
	Disposition     string
}

type AgentCloudNetworkScope struct {
	VPCID, SubnetID, SecurityGroupID    string
	SecurityGroupMode                   string
	EntryPoint                          string
	PublicIPv4                          bool
	PublicExposure                      bool
	IngressPorts                        []uint32
	Hostname                            string
	TLSRequired, AuthenticationRequired bool
}

type AgentCloudSecretScope struct{ SecretRef, Purpose, Delivery string }
type AgentCloudIntegrationScope struct {
	Kind, Name string
	Scopes     []string
}
type AgentCloudRetentionScope struct {
	Class              string
	AutoDestroy        bool
	GracePeriodSeconds uint32
	MaxLifetimeSeconds uint64
}

// ExpectedPlan is the owner-bound plan previously read from Agent. It lets the
// transport reject signed-scope substitution in challenge/approval responses.
type AgentCloudChallengeRequest struct {
	IdempotencyKey, PlanID, SignerKeyID string
	ExpectedRevision                    int64
	ExpectedPlan                        AgentCloudPlan
}

type AgentCloudChallenge struct {
	ApprovalID, ChallengeID, SignerKeyID                     string
	AgentInstanceID, OwnerID, PlanID, PlanHash               string
	ConnectionID, RecipeDigest                               string
	QuoteID, QuoteDigest, QuoteScopeDigest, QuoteCandidateID string
	PlanRevision, Revision                                   int64
	ExpiresAt                                                time.Time
	SigningPayloadCBOR                                       []byte
}

type AgentCloudApprovalSignature struct {
	ApprovalID, ChallengeID, SignerKeyID string
	ExpiresAt                            time.Time
	Signature                            []byte
}

type AgentCloudApproveRequest struct {
	IdempotencyKey, PlanID string
	ExpectedRevision       int64
	ExpectedPlan           AgentCloudPlan
	Approval               AgentCloudApprovalSignature
}

type AgentCloudEstablishRequest struct {
	IdempotencyKey, BootstrapSessionID   string
	ExpectedSessionRevision              int64
	PlanID                               string
	ExpectedPlanRevision                 int64
	Approval                             AgentCloudApprovalSignature
	ExpectedConnectionID, ExpectedRegion string
}

type AgentCloudConnection struct {
	ConnectionID, OwnerID, AccountID, Region  string
	ControlRoleARN, FoundationStackID, Status string
	Revision, CredentialGeneration            int64
	CreatedAt, UpdatedAt                      time.Time
}

type AgentCloudFoundationChallengeRequest struct {
	IdempotencyKey, Action, ConnectionID, BootstrapSessionID, SignerKeyID string
	ExpectedBootstrapRevision                                             int64
}

type AgentCloudFoundationReleaseEnvironment struct {
	PrivateSubnetCIDR string `json:"private_subnet_cidr"`
	ZeroIngress       bool   `json:"zero_ingress"`
	ArtifactBucket    string `json:"artifact_bucket"`
	KMSAlias          string `json:"kms_alias"`
	BucketVersioned   bool   `json:"bucket_versioned"`
	BucketSSEKMS      bool   `json:"bucket_sse_kms"`
}

type AgentCloudFoundationScope struct {
	SchemaVersion                string                                 `json:"schema_version"`
	AgentInstanceID              string                                 `json:"agent_instance_id"`
	OwnerID                      string                                 `json:"owner_id"`
	Action                       string                                 `json:"action"`
	ConnectionID                 string                                 `json:"connection_id"`
	ExpectedConnectionRevision   int64                                  `json:"expected_connection_revision"`
	AccountID                    string                                 `json:"account_id"`
	Region                       string                                 `json:"region"`
	BootstrapSessionID           string                                 `json:"bootstrap_session_id"`
	ExpectedBootstrapRevision    int64                                  `json:"expected_bootstrap_revision"`
	ExpectedCredentialGeneration int64                                  `json:"expected_credential_generation"`
	FoundationTemplateDigest     string                                 `json:"foundation_template_digest"`
	ReaperImageURI               string                                 `json:"reaper_image_uri"`
	ReleaseEnvironment           AgentCloudFoundationReleaseEnvironment `json:"release_environment"`
	IdentityObservedAt           time.Time                              `json:"identity_observed_at"`
	IdentityExpiresAt            time.Time                              `json:"identity_expires_at"`
}

type AgentCloudFoundationChallenge struct {
	OperationID, ChallengeID, ApprovalID, SignerKeyID, ScopeDigest string
	Scope                                                          AgentCloudFoundationScope
	ExpiresAt                                                      time.Time
	SigningPayloadCBOR                                             []byte
	Revision                                                       int64
}

type AgentCloudFoundationApproveRequest struct {
	IdempotencyKey, ExpectedOperationID, ExpectedAction, ExpectedConnectionID, ExpectedScopeDigest string
	ExpectedRevision                                                                               int64
	Approval                                                                                       AgentCloudApprovalSignature
}

type AgentCloudFoundationOperationRequest struct{ OperationID string }

type AgentCloudFoundationOperation struct {
	OperationID, OwnerID, ConnectionID, Action, ApprovalID string
	ScopeDigest, Status, ErrorCode, BlockedReason          string
	Revision                                               int64
	CreatedAt, UpdatedAt                                   time.Time
}

type AgentCloudDeploymentDestroyChallengeRequest struct {
	IdempotencyKey, DeploymentID, SignerKeyID string
	ExpectedRevision                          int64
	ExpectedDeployment                        Deployment
}

type AgentCloudResourceReadBack struct {
	Observed   bool      `json:"observed"`
	Exists     bool      `json:"exists"`
	ProviderID string    `json:"provider_id"`
	ObservedAt time.Time `json:"observed_at"`
	TagDigest  string    `json:"tag_digest"`
}

type AgentCloudDestroyResourceScope struct {
	ResourceID           string                     `json:"resource_id"`
	Type                 string                     `json:"type"`
	ProviderID           string                     `json:"provider_id"`
	Revision             int64                      `json:"revision"`
	DependsOnResourceIDs []string                   `json:"depends_on_resource_ids"`
	RetentionPolicy      string                     `json:"retention_policy"`
	DestroyDeadline      time.Time                  `json:"destroy_deadline"`
	AutoDestroyApproved  bool                       `json:"auto_destroy_approved"`
	Status               string                     `json:"status"`
	Region               string                     `json:"region"`
	SpecDigest           string                     `json:"spec_digest"`
	ApprovedPlanHash     string                     `json:"approved_plan_hash"`
	OriginalApprovalID   string                     `json:"original_approval_id"`
	ReadBack             AgentCloudResourceReadBack `json:"read_back"`
}

type AgentCloudDeploymentDestroyScope struct {
	SchemaVersion      string                           `json:"schema_version"`
	AgentInstanceID    string                           `json:"agent_instance_id"`
	OwnerID            string                           `json:"owner_id"`
	DeploymentID       string                           `json:"deployment_id"`
	DeploymentRevision int64                            `json:"deployment_revision"`
	TaskID             string                           `json:"task_id"`
	PlanID             string                           `json:"plan_id"`
	PlanHash           string                           `json:"plan_hash"`
	ConnectionID       string                           `json:"connection_id"`
	Resources          []AgentCloudDestroyResourceScope `json:"resources"`
}

type AgentCloudDeploymentDestroyChallenge struct {
	OperationID, ChallengeID, ApprovalID, SignerKeyID string
	Scope                                             AgentCloudDeploymentDestroyScope
	ExpiresAt                                         time.Time
	SigningPayloadCBOR                                []byte
	Revision                                          int64
}

type AgentCloudDeploymentDestroyApproveRequest struct {
	IdempotencyKey, DeploymentID, ExpectedOperationID string
	ExpectedRevision                                  int64
	ExpectedDeployment                                Deployment
	Approval                                          AgentCloudApprovalSignature
}

type AgentCloudDestroyOperationRequest struct{ OperationID string }

type AgentCloudDestroyOperation struct {
	OperationID, OwnerID, DeploymentID, ApprovalID string
	ScopeDigest, Status, ErrorCode, BlockedReason  string
	AutomaticAttempts                              int32
	NextAttemptAt                                  *time.Time
	RequiresNewApproval                            bool
	Revision                                       int64
	CreatedAt, UpdatedAt                           time.Time
}

type AgentCloudDeploymentDestroyResult struct {
	Operation  AgentCloudDestroyOperation
	Deployment Deployment
}
