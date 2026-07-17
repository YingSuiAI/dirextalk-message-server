// Package cloud owns the ProductCore facade's durable cloud-control records.
//
// It deliberately contains no AWS SDK client. Cloud mutations are represented
// as durable outbox entries for the separately deployed Cloud Orchestrator.
package cloud

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const (
	PlanStatusResearching           = "researching"
	PlanStatusQuoting               = "quoting"
	PlanStatusReadyForConfirmation  = "ready_for_confirmation"
	PlanStatusApproved              = "approved"
	PlanStatusExpired               = "expired"
	PlanStatusSuperseded            = "superseded"
	GoalStatusResearching           = "researching"
	OutboxKindResearchGoalRequested = "cloud.goal.research.requested"
	// OutboxKindConnectionRegistrationRequested is private control-plane work.
	// It is never delivered to ProductCore directly: the independent
	// Orchestrator must first prove the signed Broker endpoint before a public
	// Connection record can become active.
	OutboxKindConnectionRegistrationRequested = "cloud.connection.registration.requested"
	// OutboxKindDeploymentProvisionRequested is private control-plane work. The
	// Worker/provisioning runner consumes it later; ProductCore projects only
	// the de-secretsed Deployment and Job state created with the outbox row.
	OutboxKindDeploymentProvisionRequested = "cloud.deployment.provision.requested"
	// OutboxKindExecutionProbeIssueRequested is private control-plane work
	// created only after a dedicated Worker bootstrap lease is independently
	// verified. It carries sealed digest-only task intent and is never delivered
	// to ProductCore directly.
	OutboxKindExecutionProbeIssueRequested = "cloud.execution_probe.issue.requested"
	// OutboxKindRecipeExecutionInstallRequested is a sealed, digest-only
	// control-plane intent. It does not issue a Worker task yet and may only be
	// created after the separate RecipeExecutionApprovalV1 is verified.
	OutboxKindRecipeExecutionInstallRequested = "cloud.recipe_execution.install.requested"
	// OutboxKindServiceReadinessRequested is created only after a sealed Recipe
	// install succeeds. The Stack generates the fresh challenge; this outbox
	// contains no Worker-selected probe target or execution material.
	OutboxKindServiceReadinessRequested = "cloud.service_readiness.requested"
	// OutboxKindServiceOperationRequested is emitted only after a device
	// signature binds an installed managed artifact and one fixed lifecycle
	// action. It contains no command, path, URL, secret, or caller-selected
	// Worker execution material.
	OutboxKindServiceOperationRequested = "cloud.service.operation.requested"
	// OutboxKindServiceDestroyRequested is emitted only after a fresh device
	// signature binds the exact tracked EC2/EBS/ENI set. The Orchestrator, not
	// ProductCore, later issues the typed Stack command and verifies read-back.
	OutboxKindServiceDestroyRequested = "cloud.service.destroy.requested"
	// OutboxKindDeploymentDestroyRequested is the service-independent fallback
	// for retained, blocked, or orphaned Deployment resources. It carries only
	// the exact device-approved private resource scope.
	OutboxKindDeploymentDestroyRequested = "cloud.deployment.destroy.requested"
	// OutboxKindServiceBackupRequested is emitted only after a device-approved
	// exact snapshot scope has been durably committed.
	OutboxKindServiceBackupRequested = "cloud.service.backup.requested"
	// OutboxKindServiceRestorePlanRequested is read-only planning work. Its
	// payload is derived from one retained backup and the tracked original
	// instance; clients cannot supply provider resources or device mappings.
	OutboxKindServiceRestorePlanRequested = "cloud.service.restore.plan.requested"
	OutboxKindServiceRestoreRequested     = "cloud.service.restore.requested"

	ConnectionBootstrapAwaitingStack      = "awaiting_stack"
	ConnectionBootstrapVerificationQueued = "verification_queued"
	ConnectionBootstrapVerifying          = "verifying"
	ConnectionBootstrapActive             = "active"
	ConnectionBootstrapVerificationFailed = "verification_failed"
	ConnectionBootstrapExpired            = "expired"
)

var (
	ErrIdempotencyConflict                  = errors.New("cloud idempotency key was reused with a different request")
	ErrSelectedRecipeConflict               = errors.New("selected cloud recipe conflicts with current authoritative state")
	ErrConnectionBootstrapConflict          = errors.New("cloud connection bootstrap conflicts with the requested revision")
	ErrConnectionBootstrapExpired           = errors.New("cloud connection bootstrap has expired")
	ErrConnectionBootstrapInvalid           = errors.New("cloud connection bootstrap is not in a completable state")
	ErrConnectionBootstrapInputInvalid      = errors.New("cloud connection bootstrap registration input is invalid")
	ErrPlanConfirmationConflict             = errors.New("cloud plan confirmation conflicts with the requested revision")
	ErrPlanConfirmationInvalid              = errors.New("cloud plan confirmation is invalid")
	ErrPlanQuoteExpired                     = errors.New("cloud quote has expired")
	ErrPlanApprovalConflict                 = errors.New("cloud plan approval conflicts with the requested revision")
	ErrPlanApprovalInvalid                  = errors.New("cloud plan approval is invalid")
	ErrPlanApprovalExpired                  = errors.New("cloud plan approval has expired")
	ErrPlanApprovalSignature                = errors.New("cloud plan approval signature is invalid")
	ErrRecipeExecutionManifestConflict      = errors.New("cloud recipe execution manifest conflicts with the current deployment")
	ErrRecipeExecutionManifestInvalid       = errors.New("cloud recipe execution manifest is invalid")
	ErrRecipeArtifactConflict               = errors.New("cloud compiled recipe artifact conflicts with the current recipe revision")
	ErrRecipeArtifactInvalid                = errors.New("cloud compiled recipe artifact is invalid")
	ErrRecipeExecutionConfirmationConflict  = errors.New("cloud recipe execution confirmation conflicts with the requested deployment")
	ErrRecipeExecutionConfirmationInvalid   = errors.New("cloud recipe execution confirmation is invalid")
	ErrRecipeExecutionApprovalExpired       = errors.New("cloud recipe execution approval has expired")
	ErrRecipeExecutionApprovalSignature     = errors.New("cloud recipe execution approval signature is invalid")
	ErrServiceDestroyConfirmationConflict   = errors.New("cloud service destroy confirmation conflicts with the current service")
	ErrServiceDestroyConfirmationInvalid    = errors.New("cloud service destroy confirmation is invalid")
	ErrServiceDestroyApprovalExpired        = errors.New("cloud service destroy approval has expired")
	ErrServiceDestroyApprovalSignature      = errors.New("cloud service destroy approval signature is invalid")
	ErrServiceOperationConfirmationConflict = errors.New("cloud service operation conflicts with the current service")
	ErrServiceOperationConfirmationInvalid  = errors.New("cloud service operation confirmation is invalid")
	ErrServiceOperationApprovalExpired      = errors.New("cloud service operation approval has expired")
	ErrServiceOperationApprovalSignature    = errors.New("cloud service operation approval signature is invalid")
	ErrServiceBackupConfirmationConflict    = errors.New("cloud service backup conflicts with the current service")
	ErrServiceBackupConfirmationInvalid     = errors.New("cloud service backup confirmation is invalid")
	ErrServiceBackupApprovalExpired         = errors.New("cloud service backup approval has expired")
	ErrServiceBackupApprovalSignature       = errors.New("cloud service backup approval signature is invalid")
	ErrServiceRestorePlanConflict           = errors.New("cloud service restore plan conflicts with the current service")
	ErrServiceRestorePlanInvalid            = errors.New("cloud service restore plan is invalid")
	ErrServiceRestoreConfirmationConflict   = errors.New("cloud service restore confirmation conflicts with the current plan")
	ErrServiceRestoreConfirmationInvalid    = errors.New("cloud service restore confirmation is invalid")
	ErrServiceRestoreApprovalExpired        = errors.New("cloud service restore approval has expired")
	ErrServiceRestoreApprovalSignature      = errors.New("cloud service restore approval signature is invalid")
)

// Goal is the private, durable user intent. Prompt is intentionally omitted
// from every realtime/event projection; it is only delivered to the isolated
// orchestrator through the private outbox payload.
type Goal struct {
	GoalID                 string
	OwnerMXID              string
	Prompt                 string
	ConnectionID           string
	PlanID                 string
	Status                 string
	IdempotencyHash        string
	RequestDigest          string
	SelectedRecipeID       string
	SelectedRecipeRevision int64
	SelectedRecipeDigest   string
	Revision               int64
	CreatedAt              int64
	UpdatedAt              int64
}

type GoalSummary struct {
	GoalID       string `json:"goal_id"`
	PlanID       string `json:"plan_id"`
	ConnectionID string `json:"cloud_connection_id,omitempty"`
	Status       string `json:"status"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

func (g Goal) Summary() GoalSummary {
	return GoalSummary{
		GoalID: g.GoalID, PlanID: g.PlanID, ConnectionID: g.ConnectionID,
		Status: g.Status, Revision: g.Revision, CreatedAt: g.CreatedAt, UpdatedAt: g.UpdatedAt,
	}
}

// QuoteView is the owner-visible, de-secretsed projection of one immutable
// price estimate. It intentionally excludes broker command envelopes,
// receipts, endpoint details, key material, and every secret reference.
// Quotes are only hydrated for cloud.plans.get; bootstrap and list surfaces
// remain summary-only even when a plan has a quote binding.
type QuoteView struct {
	QuoteID         string               `json:"quote_id"`
	ConnectionID    string               `json:"cloud_connection_id"`
	Region          string               `json:"region"`
	Currency        string               `json:"currency"`
	QuotedAt        time.Time            `json:"quoted_at"`
	ValidUntil      time.Time            `json:"valid_until"`
	Candidates      []QuoteCandidateView `json:"candidates"`
	IncludedItems   []string             `json:"included_items,omitempty"`
	UnincludedItems []string             `json:"unincluded_items,omitempty"`
}

// QuoteCandidateView contains only the cost and capacity facts needed to
// compare the selectable quote tiers. Candidate IDs remain internal to the
// immutable quote binding and are not part of the ProductCore projection.
type QuoteCandidateView struct {
	Tier              string              `json:"tier"`
	InstanceType      string              `json:"instance_type"`
	PurchaseOption    string              `json:"purchase_option"`
	Architecture      string              `json:"architecture"`
	VCPU              uint16              `json:"vcpu"`
	MemoryMiB         uint32              `json:"memory_mib"`
	GPUCount          uint16              `json:"gpu_count"`
	GPUMemoryMiB      uint32              `json:"gpu_memory_mib"`
	HourlyMinor       int64               `json:"hourly_minor"`
	ThirtyDayMinor    int64               `json:"thirty_day_minor"`
	StartupUpperMinor int64               `json:"startup_upper_minor"`
	EstimatedDiskGiB  uint32              `json:"estimated_disk_gib"`
	AvailabilityZones []string            `json:"availability_zones,omitempty"`
	WorkerImageID     string              `json:"worker_image_id"`
	WorkerImageDigest string              `json:"worker_image_digest"`
	CostItems         []QuoteCostItemView `json:"cost_items,omitempty"`
}

// QuoteCostItemView preserves provider-owned cost categories without teaching
// ProductCore about each AWS billable dimension. Provider micros remain exact;
// clients must not reconstruct or silently round candidate totals from them.
type QuoteCostItemView struct {
	Category                  string `json:"category"`
	Description               string `json:"description"`
	SourceID                  string `json:"source_id"`
	HourlyEstimateMicros      uint64 `json:"hourly_estimate_micros"`
	MonthlyEstimateMicros     uint64 `json:"monthly_estimate_micros"`
	MaximumLaunchAmountMicros uint64 `json:"maximum_launch_amount_micros"`
}

// Plan is a de-secretsed planning artifact. PlanHash is intentionally blank
// until the external planner supplies a deterministic-CBOR digest; approval
// handlers will not accept a plan without it.
type Plan struct {
	PlanID         string     `json:"plan_id"`
	GoalID         string     `json:"goal_id"`
	ConnectionID   string     `json:"cloud_connection_id,omitempty"`
	Status         string     `json:"status"`
	Title          string     `json:"title,omitempty"`
	Summary        string     `json:"summary,omitempty"`
	RecipeDigest   string     `json:"recipe_digest,omitempty"`
	RecipeID       string     `json:"recipe_id,omitempty"`
	RecipeRevision int64      `json:"recipe_revision,omitempty"`
	QuoteID        string     `json:"quote_id,omitempty"`
	Quote          *QuoteView `json:"quote,omitempty"`
	PlanHash       string     `json:"plan_hash,omitempty"`
	Revision       int64      `json:"revision"`
	CreatedAt      int64      `json:"created_at"`
	UpdatedAt      int64      `json:"updated_at"`
}

type Connection struct {
	ConnectionID string `json:"cloud_connection_id"`
	Provider     string `json:"provider"`
	AccountID    string `json:"account_id,omitempty"`
	Region       string `json:"region,omitempty"`
	Mode         string `json:"mode"`
	Status       string `json:"status"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

// ConnectionStackConfig is non-secret deployment configuration injected into
// the ProductCore process. The Node signing private key deliberately lives
// only in the independent Cloud Orchestrator process; this facade receives
// only its public identity for the CloudFormation role plan.
type ConnectionStackConfig struct {
	// TemplateURL is retained only to fail closed during the configuration
	// migration. An executable template is always carried by
	// ConnectionTemplate; a raw URL is rejected even if it looks pinned.
	TemplateURL             string
	TemplateDigest          string
	ConnectionTemplate      ConnectionTemplateReference
	SourceTreeDigest        string
	NodeKeyID               string
	NodePublicKeySPKIBase64 string
	RolePlanTTL             time.Duration
}

// ConnectionBootstrap is private durable onboarding state. Candidate endpoint
// and Stack ARN fields must never appear in ProductCore projections, MCP, or
// realtime events. They are read only by the independently deployed
// Orchestrator while it verifies the fixed Broker command.
type ConnectionBootstrap struct {
	BootstrapID        string
	OwnerMXID          string
	ConnectionID       string
	Provider           string
	RequestedRegion    string
	ConnectionTemplate ConnectionTemplateReference
	// TemplateURL is a derived, display-only CloudFormation URL for an
	// immutable S3 binding. It is empty while a root bootstrap holds an
	// immutable publish intent and must never be configured directly.
	TemplateURL                       string
	TemplateDigest                    string
	SourceTreeDigest                  string
	StackName                         string
	NodeKeyID                         string
	NodePublicKeySPKIBase64           string
	DeviceApprovalKeyID               string
	DeviceApprovalPublicKeySPKIBase64 string
	AllowRootCredentialBootstrap      bool
	CandidateBrokerURL                string
	StackARN                          string
	Status                            string
	Revision                          int64
	IdempotencyHash                   string
	RequestDigest                     string
	CompletionIdempotencyHash         string
	CompletionRequestDigest           string
	JobID                             string
	NextNodeCounter                   int64
	ExpiresAt                         int64
	CreatedAt                         int64
	UpdatedAt                         int64
}

// ConnectionRolePlan is the owner-only, short-lived CloudFormation handoff.
// It intentionally contains public keys and an immutable template digest, but
// no AWS credential, Broker endpoint, stack ARN, private key, or service
// secret. It is returned only from HTTP role-plan creation, not via realtime.
type ConnectionRolePlan struct {
	BootstrapID                  string                      `json:"bootstrap_id"`
	CloudConnectionID            string                      `json:"cloud_connection_id"`
	Provider                     string                      `json:"provider"`
	Region                       string                      `json:"region"`
	Status                       string                      `json:"status"`
	Revision                     int64                       `json:"revision"`
	ExpiresAt                    int64                       `json:"expires_at"`
	ConnectionTemplate           ConnectionTemplateReference `json:"connection_template"`
	TemplateURL                  string                      `json:"template_url"`
	TemplateDigest               string                      `json:"template_digest"`
	SourceTreeDigest             string                      `json:"source_tree_digest"`
	StackName                    string                      `json:"stack_name"`
	AllowRootCredentialBootstrap bool                        `json:"allow_root_credential_bootstrap"`
	CloudFormationParams         map[string]string           `json:"cloudformation_parameters"`
}

// ConnectionCredentialBootstrapRequest is derived entirely from the durable
// role plan. ProductCore never accepts CloudFormation parameters or public-key
// material from the credential-session caller.
type ConnectionCredentialBootstrapRequest struct {
	Schema    string                                    `json:"schema"`
	RequestID string                                    `json:"request_id"`
	RolePlan  ConnectionCredentialBootstrapRolePlanWire `json:"role_plan"`
}

type ConnectionCredentialBootstrapRolePlanWire struct {
	BootstrapID                  string                      `json:"bootstrap_id"`
	ConnectionID                 string                      `json:"connection_id"`
	Region                       string                      `json:"region"`
	StackName                    string                      `json:"stack_name"`
	ConnectionTemplate           ConnectionTemplateReference `json:"connection_template"`
	SourceTreeDigest             string                      `json:"source_tree_digest"`
	FixedParameters              map[string]string           `json:"fixed_parameters"`
	NodeKeyID                    string                      `json:"node_key_id"`
	NodeEd25519PublicKey         string                      `json:"node_ed25519_public_key"`
	DeviceKeyID                  string                      `json:"device_key_id"`
	DeviceEd25519PublicKey       string                      `json:"device_ed25519_public_key"`
	AllowRootCredentialBootstrap bool                        `json:"allow_root_credential_bootstrap"`
	ExpiresAt                    string                      `json:"expires_at"`
}

type ConnectionCredentialBootstrapReceipt struct {
	Schema       string `json:"schema"`
	Status       string `json:"status"`
	StackID      string `json:"stack_id"`
	ConnectionID string `json:"connection_id"`
	AcceptedAt   string `json:"accepted_at"`
}

// ConnectionCredentialBootstrapSession is returned only by the independent
// bootstrap service. UploadBearer is deliberately transient and must never be
// persisted by ProductCore.
type ConnectionCredentialBootstrapSession struct {
	Schema                string                                `json:"schema"`
	Status                string                                `json:"status"`
	RequestID             string                                `json:"request_id"`
	SessionID             string                                `json:"session_id"`
	ConnectionID          string                                `json:"connection_id"`
	ServerX25519PublicKey string                                `json:"server_x25519_public_key"`
	UploadBearer          string                                `json:"upload_bearer"`
	UploadURL             string                                `json:"upload_url"`
	ExpiresAt             string                                `json:"expires_at"`
	HKDF                  string                                `json:"hkdf"`
	AAD                   string                                `json:"aad"`
	Receipt               *ConnectionCredentialBootstrapReceipt `json:"receipt,omitempty"`
}

type LoadConnectionCredentialBootstrapRequest struct {
	OwnerMXID        string
	BootstrapID      string
	ExpectedRevision int64
	Now              int64
}

type ConnectionCredentialBootstrapStore interface {
	LoadCloudConnectionCredentialBootstrap(context.Context, LoadConnectionCredentialBootstrapRequest) (ConnectionRolePlan, error)
}

type ConnectionCredentialBootstrapClient interface {
	CreateSession(context.Context, ConnectionCredentialBootstrapRequest) (ConnectionCredentialBootstrapSession, error)
}

// ConnectionRegistration is the safe response after a user submits only Stack
// outputs. It deliberately omits the endpoint and Stack ARN while the
// Orchestrator validates them with the signed Broker.
type ConnectionRegistration struct {
	BootstrapID       string `json:"bootstrap_id"`
	CloudConnectionID string `json:"cloud_connection_id"`
	Status            string `json:"status"`
	Revision          int64  `json:"revision"`
	JobID             string `json:"job_id,omitempty"`
}

type CreateConnectionBootstrapRequest struct {
	Bootstrap ConnectionBootstrap
}

type CreateConnectionBootstrapResult struct {
	Bootstrap ConnectionBootstrap
	Created   bool
}

type CompleteConnectionBootstrapRequest struct {
	OwnerMXID        string
	BootstrapID      string
	ExpectedRevision int64
	IdempotencyHash  string
	RequestDigest    string
	BrokerCommandURL string
	StackARN         string
	Job              Job
	Event            Event
	Outbox           OutboxEntry
}

type CompleteConnectionBootstrapResult struct {
	Bootstrap ConnectionBootstrap
	Created   bool
}

// Deployment tracks execution independently from resource and service state.
type Deployment struct {
	DeploymentID string `json:"deployment_id"`
	PlanID       string `json:"plan_id"`
	ConnectionID string `json:"cloud_connection_id"`
	Execution    string `json:"execution_status"`
	Outcome      string `json:"outcome_status"`
	Resource     string `json:"resource_status"`
	Health       *DeploymentHealthSummary `json:"health,omitempty"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

// DeploymentHealthSummary is a deliberately de-sensitive, independently
// revisioned health axis. It must never contain probe endpoints, headers,
// request/response bodies, pairing material, or secret references.
type DeploymentHealthSummary struct {
	Status                 string                       `json:"status"`
	Revision               int64                        `json:"revision"`
	ObservedAt             int64                        `json:"observed_at,omitempty"`
	NextDueAt              int64                        `json:"next_due_at,omitempty"`
	ProbeCount             uint32                       `json:"probe_count"`
	ProbeCounts            []DeploymentHealthProbeCount `json:"probe_counts"`
	ExternalEvidenceDigest string                       `json:"external_evidence_digest,omitempty"`
	EvidenceType           string                       `json:"evidence_type"`
}

type DeploymentHealthProbeCount struct {
	Kind  string `json:"kind"`
	Count uint32 `json:"count"`
}

// Job tracks one durable Cloud control-plane operation. It is independent
// from plan and resource state so a retry or research failure remains visible
// after websocket reconnects without implying that a resource exists.
type Job struct {
	JobID        string `json:"job_id"`
	PlanID       string `json:"plan_id"`
	DeploymentID string `json:"deployment_id"`
	Kind         string `json:"kind"`
	Execution    string `json:"execution_status"`
	Outcome      string `json:"outcome_status"`
	Checkpoint   string `json:"checkpoint"`
	ErrorCode    string `json:"error_code"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

// ResearchJobID is deterministic so an older source outbox can lazily gain a
// visible research Job after a server upgrade without creating a second task.
func ResearchJobID(outboxID string) string {
	return "cloud_job_research_" + strings.TrimSpace(outboxID)
}

// QuoteJobID is deterministic so retries of the durable quote request retain
// one visible Job without creating a second billing-related operation.
func QuoteJobID(outboxID string) string {
	return "cloud_job_quote_" + strings.TrimSpace(outboxID)
}

// Service is intentionally separate from Deployment so a failed integration
// cannot turn an otherwise running service into a failed cloud resource.
type Service struct {
	ServiceID    string           `json:"service_id"`
	DeploymentID string           `json:"deployment_id"`
	RecipeID     string           `json:"recipe_id,omitempty"`
	Name         string           `json:"name"`
	Status       string           `json:"service_status"`
	Integration  string           `json:"integration_status"`
	Revision     int64            `json:"revision"`
	CreatedAt    int64            `json:"created_at"`
	UpdatedAt    int64            `json:"updated_at"`
	Backups      []ServiceBackup  `json:"backups,omitempty"`
	Restores     []ServiceRestore `json:"restores,omitempty"`
}

type Recipe struct {
	RecipeID  string `json:"recipe_id"`
	Name      string `json:"name"`
	Version   string `json:"version"`
	Digest    string `json:"digest"`
	Maturity  string `json:"maturity"`
	Revision  int64  `json:"revision"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type Alert struct {
	AlertID      string `json:"alert_id"`
	DeploymentID string `json:"deployment_id,omitempty"`
	ServiceID    string `json:"service_id,omitempty"`
	Severity     string `json:"severity"`
	Code         string `json:"code"`
	Message      string `json:"message"`
	Acknowledged bool   `json:"acknowledged"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

// Event is the durable Cloud Orchestrator audit stream. It is not the
// ProductCore websocket sequence; revisions are aggregate-local and survive
// node process restarts.
type Event struct {
	EventID       string         `json:"event_id"`
	Type          string         `json:"type"`
	AggregateType string         `json:"aggregate_type"`
	AggregateID   string         `json:"aggregate_id"`
	Revision      int64          `json:"revision"`
	Summary       map[string]any `json:"summary"`
	SummaryJSON   string         `json:"-"`
	CreatedAt     int64          `json:"created_at"`
}

// HydrateSummary makes the persisted de-secretsed summary available to the
// owner-only history endpoint. Corrupt historical rows degrade to an empty
// summary rather than exposing raw JSON or failing an entire status refresh.
func (e *Event) HydrateSummary() {
	if e == nil || e.Summary != nil {
		return
	}
	e.Summary = map[string]any{}
	_ = json.Unmarshal([]byte(e.SummaryJSON), &e.Summary)
	if e.Summary == nil {
		e.Summary = map[string]any{}
	}
}

type OutboxEntry struct {
	OutboxID      string
	Kind          string
	AggregateType string
	AggregateID   string
	PayloadJSON   string
	CreatedAt     int64
}

type CreateGoalRequest struct {
	Goal           Goal
	Plan           Plan
	Job            Job
	Events         []Event
	Outbox         OutboxEntry
	SelectedRecipe *SelectedRecipeBinding
}

type SelectedRecipeBinding struct {
	RecipeID string
	Revision int64
	Digest   string
	Recipe   cloudcontracts.RecipeV1
}

type SelectableRecipeStore interface {
	ResolveCloudRecipeSelection(context.Context, string, string, string, int64) (SelectedRecipeBinding, bool, error)
}

type CreateGoalResult struct {
	Goal    Goal
	Plan    Plan
	Created bool
}

// Store is the only durable boundary used by the ProductCore cloud facade.
// The separate orchestrator can receive a narrower implementation/role later;
// this façade does not receive AWS credentials or an AWS API client.
type Store interface {
	CreateCloudGoal(context.Context, CreateGoalRequest) (CreateGoalResult, error)
	CreateCloudConnectionBootstrap(context.Context, CreateConnectionBootstrapRequest) (CreateConnectionBootstrapResult, error)
	CompleteCloudConnectionBootstrap(context.Context, CompleteConnectionBootstrapRequest) (CompleteConnectionBootstrapResult, error)
	ListCloudGoals(context.Context) ([]Goal, error)
	ListCloudPlans(context.Context) ([]Plan, error)
	GetCloudPlan(context.Context, string) (Plan, bool, error)
	GetCloudQuote(context.Context, string) (QuoteView, bool, error)
	ListCloudJobs(context.Context) ([]Job, error)
	ListCloudConnections(context.Context) ([]Connection, error)
	GetCloudConnection(context.Context, string) (Connection, bool, error)
	ListCloudDeployments(context.Context) ([]Deployment, error)
	GetCloudDeployment(context.Context, string) (Deployment, bool, error)
	ListCloudServices(context.Context) ([]Service, error)
	GetCloudService(context.Context, string) (Service, bool, error)
	ListCloudRecipes(context.Context) ([]Recipe, error)
	GetCloudRecipe(context.Context, string) (Recipe, bool, error)
	ListCloudAlerts(context.Context) ([]Alert, error)
	ListCloudEvents(context.Context, int) ([]Event, error)
}

// DeploymentReader is the narrow read-only boundary that may be delegated to
// the independent Agent service. It deliberately excludes every mutation so
// enabling remote status queries cannot route approvals or lifecycle actions.
type DeploymentReader interface {
	ListCloudDeployments(context.Context) ([]Deployment, error)
	GetCloudDeployment(context.Context, string) (Deployment, bool, error)
}
