package cloudorchestrator

import "time"

// SchemaVersionV1 is written into every persisted or signed V1 contract.
const SchemaVersionV1 = "cloud-orchestrator/v1"

// HashAlgorithmDeterministicCBORSHA256 identifies the RFC 8949 Core
// Deterministic CBOR encoding used by V1 plan/recipe/quote digests and
// approval payloads.
const HashAlgorithmDeterministicCBORSHA256 = "deterministic-cbor-sha256"

type PlanStatus string

const (
	PlanResearching          PlanStatus = "researching"
	PlanQuoting              PlanStatus = "quoting"
	PlanReadyForConfirmation PlanStatus = "ready_for_confirmation"
	PlanApproved             PlanStatus = "approved"
	PlanExpired              PlanStatus = "expired"
	PlanSuperseded           PlanStatus = "superseded"
)

type RecipeMaturity string

const (
	RecipeExperimental             RecipeMaturity = "experimental"
	RecipeAwaitingManagementAccept RecipeMaturity = "awaiting_management_acceptance"
	RecipeManaged                  RecipeMaturity = "managed"
)

type Architecture string

const (
	ArchitectureAMD64 Architecture = "amd64"
	ArchitectureARM64 Architecture = "arm64"
)

type PurchaseOption string

const (
	PurchaseOnDemand PurchaseOption = "on_demand"
	PurchaseSpot     PurchaseOption = "spot"
)

type QuoteTier string

const (
	QuoteTierEconomy     QuoteTier = "economy"
	QuoteTierRecommended QuoteTier = "recommended"
	QuoteTierPerformance QuoteTier = "performance"
)

// QuoteRequestCandidateV1 is the non-price-bearing candidate shape shared by
// model-produced research drafts and typed Broker quote requests. It is kept
// separate from QuoteCandidateV1 so a researcher cannot manufacture a price
// estimate, approval binding, or final plan hash.
type QuoteRequestCandidateV1 struct {
	CandidateID      string         `json:"candidate_id"`
	Tier             QuoteTier      `json:"tier"`
	InstanceType     string         `json:"instance_type"`
	PurchaseOption   PurchaseOption `json:"purchase_option"`
	EstimatedDiskGiB uint32         `json:"estimated_disk_gib"`
}

type EntryPointKind string

const (
	EntryPointNone       EntryPointKind = "none"
	EntryPointALB        EntryPointKind = "alb"
	EntryPointCloudFront EntryPointKind = "cloudfront"
	EntryPointDirect     EntryPointKind = "direct"
)

type SecretDelivery string

const (
	SecretDeliveryFile        SecretDelivery = "file"
	SecretDeliveryEnvironment SecretDelivery = "environment"
)

type IntegrationKind string

const (
	IntegrationMCP                IntegrationKind = "mcp"
	IntegrationACP                IntegrationKind = "acp"
	IntegrationDirextalkConnector IntegrationKind = "dirextalk_connector"
	IntegrationWeb                IntegrationKind = "web"
)

type ProbeKind string

const (
	ProbeHTTP    ProbeKind = "http"
	ProbeCommand ProbeKind = "command"
)

// RecipeV1 is a versioned, de-secretsed private recipe. It describes the
// deployment contract, not an executable shell script. A worker may adapt
// only inside the explicitly declared recipe boundary.
type RecipeV1 struct {
	SchemaVersion string                 `json:"schema_version"`
	RecipeID      string                 `json:"recipe_id"`
	Name          string                 `json:"name"`
	Maturity      RecipeMaturity         `json:"maturity"`
	Sources       []RecipeSourceV1       `json:"sources"`
	Requirements  ResourceRequirementsV1 `json:"requirements"`
	Install       InstallContractV1      `json:"install"`
	Health        HealthContractV1       `json:"health"`
	Lifecycle     LifecycleContractV1    `json:"lifecycle"`
}

// RecipeSourceV1 records provenance for a candidate recipe artifact.
// URL must be an HTTPS source without inline credentials or secret query data.
type RecipeSourceV1 struct {
	URL            string    `json:"url"`
	Version        string    `json:"version"`
	Commit         string    `json:"commit"`
	ArtifactDigest string    `json:"artifact_digest"`
	License        string    `json:"license"`
	RetrievedAt    time.Time `json:"retrieved_at"`
	Official       bool      `json:"official"`
}

// ResourceRequirementsV1 is the minimum resource envelope a recipe needs.
// Concrete purchasable resources live in ResourceScopeV1 on a plan.
type ResourceRequirementsV1 struct {
	MinVCPU         uint16       `json:"min_vcpu"`
	MinMemoryMiB    uint32       `json:"min_memory_mib"`
	MinDiskGiB      uint32       `json:"min_disk_gib"`
	MinGPUCount     uint16       `json:"min_gpu_count,omitempty"`
	MinGPUMemoryMiB uint32       `json:"min_gpu_memory_mib,omitempty"`
	Architecture    Architecture `json:"architecture"`
}

// InstallContractV1 is intentionally descriptive. The separately deployed
// Worker receives a compiled recipe artifact; arbitrary command text and raw
// credentials are not carried by this public domain contract.
type InstallContractV1 struct {
	RootRequired       bool            `json:"root_required"`
	TimeoutSeconds     uint32          `json:"timeout_seconds"`
	CheckpointNames    []string        `json:"checkpoint_names"`
	AllowedAdaptations []string        `json:"allowed_adaptations,omitempty"`
	Steps              []InstallStepV1 `json:"steps"`
}

type InstallStepV1 struct {
	ID             string `json:"id"`
	Summary        string `json:"summary"`
	TimeoutSeconds uint32 `json:"timeout_seconds"`
}

type HealthContractV1 struct {
	Liveness  ProbeV1 `json:"liveness"`
	Readiness ProbeV1 `json:"readiness"`
	Semantic  ProbeV1 `json:"semantic"`
}

// ProbeV1 is a declarative probe contract. Command targets are worker-local
// recipe identifiers, not arbitrary command lines.
type ProbeV1 struct {
	Kind   ProbeKind `json:"kind"`
	Target string    `json:"target"`
}

// LifecycleContractV1 declares the stable action identifiers a managed
// recipe must support. Actual implementation details stay in the recipe
// artifact and are verified by the Worker.
type LifecycleContractV1 struct {
	Start    string `json:"start"`
	Stop     string `json:"stop"`
	Restart  string `json:"restart"`
	Upgrade  string `json:"upgrade"`
	Rollback string `json:"rollback"`
	Backup   string `json:"backup"`
	Restore  string `json:"restore"`
	Destroy  string `json:"destroy"`
}

// ResearchDraftV1 is the restricted result a model may produce while
// researching a workload. It intentionally contains only an AWS region and
// one to three non-price-bearing instance candidates. The Orchestrator turns
// it into a QuoteRequestV1; it is not a plan, quote, approval, or purchase
// authorization.
type ResearchDraftV1 struct {
	SchemaVersion string                    `json:"schema_version"`
	Region        string                    `json:"region"`
	Candidates    []QuoteRequestCandidateV1 `json:"candidates"`
}

// QuoteRequestV1 is the immutable pre-price binding sent through the typed
// Broker quote command. Digest returns the deterministic-CBOR value supplied
// to the Broker payload as plan_digest. It is deliberately independent of
// PlanV1.Hash because a final approval plan cannot exist before a quote is
// issued.
type QuoteRequestV1 struct {
	SchemaVersion     string                    `json:"schema_version"`
	QuoteRequestID    string                    `json:"quote_request_id"`
	PlanID            string                    `json:"plan_id"`
	PlanRevision      uint64                    `json:"plan_revision"`
	CloudConnectionID string                    `json:"cloud_connection_id"`
	RecipeDigest      string                    `json:"recipe_digest"`
	Region            string                    `json:"region"`
	Candidates        []QuoteRequestCandidateV1 `json:"candidates"`
}

// QuoteV1 is an immutable price estimate. Monetary values are represented in
// the currency's minor unit, never float values. It is not an AWS budget or a
// billing hard stop.
type QuoteV1 struct {
	SchemaVersion     string             `json:"schema_version"`
	QuoteID           string             `json:"quote_id"`
	CloudConnectionID string             `json:"cloud_connection_id"`
	Region            string             `json:"region"`
	Currency          string             `json:"currency"`
	QuotedAt          time.Time          `json:"quoted_at"`
	ValidUntil        time.Time          `json:"valid_until"`
	Candidates        []QuoteCandidateV1 `json:"candidates"`
	IncludedItems     []string           `json:"included_items,omitempty"`
	UnincludedItems   []string           `json:"unincluded_items,omitempty"`
}

type QuoteCandidateV1 struct {
	CandidateID       string         `json:"candidate_id"`
	Tier              QuoteTier      `json:"tier"`
	InstanceType      string         `json:"instance_type"`
	PurchaseOption    PurchaseOption `json:"purchase_option"`
	Architecture      Architecture   `json:"architecture"`
	VCPU              uint16         `json:"vcpu"`
	MemoryMiB         uint32         `json:"memory_mib"`
	GPUCount          uint16         `json:"gpu_count"`
	GPUMemoryMiB      uint32         `json:"gpu_memory_mib"`
	HourlyMinor       int64          `json:"hourly_minor"`
	ThirtyDayMinor    int64          `json:"thirty_day_minor"`
	StartupUpperMinor int64          `json:"startup_upper_minor"`
	EstimatedDiskGiB  uint32         `json:"estimated_disk_gib"`
	AvailabilityZones []string       `json:"availability_zones,omitempty"`
}

// RecipeBindingV1 and QuoteBindingV1 allow a plan to bind immutable content
// without duplicating private recipe or quote material.
type RecipeBindingV1 struct {
	RecipeID string         `json:"recipe_id"`
	Digest   string         `json:"digest"`
	Maturity RecipeMaturity `json:"maturity"`
}

type QuoteBindingV1 struct {
	QuoteID     string    `json:"quote_id"`
	Digest      string    `json:"digest"`
	ValidUntil  time.Time `json:"valid_until"`
	CandidateID string    `json:"candidate_id"`
}

// PlanV1 is the immutable approval surface. Its Hash method covers every
// field that can change provider spend, instance isolation, network exposure,
// secret delivery reference, or product integration. Status and display text
// are intentionally absent so lifecycle projections do not invalidate an
// already reviewed approval surface.
type PlanV1 struct {
	SchemaVersion     string               `json:"schema_version"`
	PlanID            string               `json:"plan_id"`
	Revision          uint64               `json:"revision"`
	Status            PlanStatus           `json:"status"`
	CloudConnectionID string               `json:"cloud_connection_id"`
	Recipe            RecipeBindingV1      `json:"recipe"`
	Quote             QuoteBindingV1       `json:"quote"`
	ResourceScope     ResourceScopeV1      `json:"resource_scope"`
	NetworkScope      NetworkScopeV1       `json:"network_scope"`
	SecretScope       []SecretReferenceV1  `json:"secret_scope"`
	IntegrationScope  []IntegrationScopeV1 `json:"integration_scope"`
}

const (
	// ExecutionProbeManifestV1Schema identifies the sealed, de-secreted
	// artifact that binds the one fixed transport probe to an already-created
	// Worker deployment. It is deliberately separate from RecipeV1, PlanV1,
	// and the Connection Stack Worker resource manifest schemas.
	ExecutionProbeManifestV1Schema = "dirextalk.execution-probe-manifest/v1"
	// NoInputV1Schema identifies the sealed empty-input artifact for an
	// execution probe. It cannot carry task data, commands, URLs, or secrets.
	NoInputV1Schema = "dirextalk.no-input/v1"
	// RecipeExecutionManifestV1Schema identifies the sealed, de-secreted
	// execution boundary for a future compiled Recipe artifact. It is not yet a
	// Worker task type or a permission to execute a shell command.
	RecipeExecutionManifestV1Schema = "dirextalk.recipe-execution-manifest/v1"
	// ExecutionProbeTaskKind is the only task kind represented by these
	// artifacts. It is a transport proof, not Recipe execution or service
	// readiness.
	ExecutionProbeTaskKind = "execution_probe"
)

// ExecutionProbeManifestV1 is a sealed, digestable binding artifact for the
// restricted execution_probe Worker task. It is not a Recipe, Plan, Worker
// resource manifest, or executable instruction: it contains only immutable
// references needed to bind the task to one approved deployment.
type ExecutionProbeManifestV1 struct {
	SchemaVersion                string `json:"schema_version"`
	DeploymentID                 string `json:"deployment_id"`
	PlanID                       string `json:"plan_id"`
	PlanHash                     string `json:"plan_hash"`
	PlanRevision                 uint64 `json:"plan_revision"`
	RecipeDigest                 string `json:"recipe_digest"`
	WorkerResourceManifestDigest string `json:"worker_resource_manifest_digest"`
	TaskKind                     string `json:"task_kind"`
}

// NoInputV1 is a separate, digestable artifact that explicitly proves the
// fixed execution_probe task has no input. It binds only the deployment and
// task kind, never commands, URLs, secrets, or user-provided data.
type NoInputV1 struct {
	SchemaVersion string `json:"schema_version"`
	DeploymentID  string `json:"deployment_id"`
	TaskKind      string `json:"task_kind"`
	NoInput       bool   `json:"no_input"`
}

// RecipeExecutionManifestV1 seals the non-secret execution scope for one
// compiled Recipe artifact. It binds an execution to an approved Plan revision
// and Worker resource manifest while deliberately carrying no command text,
// URL, credential value, file path, or cloud-control instruction. The
// artifact resolver and a separately isolated executor remain responsible for
// authenticating the artifact and performing any privileged action.
type RecipeExecutionManifestV1 struct {
	SchemaVersion                string         `json:"schema_version"`
	ExecutionID                  string         `json:"execution_id"`
	DeploymentID                 string         `json:"deployment_id"`
	PlanID                       string         `json:"plan_id"`
	PlanHash                     string         `json:"plan_hash"`
	PlanRevision                 uint64         `json:"plan_revision"`
	RecipeDigest                 string         `json:"recipe_digest"`
	WorkerResourceManifestDigest string         `json:"worker_resource_manifest_digest"`
	ArtifactDigest               string         `json:"artifact_digest"`
	ActionID                     string         `json:"action_id"`
	RootRequired                 bool           `json:"root_required"`
	TimeoutSeconds               uint32         `json:"timeout_seconds"`
	CheckpointSequence           []string       `json:"checkpoint_sequence"`
	VolumeSlots                  []VolumeSlotV1 `json:"volume_slots,omitempty"`
	DataSlots                    []DataSlotV1   `json:"data_slots,omitempty"`
	SecretSlots                  []SecretSlotV1 `json:"secret_slots,omitempty"`
}

// VolumeSlotV1 binds an opaque pre-provisioned volume reference to a compiled
// artifact slot. It contains neither a host path nor a cloud volume ID.
type VolumeSlotV1 struct {
	SlotID    string `json:"slot_id"`
	VolumeRef string `json:"volume_ref"`
	ReadOnly  bool   `json:"read_only"`
}

// DataSlotV1 binds an opaque, already-authorized data reference to a compiled
// artifact slot. Delivery details stay outside this manifest.
type DataSlotV1 struct {
	SlotID   string `json:"slot_id"`
	DataRef  string `json:"data_ref"`
	ReadOnly bool   `json:"read_only"`
}

// SecretSlotV1 names a KMS/Secrets Manager reference already approved on the
// Plan. It never carries a secret value, environment variable name, or file
// path.
type SecretSlotV1 struct {
	SlotID    string `json:"slot_id"`
	SecretRef string `json:"secret_ref"`
}

// ResourceScopeV1 is the hard resource boundary approved for one exclusive
// worker VM. The Worker does not receive cloud-control credentials.
type ResourceScopeV1 struct {
	Region            string         `json:"region"`
	AvailabilityZones []string       `json:"availability_zones,omitempty"`
	InstanceType      string         `json:"instance_type"`
	Architecture      Architecture   `json:"architecture"`
	VCPU              uint16         `json:"vcpu"`
	MemoryMiB         uint32         `json:"memory_mib"`
	GPUCount          uint16         `json:"gpu_count,omitempty"`
	GPUMemoryMiB      uint32         `json:"gpu_memory_mib,omitempty"`
	DiskGiB           uint32         `json:"disk_gib"`
	PurchaseOption    PurchaseOption `json:"purchase_option"`
	Spot              *SpotScopeV1   `json:"spot,omitempty"`
}

// SpotScopeV1 is permitted only for recipes that have declared recoverability
// elsewhere. This type records the maximum retry envelope to bind approval.
type SpotScopeV1 struct {
	CheckpointRequired bool   `json:"checkpoint_required"`
	MaxRetries         uint16 `json:"max_retries"`
}

// NetworkScopeV1 declares ingress separately and requires explicit TLS and
// authentication for public endpoints. It does not by itself open a security
// group; that remains an independently approved control-plane transition.
type NetworkScopeV1 struct {
	PublicIngress          bool            `json:"public_ingress"`
	EntryPoint             EntryPointKind  `json:"entry_point"`
	TLSRequired            bool            `json:"tls_required"`
	AuthenticationRequired bool            `json:"authentication_required"`
	Ingress                []IngressRuleV1 `json:"ingress,omitempty"`
}

type IngressRuleV1 struct {
	Protocol string `json:"protocol"`
	Port     uint16 `json:"port"`
	Purpose  string `json:"purpose"`
}

// SecretReferenceV1 deliberately has no value field. SecretRef must use the
// opaque secret_ref: namespace produced by the encrypted bootstrap channel.
type SecretReferenceV1 struct {
	SecretRef string         `json:"secret_ref"`
	Purpose   string         `json:"purpose"`
	Delivery  SecretDelivery `json:"delivery"`
}

type IntegrationScopeV1 struct {
	Kind IntegrationKind `json:"kind"`
	Name string          `json:"name"`
}

// ApprovalV1 is the device-signed authorization artifact. Signature is
// base64url-encoded Ed25519 output and is deliberately excluded from the
// signing payload itself.
type ApprovalV1 struct {
	SchemaVersion     string               `json:"schema_version"`
	ApprovalID        string               `json:"approval_id"`
	ChallengeID       string               `json:"challenge_id"`
	SignerKeyID       string               `json:"signer_key_id"`
	PlanID            string               `json:"plan_id"`
	PlanHash          string               `json:"plan_hash"`
	PlanRevision      uint64               `json:"plan_revision"`
	QuoteID           string               `json:"quote_id"`
	QuoteDigest       string               `json:"quote_digest"`
	QuoteValidUntil   time.Time            `json:"quote_valid_until"`
	CloudConnectionID string               `json:"cloud_connection_id"`
	RecipeDigest      string               `json:"recipe_digest"`
	ResourceScope     ResourceScopeV1      `json:"resource_scope"`
	NetworkScope      NetworkScopeV1       `json:"network_scope"`
	SecretScope       []SecretReferenceV1  `json:"secret_scope"`
	IntegrationScope  []IntegrationScopeV1 `json:"integration_scope"`
	ExpiresAt         time.Time            `json:"expires_at"`
	Signature         string               `json:"signature,omitempty"`
}
