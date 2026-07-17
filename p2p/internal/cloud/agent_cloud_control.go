package cloud

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"reflect"
	"regexp"
	"strings"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const (
	agentCloudApprovalSchema        = "dirextalk.agent.cloud.approval/v1"
	agentCloudHashAlgorithm         = "deterministic-cbor-sha256"
	agentCloudConnectionMode        = "agent_foundation_v1"
	agentCloudReadyForConfirmation  = "ready_for_confirmation"
	agentCloudPendingReconciliation = "pending_reconciliation"
)

var (
	agentCloudVolumeDevicePattern = regexp.MustCompile(`^/dev/sd[f-p]$`)
	agentCloudVolumeKMSPattern    = regexp.MustCompile(`^(?:alias/[A-Za-z0-9/_-]{1,240}|arn:(?:aws|aws-cn|aws-us-gov):kms:[a-z0-9-]+:[0-9]{12}:(?:key/[0-9a-f-]{36}|alias/[A-Za-z0-9/_-]{1,240}))$`)
	agentCloudVolumeSlotPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

type agentCloudApprovalV1 struct {
	SchemaVersion    string                     `json:"schema_version"`
	HashAlgorithm    string                     `json:"hash_algorithm"`
	ApprovalID       string                     `json:"approval_id"`
	AgentInstanceID  string                     `json:"agent_instance_id"`
	OwnerID          string                     `json:"owner_id"`
	PlanID           string                     `json:"plan_id"`
	PlanRevision     int64                      `json:"plan_revision"`
	PlanHash         string                     `json:"plan_hash"`
	ConnectionID     string                     `json:"connection_id"`
	RecipeDigest     string                     `json:"recipe_digest"`
	QuoteID          string                     `json:"quote_id"`
	QuoteDigest      string                     `json:"quote_digest"`
	QuoteScopeDigest string                     `json:"quote_scope_digest"`
	QuoteCandidateID string                     `json:"quote_candidate_id"`
	QuoteValidUntil  time.Time                  `json:"quote_valid_until"`
	ResourceScope    agentCloudResourceScopeV1  `json:"resource_scope"`
	NetworkScope     agentCloudNetworkScopeV1   `json:"network_scope"`
	SecretScope      []agentCloudSecretScopeV1  `json:"secret_scope,omitempty"`
	IntegrationScope []agentCloudIntegrationV1  `json:"integration_scope,omitempty"`
	RetentionScope   agentCloudRetentionScopeV1 `json:"retention_scope"`
	ChallengeID      string                     `json:"challenge_id"`
	SignerKeyID      string                     `json:"signer_key_id"`
	ExpiresAt        time.Time                  `json:"expires_at"`
	Signature        string                     `json:"signature,omitempty"`
}

type agentCloudResourceScopeV1 struct {
	Region                string   `json:"region"`
	AvailabilityZones     []string `json:"availability_zones"`
	InstanceType          string   `json:"instance_type"`
	InstanceCount         uint32   `json:"instance_count"`
	Architecture          string   `json:"architecture"`
	VCPU                  uint32   `json:"vcpu"`
	MemoryMiB             uint64   `json:"memory_mib"`
	GPUType               string   `json:"gpu_type,omitempty"`
	GPUCount              uint32   `json:"gpu_count,omitempty"`
	GPUMemoryMiB          uint64   `json:"gpu_memory_mib,omitempty"`
	DiskGiB               uint64   `json:"disk_gib"`
	VolumeType            string   `json:"volume_type"`
	VolumeIOPS            uint32   `json:"volume_iops,omitempty"`
	VolumeThroughputMiBPS uint32   `json:"volume_throughput_mibps,omitempty"`
	VolumeEncrypted       bool     `json:"volume_encrypted"`
	PurchaseOption        string   `json:"purchase_option"`
	WorkerImageID         string   `json:"worker_image_id"`
	WorkerImageDigest     string   `json:"worker_image_digest"`
	VolumeScopes          []agentCloudVolumeScopeV1 `json:"volume_scopes,omitempty"`
}

type agentCloudVolumeScopeV1 struct {
	SlotID          string `json:"slot_id"`
	SizeGiB         uint32 `json:"size_gib"`
	VolumeType      string `json:"volume_type"`
	IOPS            uint32 `json:"iops,omitempty"`
	ThroughputMiBPS uint32 `json:"throughput_mibps,omitempty"`
	Encrypted       bool   `json:"encrypted"`
	KMSKeyID        string `json:"kms_key_id"`
	DeviceName      string `json:"device_name"`
	MountPath       string `json:"mount_path"`
	ReadOnly        bool   `json:"read_only"`
	Persistent      bool   `json:"persistent"`
	Disposition     string `json:"disposition"`
}

type agentCloudNetworkScopeV1 struct {
	VPCID                  string   `json:"vpc_id"`
	SubnetID               string   `json:"subnet_id"`
	SecurityGroupID        string   `json:"security_group_id,omitempty"`
	SecurityGroupMode      string   `json:"security_group_mode"`
	EntryPoint             string   `json:"entry_point"`
	PublicIPv4             bool     `json:"public_ipv4"`
	PublicExposure         bool     `json:"public_exposure"`
	IngressPorts           []uint32 `json:"ingress_ports,omitempty"`
	Hostname               string   `json:"hostname,omitempty"`
	TLSRequired            bool     `json:"tls_required"`
	AuthenticationRequired bool     `json:"authentication_required"`
}

type agentCloudSecretScopeV1 struct {
	SecretRef string `json:"secret_ref"`
	Purpose   string `json:"purpose"`
	Delivery  string `json:"delivery"`
}

type agentCloudIntegrationV1 struct {
	Kind   string   `json:"kind"`
	Name   string   `json:"name"`
	Scopes []string `json:"scopes,omitempty"`
}

type agentCloudRetentionScopeV1 struct {
	Class              string `json:"class"`
	AutoDestroy        bool   `json:"auto_destroy"`
	GracePeriodSeconds uint32 `json:"grace_period_seconds"`
	MaxLifetimeSeconds uint64 `json:"max_lifetime_seconds"`
}

func (m *Module) prepareAgentPlanConfirmation(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "plan_id", "expected_revision", "quote_id", "candidate_tier", "signer_key_id", "idempotency_key"); err != nil {
		return nil, err
	}
	values := actionbase.Params(params)
	planID, quoteID := values.String("plan_id"), values.String("quote_id")
	expectedRevision := values.Int64("expected_revision")
	tier, signerKeyID := values.String("candidate_tier"), values.String("signer_key_id")
	idempotencyKey := values.String("idempotency_key")
	candidateID := agentCandidateID(tier)
	if !canonicalUUID(planID) || !canonicalUUID(quoteID) || expectedRevision <= 0 || candidateID == "" ||
		!cloudKeyIDPattern.MatchString(signerKeyID) || !canonicalUUID(idempotencyKey) || ContainsSensitiveGoalMaterial(signerKeyID) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudPlanConfirmationInvalidCode, "cloud plan confirmation is invalid")
	}
	plan, found, err := m.cfg.AgentCloudControlClient.GetAgentCloudPlan(ctx, AgentCloudPlanRequest{PlanID: planID})
	if err != nil || !found {
		return nil, agentPlanConfirmationError(err, found)
	}
	now := m.now().UTC()
	if validateAgentCloudPlan(plan, now) != nil || plan.PlanID != planID || plan.Revision != expectedRevision ||
		plan.QuoteID != quoteID || plan.CandidateProfile != candidateID || plan.Status != agentCloudReadyForConfirmation {
		return nil, actionbase.CodedError(http.StatusConflict, cloudPlanConfirmationConflictCode, "cloud plan confirmation conflicts with the current Agent plan")
	}
	challenge, err := m.cfg.AgentCloudControlClient.CreateAgentCloudApprovalChallenge(ctx, AgentCloudChallengeRequest{
		IdempotencyKey: idempotencyKey, PlanID: planID, ExpectedRevision: expectedRevision,
		SignerKeyID: signerKeyID, ExpectedPlan: plan,
	})
	if err != nil {
		return nil, agentPlanConfirmationError(err, true)
	}
	if validateAgentCloudChallenge(challenge, plan, signerKeyID, now) != nil {
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudPlanConfirmationInvalidCode, "cloud Agent returned an invalid approval challenge")
	}
	approval := agentApprovalFromChallenge(plan, challenge)
	payloadDigest := sha256.Sum256(challenge.SigningPayloadCBOR)
	return map[string]any{"confirmation": map[string]any{
		"plan":                   agentCloudPlanView(plan),
		"approval":               approval,
		"signing_payload_cbor":   base64.RawURLEncoding.EncodeToString(challenge.SigningPayloadCBOR),
		"signing_payload_digest": "sha256:" + hex.EncodeToString(payloadDigest[:]),
	}}, nil
}

func (m *Module) approveAgentPlan(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "plan_id", "expected_revision", "approval", "idempotency_key"); err != nil {
		return nil, err
	}
	values := actionbase.Params(params)
	planID, idempotencyKey := values.String("plan_id"), values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	now := m.now().UTC()
	approval, signature, err := decodeAgentCloudApproval(params["approval"], time.Time{})
	if !canonicalUUID(planID) || expectedRevision <= 0 || !canonicalUUID(idempotencyKey) || err != nil || approval.PlanID != planID || approval.PlanRevision != expectedRevision {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudPlanApprovalInvalidCode, "cloud plan approval is invalid")
	}
	plan, found, getErr := m.cfg.AgentCloudControlClient.GetAgentCloudPlan(ctx, AgentCloudPlanRequest{PlanID: planID})
	if getErr != nil || !found {
		return nil, agentPlanApprovalError(getErr, found)
	}
	// A response may be lost after Agent durably records the approval. Return
	// only that owner-scoped approved Plan on replay; never require an expired
	// one-time challenge to authorize a second mutation and never fabricate
	// downstream work.
	if plan.Status == AgentCloudPlanStatusApproved && plan.Revision == expectedRevision+1 &&
		validateAgentCloudPlan(plan, time.Time{}) == nil && agentApprovalMatchesPlan(approval, plan, true) {
		return map[string]any{"plan": agentCloudPlanView(plan), "submission_status": "waiting_connection"}, nil
	}
	if validateAgentCloudPlan(plan, now) != nil || plan.PlanID != planID || plan.Status != agentCloudReadyForConfirmation ||
		plan.Revision != expectedRevision || !now.Before(approval.ExpiresAt) || !agentApprovalMatchesPlan(approval, plan, false) {
		return nil, actionbase.CodedError(http.StatusConflict, cloudPlanApprovalConflictCode, "cloud plan approval conflicts with the current Agent plan")
	}
	approved, callErr := m.cfg.AgentCloudControlClient.ApproveAgentCloudPlan(ctx, AgentCloudApproveRequest{
		IdempotencyKey: idempotencyKey, PlanID: planID, ExpectedRevision: expectedRevision, ExpectedPlan: plan, Approval: signature,
	})
	if callErr == nil {
		if validateApprovedAgentPlan(approved, plan, m.now().UTC()) != nil {
			callErr = ErrAgentCloudControlInvalidResponse
		} else {
			return map[string]any{"plan": agentCloudPlanView(approved), "submission_status": "waiting_connection"}, nil
		}
	}
	// Approval persistence and the post-approval launcher are separate Agent
	// steps. A lost response or a launcher precondition must never create fake
	// ProductCore Deployment/Job facts; recover only the durable approved Plan.
	recovered, recoveredFound, recoveredErr := m.cfg.AgentCloudControlClient.GetAgentCloudPlan(ctx, AgentCloudPlanRequest{PlanID: planID})
	if recoveredErr == nil && recoveredFound && validateApprovedAgentPlan(recovered, plan, m.now().UTC()) == nil {
		return map[string]any{"plan": agentCloudPlanView(recovered), "submission_status": "waiting_connection"}, nil
	}
	return nil, agentPlanApprovalError(callErr, true)
}

func (m *Module) completeAgentConnectionRegistration(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "bootstrap_id", "expected_revision", "session_id", "expected_session_revision", "plan_id", "expected_plan_revision", "approval", "idempotency_key"); err != nil {
		return nil, err
	}
	store, ok := m.store.(ConnectionCredentialBootstrapStore)
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	bootstrapID, sessionID, planID := values.String("bootstrap_id"), values.String("session_id"), values.String("plan_id")
	expectedRevision := values.Int64("expected_revision")
	expectedSessionRevision := values.Int64("expected_session_revision")
	expectedPlanRevision := values.Int64("expected_plan_revision")
	idempotencyKey := values.String("idempotency_key")
	// Agent has already accepted and persisted this exact device Approval. Its
	// challenge expiry is therefore not a second authorization deadline for
	// Foundation establishment; the approved quote, Role Plan, bootstrap
	// session and identity evidence must still be current below.
	approval, signature, decodeErr := decodeAgentCloudApproval(params["approval"], time.Time{})
	if !cloudIdentifierPattern.MatchString(bootstrapID) || expectedRevision <= 0 || !canonicalUUID(sessionID) || expectedSessionRevision <= 0 ||
		!canonicalUUID(planID) || expectedPlanRevision <= 1 || !canonicalUUID(idempotencyKey) || decodeErr != nil || approval.PlanID != planID ||
		approval.PlanRevision+1 != expectedPlanRevision {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionBootstrapInvalidCode, "cloud connection registration is invalid")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	load := LoadConnectionCredentialBootstrapRequest{OwnerMXID: ownerMXID, BootstrapID: bootstrapID, ExpectedRevision: expectedRevision, Now: m.now().UTC().UnixMilli()}
	rolePlan, err := store.LoadCloudConnectionCredentialBootstrap(ctx, load)
	if err != nil {
		return nil, connectionCredentialBootstrapStoreError(err)
	}
	roleSignerKeyID := rolePlan.CloudFormationParams["DeviceApprovalKeyId"]
	if rolePlan.Provider != "aws" || !rolePlan.AllowRootCredentialBootstrap || !canonicalUUID(rolePlan.CloudConnectionID) ||
		!cloudRegionPattern.MatchString(rolePlan.Region) || roleSignerKeyID == "" || approval.ConnectionID != rolePlan.CloudConnectionID ||
		approval.SignerKeyID != roleSignerKeyID {
		return nil, actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapConflictCode, "cloud connection registration conflicts with the owner role plan")
	}
	plan, found, getErr := m.cfg.AgentCloudControlClient.GetAgentCloudPlan(ctx, AgentCloudPlanRequest{PlanID: planID})
	if getErr != nil || !found {
		return nil, agentConnectionEstablishError(getErr, found)
	}
	if validateAgentCloudPlan(plan, m.now().UTC()) != nil || plan.Status != AgentCloudPlanStatusApproved || plan.PlanID != planID ||
		plan.Revision != expectedPlanRevision || plan.ConnectionID != rolePlan.CloudConnectionID || plan.Resource.Region != rolePlan.Region ||
		!agentApprovalMatchesPlan(approval, plan, true) {
		return nil, actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapConflictCode, "cloud connection registration conflicts with the approved Agent plan")
	}
	connection, callErr := m.cfg.AgentCloudControlClient.EstablishAgentAWSConnection(ctx, AgentCloudEstablishRequest{
		IdempotencyKey: idempotencyKey, BootstrapSessionID: sessionID, ExpectedSessionRevision: expectedSessionRevision,
		PlanID: planID, ExpectedPlanRevision: expectedPlanRevision, Approval: signature,
		ExpectedConnectionID: rolePlan.CloudConnectionID, ExpectedRegion: rolePlan.Region,
	})
	// Re-read the exact role-plan revision at the same authorization instant
	// used before the potentially slow AWS mutation. Crossing the short role-
	// plan expiry while Agent is establishing Foundation must not hide an
	// already-created, owner-bound Connection from the caller.
	if _, rereadErr := store.LoadCloudConnectionCredentialBootstrap(ctx, load); rereadErr != nil {
		return nil, connectionCredentialBootstrapStoreError(rereadErr)
	}
	if callErr == nil {
		if validateAgentCloudConnection(connection, plan.OwnerID, rolePlan.CloudConnectionID, rolePlan.Region) == nil {
			return map[string]any{"connection": agentCloudConnectionView(connection)}, nil
		}
		callErr = ErrAgentCloudControlInvalidResponse
	}
	recovered, recoveredFound, recoveredErr := m.cfg.AgentCloudControlClient.GetAgentCloudConnection(ctx, AgentCloudConnectionRequest{ConnectionID: rolePlan.CloudConnectionID})
	if recoveredErr == nil && recoveredFound && validateAgentCloudConnection(recovered, plan.OwnerID, rolePlan.CloudConnectionID, rolePlan.Region) == nil {
		return map[string]any{"connection": agentCloudConnectionView(recovered)}, nil
	}
	if errors.Is(callErr, ErrAgentCloudControlUnavailable) || errors.Is(callErr, ErrAgentCloudControlInvalidResponse) {
		return map[string]any{"connection": map[string]any{
			"cloud_connection_id": rolePlan.CloudConnectionID,
			"status":              agentCloudPendingReconciliation,
		}}, nil
	}
	return nil, agentConnectionEstablishError(callErr, true)
}

func agentCandidateID(tier string) string {
	switch tier {
	case "economy":
		return "economic"
	case "recommended", "performance":
		return tier
	default:
		return ""
	}
}

func agentCloudPlanView(plan AgentCloudPlan) map[string]any {
	return map[string]any{
		"plan_id": plan.PlanID, "cloud_connection_id": plan.ConnectionID, "status": plan.Status,
		"recipe_id": plan.Recipe.RecipeID, "recipe_digest": plan.Recipe.Digest, "recipe_maturity": plan.Recipe.Maturity,
		"quote_id": plan.QuoteID, "quote_digest": plan.QuoteDigest, "quote_scope_digest": plan.QuoteScopeDigest,
		"candidate_tier": agentCandidateTier(plan.CandidateProfile), "quote_valid_until": plan.QuoteValidUntil.UTC().Format(time.RFC3339Nano),
		"resource_scope": resourceScopeFromAgent(plan.Resource), "network_scope": networkScopeFromAgent(plan.Network),
		"secret_scope": secretScopeFromAgent(plan.SecretScope), "integration_scope": integrationScopeFromAgent(plan.IntegrationScope),
		"retention_scope": retentionScopeFromAgent(plan.Retention), "plan_hash": plan.PlanHash, "revision": plan.Revision,
	}
}

func agentCloudPlanViewWithQuote(plan AgentCloudPlan, quote AgentCloudQuote) (map[string]any, bool) {
	if quote.QuoteID != plan.QuoteID || quote.Digest != plan.QuoteDigest || !quote.ValidUntil.Equal(plan.QuoteValidUntil) {
		return nil, false
	}
	matched := false
	for _, candidate := range quote.Candidates {
		if candidate.CandidateProfile != plan.CandidateProfile {
			continue
		}
		leftResource, rightResource := candidate.Scope.Resource, plan.Resource
		leftResource.CandidateProfile, rightResource.CandidateProfile = "", ""
		matched = candidate.ScopeDigest == plan.QuoteScopeDigest && candidate.Scope.ConnectionID == plan.ConnectionID &&
			candidate.Scope.Recipe == plan.Recipe && reflect.DeepEqual(leftResource, rightResource) &&
			sameAgentCloudNetworkScope(candidate.Scope.Network, plan.Network) && reflect.DeepEqual(candidate.Scope.SecretScope, plan.SecretScope) &&
			reflect.DeepEqual(candidate.Scope.IntegrationScope, plan.IntegrationScope) && candidate.Scope.Retention == plan.Retention
		break
	}
	if !matched {
		return nil, false
	}
	projected, ok := agentCloudQuoteView(quote)
	if !ok || projected.ConnectionID != plan.ConnectionID {
		return nil, false
	}
	view := agentCloudPlanView(plan)
	view["quote"] = projected
	return view, true
}

func agentCloudQuoteView(quote AgentCloudQuote) (QuoteView, bool) {
	if quote.QuoteID == "" || quote.Currency == "" || len(quote.Candidates) != 3 || quote.QuotedAt.IsZero() || quote.ValidUntil.IsZero() {
		return QuoteView{}, false
	}
	result := QuoteView{
		QuoteID: quote.QuoteID, Currency: quote.Currency, QuotedAt: quote.QuotedAt.UTC(), ValidUntil: quote.ValidUntil.UTC(),
		Candidates: make([]QuoteCandidateView, 0, len(quote.Candidates)), IncludedItems: append([]string(nil), quote.Assumptions...),
		UnincludedItems: append([]string(nil), quote.Exclusions...),
	}
	seen := make(map[string]struct{}, len(quote.Candidates))
	for _, candidate := range quote.Candidates {
		resource := candidate.Scope.Resource
		tier := agentCandidateTier(candidate.CandidateProfile)
		if tier == "" || candidate.Scope.ConnectionID == "" || resource.Region == "" || resource.VCPU > 65535 || resource.MemoryMiB > 1<<32-1 ||
			resource.GPUCount > 65535 || resource.GPUMemoryMiB > 1<<32-1 || resource.DiskGiB > 1<<32-1 {
			return QuoteView{}, false
		}
		if _, duplicate := seen[tier]; duplicate {
			return QuoteView{}, false
		}
		seen[tier] = struct{}{}
		if result.ConnectionID == "" {
			result.ConnectionID, result.Region = candidate.Scope.ConnectionID, resource.Region
		} else if result.ConnectionID != candidate.Scope.ConnectionID || result.Region != resource.Region {
			return QuoteView{}, false
		}
		costItems := make([]QuoteCostItemView, 0, len(candidate.CostItems))
		for _, item := range candidate.CostItems {
			if item.Category == "" || item.Description == "" || item.SourceID == "" {
				return QuoteView{}, false
			}
			costItems = append(costItems, QuoteCostItemView{
				Category: item.Category, Description: item.Description, SourceID: item.SourceID,
				HourlyEstimateMicros: item.HourlyEstimateMicros, MonthlyEstimateMicros: item.MonthlyEstimateMicros,
				MaximumLaunchAmountMicros: item.MaximumLaunchAmountMicros,
			})
		}
		result.Candidates = append(result.Candidates, QuoteCandidateView{
			Tier: tier, InstanceType: resource.InstanceType, PurchaseOption: resource.PurchaseOption, Architecture: resource.Architecture,
			VCPU: uint16(resource.VCPU), MemoryMiB: uint32(resource.MemoryMiB), GPUCount: uint16(resource.GPUCount),
			GPUMemoryMiB: uint32(resource.GPUMemoryMiB), HourlyMinor: agentCloudMicrosToMinor(candidate.HourlyEstimateMicros),
			ThirtyDayMinor: agentCloudMicrosToMinor(candidate.MonthlyEstimateMicros), StartupUpperMinor: agentCloudMicrosToMinor(candidate.MaximumLaunchAmountMicros),
			EstimatedDiskGiB: uint32(resource.DiskGiB), AvailabilityZones: append([]string(nil), candidate.OfferedAvailabilityZones...),
			WorkerImageID: resource.WorkerImageID, WorkerImageDigest: resource.WorkerImageDigest, CostItems: costItems,
		})
	}
	return result, result.ConnectionID != "" && result.Region != ""
}

func agentCloudMicrosToMinor(value uint64) int64 {
	minor := value / 10_000
	if value%10_000 != 0 {
		minor++
	}
	return int64(minor)
}

func agentCloudPlanSummary(plan AgentCloudPlan) Plan {
	return Plan{
		PlanID: plan.PlanID, ConnectionID: plan.ConnectionID, Status: plan.Status,
		RecipeDigest: plan.Recipe.Digest, RecipeID: plan.Recipe.RecipeID,
		QuoteID: plan.QuoteID, PlanHash: plan.PlanHash, Revision: plan.Revision,
	}
}

func agentCandidateTier(candidateID string) string {
	if candidateID == "economic" {
		return "economy"
	}
	if candidateID == "recommended" || candidateID == "performance" {
		return candidateID
	}
	return ""
}

func agentApprovalFromChallenge(plan AgentCloudPlan, challenge AgentCloudChallenge) agentCloudApprovalV1 {
	return agentCloudApprovalV1{
		SchemaVersion: agentCloudApprovalSchema, HashAlgorithm: agentCloudHashAlgorithm,
		ApprovalID: challenge.ApprovalID, AgentInstanceID: challenge.AgentInstanceID, OwnerID: challenge.OwnerID,
		PlanID: plan.PlanID, PlanRevision: plan.Revision, PlanHash: plan.PlanHash, ConnectionID: plan.ConnectionID,
		RecipeDigest: plan.Recipe.Digest, QuoteID: plan.QuoteID, QuoteDigest: plan.QuoteDigest,
		QuoteScopeDigest: plan.QuoteScopeDigest, QuoteCandidateID: challenge.QuoteCandidateID,
		QuoteValidUntil: plan.QuoteValidUntil.UTC(), ResourceScope: resourceScopeFromAgent(plan.Resource),
		NetworkScope: networkScopeFromAgent(plan.Network), SecretScope: secretScopeFromAgent(plan.SecretScope),
		IntegrationScope: integrationScopeFromAgent(plan.IntegrationScope), RetentionScope: retentionScopeFromAgent(plan.Retention),
		ChallengeID: challenge.ChallengeID, SignerKeyID: challenge.SignerKeyID, ExpiresAt: challenge.ExpiresAt.UTC(),
	}
}

func resourceScopeFromAgent(value AgentCloudResourceScope) agentCloudResourceScopeV1 {
	volumes := make([]agentCloudVolumeScopeV1, len(value.VolumeScopes))
	for index, volume := range value.VolumeScopes {
		volumes[index] = agentCloudVolumeScopeV1{
			SlotID: volume.SlotID, SizeGiB: volume.SizeGiB, VolumeType: volume.VolumeType, IOPS: volume.IOPS,
			ThroughputMiBPS: volume.ThroughputMiBPS, Encrypted: volume.Encrypted, KMSKeyID: volume.KMSKeyID,
			DeviceName: volume.DeviceName, MountPath: volume.MountPath, ReadOnly: volume.ReadOnly,
			Persistent: volume.Persistent, Disposition: volume.Disposition,
		}
	}
	return agentCloudResourceScopeV1{
		Region: value.Region, AvailabilityZones: append([]string(nil), value.AvailabilityZones...), InstanceType: value.InstanceType,
		InstanceCount: value.InstanceCount, Architecture: value.Architecture, VCPU: value.VCPU, MemoryMiB: value.MemoryMiB,
		GPUType: value.GPUType, GPUCount: value.GPUCount, GPUMemoryMiB: value.GPUMemoryMiB, DiskGiB: value.DiskGiB,
		VolumeType: value.VolumeType, VolumeIOPS: value.VolumeIOPS, VolumeThroughputMiBPS: value.VolumeThroughputMiBPS,
		VolumeEncrypted: value.VolumeEncrypted, PurchaseOption: value.PurchaseOption,
		WorkerImageID: value.WorkerImageID, WorkerImageDigest: value.WorkerImageDigest,
		VolumeScopes: volumes,
	}
}

func networkScopeFromAgent(value AgentCloudNetworkScope) agentCloudNetworkScopeV1 {
	value = normalizeAgentCloudNetworkScope(value)
	return agentCloudNetworkScopeV1{
		VPCID: value.VPCID, SubnetID: value.SubnetID, SecurityGroupID: value.SecurityGroupID, SecurityGroupMode: value.SecurityGroupMode, EntryPoint: value.EntryPoint,
		PublicIPv4: value.PublicIPv4, PublicExposure: value.PublicExposure, IngressPorts: append([]uint32(nil), value.IngressPorts...), Hostname: value.Hostname,
		TLSRequired: value.TLSRequired, AuthenticationRequired: value.AuthenticationRequired,
	}
}

func normalizeAgentCloudNetworkScope(value AgentCloudNetworkScope) AgentCloudNetworkScope {
	if value.SecurityGroupMode == "" && value.SecurityGroupID != "" {
		value.SecurityGroupMode = "existing"
	}
	return value
}

func sameAgentCloudNetworkScope(left, right AgentCloudNetworkScope) bool {
	return reflect.DeepEqual(normalizeAgentCloudNetworkScope(left), normalizeAgentCloudNetworkScope(right))
}

func secretScopeFromAgent(values []AgentCloudSecretScope) []agentCloudSecretScopeV1 {
	result := make([]agentCloudSecretScopeV1, len(values))
	for index, value := range values {
		result[index] = agentCloudSecretScopeV1{SecretRef: value.SecretRef, Purpose: value.Purpose, Delivery: value.Delivery}
	}
	return result
}

func integrationScopeFromAgent(values []AgentCloudIntegrationScope) []agentCloudIntegrationV1 {
	result := make([]agentCloudIntegrationV1, len(values))
	for index, value := range values {
		result[index] = agentCloudIntegrationV1{Kind: value.Kind, Name: value.Name, Scopes: append([]string(nil), value.Scopes...)}
	}
	return result
}

func retentionScopeFromAgent(value AgentCloudRetentionScope) agentCloudRetentionScopeV1 {
	return agentCloudRetentionScopeV1{Class: value.Class, AutoDestroy: value.AutoDestroy, GracePeriodSeconds: value.GracePeriodSeconds, MaxLifetimeSeconds: value.MaxLifetimeSeconds}
}

func decodeAgentCloudApproval(raw any, now time.Time) (agentCloudApprovalV1, AgentCloudApprovalSignature, error) {
	encoded, err := json.Marshal(raw)
	if err != nil || len(encoded) > 64*1024 {
		return agentCloudApprovalV1{}, AgentCloudApprovalSignature{}, ErrAgentCloudControlInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var approval agentCloudApprovalV1
	if err = decoder.Decode(&approval); err != nil {
		return agentCloudApprovalV1{}, AgentCloudApprovalSignature{}, ErrAgentCloudControlInvalid
	}
	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return agentCloudApprovalV1{}, AgentCloudApprovalSignature{}, ErrAgentCloudControlInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(approval.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize || approval.SchemaVersion != agentCloudApprovalSchema || approval.HashAlgorithm != agentCloudHashAlgorithm ||
		!canonicalUUID(approval.ApprovalID) || !canonicalUUID(approval.PlanID) || approval.PlanRevision <= 0 || !namedSHA256Pattern.MatchString(approval.PlanHash) ||
		!canonicalUUID(approval.ConnectionID) || !namedSHA256Pattern.MatchString(approval.RecipeDigest) || !canonicalUUID(approval.QuoteID) ||
		!namedSHA256Pattern.MatchString(approval.QuoteDigest) || !namedSHA256Pattern.MatchString(approval.QuoteScopeDigest) ||
		agentCandidateTier(approval.QuoteCandidateID) == "" || !cloudKeyIDPattern.MatchString(approval.SignerKeyID) || approval.ChallengeID == "" ||
		approval.AgentInstanceID == "" || approval.OwnerID == "" || approval.QuoteValidUntil.IsZero() || approval.ExpiresAt.IsZero() ||
		(!now.IsZero() && !now.Before(approval.ExpiresAt)) || approval.ExpiresAt.After(approval.QuoteValidUntil) || validateAgentApprovalScopes(approval) != nil {
		return agentCloudApprovalV1{}, AgentCloudApprovalSignature{}, ErrAgentCloudControlInvalid
	}
	return approval, AgentCloudApprovalSignature{
		ApprovalID: approval.ApprovalID, ChallengeID: approval.ChallengeID, SignerKeyID: approval.SignerKeyID,
		ExpiresAt: approval.ExpiresAt.UTC(), Signature: append([]byte(nil), signature...),
	}, nil
}

func validateAgentCloudPlan(plan AgentCloudPlan, now time.Time) error {
	if validateReadableAgentCloudPlan(plan) != nil || !now.Before(plan.QuoteValidUntil) ||
		(plan.Status != agentCloudReadyForConfirmation && plan.Status != AgentCloudPlanStatusApproved) {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func validateReadableAgentCloudPlan(plan AgentCloudPlan) error {
	if !canonicalUUID(plan.PlanID) || plan.OwnerID == "" || !canonicalUUID(plan.ConnectionID) || !canonicalUUID(plan.QuoteID) ||
		plan.Revision <= 0 || !namedSHA256Pattern.MatchString(plan.PlanHash) || !namedSHA256Pattern.MatchString(plan.Recipe.Digest) ||
		!namedSHA256Pattern.MatchString(plan.QuoteDigest) || !namedSHA256Pattern.MatchString(plan.QuoteScopeDigest) ||
		agentCandidateTier(plan.CandidateProfile) == "" || plan.Recipe.RecipeID == "" || plan.QuoteValidUntil.IsZero() ||
		!readableAgentCloudPlanStatus(plan.Status) {
		return ErrAgentCloudControlInvalidResponse
	}
	probe := agentCloudApprovalV1{
		ResourceScope: resourceScopeFromAgent(plan.Resource), NetworkScope: networkScopeFromAgent(plan.Network),
		SecretScope: secretScopeFromAgent(plan.SecretScope), IntegrationScope: integrationScopeFromAgent(plan.IntegrationScope),
		RetentionScope: retentionScopeFromAgent(plan.Retention),
	}
	return validateAgentApprovalScopes(probe)
}

func readableAgentCloudPlanStatus(status string) bool {
	switch status {
	case "researching", "quoting", agentCloudReadyForConfirmation, AgentCloudPlanStatusApproved, "expired", "superseded":
		return true
	default:
		return false
	}
}

func validateAgentCloudChallenge(value AgentCloudChallenge, plan AgentCloudPlan, signerKeyID string, now time.Time) error {
	if !canonicalUUID(value.ApprovalID) || value.ChallengeID == "" || value.SignerKeyID != signerKeyID || value.AgentInstanceID == "" ||
		value.OwnerID != plan.OwnerID || value.PlanID != plan.PlanID || value.PlanRevision != plan.Revision || value.PlanHash != plan.PlanHash ||
		value.ConnectionID != plan.ConnectionID || value.RecipeDigest != plan.Recipe.Digest || value.QuoteID != plan.QuoteID ||
		value.QuoteDigest != plan.QuoteDigest || value.QuoteScopeDigest != plan.QuoteScopeDigest || value.QuoteCandidateID != plan.CandidateProfile ||
		value.Revision <= 0 || value.ExpiresAt.IsZero() || !now.Before(value.ExpiresAt) || value.ExpiresAt.After(plan.QuoteValidUntil) ||
		len(value.SigningPayloadCBOR) == 0 || len(value.SigningPayloadCBOR) > 64*1024 {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func validateApprovedAgentPlan(approved, prior AgentCloudPlan, now time.Time) error {
	if validateAgentCloudPlan(approved, now) != nil || approved.Status != AgentCloudPlanStatusApproved || approved.Revision != prior.Revision+1 ||
		approved.PlanHash == prior.PlanHash ||
		approved.PlanID != prior.PlanID || approved.OwnerID != prior.OwnerID || approved.ConnectionID != prior.ConnectionID ||
		approved.Recipe != prior.Recipe || approved.QuoteID != prior.QuoteID || approved.QuoteDigest != prior.QuoteDigest ||
		approved.QuoteScopeDigest != prior.QuoteScopeDigest || approved.CandidateProfile != prior.CandidateProfile ||
		!approved.QuoteValidUntil.Equal(prior.QuoteValidUntil) || !reflect.DeepEqual(approved.Resource, prior.Resource) ||
		!reflect.DeepEqual(approved.Network, prior.Network) || !reflect.DeepEqual(approved.SecretScope, prior.SecretScope) ||
		!reflect.DeepEqual(approved.IntegrationScope, prior.IntegrationScope) || approved.Retention != prior.Retention {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func agentApprovalMatchesPlan(approval agentCloudApprovalV1, plan AgentCloudPlan, approved bool) bool {
	revisionMatches := approval.PlanRevision == plan.Revision && approval.PlanHash == plan.PlanHash
	if approved {
		revisionMatches = approval.PlanRevision+1 == plan.Revision
	}
	return revisionMatches && approval.OwnerID == plan.OwnerID && approval.PlanID == plan.PlanID && approval.ConnectionID == plan.ConnectionID &&
		approval.RecipeDigest == plan.Recipe.Digest && approval.QuoteID == plan.QuoteID && approval.QuoteDigest == plan.QuoteDigest &&
		approval.QuoteScopeDigest == plan.QuoteScopeDigest && approval.QuoteCandidateID == plan.CandidateProfile &&
		approval.QuoteValidUntil.Equal(plan.QuoteValidUntil) && reflect.DeepEqual(approval.ResourceScope, resourceScopeFromAgent(plan.Resource)) &&
		reflect.DeepEqual(approval.NetworkScope, networkScopeFromAgent(plan.Network)) && reflect.DeepEqual(approval.SecretScope, secretScopeFromAgent(plan.SecretScope)) &&
		reflect.DeepEqual(approval.IntegrationScope, integrationScopeFromAgent(plan.IntegrationScope)) && approval.RetentionScope == retentionScopeFromAgent(plan.Retention)
}

func validateAgentApprovalScopes(value agentCloudApprovalV1) error {
	r := value.ResourceScope
	if !cloudRegionPattern.MatchString(r.Region) || strings.TrimSpace(r.InstanceType) == "" || r.InstanceCount == 0 || r.VCPU == 0 || r.MemoryMiB == 0 ||
		r.DiskGiB == 0 || strings.TrimSpace(r.Architecture) == "" || strings.TrimSpace(r.VolumeType) == "" ||
		(r.PurchaseOption != "on_demand" && r.PurchaseOption != "spot") || strings.TrimSpace(r.WorkerImageID) == "" || !namedSHA256Pattern.MatchString(r.WorkerImageDigest) {
		return ErrAgentCloudControlInvalidResponse
	}
	n := value.NetworkScope
	if n.EntryPoint != "none" && n.EntryPoint != "alb" && n.EntryPoint != "cloudfront" {
		return ErrAgentCloudControlInvalidResponse
	}
	for _, port := range n.IngressPorts {
		if port == 0 || port > 65535 {
			return ErrAgentCloudControlInvalidResponse
		}
	}
	for _, secret := range value.SecretScope {
		if strings.TrimSpace(secret.SecretRef) == "" || strings.TrimSpace(secret.Purpose) == "" || strings.TrimSpace(secret.Delivery) == "" ||
			ContainsSensitiveGoalMaterial(secret.SecretRef) || ContainsSensitiveGoalMaterial(secret.Purpose) {
			return ErrAgentCloudControlInvalidResponse
		}
	}
	for _, integration := range value.IntegrationScope {
		if strings.TrimSpace(integration.Kind) == "" || strings.TrimSpace(integration.Name) == "" {
			return ErrAgentCloudControlInvalidResponse
		}
	}
	if value.RetentionScope.Class != "ephemeral" && value.RetentionScope.Class != "managed" {
		return ErrAgentCloudControlInvalidResponse
	}
	if !validAgentApprovalVolumes(r.VolumeScopes, value.RetentionScope.Class) {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func validAgentApprovalVolumes(values []agentCloudVolumeScopeV1, retentionClass string) bool {
	if len(values) > 11 {
		return false
	}
	seenSlots := make(map[string]struct{}, len(values))
	seenDevices := make(map[string]struct{}, len(values))
	seenMounts := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !agentCloudVolumeSlotPattern.MatchString(value.SlotID) || value.SizeGiB == 0 || value.SizeGiB > 65_536 || value.VolumeType != "gp3" ||
			value.IOPS < 3_000 || value.IOPS > 80_000 || value.ThroughputMiBPS < 125 || value.ThroughputMiBPS > 2_000 ||
			!value.Encrypted || !agentCloudVolumeKMSPattern.MatchString(value.KMSKeyID) || !agentCloudVolumeDevicePattern.MatchString(value.DeviceName) ||
			value.MountPath == "" || value.MountPath == "/" || !strings.HasPrefix(value.MountPath, "/") ||
			path.Clean(value.MountPath) != value.MountPath || strings.Contains(value.MountPath, "\\") {
			return false
		}
		for _, reserved := range []string{"/dev", "/proc", "/sys", "/run/secrets"} {
			if value.MountPath == reserved || strings.HasPrefix(value.MountPath, reserved+"/") {
				return false
			}
		}
		if _, duplicate := seenSlots[value.SlotID]; duplicate {
			return false
		}
		if _, duplicate := seenDevices[value.DeviceName]; duplicate {
			return false
		}
		if _, duplicate := seenMounts[value.MountPath]; duplicate {
			return false
		}
		seenSlots[value.SlotID] = struct{}{}
		seenDevices[value.DeviceName] = struct{}{}
		seenMounts[value.MountPath] = struct{}{}
		if (retentionClass == "ephemeral" && value.Disposition != "delete_with_deployment") ||
			(retentionClass == "managed" && value.Disposition != "retain_with_managed_service") {
			return false
		}
	}
	return true
}

func validateAgentCloudConnection(value AgentCloudConnection, ownerID, connectionID, region string) error {
	if validateReadableAgentCloudConnection(value, connectionID) != nil || value.OwnerID != ownerID || value.Region != region ||
		value.Status != "active" {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func validateReadableAgentCloudConnection(value AgentCloudConnection, connectionID string) error {
	if value.ConnectionID != connectionID || value.OwnerID == "" || !canonicalUUID(value.ConnectionID) ||
		!awsAccountIDPattern.MatchString(value.AccountID) || strings.TrimSpace(value.ControlRoleARN) == "" || strings.TrimSpace(value.FoundationStackID) == "" ||
		!readableAgentCloudConnectionStatus(value.Status) || value.Revision <= 0 || value.CredentialGeneration <= 0 || value.CreatedAt.IsZero() ||
		value.UpdatedAt.Before(value.CreatedAt) {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func readableAgentCloudConnectionStatus(status string) bool {
	switch status {
	case "establishing", "active", "degraded", "teardown_blocked", "destroyed":
		return true
	default:
		return false
	}
}

func agentCloudConnectionView(value AgentCloudConnection) map[string]any {
	return map[string]any{
		"cloud_connection_id": value.ConnectionID, "provider": "aws", "account_id": value.AccountID, "region": value.Region,
		"control_role_arn": value.ControlRoleARN, "foundation_stack_id": value.FoundationStackID, "status": value.Status,
		"revision": value.Revision, "credential_generation": value.CredentialGeneration,
		"created_at": value.CreatedAt.UTC().UnixMilli(), "updated_at": value.UpdatedAt.UTC().UnixMilli(),
	}
}

func agentCloudConnectionSummary(value AgentCloudConnection) Connection {
	return Connection{
		ConnectionID: value.ConnectionID, Provider: "aws", AccountID: value.AccountID, Region: value.Region,
		Mode: agentCloudConnectionMode, Status: value.Status, Revision: value.Revision,
		CreatedAt: value.CreatedAt.UTC().UnixMilli(), UpdatedAt: value.UpdatedAt.UTC().UnixMilli(),
	}
}

func agentPlanConfirmationError(err error, found bool) *actionbase.Error {
	if !found || errors.Is(err, ErrAgentCloudControlConflict) {
		return actionbase.CodedError(http.StatusConflict, cloudPlanConfirmationConflictCode, "cloud plan confirmation conflicts with the current Agent plan")
	}
	if errors.Is(err, ErrAgentCloudControlInvalid) {
		return actionbase.CodedError(http.StatusBadRequest, cloudPlanConfirmationInvalidCode, "cloud plan confirmation is invalid")
	}
	if errors.Is(err, ErrAgentCloudControlRejected) {
		return actionbase.CodedError(http.StatusForbidden, cloudPlanApprovalSignatureCode, "cloud approval device is not registered for this owner")
	}
	if errors.Is(err, ErrAgentCloudControlInvalidResponse) {
		return actionbase.CodedError(http.StatusBadGateway, cloudPlanConfirmationInvalidCode, "cloud Agent returned an invalid plan confirmation")
	}
	return actionbase.CodedError(http.StatusServiceUnavailable, cloudUnavailableCode, "cloud Agent control is unavailable")
}

func agentPlanApprovalError(err error, found bool) *actionbase.Error {
	if !found || errors.Is(err, ErrAgentCloudControlConflict) {
		return actionbase.CodedError(http.StatusConflict, cloudPlanApprovalConflictCode, "cloud plan approval conflicts with the current Agent plan")
	}
	if errors.Is(err, ErrAgentCloudControlInvalid) {
		return actionbase.CodedError(http.StatusBadRequest, cloudPlanApprovalInvalidCode, "cloud plan approval is invalid")
	}
	if errors.Is(err, ErrAgentCloudControlRejected) {
		return actionbase.CodedError(http.StatusUnauthorized, cloudPlanApprovalSignatureCode, "cloud plan approval signature is invalid")
	}
	if errors.Is(err, ErrAgentCloudControlInvalidResponse) {
		return actionbase.CodedError(http.StatusBadGateway, cloudPlanApprovalInvalidCode, "cloud Agent returned an invalid approval result")
	}
	return actionbase.CodedError(http.StatusServiceUnavailable, cloudUnavailableCode, "cloud Agent control is unavailable")
}

func agentConnectionEstablishError(err error, found bool) *actionbase.Error {
	if !found || errors.Is(err, ErrAgentCloudControlConflict) {
		return actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapConflictCode, "cloud connection establishment conflicts with current Agent state")
	}
	if errors.Is(err, ErrAgentCloudControlInvalid) {
		return actionbase.CodedError(http.StatusBadRequest, cloudConnectionBootstrapInvalidCode, "cloud connection establishment is invalid")
	}
	if errors.Is(err, ErrAgentCloudControlRejected) {
		return actionbase.CodedError(http.StatusUnauthorized, cloudPlanApprovalSignatureCode, "cloud connection approval signature is invalid")
	}
	if errors.Is(err, ErrAgentCloudControlInvalidResponse) {
		return actionbase.CodedError(http.StatusBadGateway, cloudConnectionBootstrapInvalidCode, "cloud Agent returned an invalid connection")
	}
	return actionbase.CodedError(http.StatusServiceUnavailable, cloudUnavailableCode, "cloud Agent control is unavailable")
}
