package agentgrpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strconv"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/fxamacker/cbor/v2"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	managedPreparationInstancePattern = regexp.MustCompile(`^i-[0-9a-f]{8,17}$`)
	managedPreparationVolumePattern   = regexp.MustCompile(`^vol-[0-9a-f]{8,17}$`)
	managedPreparationZonePattern     = regexp.MustCompile(`^[a-z]{2}(?:-[a-z0-9]+)+-[0-9]+[a-z]$`)
	managedPreparationDevicePattern   = regexp.MustCompile(`^/dev/sd[f-p]$`)
	managedPreparationCurrencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
	managedPreparationKMSPattern      = regexp.MustCompile(`^(?:alias/[A-Za-z0-9/_-]{1,240}|arn:(?:aws|aws-cn|aws-us-gov):kms:[a-z0-9-]+:[0-9]{12}:(?:key/[0-9a-f-]{36}|alias/[A-Za-z0-9/_-]{1,240}))$`)
)

func (runner *Runner) CreateCloudManagedPreparation(ctx context.Context, request cloudmodule.AgentCloudManagedPreparationCreateRequest) (cloudmodule.AgentCloudManagedPreparationChallenge, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudManagedPreparationChallenge{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.DeploymentID) || request.ExpectedDeploymentRevision <= 0 ||
		request.CostAlertAmountMinor <= 0 || !agentFoundationKeyIDPattern.MatchString(request.SignerKeyID) ||
		cloudmodule.ContainsSensitiveGoalMaterial(request.SignerKeyID) {
		return cloudmodule.AgentCloudManagedPreparationChallenge{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.CreateCloudManagedPreparation(callContext, &agentv1.CreateCloudManagedPreparationRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, DeploymentId: request.DeploymentID,
		SignerKeyId: request.SignerKeyID, ExpectedDeploymentRevision: request.ExpectedDeploymentRevision,
		CostAlertAmountMinor: request.CostAlertAmountMinor,
	})
	if err != nil {
		return cloudmodule.AgentCloudManagedPreparationChallenge{}, mapAgentCloudControlRPCError(callContext, err)
	}
	challenge, mapErr := mapManagedPreparationChallenge(response.GetChallenge())
	if mapErr != nil || challenge.Scope.OwnerID != runner.ownerID || challenge.Scope.DeploymentID != request.DeploymentID ||
		challenge.Scope.DeploymentRevision != request.ExpectedDeploymentRevision || challenge.SignerKeyID != request.SignerKeyID ||
		challenge.Scope.CostAlertAmountMinor != request.CostAlertAmountMinor {
		return cloudmodule.AgentCloudManagedPreparationChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return challenge, nil
}

func (runner *Runner) ApproveCloudManagedPreparation(ctx context.Context, request cloudmodule.AgentCloudManagedPreparationApproveRequest) (cloudmodule.AgentCloudManagedPreparationOperation, error) {
	approval := request.Approval
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudManagedPreparationOperation{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.OperationID) || !validUUID(request.DeploymentID) ||
		request.ExpectedRevision != 1 || !agentCloudDigestPattern.MatchString(request.ScopeDigest) ||
		approval.ApprovalID != request.OperationID || approval.OperationID != request.OperationID ||
		!validUUID(approval.ChallengeID) || !agentFoundationKeyIDPattern.MatchString(approval.SignerKeyID) ||
		approval.ExpiresAt.IsZero() || len(approval.Signature) != 64 {
		return cloudmodule.AgentCloudManagedPreparationOperation{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.ApproveCloudManagedPreparation(callContext, &agentv1.ApproveCloudManagedPreparationRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, OperationId: request.OperationID,
		DeploymentId: request.DeploymentID, ScopeDigest: request.ScopeDigest, ExpectedRevision: request.ExpectedRevision,
		Approval: &agentv1.DeviceApprovalSignature{ApprovalId: approval.ApprovalID, ChallengeId: approval.ChallengeID,
			SignerKeyId: approval.SignerKeyID, ExpiresAt: timestamppb.New(approval.ExpiresAt), Signature: append([]byte(nil), approval.Signature...)},
	})
	if err != nil {
		return cloudmodule.AgentCloudManagedPreparationOperation{}, mapAgentCloudControlRPCError(callContext, err)
	}
	operation, mapErr := mapManagedPreparationOperation(response.GetOperation())
	if mapErr != nil || operation.OperationID != request.OperationID || operation.Challenge.Scope.OwnerID != runner.ownerID ||
		operation.Challenge.Scope.DeploymentID != request.DeploymentID || operation.Challenge.ScopeDigest != request.ScopeDigest {
		return cloudmodule.AgentCloudManagedPreparationOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return operation, nil
}

func (runner *Runner) GetCloudManagedPreparation(ctx context.Context, request cloudmodule.AgentCloudManagedPreparationGetRequest) (cloudmodule.AgentCloudManagedPreparationOperation, bool, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudManagedPreparationOperation{}, false, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.OperationID) {
		return cloudmodule.AgentCloudManagedPreparationOperation{}, false, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.GetCloudManagedPreparation(callContext, &agentv1.GetCloudManagedPreparationRequest{
		OwnerId: runner.ownerID, OperationId: request.OperationID,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound && callContext.Err() == nil {
			return cloudmodule.AgentCloudManagedPreparationOperation{}, false, nil
		}
		return cloudmodule.AgentCloudManagedPreparationOperation{}, false, mapAgentCloudControlRPCError(callContext, err)
	}
	operation, mapErr := mapManagedPreparationOperation(response.GetOperation())
	if mapErr != nil || operation.OperationID != request.OperationID || operation.Challenge.Scope.OwnerID != runner.ownerID {
		return cloudmodule.AgentCloudManagedPreparationOperation{}, false, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return operation, true, nil
}

type managedPreparationSigningPayload struct {
	SchemaVersion  string                                        `json:"schema_version"`
	PayloadVersion string                                        `json:"payload_version"`
	Intent         string                                        `json:"intent"`
	ChallengeID    string                                        `json:"challenge_id"`
	OperationID    string                                        `json:"operation_id"`
	SignerKeyID    string                                        `json:"signer_key_id"`
	Scope          cloudmodule.AgentCloudManagedPreparationScope `json:"scope"`
	IssuedAt       time.Time                                     `json:"issued_at"`
	ExpiresAt      time.Time                                     `json:"expires_at"`
}

func mapManagedPreparationChallenge(value *agentv1.CloudManagedPreparationChallenge) (cloudmodule.AgentCloudManagedPreparationChallenge, error) {
	if value == nil || value.GetScope() == nil || value.GetSchemaVersion() != "dirextalk.agent.cloud.service-operation-challenge/v1" ||
		!validUUID(value.GetChallengeId()) || !validUUID(value.GetOperationId()) ||
		!agentFoundationKeyIDPattern.MatchString(value.GetSignerKeyId()) || !agentCloudDigestPattern.MatchString(value.GetScopeDigest()) ||
		len(value.GetSigningPayloadCbor()) == 0 || len(value.GetSigningPayloadCbor()) > 64*1024 {
		return cloudmodule.AgentCloudManagedPreparationChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	issuedAt, issuedErr := exactAgentCloudTimestamp(value.GetIssuedAt())
	expiresAt, expiresErr := exactAgentCloudTimestamp(value.GetExpiresAt())
	if issuedErr != nil || expiresErr != nil || !issuedAt.Before(expiresAt) || expiresAt.Sub(issuedAt) > 5*time.Minute {
		return cloudmodule.AgentCloudManagedPreparationChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	scope, err := mapManagedPreparationScope(value.GetScope())
	if err != nil || scope.PreparationOperationID != value.GetOperationId() {
		return cloudmodule.AgentCloudManagedPreparationChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	payload, err := canonicalManagedPreparationPayload(managedPreparationSigningPayload{
		SchemaVersion: value.GetSchemaVersion(), PayloadVersion: "dirextalk.agent.cloud.service-operation-signing-payload/v1",
		Intent: "MANAGED_PREPARATION", ChallengeID: value.GetChallengeId(), OperationID: value.GetOperationId(),
		SignerKeyID: value.GetSignerKeyId(), Scope: scope, IssuedAt: issuedAt, ExpiresAt: expiresAt,
	})
	if err != nil || !bytes.Equal(payload, value.GetSigningPayloadCbor()) {
		return cloudmodule.AgentCloudManagedPreparationChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	sum := sha256.Sum256(payload)
	if value.GetScopeDigest() != "sha256:"+hex.EncodeToString(sum[:]) {
		return cloudmodule.AgentCloudManagedPreparationChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.AgentCloudManagedPreparationChallenge{
		SchemaVersion: value.GetSchemaVersion(), ChallengeID: value.GetChallengeId(), OperationID: value.GetOperationId(),
		SignerKeyID: value.GetSignerKeyId(), Scope: scope, ScopeDigest: value.GetScopeDigest(), IssuedAt: issuedAt,
		ExpiresAt: expiresAt, SigningPayloadCBOR: append([]byte(nil), payload...), Revision: 1,
	}, nil
}

func mapManagedPreparationScope(value *agentv1.CloudManagedPreparationScope) (cloudmodule.AgentCloudManagedPreparationScope, error) {
	if value == nil || value.GetEc2() == nil || value.GetRestart() == nil ||
		value.GetSchemaVersion() != "dirextalk.agent.cloud.service-operation-scope/v1" || value.GetIntent() != "MANAGED_PREPARATION" ||
		!validUUID(value.GetPreparationOperationId()) || !agentCloudIdentifierPattern.MatchString(value.GetOwnerId()) ||
		!validUUID(value.GetAgentInstanceId()) || !validUUID(value.GetDeploymentId()) || value.GetDeploymentRevision() <= 0 ||
		!validUUID(value.GetConnectionId()) || value.GetConnectionRevision() <= 0 || !validUUID(value.GetPlanId()) ||
		value.GetPlanRevision() <= 0 || !agentCloudDigestPattern.MatchString(value.GetPlanHash()) ||
		!agentCloudIdentifierPattern.MatchString(value.GetRecipeId()) || !agentCloudDigestPattern.MatchString(value.GetRecipeDigest()) ||
		value.GetRecipeRevision() <= 0 || value.GetServiceMonitorRevision() <= 0 ||
		!agentCloudDigestPattern.MatchString(value.GetServiceMonitorSuiteDigest()) ||
		!managedPreparationCurrencyPattern.MatchString(value.GetCurrency()) || value.GetCostAlertAmountMinor() <= 0 ||
		!agentCloudDigestPattern.MatchString(value.GetExpectedInstalledManifestDigest()) ||
		len(value.GetSourceVolumes()) == 0 || len(value.GetSourceVolumes()) != len(value.GetVolumes()) {
		return cloudmodule.AgentCloudManagedPreparationScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	ec2, err := mapManagedPreparationResource(value.GetEc2(), managedPreparationInstancePattern)
	if err != nil {
		return cloudmodule.AgentCloudManagedPreparationScope{}, err
	}
	restart := value.GetRestart()
	expectedRestart := uuid.NewSHA1(uuid.MustParse(value.GetPreparationOperationId()), []byte("restart")).String()
	if restart.GetOperationId() != expectedRestart || restart.GetExpectedInitialRevision() != 1 || restart.GetAction() != "restart" ||
		!agentCloudIdentifierPattern.MatchString(restart.GetLifecycleRestartRef()) ||
		!agentCloudDigestPattern.MatchString(restart.GetExecutionBundleDigest()) {
		return cloudmodule.AgentCloudManagedPreparationScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	result := cloudmodule.AgentCloudManagedPreparationScope{
		SchemaVersion: value.GetSchemaVersion(), Intent: value.GetIntent(), PreparationOperationID: value.GetPreparationOperationId(),
		OwnerID: value.GetOwnerId(), AgentInstanceID: value.GetAgentInstanceId(), DeploymentID: value.GetDeploymentId(),
		DeploymentRevision: value.GetDeploymentRevision(), ConnectionID: value.GetConnectionId(), ConnectionRevision: value.GetConnectionRevision(),
		PlanID: value.GetPlanId(), PlanRevision: value.GetPlanRevision(), PlanHash: value.GetPlanHash(), RecipeID: value.GetRecipeId(),
		RecipeDigest: value.GetRecipeDigest(), RecipeRevision: value.GetRecipeRevision(), EC2: ec2,
		Restart: cloudmodule.AgentCloudManagedPreparationRestart{OperationID: restart.GetOperationId(),
			ExpectedInitialRevision: restart.GetExpectedInitialRevision(), Action: restart.GetAction(),
			LifecycleRestartRef: restart.GetLifecycleRestartRef(), ExecutionBundleDigest: restart.GetExecutionBundleDigest()},
		ServiceMonitorRevision: value.GetServiceMonitorRevision(), ServiceMonitorSuiteDigest: value.GetServiceMonitorSuiteDigest(),
		Currency: value.GetCurrency(), CostAlertAmountMinor: value.GetCostAlertAmountMinor(),
		ExpectedInstalledManifestDigest: value.GetExpectedInstalledManifestDigest(),
	}
	sources := make(map[string]cloudmodule.AgentCloudManagedPreparationResourceFact, len(value.GetSourceVolumes()))
	previousResource := ""
	for _, item := range value.GetSourceVolumes() {
		mapped, mapErr := mapManagedPreparationResource(item, managedPreparationVolumePattern)
		if mapErr != nil || mapped.ResourceID <= previousResource || mapped.ResourceID == ec2.ResourceID || mapped.ProviderID == ec2.ProviderID {
			return cloudmodule.AgentCloudManagedPreparationScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		for _, prior := range result.SourceVolumes {
			if prior.ProviderID == mapped.ProviderID {
				return cloudmodule.AgentCloudManagedPreparationScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
			}
		}
		sources[mapped.ResourceID], previousResource = mapped, mapped.ResourceID
		result.SourceVolumes = append(result.SourceVolumes, mapped)
	}
	previousSlot := ""
	usedSources, usedDevices := map[string]bool{}, map[string]bool{}
	for _, item := range value.GetVolumes() {
		if item == nil || item.GetSourceVolume() == nil {
			return cloudmodule.AgentCloudManagedPreparationScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		source, found := sources[item.GetSourceVolume().GetResourceId()]
		mappedSource, mapErr := mapManagedPreparationResource(item.GetSourceVolume(), managedPreparationVolumePattern)
		expectedSnapshot := uuid.NewSHA1(uuid.MustParse(value.GetPreparationOperationId()), []byte("snapshot:"+source.ResourceID+":"+item.GetSlotId())).String()
		expectedReplacement := uuid.NewSHA1(uuid.MustParse(value.GetPreparationOperationId()), []byte("replacement:"+source.ResourceID+":"+item.GetSlotId())).String()
		if !found || mapErr != nil || mappedSource != source || !agentCloudIdentifierPattern.MatchString(item.GetSlotId()) ||
			item.GetSlotId() <= previousSlot || item.GetSnapshotResourceId() != expectedSnapshot ||
			item.GetReplacementVolumeResourceId() != expectedReplacement ||
			!managedPreparationZonePattern.MatchString(item.GetAvailabilityZone()) || item.GetSizeGib() == 0 || item.GetSizeGib() > 65_536 ||
			!managedPreparationKMSPattern.MatchString(item.GetKmsKeyId()) || !managedPreparationDevicePattern.MatchString(item.GetDeviceName()) ||
			usedSources[source.ResourceID] || usedDevices[item.GetDeviceName()] {
			return cloudmodule.AgentCloudManagedPreparationScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		usedSources[source.ResourceID], usedDevices[item.GetDeviceName()], previousSlot = true, true, item.GetSlotId()
		volume := cloudmodule.AgentCloudManagedPreparationVolume{
			SlotID: item.GetSlotId(), SourceVolume: source, SnapshotResourceID: item.GetSnapshotResourceId(),
			ReplacementVolumeResourceID: item.GetReplacementVolumeResourceId(), AvailabilityZone: item.GetAvailabilityZone(),
			SizeGiB: item.GetSizeGib(), VolumeType: item.GetVolumeType(), IOPS: item.GetIops(),
			ThroughputMiBPS: item.GetThroughputMibps(), KMSKeyID: item.GetKmsKeyId(), DeviceName: item.GetDeviceName(),
			MountPath: item.GetMountPath(), ReadOnly: item.GetReadOnly(), Persistent: item.GetPersistent(),
			Disposition: item.GetDisposition(),
		}
		sourceSpecDigest, digestErr := cloudmodule.ManagedPreparationVolumeSourceSpecDigest(volume)
		if digestErr != nil || sourceSpecDigest != source.SpecDigest {
			return cloudmodule.AgentCloudManagedPreparationScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		result.Volumes = append(result.Volumes, volume)
	}
	return result, nil
}

func mapManagedPreparationResource(value *agentv1.CloudManagedPreparationResourceFact, providerPattern *regexp.Regexp) (cloudmodule.AgentCloudManagedPreparationResourceFact, error) {
	if value == nil || !validUUID(value.GetResourceId()) || !providerPattern.MatchString(value.GetProviderId()) ||
		value.GetRevision() <= 0 || !agentCloudDigestPattern.MatchString(value.GetSpecDigest()) ||
		!agentCloudDigestPattern.MatchString(value.GetTagDigest()) {
		return cloudmodule.AgentCloudManagedPreparationResourceFact{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.AgentCloudManagedPreparationResourceFact{
		ResourceID: value.GetResourceId(), ProviderID: value.GetProviderId(), Revision: value.GetRevision(),
		SpecDigest: value.GetSpecDigest(), TagDigest: value.GetTagDigest(),
	}, nil
}

func mapManagedPreparationOperation(value *agentv1.CloudManagedPreparationOperation) (cloudmodule.AgentCloudManagedPreparationOperation, error) {
	if value == nil || value.GetChallenge() == nil || value.GetRevision() < 1 || len(value.GetSteps()) != 6 {
		return cloudmodule.AgentCloudManagedPreparationOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	challenge, err := mapManagedPreparationChallenge(value.GetChallenge())
	if err != nil || value.GetOperationId() != challenge.OperationID {
		return cloudmodule.AgentCloudManagedPreparationOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	statuses := map[agentv1.CloudManagedPreparationStatus]string{
		agentv1.CloudManagedPreparationStatus_CLOUD_MANAGED_PREPARATION_STATUS_AWAITING_APPROVAL: "awaiting_approval",
		agentv1.CloudManagedPreparationStatus_CLOUD_MANAGED_PREPARATION_STATUS_APPROVED:          "approved",
		agentv1.CloudManagedPreparationStatus_CLOUD_MANAGED_PREPARATION_STATUS_RUNNING:           "running",
		agentv1.CloudManagedPreparationStatus_CLOUD_MANAGED_PREPARATION_STATUS_SUCCEEDED:         "succeeded",
		agentv1.CloudManagedPreparationStatus_CLOUD_MANAGED_PREPARATION_STATUS_FAILED_TERMINAL:   "failed_terminal",
	}
	mappedStatus, found := statuses[value.GetStatus()]
	if !found || (mappedStatus == "succeeded") != (value.GetResult() != nil) {
		return cloudmodule.AgentCloudManagedPreparationOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	createdAt, createdErr := exactAgentCloudTimestamp(value.GetCreatedAt())
	updatedAt, updatedErr := exactAgentCloudTimestamp(value.GetUpdatedAt())
	approvedAt, approvedErr := optionalManagedPreparationTimestamp(value.GetApprovedAt())
	if createdErr != nil || updatedErr != nil || approvedErr != nil || updatedAt.Before(createdAt) {
		return cloudmodule.AgentCloudManagedPreparationOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	result := cloudmodule.AgentCloudManagedPreparationOperation{
		OperationID: value.GetOperationId(), Challenge: challenge, Status: mappedStatus, CurrentPhase: value.GetCurrentPhase(),
		Revision: value.GetRevision(), CreatedAt: createdAt, UpdatedAt: updatedAt, ApprovedAt: approvedAt,
	}
	stepStatuses := map[agentv1.CloudManagedPreparationStepStatus]string{
		agentv1.CloudManagedPreparationStepStatus_CLOUD_MANAGED_PREPARATION_STEP_STATUS_PENDING:   "pending",
		agentv1.CloudManagedPreparationStepStatus_CLOUD_MANAGED_PREPARATION_STEP_STATUS_RUNNING:   "running",
		agentv1.CloudManagedPreparationStepStatus_CLOUD_MANAGED_PREPARATION_STEP_STATUS_SUCCEEDED: "succeeded",
	}
	for index, item := range value.GetSteps() {
		stepStatus, ok := stepStatuses[item.GetStatus()]
		startedAt, startedErr := optionalManagedPreparationTimestamp(item.GetStartedAt())
		completedAt, completedErr := optionalManagedPreparationTimestamp(item.GetCompletedAt())
		if !ok || item.GetPhase() != [...]string{"restart", "backup", "restore_create", "restore_swap", "semantic_health", "finalize"}[index] ||
			item.GetOrdinal() != int32(index+1) || item.GetRevision() < 1 ||
			startedErr != nil || completedErr != nil || (startedAt != nil && completedAt != nil && completedAt.Before(*startedAt)) {
			return cloudmodule.AgentCloudManagedPreparationOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		switch stepStatus {
		case "pending":
			if item.GetIntentDigest() != "" || startedAt != nil || completedAt != nil {
				return cloudmodule.AgentCloudManagedPreparationOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
			}
		case "running":
			if !agentCloudDigestPattern.MatchString(item.GetIntentDigest()) || startedAt == nil || completedAt != nil {
				return cloudmodule.AgentCloudManagedPreparationOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
			}
		case "succeeded":
			if !agentCloudDigestPattern.MatchString(item.GetIntentDigest()) || startedAt == nil || completedAt == nil {
				return cloudmodule.AgentCloudManagedPreparationOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
			}
		}
		result.Steps = append(result.Steps, cloudmodule.AgentCloudManagedPreparationStep{
			Phase: item.GetPhase(), Ordinal: item.GetOrdinal(), Status: stepStatus, Revision: item.GetRevision(),
			IntentDigest: item.GetIntentDigest(), StartedAt: startedAt, CompletedAt: completedAt,
		})
	}
	if value.GetResult() != nil {
		item := value.GetResult()
		healthAt, healthErr := exactAgentCloudTimestamp(item.GetFreshHealthObservedAt())
		costAt, costErr := exactAgentCloudTimestamp(item.GetCostObservedAt())
		stackAt, stackErr := exactAgentCloudTimestamp(item.GetStackObservedAt())
		if !validUUID(item.GetPreparationId()) || !agentCloudDigestPattern.MatchString(item.GetPreparationDigest()) ||
			!agentCloudDigestPattern.MatchString(item.GetFreshHealthDigest()) || item.GetFreshHealthRevision() <= 0 || healthErr != nil ||
			!agentCloudDigestPattern.MatchString(item.GetCostDigest()) || item.GetCostPolicyRevision() <= 0 || costErr != nil ||
			!agentCloudDigestPattern.MatchString(item.GetStackDigest()) || item.GetStackRevision() <= 0 || stackErr != nil {
			return cloudmodule.AgentCloudManagedPreparationOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		result.Result = &cloudmodule.AgentCloudManagedPreparationResult{
			PreparationID: item.GetPreparationId(), PreparationDigest: item.GetPreparationDigest(),
			FreshHealthDigest: item.GetFreshHealthDigest(), FreshHealthRevision: item.GetFreshHealthRevision(),
			FreshHealthObservedAt: healthAt, CostDigest: item.GetCostDigest(), CostPolicyRevision: item.GetCostPolicyRevision(),
			CostObservedAt: costAt, StackDigest: item.GetStackDigest(), StackRevision: item.GetStackRevision(), StackObservedAt: stackAt,
		}
	}
	return result, nil
}

func optionalManagedPreparationTimestamp(value *timestamppb.Timestamp) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	parsed, err := exactAgentCloudTimestamp(value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func canonicalManagedPreparationPayload(value managedPreparationSigningPayload) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var projected any
	if err = decoder.Decode(&projected); err != nil {
		return nil, err
	}
	var extra any
	if err = decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, err
	}
	projected, err = normalizeManagedPreparationJSONNumbers(projected)
	if err != nil {
		return nil, err
	}
	mode, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	return mode.Marshal(projected)
}

func normalizeManagedPreparationJSONNumbers(value any) (any, error) {
	switch value := value.(type) {
	case json.Number:
		if signed, err := strconv.ParseInt(string(value), 10, 64); err == nil {
			return signed, nil
		}
		unsigned, err := strconv.ParseUint(string(value), 10, 64)
		if err != nil {
			return nil, err
		}
		return unsigned, nil
	case []any:
		for index, item := range value {
			normalized, err := normalizeManagedPreparationJSONNumbers(item)
			if err != nil {
				return nil, err
			}
			value[index] = normalized
		}
		return value, nil
	case map[string]any:
		for key, item := range value {
			normalized, err := normalizeManagedPreparationJSONNumbers(item)
			if err != nil {
				return nil, err
			}
			value[key] = normalized
		}
		return value, nil
	default:
		return value, nil
	}
}
