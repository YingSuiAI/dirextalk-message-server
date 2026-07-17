package agentgrpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (runner *Runner) CreateCloudManagedAcceptanceChallenge(ctx context.Context, request cloudmodule.AgentCloudManagedAcceptanceChallengeRequest) (cloudmodule.AgentCloudManagedAcceptanceChallenge, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudManagedAcceptanceChallenge{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.DeploymentID) || request.ExpectedDeploymentRevision <= 0 ||
		!agentFoundationKeyIDPattern.MatchString(request.SignerKeyID) || cloudmodule.ContainsSensitiveGoalMaterial(request.SignerKeyID) {
		return cloudmodule.AgentCloudManagedAcceptanceChallenge{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.CreateCloudManagedAcceptanceChallenge(callContext, &agentv1.CreateCloudManagedAcceptanceChallengeRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, DeploymentId: request.DeploymentID,
		SignerKeyId: request.SignerKeyID, ExpectedDeploymentRevision: request.ExpectedDeploymentRevision,
	})
	if err != nil {
		return cloudmodule.AgentCloudManagedAcceptanceChallenge{}, mapAgentCloudControlRPCError(callContext, err)
	}
	challenge, mapErr := runner.mapManagedAcceptanceChallenge(response.GetChallenge())
	if mapErr != nil || challenge.Confirmation.Approval.ServiceID != request.ServiceID ||
		challenge.Confirmation.Approval.DeploymentID != request.DeploymentID ||
		challenge.Confirmation.Approval.DeploymentRevision != uint64(request.ExpectedDeploymentRevision) ||
		challenge.Confirmation.Approval.SignerKeyID != request.SignerKeyID {
		return cloudmodule.AgentCloudManagedAcceptanceChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return challenge, nil
}

func (runner *Runner) ApproveCloudManagedAcceptance(ctx context.Context, request cloudmodule.AgentCloudManagedAcceptanceApproveRequest) (cloudmodule.AgentCloudManagedAcceptanceOperation, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.OperationID) || !validUUID(request.DeploymentID) ||
		request.ExpectedOperationRevision != 1 || request.ExpectedServiceRevision <= 0 ||
		!agentCloudDigestPattern.MatchString(request.ExpectedScopeDigest) || !validAgentCloudApproval(request.ApprovalSignature) ||
		request.Approval.AcceptanceID != request.OperationID || request.Approval.ServiceID != request.ServiceID ||
		request.Approval.DeploymentID != request.DeploymentID || int64(request.Approval.ServiceRevision) != request.ExpectedServiceRevision {
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.ApproveCloudManagedAcceptance(callContext, &agentv1.ApproveCloudManagedAcceptanceRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, AcceptanceId: request.OperationID,
		DeploymentId: request.DeploymentID, ExpectedRevision: request.ExpectedOperationRevision,
		ScopeDigest: request.ExpectedScopeDigest, Approval: agentCloudApprovalToProto(request.ApprovalSignature),
	})
	if err != nil {
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, mapAgentCloudControlRPCError(callContext, err)
	}
	operation, mapErr := runner.mapManagedAcceptanceOperation(response.GetOperation())
	if mapErr != nil || operation.OperationID != request.OperationID || operation.DeploymentID != request.DeploymentID ||
		operation.ScopeDigest != request.ExpectedScopeDigest || operation.OwnerID != runner.ownerID {
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return operation, nil
}

func (runner *Runner) GetCloudManagedAcceptanceOperation(ctx context.Context, request cloudmodule.AgentCloudManagedAcceptanceOperationRequest) (cloudmodule.AgentCloudManagedAcceptanceOperation, bool, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, false, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.OperationID) {
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, false, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.GetCloudManagedAcceptanceOperation(callContext, &agentv1.GetCloudManagedAcceptanceOperationRequest{
		OwnerId: runner.ownerID, OperationId: request.OperationID,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound && callContext.Err() == nil {
			return cloudmodule.AgentCloudManagedAcceptanceOperation{}, false, nil
		}
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, false, mapAgentCloudControlRPCError(callContext, err)
	}
	operation, mapErr := runner.mapManagedAcceptanceOperation(response.GetOperation())
	if mapErr != nil || operation.OperationID != request.OperationID || operation.OwnerID != runner.ownerID {
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, false, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return operation, true, nil
}

func (runner *Runner) mapManagedAcceptanceChallenge(value *agentv1.CloudManagedAcceptanceChallenge) (cloudmodule.AgentCloudManagedAcceptanceChallenge, error) {
	if value == nil || value.GetScope() == nil || value.GetCompatibilityService() == nil || value.GetCompatibilityRecipe() == nil ||
		!validUUID(value.GetOperationId()) || value.GetOperationId() != value.GetScope().GetAcceptanceId() ||
		!validUUID(value.GetApprovalId()) || !validUUID(value.GetChallengeId()) ||
		!agentFoundationKeyIDPattern.MatchString(value.GetSignerKeyId()) || !agentCloudDigestPattern.MatchString(value.GetScopeDigest()) ||
		value.GetRevision() != 1 || len(value.GetSigningPayloadCbor()) == 0 || len(value.GetSigningPayloadCbor()) > 64*1024 {
		return cloudmodule.AgentCloudManagedAcceptanceChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	issuedAt, issuedErr := exactAgentCloudTimestamp(value.GetIssuedAt())
	expiresAt, expiresErr := exactAgentCloudTimestamp(value.GetExpiresAt())
	if issuedErr != nil || expiresErr != nil || !issuedAt.Before(expiresAt) || expiresAt.Sub(issuedAt) > 5*time.Minute {
		return cloudmodule.AgentCloudManagedAcceptanceChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	healthObservedAt, healthObservedErr := exactAgentCloudTimestamp(value.GetScope().GetHealthObservedAt())
	if healthObservedErr != nil || value.GetScope().GetHealthRevision() <= 0 ||
		!agentCloudDigestPattern.MatchString(value.GetScope().GetHealthEvidenceDigest()) ||
		value.GetScope().GetHealthStatus() != "healthy" || value.GetScope().GetHealthEvidenceType() != "independent_external" ||
		healthObservedAt.After(issuedAt) || issuedAt.Sub(healthObservedAt) > 5*time.Minute {
		return cloudmodule.AgentCloudManagedAcceptanceChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	target, err := runner.mapManagedAcceptanceTarget(value.GetScope())
	if err != nil {
		return cloudmodule.AgentCloudManagedAcceptanceChallenge{}, err
	}
	approval := cloudcontracts.ServiceManagementAcceptanceApprovalV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1, Intent: cloudcontracts.ServiceManagementAcceptanceIntent,
		ApprovalID: value.GetApprovalId(), ChallengeID: value.GetChallengeId(), SignerKeyID: value.GetSignerKeyId(),
		ServiceManagementAcceptanceTargetV1: target, IssuedAt: issuedAt, ExpiresAt: expiresAt,
	}
	signingPayload, err := approval.SigningPayload()
	if err != nil || !bytes.Equal(signingPayload, value.GetSigningPayloadCbor()) {
		return cloudmodule.AgentCloudManagedAcceptanceChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	sum := sha256.Sum256(signingPayload)
	if value.GetScopeDigest() != fmt.Sprintf("sha256:%x", sum[:]) {
		return cloudmodule.AgentCloudManagedAcceptanceChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	service, err := mapManagedCompatibilityService(value.GetCompatibilityService())
	if err != nil {
		return cloudmodule.AgentCloudManagedAcceptanceChallenge{}, err
	}
	recipe, err := mapManagedCompatibilityRecipe(value.GetCompatibilityRecipe())
	if err != nil || service.ServiceID != target.ServiceID || service.DeploymentID != target.DeploymentID ||
		service.RecipeID != target.RecipeID || service.Revision != int64(target.ServiceRevision) ||
		service.Status != "awaiting_management_acceptance" || recipe.RecipeID != target.RecipeID ||
		recipe.Revision != int64(target.RecipeRevision) || recipe.Digest != target.RecipeDigest ||
		recipe.Maturity != string(target.RecipeMaturity) || validateManagedServiceEvidence(service, target) != nil {
		return cloudmodule.AgentCloudManagedAcceptanceChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.AgentCloudManagedAcceptanceChallenge{
		OperationID: value.GetOperationId(), OwnerID: value.GetScope().GetOwnerId(), ScopeDigest: value.GetScopeDigest(), Revision: value.GetRevision(),
		Confirmation: cloudmodule.ServiceManagementAcceptanceConfirmation{Service: service, Recipe: recipe, Approval: approval},
	}, nil
}

func (runner *Runner) mapManagedAcceptanceOperation(value *agentv1.CloudManagedAcceptanceOperation) (cloudmodule.AgentCloudManagedAcceptanceOperation, error) {
	if value == nil || value.GetStatus() != agentv1.CloudManagedAcceptanceOperationStatus_CLOUD_MANAGED_ACCEPTANCE_OPERATION_STATUS_SUCCEEDED ||
		value.GetCompatibilityService() == nil || value.GetCompatibilityRecipe() == nil || value.GetCompatibilityAcceptance() == nil ||
		value.GetRevision() < 2 || value.GetErrorCode() != "" || value.GetErrorSummary() != "" ||
		cloudmodule.ContainsSensitiveGoalMaterial(value.GetErrorCode()) ||
		cloudmodule.ContainsSensitiveGoalMaterial(value.GetErrorSummary()) {
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	createdAt, createdErr := exactAgentCloudTimestamp(value.GetCreatedAt())
	updatedAt, updatedErr := exactAgentCloudTimestamp(value.GetUpdatedAt())
	if createdErr != nil || updatedErr != nil || updatedAt.Before(createdAt) {
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	challenge, err := runner.mapManagedAcceptanceChallenge(value.GetChallenge())
	if err != nil || challenge.OperationID != value.GetOperationId() {
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	service, err := mapManagedCompatibilityService(value.GetCompatibilityService())
	if err != nil {
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, err
	}
	recipe, err := mapManagedCompatibilityRecipe(value.GetCompatibilityRecipe())
	if err != nil {
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, err
	}
	acceptance, err := mapManagedCompatibilityAcceptance(value.GetCompatibilityAcceptance())
	if err != nil || service.ServiceID != challenge.Confirmation.Approval.ServiceID ||
		service.DeploymentID != challenge.Confirmation.Approval.DeploymentID ||
		service.Status != "active" || recipe.RecipeID != challenge.Confirmation.Approval.RecipeID || recipe.Maturity != "managed" ||
		acceptance.AcceptanceID != value.GetOperationId() || acceptance.ServiceID != service.ServiceID || acceptance.RecipeID != recipe.RecipeID {
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	if validateManagedServiceEvidence(service, challenge.Confirmation.Approval.ServiceManagementAcceptanceTargetV1) != nil {
		return cloudmodule.AgentCloudManagedAcceptanceOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.AgentCloudManagedAcceptanceOperation{
		OperationID: value.GetOperationId(), OwnerID: challenge.OwnerID, ApprovalID: challenge.Confirmation.Approval.ApprovalID,
		DeploymentID: service.DeploymentID, ScopeDigest: challenge.ScopeDigest, Status: "succeeded", Revision: value.GetRevision(),
		Service: service, Recipe: recipe, Acceptance: acceptance,
	}, nil
}

func (runner *Runner) mapManagedAcceptanceTarget(scope *agentv1.CloudManagedAcceptanceScope) (cloudcontracts.ServiceManagementAcceptanceTargetV1, error) {
	if scope == nil || scope.GetDeploymentRevision() <= 0 || scope.GetConnectionRevision() <= 0 ||
		scope.GetLifecycle() == nil || scope.GetHealth() == nil ||
		strings.TrimSpace(scope.GetLifecycle().GetMaintenance()) == "" || len(scope.GetLifecycle().GetMaintenance()) > 512 ||
		cloudmodule.ContainsSensitiveGoalMaterial(scope.GetLifecycle().GetMaintenance()) {
		return cloudcontracts.ServiceManagementAcceptanceTargetV1{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	health := scope.GetHealth()
	if health.GetLiveness() == nil || health.GetReadiness() == nil || health.GetSemantic() == nil {
		return cloudcontracts.ServiceManagementAcceptanceTargetV1{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	healthObservedAt, err := exactAgentCloudTimestamp(scope.GetHealthObservedAt())
	if err != nil {
		return cloudcontracts.ServiceManagementAcceptanceTargetV1{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	target := cloudcontracts.ServiceManagementAcceptanceTargetV1{
		AgentInstanceID: scope.GetAgentInstanceId(), OwnerID: scope.GetOwnerId(),
		AcceptanceID: scope.GetAcceptanceId(), ServiceID: scope.GetServiceId(), ServiceRevision: scope.GetServiceRevision(),
		DeploymentID: scope.GetDeploymentId(), DeploymentRevision: uint64(scope.GetDeploymentRevision()),
		CloudConnectionID: scope.GetConnectionId(), ConnectionRevision: scope.GetConnectionRevision(),
		PlanID: scope.GetPlanId(), PlanRevision: scope.GetPlanRevision(), PlanHash: scope.GetPlanHash(),
		RecipeID: scope.GetRecipeId(), RecipeDigest: scope.GetRecipeDigest(),
		RecipeRevision: scope.GetRecipeRevision(), RecipeMaturity: cloudcontracts.RecipeMaturity(scope.GetRecipeMaturity()),
		InstalledManifestDigest: scope.GetInstalledManifestDigest(), ArtifactDigest: scope.GetArtifactDigest(),
		ReadinessSemanticEvidenceDigest: scope.GetReadinessSemanticEvidenceDigest(),
		ReadinessStackObservationDigest: scope.GetReadinessStackObservationDigest(),
		RestartOperationID:              scope.GetRestartOperationId(), RestartOperationRevision: scope.GetRestartOperationRevision(),
		BackupID: scope.GetBackupId(), BackupRevision: scope.GetBackupRevision(), RestoreID: scope.GetRestoreId(),
		RestoreRevision: scope.GetRestoreRevision(), SourceArtifactDigests: append([]string(nil), scope.GetSourceArtifactDigests()...),
		HealthRevision: scope.GetHealthRevision(), HealthMonitorKind: scope.GetHealthMonitorKind(), HealthStatus: scope.GetHealthStatus(),
		HealthEvidenceType: scope.GetHealthEvidenceType(), HealthEvidenceDigest: scope.GetHealthEvidenceDigest(), HealthObservedAt: healthObservedAt,
		Currency: scope.GetCurrency(), CostAlertAmountMinor: scope.GetCostAlertAmountMinor(),
		Health: cloudcontracts.HealthContractV1{
			Liveness:  cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeKind(health.GetLiveness().GetKind()), Target: health.GetLiveness().GetTarget()},
			Readiness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeKind(health.GetReadiness().GetKind()), Target: health.GetReadiness().GetTarget()},
			Semantic:  cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeKind(health.GetSemantic().GetKind()), Target: health.GetSemantic().GetTarget()},
		},
		Lifecycle: cloudcontracts.ServiceManagementAcceptanceLifecycleV2{
			Start: scope.GetLifecycle().GetStart(), Stop: scope.GetLifecycle().GetStop(), Maintenance: scope.GetLifecycle().GetMaintenance(), Restart: scope.GetLifecycle().GetRestart(),
			Upgrade: scope.GetLifecycle().GetUpgrade(), Rollback: scope.GetLifecycle().GetRollback(), Backup: scope.GetLifecycle().GetBackup(),
			Restore: scope.GetLifecycle().GetRestore(), Destroy: scope.GetLifecycle().GetDestroy(),
		},
		DestroyInstanceID: scope.GetDestroyInstanceId(), DestroyVolumeIDs: append([]string(nil), scope.GetDestroyVolumeIds()...),
		DestroyNetworkInterfaceIDs: append([]string(nil), scope.GetDestroyNetworkInterfaceIds()...), AcceptancePolicy: scope.GetAcceptancePolicy(),
	}
	for _, slot := range scope.GetVolumeSlots() {
		if slot == nil {
			return cloudcontracts.ServiceManagementAcceptanceTargetV1{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		target.VolumeSlots = append(target.VolumeSlots, cloudcontracts.VolumeSlotV1{SlotID: slot.GetSlotId(), VolumeRef: slot.GetVolumeRef(), ReadOnly: slot.GetReadOnly()})
	}
	for _, slot := range scope.GetDataSlots() {
		if slot == nil {
			return cloudcontracts.ServiceManagementAcceptanceTargetV1{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		target.DataSlots = append(target.DataSlots, cloudcontracts.DataSlotV1{SlotID: slot.GetSlotId(), DataRef: slot.GetDataRef(), ReadOnly: slot.GetReadOnly()})
	}
	for _, slot := range scope.GetSecretSlots() {
		if slot == nil {
			return cloudcontracts.ServiceManagementAcceptanceTargetV1{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		target.SecretSlots = append(target.SecretSlots, cloudcontracts.SecretSlotV1{SlotID: slot.GetSlotId(), SecretRef: slot.GetSecretRef()})
	}
	for _, item := range scope.GetResources() {
		if item == nil {
			return cloudcontracts.ServiceManagementAcceptanceTargetV1{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		itemType, ok := managedAcceptanceResourceType(item.GetType())
		if !ok {
			return cloudcontracts.ServiceManagementAcceptanceTargetV1{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		target.Resources = append(target.Resources, cloudcontracts.ServiceManagementAcceptanceResourceV2{
			ResourceID: item.GetResourceId(), Type: itemType, Revision: item.GetRevision(),
			ProviderID: item.GetProviderId(), TagDigest: item.GetTagDigest(),
		})
	}
	if scope.GetOwnerId() != runner.ownerID || target.Validate() != nil {
		return cloudcontracts.ServiceManagementAcceptanceTargetV1{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return target, nil
}

func managedAcceptanceResourceType(value agentv1.CloudResourceType) (string, bool) {
	switch value {
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_EC2:
		return "ec2", true
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_EBS:
		return "ebs", true
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_ENI:
		return "eni", true
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_EIP:
		return "eip", true
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_SECURITY_GROUP:
		return "security_group", true
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_ENDPOINT:
		return "endpoint", true
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_SNAPSHOT:
		return "snapshot", true
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_ALB:
		return "alb", true
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_TARGET_GROUP:
		return "target_group", true
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_LISTENER:
		return "listener", true
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_SECURITY_GROUP_RULE:
		return "security_group_rule", true
	default:
		return "", false
	}
}

func mapManagedCompatibilityService(value *agentv1.CloudManagedCompatibilityService) (cloudmodule.Service, error) {
	if value == nil || strings.TrimSpace(value.GetServiceId()) == "" || strings.TrimSpace(value.GetDeploymentId()) == "" ||
		strings.TrimSpace(value.GetRecipeId()) == "" || strings.TrimSpace(value.GetName()) == "" ||
		value.GetRevision() <= 0 || value.GetCreatedAtUnixMs() <= 0 || value.GetUpdatedAtUnixMs() < value.GetCreatedAtUnixMs() ||
		cloudmodule.ContainsSensitiveGoalMaterial(value.GetName()) || cloudmodule.ContainsSensitiveGoalMaterial(value.GetServiceStatus()) ||
		cloudmodule.ContainsSensitiveGoalMaterial(value.GetIntegrationStatus()) {
		return cloudmodule.Service{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	service := cloudmodule.Service{ServiceID: value.GetServiceId(), DeploymentID: value.GetDeploymentId(), RecipeID: value.GetRecipeId(),
		Name: value.GetName(), Status: value.GetServiceStatus(), Integration: value.GetIntegrationStatus(), Revision: value.GetRevision(),
		CreatedAt: value.GetCreatedAtUnixMs(), UpdatedAt: value.GetUpdatedAtUnixMs()}
	if len(value.GetBackups()) == 0 || len(value.GetRestores()) == 0 {
		return cloudmodule.Service{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	seenBackups := make(map[string]struct{}, len(value.GetBackups()))
	for _, item := range value.GetBackups() {
		backup, err := mapManagedCompatibilityBackup(item, service.ServiceID, service.DeploymentID)
		if err != nil {
			return cloudmodule.Service{}, err
		}
		if _, exists := seenBackups[backup.BackupID]; exists {
			return cloudmodule.Service{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		seenBackups[backup.BackupID] = struct{}{}
		service.Backups = append(service.Backups, backup)
	}
	seenRestores := make(map[string]struct{}, len(value.GetRestores()))
	for _, item := range value.GetRestores() {
		restore, err := mapManagedCompatibilityRestore(item, service.ServiceID, service.DeploymentID)
		if err != nil {
			return cloudmodule.Service{}, err
		}
		if _, exists := seenRestores[restore.RestoreID]; exists {
			return cloudmodule.Service{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		seenRestores[restore.RestoreID] = struct{}{}
		service.Restores = append(service.Restores, restore)
	}
	return service, nil
}

func mapManagedCompatibilityBackup(value *agentv1.CloudManagedCompatibilityBackup, serviceID, deploymentID string) (cloudmodule.ServiceBackup, error) {
	if value == nil || strings.TrimSpace(value.GetBackupId()) == "" || value.GetServiceId() != serviceID ||
		value.GetDeploymentId() != deploymentID || value.GetRetentionPolicy() != "manual" || value.GetRevision() <= 0 ||
		value.GetCreatedAtUnixMs() <= 0 || value.GetUpdatedAtUnixMs() < value.GetCreatedAtUnixMs() {
		return cloudmodule.ServiceBackup{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	backup := cloudmodule.ServiceBackup{BackupID: value.GetBackupId(), ServiceID: value.GetServiceId(), DeploymentID: value.GetDeploymentId(),
		Status: value.GetStatus(), RetentionPolicy: value.GetRetentionPolicy(), ImageID: value.GetImageId(),
		SnapshotIDs: append([]string(nil), value.GetSnapshotIds()...), Revision: value.GetRevision(),
		CreatedAt: value.GetCreatedAtUnixMs(), UpdatedAt: value.GetUpdatedAtUnixMs()}
	switch backup.Status {
	case "available":
		if !validManagedEC2ResourceID(backup.ImageID, "ami-") || len(backup.SnapshotIDs) == 0 || !uniqueManagedEC2ResourceIDs(backup.SnapshotIDs, "snap-") {
			return cloudmodule.ServiceBackup{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
	case "failed":
		if backup.ImageID != "" || len(backup.SnapshotIDs) != 0 {
			return cloudmodule.ServiceBackup{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
	default:
		return cloudmodule.ServiceBackup{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return backup, nil
}

func mapManagedCompatibilityRestore(value *agentv1.CloudManagedCompatibilityRestore, serviceID, deploymentID string) (cloudmodule.ServiceRestore, error) {
	if value == nil || strings.TrimSpace(value.GetRestoreId()) == "" || strings.TrimSpace(value.GetRestorePlanId()) == "" ||
		strings.TrimSpace(value.GetBackupId()) == "" || value.GetServiceId() != serviceID || value.GetDeploymentId() != deploymentID ||
		value.GetRevision() <= 0 || value.GetCreatedAtUnixMs() <= 0 || value.GetUpdatedAtUnixMs() < value.GetCreatedAtUnixMs() {
		return cloudmodule.ServiceRestore{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	restore := cloudmodule.ServiceRestore{RestoreID: value.GetRestoreId(), RestorePlanID: value.GetRestorePlanId(),
		ServiceID: value.GetServiceId(), DeploymentID: value.GetDeploymentId(), BackupID: value.GetBackupId(), Status: value.GetStatus(),
		OriginalVolumeIDs:    append([]string(nil), value.GetOriginalVolumeIds()...),
		ReplacementVolumeIDs: append([]string(nil), value.GetReplacementVolumeIds()...),
		Revision:             value.GetRevision(), CreatedAt: value.GetCreatedAtUnixMs(), UpdatedAt: value.GetUpdatedAtUnixMs()}
	switch restore.Status {
	case "queued", "running":
		if len(restore.OriginalVolumeIDs) == 0 && len(restore.ReplacementVolumeIDs) == 0 {
			return restore, nil
		}
	case "verifying", "succeeded", "failed", "restore_blocked":
	default:
		return cloudmodule.ServiceRestore{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	if len(restore.OriginalVolumeIDs) == 0 || len(restore.ReplacementVolumeIDs) == 0 ||
		!uniqueManagedEC2ResourceIDs(append(append([]string(nil), restore.OriginalVolumeIDs...), restore.ReplacementVolumeIDs...), "vol-") {
		return cloudmodule.ServiceRestore{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return restore, nil
}

func validateManagedServiceEvidence(service cloudmodule.Service, target cloudcontracts.ServiceManagementAcceptanceTargetV1) error {
	var matchingBackup *cloudmodule.ServiceBackup
	for index := range service.Backups {
		backup := &service.Backups[index]
		if backup.BackupID == target.BackupID {
			matchingBackup = backup
			break
		}
	}
	if matchingBackup == nil || matchingBackup.Status != "available" || matchingBackup.Revision != int64(target.BackupRevision) {
		return cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	var matchingRestore *cloudmodule.ServiceRestore
	for index := range service.Restores {
		restore := &service.Restores[index]
		if restore.RestoreID == target.RestoreID {
			matchingRestore = restore
			break
		}
	}
	if matchingRestore == nil || matchingRestore.Status != "succeeded" || matchingRestore.BackupID != target.BackupID ||
		matchingRestore.Revision != int64(target.RestoreRevision) ||
		!sameManagedStringSet(matchingRestore.ReplacementVolumeIDs, target.DestroyVolumeIDs) {
		return cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func uniqueManagedEC2ResourceIDs(values []string, prefix string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !validManagedEC2ResourceID(value, prefix) {
			return false
		}
		if _, exists := seen[value]; exists {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func validManagedEC2ResourceID(value, prefix string) bool {
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(value, prefix)
	if len(suffix) < 8 || len(suffix) > 17 {
		return false
	}
	for _, char := range suffix {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func sameManagedStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	counts := make(map[string]int, len(left))
	for _, value := range left {
		counts[value]++
	}
	for _, value := range right {
		counts[value]--
		if counts[value] < 0 {
			return false
		}
	}
	return true
}

func mapManagedCompatibilityRecipe(value *agentv1.CloudManagedCompatibilityRecipe) (cloudmodule.Recipe, error) {
	if value == nil || strings.TrimSpace(value.GetRecipeId()) == "" || strings.TrimSpace(value.GetName()) == "" ||
		strings.TrimSpace(value.GetVersion()) == "" || !agentCloudDigestPattern.MatchString(value.GetDigest()) ||
		value.GetRevision() <= 0 || value.GetCreatedAtUnixMs() <= 0 || value.GetUpdatedAtUnixMs() < value.GetCreatedAtUnixMs() ||
		cloudmodule.ContainsSensitiveGoalMaterial(value.GetName()) || cloudmodule.ContainsSensitiveGoalMaterial(value.GetVersion()) ||
		cloudmodule.ContainsSensitiveGoalMaterial(value.GetMaturity()) {
		return cloudmodule.Recipe{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.Recipe{RecipeID: value.GetRecipeId(), Name: value.GetName(), Version: value.GetVersion(), Digest: value.GetDigest(),
		Maturity: value.GetMaturity(), Revision: value.GetRevision(), CreatedAt: value.GetCreatedAtUnixMs(), UpdatedAt: value.GetUpdatedAtUnixMs()}, nil
}

func mapManagedCompatibilityAcceptance(value *agentv1.CloudManagedCompatibilityAcceptance) (cloudmodule.ServiceManagementAcceptance, error) {
	if value == nil || !validUUID(value.GetAcceptanceId()) || strings.TrimSpace(value.GetServiceId()) == "" ||
		strings.TrimSpace(value.GetRecipeId()) == "" || value.GetStatus() != "approved" ||
		value.GetRevision() <= 0 || value.GetCreatedAtUnixMs() <= 0 || value.GetUpdatedAtUnixMs() < value.GetCreatedAtUnixMs() {
		return cloudmodule.ServiceManagementAcceptance{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.ServiceManagementAcceptance{AcceptanceID: value.GetAcceptanceId(), ServiceID: value.GetServiceId(),
		RecipeID: value.GetRecipeId(), Status: value.GetStatus(), Revision: value.GetRevision(),
		CreatedAt: value.GetCreatedAtUnixMs(), UpdatedAt: value.GetUpdatedAtUnixMs()}, nil
}
