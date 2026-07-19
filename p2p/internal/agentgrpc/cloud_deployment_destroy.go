package agentgrpc

import (
	"context"
	"regexp"
	"sort"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const agentCloudDeploymentDestroyScopeSchema = "dirextalk.agent.cloud-deployment-destroy-scope/v1"

var (
	agentDestroyEC2Pattern = regexp.MustCompile(`^i-[0-9a-f]{8,17}$`)
	agentDestroyEBSPattern = regexp.MustCompile(`^vol-[0-9a-f]{8,17}$`)
	agentDestroyENIPattern = regexp.MustCompile(`^eni-[0-9a-f]{8,17}$`)
	agentDestroySGPattern  = regexp.MustCompile(`^sg-[0-9a-f]{8,17}$`)
)

func (runner *Runner) CreateAgentCloudDeploymentDestroyChallenge(ctx context.Context, request cloudmodule.AgentCloudDeploymentDestroyChallengeRequest) (cloudmodule.AgentCloudDeploymentDestroyChallenge, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudDeploymentDestroyChallenge{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.DeploymentID) || request.ExpectedRevision <= 0 ||
		!agentCloudIdentifierPattern.MatchString(request.SignerKeyID) || !sameExpectedAgentDestroyDeployment(request.ExpectedDeployment, request.DeploymentID, request.ExpectedRevision) {
		return cloudmodule.AgentCloudDeploymentDestroyChallenge{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.CreateCloudDeploymentDestroyChallenge(callContext, &agentv1.CreateCloudDeploymentDestroyChallengeRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, DeploymentId: request.DeploymentID,
		ExpectedRevision: request.ExpectedRevision, SignerKeyId: request.SignerKeyID,
	})
	if err != nil {
		return cloudmodule.AgentCloudDeploymentDestroyChallenge{}, mapAgentCloudControlRPCError(callContext, err)
	}
	challenge, err := runner.mapAgentDestroyChallenge(response.GetChallenge(), request)
	if err != nil {
		return cloudmodule.AgentCloudDeploymentDestroyChallenge{}, err
	}
	return challenge, nil
}

func (runner *Runner) ApproveAgentCloudDeploymentDestroy(ctx context.Context, request cloudmodule.AgentCloudDeploymentDestroyApproveRequest) (cloudmodule.AgentCloudDeploymentDestroyResult, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudDeploymentDestroyResult{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.DeploymentID) || !validUUID(request.ExpectedOperationID) ||
		request.ExpectedRevision <= 0 || !sameExpectedAgentDestroyDeployment(request.ExpectedDeployment, request.DeploymentID, request.ExpectedRevision) ||
		!validAgentCloudApproval(request.Approval) {
		return cloudmodule.AgentCloudDeploymentDestroyResult{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.ApproveCloudDeploymentDestroy(callContext, &agentv1.ApproveCloudDeploymentDestroyRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, DeploymentId: request.DeploymentID,
		ExpectedRevision: request.ExpectedRevision, Approval: agentCloudApprovalToProto(request.Approval),
	})
	if err != nil {
		return cloudmodule.AgentCloudDeploymentDestroyResult{}, mapAgentCloudControlRPCError(callContext, err)
	}
	if response == nil {
		return cloudmodule.AgentCloudDeploymentDestroyResult{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	operation, err := runner.mapAgentDestroyOperation(response.GetOperation(), request.ExpectedOperationID, request.DeploymentID, request.Approval.ApprovalID)
	if err != nil || operation.Status == "awaiting_approval" {
		return cloudmodule.AgentCloudDeploymentDestroyResult{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	deployment, err := runner.mapCloudDeployment(response.GetDeployment())
	if err != nil || !validAgentDestroyOperationDeployment(operation, deployment, request.ExpectedDeployment) {
		return cloudmodule.AgentCloudDeploymentDestroyResult{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.AgentCloudDeploymentDestroyResult{Operation: operation, Deployment: deployment}, nil
}

func (runner *Runner) GetAgentCloudDestroyOperation(ctx context.Context, request cloudmodule.AgentCloudDestroyOperationRequest) (cloudmodule.AgentCloudDestroyOperation, bool, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudDestroyOperation{}, false, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.OperationID) {
		return cloudmodule.AgentCloudDestroyOperation{}, false, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.GetCloudDestroyOperation(callContext, &agentv1.GetCloudDestroyOperationRequest{OwnerId: runner.ownerID, OperationId: request.OperationID})
	if err != nil {
		if callContext.Err() == nil && status.Code(err) == codes.NotFound {
			return cloudmodule.AgentCloudDestroyOperation{}, false, nil
		}
		return cloudmodule.AgentCloudDestroyOperation{}, false, mapAgentCloudControlRPCError(callContext, err)
	}
	if response == nil {
		return cloudmodule.AgentCloudDestroyOperation{}, false, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	operation, err := runner.mapAgentDestroyOperation(response.GetOperation(), request.OperationID, "", "")
	if err != nil {
		return cloudmodule.AgentCloudDestroyOperation{}, false, err
	}
	return operation, true, nil
}

func (runner *Runner) mapAgentDestroyChallenge(remote *agentv1.CloudDeploymentDestroyChallenge, request cloudmodule.AgentCloudDeploymentDestroyChallengeRequest) (cloudmodule.AgentCloudDeploymentDestroyChallenge, error) {
	if remote == nil || !validUUID(remote.GetOperationId()) || !validUUID(remote.GetChallengeId()) ||
		!validUUID(remote.GetApprovalId()) || remote.GetSignerKeyId() != request.SignerKeyID || remote.GetRevision() <= 0 ||
		len(remote.GetSigningPayloadCbor()) == 0 || len(remote.GetSigningPayloadCbor()) > 256*1024 {
		return cloudmodule.AgentCloudDeploymentDestroyChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	expiresAt, err := exactAgentCloudTimestamp(remote.GetExpiresAt())
	if err != nil {
		return cloudmodule.AgentCloudDeploymentDestroyChallenge{}, err
	}
	scope, err := runner.mapAgentDestroyScope(remote.GetScope())
	if err != nil || scope.DeploymentID != request.DeploymentID || scope.DeploymentRevision != request.ExpectedRevision ||
		scope.PlanID != request.ExpectedDeployment.PlanID || scope.ConnectionID != request.ExpectedDeployment.ConnectionID ||
		agentDestroyScopeProjection(scope.Resources) != request.ExpectedDeployment.Resource {
		return cloudmodule.AgentCloudDeploymentDestroyChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.AgentCloudDeploymentDestroyChallenge{
		OperationID: remote.GetOperationId(), ChallengeID: remote.GetChallengeId(), ApprovalID: remote.GetApprovalId(),
		SignerKeyID: remote.GetSignerKeyId(), Scope: scope, ExpiresAt: expiresAt,
		SigningPayloadCBOR: append([]byte(nil), remote.GetSigningPayloadCbor()...), Revision: remote.GetRevision(),
	}, nil
}

func (runner *Runner) mapAgentDestroyScope(remote *agentv1.CloudDeploymentDestroyScope) (cloudmodule.AgentCloudDeploymentDestroyScope, error) {
	if remote == nil || remote.GetSchemaVersion() != agentCloudDeploymentDestroyScopeSchema ||
		!validUUID(remote.GetAgentInstanceId()) || remote.GetOwnerId() != runner.ownerID ||
		!validUUID(remote.GetDeploymentId()) || remote.GetDeploymentRevision() <= 0 || !validUUID(remote.GetTaskId()) ||
		!validUUID(remote.GetPlanId()) || !agentCloudDigestPattern.MatchString(remote.GetPlanHash()) || !validUUID(remote.GetConnectionId()) ||
		len(remote.GetResources()) == 0 || len(remote.GetResources()) > 128 {
		return cloudmodule.AgentCloudDeploymentDestroyScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	resources := make([]cloudmodule.AgentCloudDestroyResourceScope, 0, len(remote.GetResources()))
	resourceIDs := make(map[string]struct{}, len(remote.GetResources()))
	lastResourceID := ""
	remaining := 0
	for _, value := range remote.GetResources() {
		resource, err := mapAgentDestroyResource(value)
		if err != nil || resource.ResourceID <= lastResourceID {
			return cloudmodule.AgentCloudDeploymentDestroyScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		lastResourceID = resource.ResourceID
		resourceIDs[resource.ResourceID] = struct{}{}
		resources = append(resources, resource)
		if resource.Status != "verified_destroyed" {
			remaining++
		}
	}
	if remaining == 0 {
		return cloudmodule.AgentCloudDeploymentDestroyScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	for _, resource := range resources {
		if !sort.StringsAreSorted(resource.DependsOnResourceIDs) {
			return cloudmodule.AgentCloudDeploymentDestroyScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		lastDependency := ""
		for _, dependency := range resource.DependsOnResourceIDs {
			if dependency == resource.ResourceID || dependency == lastDependency {
				return cloudmodule.AgentCloudDeploymentDestroyScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
			}
			if _, ok := resourceIDs[dependency]; !ok {
				return cloudmodule.AgentCloudDeploymentDestroyScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
			}
			lastDependency = dependency
		}
	}
	return cloudmodule.AgentCloudDeploymentDestroyScope{
		SchemaVersion: remote.GetSchemaVersion(), AgentInstanceID: remote.GetAgentInstanceId(), OwnerID: remote.GetOwnerId(),
		DeploymentID: remote.GetDeploymentId(), DeploymentRevision: remote.GetDeploymentRevision(), TaskID: remote.GetTaskId(),
		PlanID: remote.GetPlanId(), PlanHash: remote.GetPlanHash(), ConnectionID: remote.GetConnectionId(), Resources: resources,
	}, nil
}

func mapAgentDestroyResource(remote *agentv1.CloudDestroyResourceScope) (cloudmodule.AgentCloudDestroyResourceScope, error) {
	if remote == nil || !validUUID(remote.GetResourceId()) || remote.GetRevision() <= 0 || !remoteAWSRegionPattern.MatchString(remote.GetRegion()) ||
		remote.GetRetentionPolicy() != agentv1.RetentionPolicy_RETENTION_POLICY_EPHEMERAL_AUTO_DESTROY || !remote.GetAutoDestroyApproved() ||
		!agentCloudDigestPattern.MatchString(remote.GetSpecDigest()) || !agentCloudDigestPattern.MatchString(remote.GetApprovedPlanHash()) ||
		!validUUID(remote.GetOriginalApprovalId()) {
		return cloudmodule.AgentCloudDestroyResourceScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	typeName, providerOK := agentDestroyProvider(remote.GetType(), remote.GetProviderId())
	if !providerOK {
		return cloudmodule.AgentCloudDestroyResourceScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	statusName, exists, ok := agentDestroyResourceScopeStatus(remote.GetStatus())
	if !ok {
		return cloudmodule.AgentCloudDestroyResourceScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	destroyDeadline, err := exactAgentCloudTimestamp(remote.GetDestroyDeadline())
	if err != nil {
		return cloudmodule.AgentCloudDestroyResourceScope{}, err
	}
	readBack, err := mapAgentDestroyReadBack(remote.GetReadBack(), remote.GetProviderId(), exists)
	if err != nil {
		return cloudmodule.AgentCloudDestroyResourceScope{}, err
	}
	return cloudmodule.AgentCloudDestroyResourceScope{
		ResourceID: remote.GetResourceId(), Type: typeName, ProviderID: remote.GetProviderId(), Revision: remote.GetRevision(),
		DependsOnResourceIDs: append([]string{}, remote.GetDependsOnResourceIds()...), RetentionPolicy: "ephemeral_auto_destroy",
		DestroyDeadline: destroyDeadline, AutoDestroyApproved: remote.GetAutoDestroyApproved(), Status: statusName, Region: remote.GetRegion(),
		SpecDigest: remote.GetSpecDigest(), ApprovedPlanHash: remote.GetApprovedPlanHash(), OriginalApprovalID: remote.GetOriginalApprovalId(), ReadBack: readBack,
	}, nil
}

func mapAgentDestroyReadBack(remote *agentv1.CloudResourceReadBack, providerID string, expectedExists bool) (cloudmodule.AgentCloudResourceReadBack, error) {
	if remote == nil || !remote.GetObserved() || remote.GetExists() != expectedExists || remote.GetProviderId() != providerID ||
		(remote.GetTagDigest() != "" && !agentCloudDigestPattern.MatchString(remote.GetTagDigest())) {
		return cloudmodule.AgentCloudResourceReadBack{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	observedAt, err := exactAgentCloudTimestamp(remote.GetObservedAt())
	if err != nil {
		return cloudmodule.AgentCloudResourceReadBack{}, err
	}
	return cloudmodule.AgentCloudResourceReadBack{Observed: true, Exists: remote.GetExists(), ProviderID: providerID, ObservedAt: observedAt, TagDigest: remote.GetTagDigest()}, nil
}

func (runner *Runner) mapAgentDestroyOperation(remote *agentv1.CloudDestroyOperation, expectedOperationID, expectedDeploymentID, expectedApprovalID string) (cloudmodule.AgentCloudDestroyOperation, error) {
	if remote == nil || !validUUID(remote.GetOperationId()) || (expectedOperationID != "" && remote.GetOperationId() != expectedOperationID) ||
		remote.GetOwnerId() != runner.ownerID || !validUUID(remote.GetDeploymentId()) ||
		(expectedDeploymentID != "" && remote.GetDeploymentId() != expectedDeploymentID) || !validUUID(remote.GetApprovalId()) ||
		(expectedApprovalID != "" && remote.GetApprovalId() != expectedApprovalID) || !agentCloudDigestPattern.MatchString(remote.GetScopeDigest()) ||
		remote.GetRevision() <= 0 {
		return cloudmodule.AgentCloudDestroyOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	createdAt, createErr := exactAgentCloudTimestamp(remote.GetCreatedAt())
	updatedAt, updateErr := exactAgentCloudTimestamp(remote.GetUpdatedAt())
	if createErr != nil || updateErr != nil || updatedAt.Before(createdAt) {
		return cloudmodule.AgentCloudDestroyOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	statusName, ok := agentDestroyOperationStatus(remote.GetStatus())
	if !ok {
		return cloudmodule.AgentCloudDestroyOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	var nextAttemptAt *time.Time
	if remote.GetNextAttemptAt() != nil {
		parsed, parseErr := exactAgentCloudTimestamp(remote.GetNextAttemptAt())
		if parseErr != nil || !parsed.After(updatedAt) {
			return cloudmodule.AgentCloudDestroyOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		nextAttemptAt = &parsed
	}
	if remote.GetAutomaticAttempts() < 0 || remote.GetAutomaticAttempts() > 3 {
		return cloudmodule.AgentCloudDestroyOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	if statusName == "destroy_blocked" {
		if !agentCloudIdentifierPattern.MatchString(remote.GetErrorCode()) || !validAgentCloudText(remote.GetBlockedReason(), 512) ||
			cloudmodule.ContainsSensitiveGoalMaterial(remote.GetBlockedReason()) || !remote.GetRequiresNewApproval() || nextAttemptAt != nil || remote.GetAutomaticAttempts() < 1 {
			return cloudmodule.AgentCloudDestroyOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
	} else if statusName == "destroying" {
		if remote.GetRequiresNewApproval() || remote.GetAutomaticAttempts() < 1 || remote.GetBlockedReason() != "" ||
			(nextAttemptAt == nil && remote.GetErrorCode() != "") || (nextAttemptAt != nil && !agentCloudIdentifierPattern.MatchString(remote.GetErrorCode())) {
			return cloudmodule.AgentCloudDestroyOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
	} else if remote.GetErrorCode() != "" || remote.GetBlockedReason() != "" || nextAttemptAt != nil || remote.GetRequiresNewApproval() ||
		((statusName == "awaiting_approval" || statusName == "approved") && remote.GetAutomaticAttempts() != 0) ||
		(statusName == "verified_destroyed" && remote.GetAutomaticAttempts() < 1) {
		return cloudmodule.AgentCloudDestroyOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.AgentCloudDestroyOperation{
		OperationID: remote.GetOperationId(), OwnerID: remote.GetOwnerId(), DeploymentID: remote.GetDeploymentId(), ApprovalID: remote.GetApprovalId(),
		ScopeDigest: remote.GetScopeDigest(), Status: statusName, ErrorCode: remote.GetErrorCode(), BlockedReason: remote.GetBlockedReason(),
		AutomaticAttempts: remote.GetAutomaticAttempts(), NextAttemptAt: nextAttemptAt, RequiresNewApproval: remote.GetRequiresNewApproval(),
		Revision: remote.GetRevision(), CreatedAt: createdAt, UpdatedAt: updatedAt,
	}, nil
}

func sameExpectedAgentDestroyDeployment(value cloudmodule.Deployment, deploymentID string, revision int64) bool {
	return value.DeploymentID == deploymentID && value.Revision == revision && validUUID(value.DeploymentID) && validUUID(value.PlanID) &&
		validUUID(value.ConnectionID) && agentDestroyableDeploymentStatus(value.Resource) && value.CreatedAt > 0 && value.UpdatedAt >= value.CreatedAt
}

func agentDestroyableDeploymentStatus(value string) bool {
	switch value {
	case "active", "destroy_scheduled", "destroy_blocked", "mixed":
		return true
	default:
		return false
	}
}

func agentDestroyResourceScopeStatus(value agentv1.CloudResourceStatus) (string, bool, bool) {
	switch value {
	case agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_ACTIVE:
		return "active", true, true
	case agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_DESTROY_SCHEDULED:
		return "destroy_scheduled", true, true
	case agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_DESTROY_BLOCKED:
		return "destroy_blocked", true, true
	case agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_VERIFIED_DESTROYED:
		return "verified_destroyed", false, true
	default:
		return "", false, false
	}
}

func agentDestroyScopeProjection(resources []cloudmodule.AgentCloudDestroyResourceScope) string {
	if len(resources) == 0 {
		return ""
	}
	status := resources[0].Status
	for _, resource := range resources[1:] {
		if resource.Status != status {
			return "mixed"
		}
	}
	return status
}

func validAgentDestroyOperationDeployment(operation cloudmodule.AgentCloudDestroyOperation, deployment, expected cloudmodule.Deployment) bool {
	if deployment.DeploymentID != operation.DeploymentID || deployment.PlanID != expected.PlanID || deployment.ConnectionID != expected.ConnectionID ||
		deployment.Revision < expected.Revision {
		return false
	}
	if operation.Status == "verified_destroyed" {
		return deployment.Resource == "verified_destroyed"
	}
	return deployment.Resource != "verified_destroyed"
}

func agentDestroyProvider(resourceType agentv1.CloudResourceType, providerID string) (string, bool) {
	switch resourceType {
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_EC2:
		return "ec2", agentDestroyEC2Pattern.MatchString(providerID)
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_EBS:
		return "ebs", agentDestroyEBSPattern.MatchString(providerID)
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_ENI:
		return "eni", agentDestroyENIPattern.MatchString(providerID)
	case agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_SECURITY_GROUP:
		return "security_group", agentDestroySGPattern.MatchString(providerID)
	default:
		return "", false
	}
}

func agentDestroyOperationStatus(value agentv1.CloudDestroyOperationStatus) (string, bool) {
	switch value {
	case agentv1.CloudDestroyOperationStatus_CLOUD_DESTROY_OPERATION_STATUS_AWAITING_APPROVAL:
		return "awaiting_approval", true
	case agentv1.CloudDestroyOperationStatus_CLOUD_DESTROY_OPERATION_STATUS_APPROVED:
		return "approved", true
	case agentv1.CloudDestroyOperationStatus_CLOUD_DESTROY_OPERATION_STATUS_DESTROYING:
		return "destroying", true
	case agentv1.CloudDestroyOperationStatus_CLOUD_DESTROY_OPERATION_STATUS_VERIFIED_DESTROYED:
		return "verified_destroyed", true
	case agentv1.CloudDestroyOperationStatus_CLOUD_DESTROY_OPERATION_STATUS_DESTROY_BLOCKED:
		return "destroy_blocked", true
	default:
		return "", false
	}
}
