package agentgrpc

import (
	"context"
	"regexp"
	"strings"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const agentFoundationScopeSchema = "dirextalk.agent.aws-foundation-operation-scope/v1"

var agentFoundationAccountPattern = regexp.MustCompile(`^[0-9]{12}$`)
var agentFoundationKeyIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func (runner *Runner) CreateAgentAWSFoundationChallenge(ctx context.Context, request cloudmodule.AgentCloudFoundationChallengeRequest) (cloudmodule.AgentCloudFoundationChallenge, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudFoundationChallenge{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	action, ok := agentFoundationActionToProto(request.Action)
	if !ok || !validUUID(request.IdempotencyKey) || !validUUID(request.ConnectionID) || !validUUID(request.BootstrapSessionID) ||
		request.ExpectedBootstrapRevision <= 0 || !agentFoundationKeyIDPattern.MatchString(request.SignerKeyID) || cloudmodule.ContainsSensitiveGoalMaterial(request.SignerKeyID) {
		return cloudmodule.AgentCloudFoundationChallenge{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.CreateAwsFoundationOperationChallenge(callContext, &agentv1.CreateAwsFoundationOperationChallengeRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, Action: action, ConnectionId: request.ConnectionID,
		BootstrapSessionId: request.BootstrapSessionID, ExpectedBootstrapRevision: request.ExpectedBootstrapRevision, SignerKeyId: request.SignerKeyID,
	})
	if err != nil {
		return cloudmodule.AgentCloudFoundationChallenge{}, mapAgentCloudControlRPCError(callContext, err)
	}
	challenge, mapErr := runner.mapAgentFoundationChallenge(response.GetChallenge())
	if mapErr != nil || challenge.Scope.Action != request.Action || challenge.Scope.ConnectionID != request.ConnectionID ||
		challenge.Scope.BootstrapSessionID != request.BootstrapSessionID || challenge.Scope.ExpectedBootstrapRevision != request.ExpectedBootstrapRevision ||
		challenge.SignerKeyID != request.SignerKeyID {
		return cloudmodule.AgentCloudFoundationChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return challenge, nil
}

func (runner *Runner) ApproveAgentAWSFoundation(ctx context.Context, request cloudmodule.AgentCloudFoundationApproveRequest) (cloudmodule.AgentCloudFoundationOperation, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudFoundationOperation{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.ExpectedOperationID) || !validUUID(request.ExpectedConnectionID) ||
		request.ExpectedRevision != 1 || !agentCloudDigestPattern.MatchString(request.ExpectedScopeDigest) || !validAgentCloudApproval(request.Approval) {
		return cloudmodule.AgentCloudFoundationOperation{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	if _, ok := agentFoundationActionToProto(request.ExpectedAction); !ok {
		return cloudmodule.AgentCloudFoundationOperation{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	action, _ := agentFoundationActionToProto(request.ExpectedAction)
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.ApproveAwsFoundationOperation(callContext, &agentv1.ApproveAwsFoundationOperationRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, Approval: agentCloudApprovalToProto(request.Approval),
		OperationId: request.ExpectedOperationID, ExpectedRevision: request.ExpectedRevision, ConnectionId: request.ExpectedConnectionID,
		Action: action, ScopeDigest: request.ExpectedScopeDigest,
	})
	if err != nil {
		return cloudmodule.AgentCloudFoundationOperation{}, mapAgentCloudControlRPCError(callContext, err)
	}
	operation, mapErr := runner.mapAgentFoundationOperation(response.GetOperation())
	if mapErr != nil || operation.OperationID != request.ExpectedOperationID || operation.ConnectionID != request.ExpectedConnectionID ||
		operation.Action != request.ExpectedAction || operation.ApprovalID != request.Approval.ApprovalID || operation.ScopeDigest != request.ExpectedScopeDigest ||
		operation.Status != "approved" || operation.Revision != request.ExpectedRevision+1 {
		return cloudmodule.AgentCloudFoundationOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return operation, nil
}

func (runner *Runner) GetAgentAWSFoundationOperation(ctx context.Context, request cloudmodule.AgentCloudFoundationOperationRequest) (cloudmodule.AgentCloudFoundationOperation, bool, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudFoundationOperation{}, false, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.OperationID) {
		return cloudmodule.AgentCloudFoundationOperation{}, false, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.GetAwsFoundationOperation(callContext, &agentv1.GetAwsFoundationOperationRequest{OwnerId: runner.ownerID, OperationId: request.OperationID})
	if err != nil {
		if status.Code(err) == codes.NotFound && callContext.Err() == nil {
			return cloudmodule.AgentCloudFoundationOperation{}, false, nil
		}
		return cloudmodule.AgentCloudFoundationOperation{}, false, mapAgentCloudControlRPCError(callContext, err)
	}
	operation, mapErr := runner.mapAgentFoundationOperation(response.GetOperation())
	if mapErr != nil || operation.OperationID != request.OperationID {
		return cloudmodule.AgentCloudFoundationOperation{}, false, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return operation, true, nil
}

func (runner *Runner) mapAgentFoundationChallenge(value *agentv1.AwsFoundationOperationChallenge) (cloudmodule.AgentCloudFoundationChallenge, error) {
	if value == nil || value.GetScope() == nil || !validUUID(value.GetOperationId()) || !validUUID(value.GetChallengeId()) || !validUUID(value.GetApprovalId()) ||
		!agentFoundationKeyIDPattern.MatchString(value.GetSignerKeyId()) || !agentCloudDigestPattern.MatchString(value.GetScopeDigest()) || value.GetRevision() != 1 ||
		len(value.GetSigningPayloadCbor()) == 0 || len(value.GetSigningPayloadCbor()) > 64*1024 {
		return cloudmodule.AgentCloudFoundationChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	expiresAt, err := exactAgentCloudTimestamp(value.GetExpiresAt())
	if err != nil {
		return cloudmodule.AgentCloudFoundationChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	scope, err := runner.mapAgentFoundationScope(value.GetScope())
	if err != nil {
		return cloudmodule.AgentCloudFoundationChallenge{}, err
	}
	return cloudmodule.AgentCloudFoundationChallenge{OperationID: value.GetOperationId(), ChallengeID: value.GetChallengeId(), ApprovalID: value.GetApprovalId(),
		SignerKeyID: value.GetSignerKeyId(), ScopeDigest: value.GetScopeDigest(), Scope: scope, ExpiresAt: expiresAt,
		SigningPayloadCBOR: append([]byte(nil), value.GetSigningPayloadCbor()...), Revision: value.GetRevision()}, nil
}

func (runner *Runner) mapAgentFoundationScope(value *agentv1.AwsFoundationOperationScope) (cloudmodule.AgentCloudFoundationScope, error) {
	action, ok := agentFoundationActionFromProto(value.GetAction())
	environment := value.GetReleaseEnvironment()
	observedAt, observedErr := exactAgentCloudTimestamp(value.GetIdentityObservedAt())
	expiresAt, expiresErr := exactAgentCloudTimestamp(value.GetIdentityExpiresAt())
	if !ok || value.GetSchemaVersion() != agentFoundationScopeSchema || value.GetAgentInstanceId() != runner.agentInstanceID || value.GetOwnerId() != runner.ownerID ||
		!validUUID(value.GetConnectionId()) || !validUUID(value.GetBootstrapSessionId()) || !agentFoundationAccountPattern.MatchString(value.GetAccountId()) ||
		!remoteAWSRegionPattern.MatchString(value.GetRegion()) || value.GetExpectedBootstrapRevision() <= 0 || value.GetExpectedCredentialGeneration() < 0 ||
		!agentCloudDigestPattern.MatchString(value.GetFoundationTemplateDigest()) || !strings.Contains(value.GetReaperImageUri(), "@sha256:") ||
		environment == nil || environment.GetPrivateSubnetCidr() != "10.255.0.0/26" || !environment.GetZeroIngress() || !environment.GetBucketVersioned() || !environment.GetBucketSseKms() ||
		strings.TrimSpace(environment.GetArtifactBucket()) == "" || !strings.HasPrefix(environment.GetKmsAlias(), "alias/dtx-agent-") || observedErr != nil || expiresErr != nil || !observedAt.Before(expiresAt) {
		return cloudmodule.AgentCloudFoundationScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	if (action == "establish" && (value.GetExpectedConnectionRevision() != 0 || value.GetExpectedCredentialGeneration() != 0)) ||
		(action != "establish" && (value.GetExpectedConnectionRevision() <= 0 || value.GetExpectedCredentialGeneration() <= 0)) {
		return cloudmodule.AgentCloudFoundationScope{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.AgentCloudFoundationScope{SchemaVersion: value.GetSchemaVersion(), AgentInstanceID: value.GetAgentInstanceId(), OwnerID: value.GetOwnerId(), Action: action,
		ConnectionID: value.GetConnectionId(), ExpectedConnectionRevision: value.GetExpectedConnectionRevision(), AccountID: value.GetAccountId(), Region: value.GetRegion(),
		BootstrapSessionID: value.GetBootstrapSessionId(), ExpectedBootstrapRevision: value.GetExpectedBootstrapRevision(), ExpectedCredentialGeneration: value.GetExpectedCredentialGeneration(),
		FoundationTemplateDigest: value.GetFoundationTemplateDigest(), ReaperImageURI: value.GetReaperImageUri(), IdentityObservedAt: observedAt, IdentityExpiresAt: expiresAt,
		ReleaseEnvironment: cloudmodule.AgentCloudFoundationReleaseEnvironment{PrivateSubnetCIDR: environment.GetPrivateSubnetCidr(), ZeroIngress: environment.GetZeroIngress(),
			ArtifactBucket: environment.GetArtifactBucket(), KMSAlias: environment.GetKmsAlias(), BucketVersioned: environment.GetBucketVersioned(), BucketSSEKMS: environment.GetBucketSseKms()}}, nil
}

func (runner *Runner) mapAgentFoundationOperation(value *agentv1.AwsFoundationOperation) (cloudmodule.AgentCloudFoundationOperation, error) {
	if value == nil {
		return cloudmodule.AgentCloudFoundationOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	action, actionOK := agentFoundationActionFromProto(value.GetAction())
	operationStatus, statusOK := agentFoundationStatus(value.GetStatus())
	createdAt, createdErr := exactAgentCloudTimestamp(value.GetCreatedAt())
	updatedAt, updatedErr := exactAgentCloudTimestamp(value.GetUpdatedAt())
	if !actionOK || !statusOK || !validUUID(value.GetOperationId()) || value.GetOwnerId() != runner.ownerID || !validUUID(value.GetConnectionId()) ||
		!validUUID(value.GetApprovalId()) || !agentCloudDigestPattern.MatchString(value.GetScopeDigest()) || value.GetRevision() <= 0 || createdErr != nil || updatedErr != nil || updatedAt.Before(createdAt) ||
		cloudmodule.ContainsSensitiveGoalMaterial(value.GetErrorCode()) || cloudmodule.ContainsSensitiveGoalMaterial(value.GetBlockedReason()) {
		return cloudmodule.AgentCloudFoundationOperation{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.AgentCloudFoundationOperation{OperationID: value.GetOperationId(), OwnerID: value.GetOwnerId(), ConnectionID: value.GetConnectionId(), Action: action,
		ApprovalID: value.GetApprovalId(), ScopeDigest: value.GetScopeDigest(), Status: operationStatus, ErrorCode: value.GetErrorCode(), BlockedReason: value.GetBlockedReason(),
		Revision: value.GetRevision(), CreatedAt: createdAt, UpdatedAt: updatedAt}, nil
}

func agentFoundationActionToProto(value string) (agentv1.AwsFoundationOperationAction, bool) {
	switch value {
	case "establish":
		return agentv1.AwsFoundationOperationAction_AWS_FOUNDATION_OPERATION_ACTION_ESTABLISH, true
	case "upgrade":
		return agentv1.AwsFoundationOperationAction_AWS_FOUNDATION_OPERATION_ACTION_UPGRADE, true
	case "teardown":
		return agentv1.AwsFoundationOperationAction_AWS_FOUNDATION_OPERATION_ACTION_TEARDOWN, true
	case "remediate_destroy_blocked":
		return agentv1.AwsFoundationOperationAction_AWS_FOUNDATION_OPERATION_ACTION_REMEDIATE_DESTROY_BLOCKED, true
	default:
		return agentv1.AwsFoundationOperationAction_AWS_FOUNDATION_OPERATION_ACTION_UNSPECIFIED, false
	}
}

func agentFoundationActionFromProto(value agentv1.AwsFoundationOperationAction) (string, bool) {
	for _, action := range []string{"establish", "upgrade", "teardown", "remediate_destroy_blocked"} {
		if candidate, _ := agentFoundationActionToProto(action); candidate == value {
			return action, true
		}
	}
	return "", false
}

func agentFoundationStatus(value agentv1.AwsFoundationOperationStatus) (string, bool) {
	switch value {
	case agentv1.AwsFoundationOperationStatus_AWS_FOUNDATION_OPERATION_STATUS_AWAITING_APPROVAL:
		return "awaiting_approval", true
	case agentv1.AwsFoundationOperationStatus_AWS_FOUNDATION_OPERATION_STATUS_APPROVED:
		return "approved", true
	case agentv1.AwsFoundationOperationStatus_AWS_FOUNDATION_OPERATION_STATUS_RUNNING:
		return "running", true
	case agentv1.AwsFoundationOperationStatus_AWS_FOUNDATION_OPERATION_STATUS_SUCCEEDED:
		return "succeeded", true
	case agentv1.AwsFoundationOperationStatus_AWS_FOUNDATION_OPERATION_STATUS_FAILED_RETRIABLE:
		return "failed_retriable", true
	case agentv1.AwsFoundationOperationStatus_AWS_FOUNDATION_OPERATION_STATUS_FAILED_TERMINAL:
		return "failed_terminal", true
	case agentv1.AwsFoundationOperationStatus_AWS_FOUNDATION_OPERATION_STATUS_DESTROY_BLOCKED:
		return "destroy_blocked", true
	default:
		return "", false
	}
}
