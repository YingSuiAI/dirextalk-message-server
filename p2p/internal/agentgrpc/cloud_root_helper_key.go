package agentgrpc

import (
	"bytes"
	"context"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (runner *Runner) PrepareRootHelperKey(ctx context.Context, request cloudmodule.AgentRootHelperKeyPrepareRequest) (cloudmodule.AgentRootHelperKeyApproval, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentRootHelperKeyApproval{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.DeploymentID) || request.ExpectedDeploymentRevision <= 0 ||
		!agentFoundationKeyIDPattern.MatchString(request.DeviceSignerKeyID) ||
		cloudmodule.ContainsSensitiveGoalMaterial(request.DeviceSignerKeyID) {
		return cloudmodule.AgentRootHelperKeyApproval{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.PrepareRootHelperKeyDeliveryApproval(callContext, &agentv1.PrepareRootHelperKeyDeliveryApprovalRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, DeploymentId: request.DeploymentID,
		ExpectedDeploymentRevision: request.ExpectedDeploymentRevision, DeviceSignerKeyId: request.DeviceSignerKeyID,
	})
	if err != nil {
		return cloudmodule.AgentRootHelperKeyApproval{}, mapAgentCloudControlRPCError(callContext, err)
	}
	approval, mapErr := mapRootHelperKeyApproval(response.GetApproval())
	compatibility := cloudmodule.ManagedAcceptanceCompatibility{
		DeploymentID: request.DeploymentID, DeploymentRevision: request.ExpectedDeploymentRevision, SignerKeyID: request.DeviceSignerKeyID,
	}
	if mapErr != nil || cloudmodule.ValidateAgentRootHelperKeyApproval(approval, runner.ownerID, compatibility) != nil ||
		approval.Status != "awaiting_approval" {
		return cloudmodule.AgentRootHelperKeyApproval{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return approval, nil
}

func (runner *Runner) ApproveRootHelperKey(ctx context.Context, request cloudmodule.AgentRootHelperKeyApproveRequest) (cloudmodule.AgentRootHelperKeyApproval, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentRootHelperKeyApproval{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.DeploymentID) || !validUUID(request.DeliveryID) ||
		request.ExpectedRevision != 1 || len(request.DeviceSignature) != 64 {
		return cloudmodule.AgentRootHelperKeyApproval{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.ApproveRootHelperKeyDelivery(callContext, &agentv1.ApproveRootHelperKeyDeliveryRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, DeploymentId: request.DeploymentID,
		DeliveryId: request.DeliveryID, ExpectedRevision: request.ExpectedRevision,
		DeviceSignature: append([]byte(nil), request.DeviceSignature...),
	})
	if err != nil {
		return cloudmodule.AgentRootHelperKeyApproval{}, mapAgentCloudControlRPCError(callContext, err)
	}
	approval, mapErr := mapRootHelperKeyApproval(response.GetApproval())
	if mapErr != nil || approval.Binding.OwnerID != runner.ownerID || approval.Binding.DeploymentID != request.DeploymentID ||
		approval.Binding.DeliveryID != request.DeliveryID || approval.Status != "approved" {
		return cloudmodule.AgentRootHelperKeyApproval{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return approval, nil
}

func (runner *Runner) GetRootHelperKey(ctx context.Context, request cloudmodule.AgentRootHelperKeyGetRequest) (cloudmodule.AgentRootHelperKeyApproval, bool, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentRootHelperKeyApproval{}, false, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.DeploymentID) || !validUUID(request.DeliveryID) {
		return cloudmodule.AgentRootHelperKeyApproval{}, false, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.GetRootHelperKeyDeliveryApproval(callContext, &agentv1.GetRootHelperKeyDeliveryApprovalRequest{
		OwnerId: runner.ownerID, DeploymentId: request.DeploymentID, DeliveryId: request.DeliveryID,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound && callContext.Err() == nil {
			return cloudmodule.AgentRootHelperKeyApproval{}, false, nil
		}
		return cloudmodule.AgentRootHelperKeyApproval{}, false, mapAgentCloudControlRPCError(callContext, err)
	}
	approval, mapErr := mapRootHelperKeyApproval(response.GetApproval())
	if mapErr != nil || approval.Binding.OwnerID != runner.ownerID || approval.Binding.DeploymentID != request.DeploymentID ||
		approval.Binding.DeliveryID != request.DeliveryID {
		return cloudmodule.AgentRootHelperKeyApproval{}, false, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return approval, true, nil
}

func mapRootHelperKeyApproval(value *agentv1.RootHelperKeyDeliveryApproval) (cloudmodule.AgentRootHelperKeyApproval, error) {
	if value == nil || value.GetBinding() == nil || value.GetBinding().GetSecretPlan() == nil {
		return cloudmodule.AgentRootHelperKeyApproval{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	statuses := map[agentv1.RootHelperKeyDeliveryApprovalStatus]string{
		agentv1.RootHelperKeyDeliveryApprovalStatus_ROOT_HELPER_KEY_DELIVERY_APPROVAL_STATUS_AWAITING_APPROVAL: "awaiting_approval",
		agentv1.RootHelperKeyDeliveryApprovalStatus_ROOT_HELPER_KEY_DELIVERY_APPROVAL_STATUS_APPROVED:          "approved",
	}
	mappedStatus, found := statuses[value.GetStatus()]
	createdAt, createdErr := exactAgentCloudTimestamp(value.GetCreatedAt())
	updatedAt, updatedErr := exactAgentCloudTimestamp(value.GetUpdatedAt())
	if !found || createdErr != nil || updatedErr != nil || updatedAt.Before(createdAt) {
		return cloudmodule.AgentRootHelperKeyApproval{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	binding := mapRootHelperKeyBinding(value.GetBinding())
	result := cloudmodule.AgentRootHelperKeyApproval{
		SchemaVersion: value.GetSchemaVersion(), ChallengeID: value.GetChallengeId(),
		DeviceSignerKeyID: value.GetDeviceSignerKeyId(), Binding: binding,
		PublicKey: append([]byte(nil), value.GetPublicKey()...), Nonce: append([]byte(nil), value.GetNonce()...),
		SigningPayloadCBOR:   append([]byte(nil), value.GetSigningPayloadCbor()...),
		SigningPayloadDigest: value.GetSigningPayloadDigest(), Status: mappedStatus, Revision: value.GetRevision(),
		DeviceSignature: append([]byte(nil), value.GetDeviceSignature()...), CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
	payload, err := cloudmodule.RootHelperKeyBindingSigningPayload(binding)
	if err != nil || !bytes.Equal(payload, result.SigningPayloadCBOR) {
		return cloudmodule.AgentRootHelperKeyApproval{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return result, nil
}

func mapRootHelperKeyBinding(value *agentv1.RootHelperKeyDeviceBinding) cloudmodule.AgentRootHelperKeyBinding {
	plan := value.GetSecretPlan()
	result := cloudmodule.AgentRootHelperKeyBinding{
		SchemaVersion: value.GetSchemaVersion(), AgentInstanceID: value.GetAgentInstanceId(), OwnerID: value.GetOwnerId(),
		DeliveryID: value.GetDeliveryId(), DeploymentID: value.GetDeploymentId(), BindingRevision: value.GetBindingRevision(),
		InstanceID: value.GetInstanceId(), WorkerRoleARN: value.GetWorkerRoleArn(),
		WorkerPrincipalID: value.GetWorkerPrincipalId(), HelperID: value.GetHelperId(), SignerKeyID: value.GetSignerKeyId(),
		PublicKeyDigest: value.GetPublicKeyDigest(), NonceDigest: value.GetNonceDigest(),
		SecretPlan: cloudmodule.AgentRootHelperKeySecretPlan{
			Partition: plan.GetPartition(), AccountID: plan.GetAccountId(), Region: plan.GetRegion(), Name: plan.GetName(),
			VersionID: plan.GetVersionId(), KMSKeyARN: plan.GetKmsKeyArn(), TargetPath: plan.GetTargetPath(), FileMode: plan.GetFileMode(),
		},
	}
	if secret := value.GetSecret(); secret != nil {
		result.Secret = cloudmodule.AgentRootHelperKeySecretCoordinate{
			ARN: secret.GetArn(), Name: secret.GetName(), VersionID: secret.GetVersionId(), KMSKeyARN: secret.GetKmsKeyArn(),
		}
	}
	return result
}
