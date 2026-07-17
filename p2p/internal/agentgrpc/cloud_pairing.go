package agentgrpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func validPairingRecipient(value string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(value) == 43 && len(decoded) == 32
}

func (runner *Runner) GetAgentCloudPairing(ctx context.Context, request cloudmodule.AgentCloudPairingGetRequest) (cloudmodule.AgentCloudPairingSession, bool, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudPairingSession{}, false, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if request.PairingID != "" && !validUUID(request.PairingID) {
		return cloudmodule.AgentCloudPairingSession{}, false, cloudmodule.ErrAgentCloudControlInvalid
	}
	if request.DeploymentID != "" && !validUUID(request.DeploymentID) {
		return cloudmodule.AgentCloudPairingSession{}, false, cloudmodule.ErrAgentCloudControlInvalid
	}
	if request.PairingID == "" && request.DeploymentID == "" {
		return cloudmodule.AgentCloudPairingSession{}, false, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.GetCloudPairing(callContext, &agentv1.GetCloudPairingRequest{
		OwnerId: runner.ownerID, PairingId: request.PairingID, DeploymentId: request.DeploymentID,
	})
	if err != nil {
		if callContext.Err() == nil && status.Code(err) == codes.NotFound {
			return cloudmodule.AgentCloudPairingSession{}, false, nil
		}
		return cloudmodule.AgentCloudPairingSession{}, false, mapAgentCloudControlRPCError(callContext, err)
	}
	if response == nil || response.GetPairing() == nil {
		return cloudmodule.AgentCloudPairingSession{}, false, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	result, err := runner.mapCloudPairingSession(response.GetPairing())
	if err != nil || (request.PairingID != "" && result.PairingID != request.PairingID) ||
		(request.DeploymentID != "" && result.DeploymentID != request.DeploymentID) {
		return cloudmodule.AgentCloudPairingSession{}, false, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return result, true, nil
}

func (runner *Runner) CreateAgentCloudPairingResumeChallenge(ctx context.Context, request cloudmodule.AgentCloudPairingResumeChallengeRequest) (cloudmodule.AgentCloudPairingResumeChallenge, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudPairingResumeChallenge{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.PairingID) || !validUUID(request.DeploymentID) ||
		request.ExpectedPairingRevision <= 0 {
		return cloudmodule.AgentCloudPairingResumeChallenge{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.CreateCloudPairingResumeChallenge(callContext, &agentv1.CreateCloudPairingResumeChallengeRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, PairingId: request.PairingID,
		DeploymentId: request.DeploymentID, ExpectedPairingRevision: request.ExpectedPairingRevision,
		SignerKeyId: request.SignerKeyID,
	})
	if err != nil {
		return cloudmodule.AgentCloudPairingResumeChallenge{}, mapAgentCloudControlRPCError(callContext, err)
	}
	if response == nil || response.GetChallenge() == nil {
		return cloudmodule.AgentCloudPairingResumeChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return runner.mapCloudPairingResumeChallenge(response.GetChallenge(), request)
}

func (runner *Runner) ApproveAgentCloudPairingResume(ctx context.Context, request cloudmodule.AgentCloudPairingResumeApproveRequest) (cloudmodule.AgentCloudPairingSession, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudPairingSession{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	approval := request.Approval
	signature, err := base64.RawURLEncoding.DecodeString(approval.Signature)
	if !validUUID(request.IdempotencyKey) || !validUUID(request.PairingID) || !validUUID(request.DeploymentID) ||
		request.ExpectedPairingRevision <= 0 || approval.Validate() != nil || len(signature) != 64 {
		return cloudmodule.AgentCloudPairingSession{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.ApproveCloudPairingResume(callContext, &agentv1.ApproveCloudPairingResumeRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, PairingId: request.PairingID,
		DeploymentId: request.DeploymentID, ExpectedPairingRevision: request.ExpectedPairingRevision,
		Approval: &agentv1.DeviceApprovalSignature{
			ApprovalId: approval.ApprovalID, ChallengeId: approval.ChallengeID, SignerKeyId: approval.SignerKeyID,
			ExpiresAt: timestamppb.New(approval.ExpiresAt), Signature: signature,
		},
	})
	clear(signature)
	if err != nil {
		return cloudmodule.AgentCloudPairingSession{}, mapAgentCloudControlRPCError(callContext, err)
	}
	if response == nil || response.GetPairing() == nil {
		return cloudmodule.AgentCloudPairingSession{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	result, err := runner.mapCloudPairingSession(response.GetPairing())
	if err != nil || result.PairingID != request.PairingID || result.DeploymentID != request.DeploymentID {
		return cloudmodule.AgentCloudPairingSession{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return result, nil
}

func (runner *Runner) mapCloudPairingResumeChallenge(value *agentv1.CloudPairingResumeChallenge, request cloudmodule.AgentCloudPairingResumeChallengeRequest) (cloudmodule.AgentCloudPairingResumeChallenge, error) {
	scope := value.GetScope()
	issuedAt, err := exactAgentCloudTimestamp(value.GetIssuedAt())
	if err != nil || scope == nil || value.GetSchemaVersion() != "dirextalk.agent.pairing-resume-challenge/v1" ||
		scope.GetSchemaVersion() != "dirextalk.agent.pairing-resume-scope/v1" ||
		scope.GetIntent() != cloudcontracts.PairingResumeIntent ||
		scope.GetOwnerId() != runner.ownerID || scope.GetPairingId() != request.PairingID ||
		scope.GetDeploymentId() != request.DeploymentID || scope.GetPairingRevision() != request.ExpectedPairingRevision ||
		!validUUID(scope.GetTaskId()) || !validUUID(scope.GetStepId()) || !validUUID(scope.GetPlanId()) ||
		!validUUID(scope.GetConnectionId()) || scope.GetDeploymentRevision() <= 0 ||
		!agentCloudDigestPattern.MatchString(scope.GetRecipeDigest()) ||
		!agentCloudDigestPattern.MatchString(scope.GetExecutionManifestDigest()) ||
		!validUUID(value.GetApprovalId()) || !validUUID(value.GetChallengeId()) ||
		!agentCloudIdentifierPattern.MatchString(value.GetSignerKeyId()) ||
		!agentCloudDigestPattern.MatchString(value.GetScopeDigest()) {
		return cloudmodule.AgentCloudPairingResumeChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	expiresAt, err := exactAgentCloudTimestamp(value.GetExpiresAt())
	if err != nil || !issuedAt.Before(expiresAt) || expiresAt.Sub(issuedAt) > 5*time.Minute {
		return cloudmodule.AgentCloudPairingResumeChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	approval, err := cloudcontracts.NewPairingResumeApprovalV1(cloudcontracts.PairingResumeTargetV1{
		DeploymentID: scope.GetDeploymentId(), DeploymentRevision: uint64(scope.GetDeploymentRevision()),
		PlanID: scope.GetPlanId(), CloudConnectionID: scope.GetConnectionId(), ExecutionID: scope.GetTaskId(),
		RecipeExecutionManifestDigest: scope.GetExecutionManifestDigest(),
		JobID:                         scope.GetPairingId(), JobRevision: uint64(scope.GetPairingRevision()),
	}, value.GetApprovalId(), value.GetChallengeId(), value.GetSignerKeyId(), issuedAt, expiresAt)
	if err != nil {
		return cloudmodule.AgentCloudPairingResumeChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	signing := value.GetSigningPayloadCbor()
	expected, err := approval.SigningPayload()
	if err != nil || len(signing) == 0 || !bytes.Equal(signing, expected) {
		clear(expected)
		return cloudmodule.AgentCloudPairingResumeChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	clear(expected)
	return cloudmodule.AgentCloudPairingResumeChallenge{
		Approval: approval, SigningPayloadCBOR: bytes.Clone(signing),
	}, nil
}

// RetrieveAgentCloudPairingPayload forwards one owner-triggered, one-time
// recipient request and returns only the Agent's authenticated ciphertext.
// No plaintext or recipient private key crosses this boundary.
func (runner *Runner) RetrieveAgentCloudPairingPayload(ctx context.Context, request cloudmodule.AgentCloudPairingPayloadRequest) (cloudmodule.AgentCloudPairingPayloadResult, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudPairingPayloadResult{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.PairingID) || !validUUID(request.DeploymentID) ||
		request.ExpectedRevision <= 0 || !validPairingRecipient(request.RecipientPublicKey) {
		return cloudmodule.AgentCloudPairingPayloadResult{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.RetrieveCloudPairingPayload(callContext, &agentv1.RetrieveCloudPairingPayloadRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID,
		PairingId: request.PairingID, DeploymentId: request.DeploymentID,
		ExpectedRevision: request.ExpectedRevision, RecipientPublicKey: request.RecipientPublicKey,
	})
	if err != nil {
		return cloudmodule.AgentCloudPairingPayloadResult{}, mapAgentCloudControlRPCError(callContext, err)
	}
	if response == nil || response.GetPairing() == nil || response.GetPayload() == nil {
		return cloudmodule.AgentCloudPairingPayloadResult{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	session, err := runner.mapCloudPairingSession(response.GetPairing())
	if err != nil {
		return cloudmodule.AgentCloudPairingPayloadResult{}, err
	}
	payload, err := mapCloudPairingPayload(response.GetPayload())
	if err != nil {
		return cloudmodule.AgentCloudPairingPayloadResult{}, err
	}
	return cloudmodule.AgentCloudPairingPayloadResult{Pairing: session, Payload: payload}, nil
}

func (runner *Runner) mapCloudPairingSession(value *agentv1.CloudPairingSession) (cloudmodule.AgentCloudPairingSession, error) {
	if value == nil {
		return cloudmodule.AgentCloudPairingSession{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	expiresAt, err := exactAgentCloudTimestamp(value.GetExpiresAt())
	if err != nil {
		return cloudmodule.AgentCloudPairingSession{}, err
	}
	createdAt, err := exactAgentCloudTimestamp(value.GetCreatedAt())
	if err != nil {
		return cloudmodule.AgentCloudPairingSession{}, err
	}
	updatedAt, err := exactAgentCloudTimestamp(value.GetUpdatedAt())
	if err != nil {
		return cloudmodule.AgentCloudPairingSession{}, err
	}
	status, ok := cloudPairingStatus(value.GetStatus())
	if !ok {
		return cloudmodule.AgentCloudPairingSession{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	result := cloudmodule.AgentCloudPairingSession{
		PairingID: value.GetPairingId(), OwnerID: value.GetOwnerId(), DeploymentID: value.GetDeploymentId(),
		TaskID: value.GetTaskId(), StepID: value.GetStepId(), PlanID: value.GetPlanId(), ConnectionID: value.GetConnectionId(),
		RecipeID: value.GetRecipeId(), RecipeDigest: value.GetRecipeDigest(), RecipeRevision: value.GetRecipeRevision(),
		BeginCommandID: value.GetBeginCommandId(), ResumeCommandID: value.GetResumeCommandId(),
		ExecutionManifestDigest: value.GetExecutionManifestDigest(), Status: status, PayloadReady: value.GetPayloadReady(),
		DeploymentRevision: value.GetDeploymentRevision(), PayloadScopeRevision: value.GetPayloadScopeRevision(),
		Revision: value.GetRevision(), ExpiresAt: expiresAt, CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
	if !cloudmodule.ValidateAgentCloudPairingSession(result, runner.ownerID) {
		return cloudmodule.AgentCloudPairingSession{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return result, nil
}

func mapCloudPairingPayload(value *agentv1.EncryptedPairingPayload) (cloudmodule.AgentCloudPairingPayload, error) {
	expiresAt, err := exactAgentCloudTimestamp(value.GetExpiresAt())
	if err != nil {
		return cloudmodule.AgentCloudPairingPayload{}, err
	}
	return cloudmodule.AgentCloudPairingPayload{
		SchemaVersion: value.GetSchemaVersion(), ServerEphemeralPublicKey: value.GetServerPublicKey(),
		Nonce: value.GetNonce(), Ciphertext: value.GetCiphertext(),
		AssociatedDataCBOR: bytes.Clone(value.GetAssociatedDataCbor()),
		PayloadDigest:      value.GetPayloadDigest(), ExpiresAt: expiresAt,
	}, nil
}

func cloudPairingStatus(value agentv1.CloudPairingStatus) (string, bool) {
	switch value {
	case agentv1.CloudPairingStatus_CLOUD_PAIRING_STATUS_WAITING_PAYLOAD:
		return "waiting_payload", true
	case agentv1.CloudPairingStatus_CLOUD_PAIRING_STATUS_PAYLOAD_READY:
		return "payload_ready", true
	case agentv1.CloudPairingStatus_CLOUD_PAIRING_STATUS_WAITING_USER:
		return "waiting_user", true
	case agentv1.CloudPairingStatus_CLOUD_PAIRING_STATUS_RESUMING:
		return "resuming", true
	case agentv1.CloudPairingStatus_CLOUD_PAIRING_STATUS_SUCCEEDED:
		return "succeeded", true
	case agentv1.CloudPairingStatus_CLOUD_PAIRING_STATUS_TIMED_OUT:
		return "timed_out", true
	case agentv1.CloudPairingStatus_CLOUD_PAIRING_STATUS_FAILED:
		return "failed", true
	default:
		return "", false
	}
}
