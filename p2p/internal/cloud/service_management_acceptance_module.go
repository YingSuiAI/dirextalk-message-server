package cloud

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func (m *Module) prepareServiceManagementAcceptance(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id", "expected_revision", "idempotency_key"); err != nil {
		return nil, err
	}
	client, ok := m.agentManagedAcceptanceClient()
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	serviceID, idempotencyKey := values.String("service_id"), values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(serviceID) || expectedRevision <= 0 || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceManagementAcceptanceInvalidCode, "cloud service management acceptance is invalid")
	}
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	compatibility, apiErr := m.loadManagedAcceptanceCompatibility(ctx, serviceID)
	if apiErr != nil {
		return nil, apiErr
	}
	challenge, err := client.CreateCloudManagedAcceptanceChallenge(ctx, AgentCloudManagedAcceptanceChallengeRequest{
		IdempotencyKey: idempotencyKey, ServiceID: serviceID, DeploymentID: compatibility.DeploymentID,
		SignerKeyID: compatibility.SignerKeyID, ExpectedDeploymentRevision: compatibility.DeploymentRevision,
	})
	if err != nil {
		return nil, agentManagedAcceptanceError(err, true)
	}
	if validateAgentManagedAcceptanceChallenge(challenge, m.ownerMXID(), serviceID, compatibility.DeploymentID, compatibility.SignerKeyID, expectedRevision, m.now().UTC()) != nil {
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudServiceManagementAcceptanceInvalidCode, "cloud Agent returned an invalid management acceptance challenge")
	}
	return map[string]any{"confirmation": challenge.Confirmation}, nil
}

func (m *Module) approveServiceManagementAcceptance(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id", "expected_revision", "approval", "idempotency_key"); err != nil {
		return nil, err
	}
	client, ok := m.agentManagedAcceptanceClient()
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	serviceID, idempotencyKey := values.String("service_id"), values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(serviceID) || expectedRevision <= 0 || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceManagementAcceptanceInvalidCode, "cloud service management acceptance is invalid")
	}
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	approval, err := decodeServiceManagementAcceptanceApproval(params["approval"])
	if err != nil || approval.Signature == "" {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceManagementAcceptanceInvalidCode, "cloud service management acceptance is invalid")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" || approval.ServiceID != serviceID || int64(approval.ServiceRevision) != expectedRevision ||
		!canonicalUUID(approval.AcceptanceID) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceManagementAcceptanceInvalidCode, "cloud service management acceptance is invalid")
	}
	expired := !m.now().UTC().Before(approval.ExpiresAt)
	compatibility, apiErr := m.loadManagedAcceptanceCompatibility(ctx, serviceID)
	if apiErr != nil {
		return nil, apiErr
	}
	if approval.DeploymentID != compatibility.DeploymentID {
		return nil, actionbase.CodedError(http.StatusConflict, cloudServiceManagementAcceptanceConflictCode, "cloud service management acceptance conflicts with current state")
	}
	signerChanged := approval.SignerKeyID != compatibility.SignerKeyID
	scopeDigest, err := managedAcceptanceScopeDigest(approval)
	if err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceManagementAcceptanceInvalidCode, "cloud service management acceptance is invalid")
	}
	signature, err := base64.RawURLEncoding.DecodeString(approval.Signature)
	if err != nil || len(signature) != 64 {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceManagementAcceptanceInvalidCode, "cloud service management acceptance is invalid")
	}
	request := AgentCloudManagedAcceptanceApproveRequest{
		IdempotencyKey: idempotencyKey, OperationID: approval.AcceptanceID, ServiceID: serviceID, DeploymentID: compatibility.DeploymentID,
		ExpectedServiceRevision: expectedRevision, ExpectedOperationRevision: 1, ExpectedScopeDigest: scopeDigest, Approval: approval,
		ApprovalSignature: AgentCloudApprovalSignature{
			ApprovalID: approval.ApprovalID, ChallengeID: approval.ChallengeID, SignerKeyID: approval.SignerKeyID,
			ExpiresAt: approval.ExpiresAt.UTC(), Signature: signature,
		},
	}
	recovered, found, getErr := client.GetCloudManagedAcceptanceOperation(ctx, AgentCloudManagedAcceptanceOperationRequest{OperationID: approval.AcceptanceID})
	if getErr != nil {
		return nil, agentManagedAcceptanceError(getErr, true)
	}
	if found {
		if validateAgentManagedAcceptanceOperation(recovered, ownerMXID, request) != nil {
			return nil, agentManagedAcceptanceError(ErrAgentCloudControlInvalidResponse, true)
		}
		return managedAcceptanceOperationResult(recovered), nil
	}
	// A clean owner-bound NotFound is the only state in which the device
	// signature may be sent. Expired or rotated approvals can still recover a
	// previously committed operation above, but can never start a new one.
	if expired {
		return nil, actionbase.CodedError(http.StatusConflict, cloudServiceManagementAcceptanceExpiredCode, "cloud service management acceptance has expired")
	}
	if signerChanged {
		return nil, actionbase.CodedError(http.StatusConflict, cloudServiceManagementAcceptanceConflictCode, "cloud service management acceptance conflicts with current state")
	}
	var operation AgentCloudManagedAcceptanceOperation
	operation, callErr := client.ApproveCloudManagedAcceptance(ctx, request)
	if callErr == nil && validateAgentManagedAcceptanceOperation(operation, ownerMXID, request) == nil {
		return managedAcceptanceOperationResult(operation), nil
	}
	// Agent owns the durable operation. A possibly consumed signature is never
	// retried after response loss; only its exact acceptance/operation id may be
	// read back.
	recovered, found, getErr = client.GetCloudManagedAcceptanceOperation(ctx, AgentCloudManagedAcceptanceOperationRequest{OperationID: approval.AcceptanceID})
	if getErr != nil {
		return nil, agentManagedAcceptanceError(getErr, true)
	}
	if found {
		if validateAgentManagedAcceptanceOperation(recovered, ownerMXID, request) != nil {
			return nil, agentManagedAcceptanceError(ErrAgentCloudControlInvalidResponse, true)
		}
		return managedAcceptanceOperationResult(recovered), nil
	}
	if callErr == nil {
		callErr = ErrAgentCloudControlInvalidResponse
	}
	return nil, agentManagedAcceptanceError(callErr, true)
}

func (m *Module) agentManagedAcceptanceClient() (AgentCloudManagedAcceptanceClient, bool) {
	if m == nil || m.cfg.AgentCloudControlClient == nil {
		return nil, false
	}
	client, ok := m.cfg.AgentCloudControlClient.(AgentCloudManagedAcceptanceClient)
	return client, ok && client != nil
}

func (m *Module) loadManagedAcceptanceCompatibility(ctx context.Context, serviceID string) (ManagedAcceptanceCompatibility, *actionbase.Error) {
	if m == nil || m.store == nil || m.ownerMXID() == "" {
		return ManagedAcceptanceCompatibility{}, unavailableError()
	}
	reader, ok := m.store.(ManagedAcceptanceCompatibilityReader)
	if !ok {
		return ManagedAcceptanceCompatibility{}, unavailableError()
	}
	compatibility, found, err := reader.GetCloudManagedAcceptanceCompatibility(ctx, m.ownerMXID(), serviceID)
	if err != nil {
		return ManagedAcceptanceCompatibility{}, actionbase.InternalError(err)
	}
	if !found {
		return ManagedAcceptanceCompatibility{}, actionbase.CodedError(http.StatusNotFound, cloudServiceManagementAcceptanceInvalidCode, "cloud service was not found")
	}
	if !cloudIdentifierPattern.MatchString(compatibility.DeploymentID) || compatibility.DeploymentRevision <= 0 ||
		!cloudKeyIDPattern.MatchString(compatibility.SignerKeyID) ||
		ContainsSensitiveGoalMaterial(compatibility.SignerKeyID) {
		return ManagedAcceptanceCompatibility{}, actionbase.CodedError(http.StatusBadGateway, cloudServiceManagementAcceptanceInvalidCode, "cloud service compatibility projection is invalid")
	}
	return compatibility, nil
}

func validateAgentManagedAcceptanceChallenge(challenge AgentCloudManagedAcceptanceChallenge, ownerID, serviceID, deploymentID, signerKeyID string, expectedServiceRevision int64, now time.Time) error {
	approval := challenge.Confirmation.Approval
	scopeDigest, err := managedAcceptanceScopeDigest(approval)
	if err != nil || !canonicalUUID(challenge.OperationID) || challenge.OperationID != approval.AcceptanceID || challenge.OwnerID != ownerID ||
		challenge.ScopeDigest != scopeDigest || challenge.Revision != 1 || approval.Signature != "" ||
		approval.ServiceID != serviceID || approval.DeploymentID != deploymentID || approval.SignerKeyID != signerKeyID || !now.Before(approval.ExpiresAt) ||
		approval.ServiceRevision != uint64(expectedServiceRevision) ||
		challenge.Confirmation.Service.ServiceID != serviceID || challenge.Confirmation.Service.DeploymentID != deploymentID ||
		challenge.Confirmation.Service.Revision != int64(approval.ServiceRevision) ||
		challenge.Confirmation.Recipe.RecipeID != approval.RecipeID || challenge.Confirmation.Recipe.Revision != int64(approval.RecipeRevision) {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func validateAgentManagedAcceptanceOperation(operation AgentCloudManagedAcceptanceOperation, owner string, request AgentCloudManagedAcceptanceApproveRequest) error {
	if operation.OperationID != request.OperationID || operation.OwnerID != owner || operation.DeploymentID != request.DeploymentID ||
		operation.ApprovalID != request.Approval.ApprovalID ||
		operation.ScopeDigest != request.ExpectedScopeDigest || operation.Revision < request.ExpectedOperationRevision+1 ||
		operation.Status != "succeeded" ||
		operation.Service.ServiceID != request.ServiceID || operation.Service.DeploymentID != request.DeploymentID ||
		operation.Service.Revision != request.ExpectedServiceRevision+1 ||
		operation.Recipe.RecipeID != request.Approval.RecipeID ||
		operation.Acceptance.AcceptanceID != request.OperationID || operation.Acceptance.ServiceID != request.ServiceID ||
		operation.Acceptance.RecipeID != request.Approval.RecipeID || operation.Acceptance.Status != "approved" ||
		operation.Service.Revision <= 0 || operation.Recipe.Revision <= 0 || operation.Acceptance.Revision <= 0 {
		return ErrAgentCloudControlInvalidResponse
	}
	expectedRecipeRevision := int64(request.Approval.RecipeRevision)
	if request.Approval.RecipeMaturity == cloudcontracts.RecipeAwaitingManagementAccept {
		expectedRecipeRevision++
	}
	if operation.Recipe.Revision != expectedRecipeRevision {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func managedAcceptanceScopeDigest(approval cloudcontracts.ServiceManagementAcceptanceApprovalV1) (string, error) {
	payload, err := approval.SigningPayload()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("sha256:%x", sum[:]), nil
}

func managedAcceptanceOperationResult(operation AgentCloudManagedAcceptanceOperation) map[string]any {
	return map[string]any{"service": operation.Service, "recipe": operation.Recipe, "acceptance": operation.Acceptance}
}

func agentManagedAcceptanceError(err error, found bool) *actionbase.Error {
	switch {
	case !found:
		return actionbase.CodedError(http.StatusNotFound, cloudServiceManagementAcceptanceInvalidCode, "cloud management acceptance operation was not found")
	case errors.Is(err, ErrAgentCloudControlInvalid):
		return actionbase.CodedError(http.StatusBadRequest, cloudServiceManagementAcceptanceInvalidCode, "cloud service management acceptance is invalid")
	case errors.Is(err, ErrAgentCloudControlConflict), errors.Is(err, ErrAgentCloudControlRejected):
		return actionbase.CodedError(http.StatusConflict, cloudServiceManagementAcceptanceConflictCode, "cloud service management acceptance conflicts with current state")
	case errors.Is(err, ErrAgentCloudControlInvalidResponse):
		return actionbase.CodedError(http.StatusBadGateway, cloudServiceManagementAcceptanceInvalidCode, "cloud Agent returned an invalid management acceptance response")
	default:
		return unavailableError()
	}
}

func decodeServiceManagementAcceptanceApproval(value any) (cloudcontracts.ServiceManagementAcceptanceApprovalV1, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return cloudcontracts.ServiceManagementAcceptanceApprovalV1{}, err
	}
	var approval cloudcontracts.ServiceManagementAcceptanceApprovalV1
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&approval); err != nil {
		return cloudcontracts.ServiceManagementAcceptanceApprovalV1{}, err
	}
	var trailing any
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return cloudcontracts.ServiceManagementAcceptanceApprovalV1{}, errors.New("service management acceptance contains trailing JSON")
	}
	return approval, approval.Validate()
}
