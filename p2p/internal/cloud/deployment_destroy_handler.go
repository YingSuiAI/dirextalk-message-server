package cloud

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/google/uuid"
)

const (
	actionDeploymentsDestroyPlan    = "cloud.deployments.destroy.plan"
	actionDeploymentsDestroyApprove = "cloud.deployments.destroy.approve"

	cloudDeploymentDestroyInvalidCode   = "cloud_deployment_destroy_invalid"
	cloudDeploymentDestroyConflictCode  = "cloud_deployment_destroy_conflict"
	cloudDeploymentDestroyExpiredCode   = "cloud_deployment_destroy_approval_expired"
	cloudDeploymentDestroySignatureCode = "cloud_deployment_destroy_approval_signature_invalid"
)

func (m *Module) prepareDeploymentDestroy(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if canonicalUUID(actionbase.Params(params).String("deployment_id")) {
		if m == nil || m.cfg.AgentCloudControlClient == nil {
			return nil, unavailableError()
		}
		return m.prepareAgentDeploymentDestroy(ctx, params)
	}
	if err := only(params, "deployment_id", "expected_revision", "idempotency_key"); err != nil {
		return nil, err
	}
	store, failure := m.deploymentDestroyStore()
	if failure != nil {
		return nil, failure
	}
	v := actionbase.Params(params)
	deploymentID, key, revision := v.String("deployment_id"), v.String("idempotency_key"), v.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(deploymentID) || revision <= 0 || ContainsSensitiveGoalMaterial(key) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudDeploymentDestroyInvalidCode, "cloud deployment destruction is invalid")
	}
	if _, err := uuid.Parse(key); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	owner := m.ownerMXID()
	if owner == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UTC().Truncate(time.Second)
	prepared, err := store.PrepareCloudDeploymentDestroy(ctx, PrepareDeploymentDestroyRequest{
		OwnerMXID: owner, DeploymentID: deploymentID, ExpectedRevision: revision, IdempotencyHash: digest(key),
		ApprovalID: m.newID("deployment_destroy_approval"), ChallengeID: m.newID("deployment_destroy_challenge"),
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	})
	if err != nil {
		return nil, deploymentDestroyError(err)
	}
	return map[string]any{"confirmation": prepared.Confirmation}, nil
}

func (m *Module) approveDeploymentDestroy(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if canonicalUUID(actionbase.Params(params).String("deployment_id")) {
		if m == nil || m.cfg.AgentCloudControlClient == nil {
			return nil, unavailableError()
		}
		return m.approveAgentDeploymentDestroy(ctx, params)
	}
	if err := only(params, "deployment_id", "expected_revision", "idempotency_key", "approval"); err != nil {
		return nil, err
	}
	store, failure := m.deploymentDestroyStore()
	if failure != nil {
		return nil, failure
	}
	v := actionbase.Params(params)
	deploymentID, key, revision := v.String("deployment_id"), v.String("idempotency_key"), v.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(deploymentID) || revision <= 0 || ContainsSensitiveGoalMaterial(key) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudDeploymentDestroyInvalidCode, "cloud deployment destruction is invalid")
	}
	if _, err := uuid.Parse(key); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	approval, err := decodeDeploymentDestroyApprovalV1(params["approval"])
	if err != nil || approval.Signature == "" {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudDeploymentDestroyInvalidCode, "cloud deployment destruction approval is invalid")
	}
	owner := m.ownerMXID()
	if owner == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UTC().Truncate(time.Second)
	deploymentEventID, jobEventID := m.newID("event"), m.newID("event")
	approved, err := store.ApproveCloudDeploymentDestroy(ctx, ApproveDeploymentDestroyRequest{
		OwnerMXID: owner, DeploymentID: deploymentID, ExpectedRevision: revision, IdempotencyHash: digest(key), Approval: approval,
		JobID: m.newID("deployment_destroy_job"), OutboxID: m.newID("outbox"), DeploymentEventID: deploymentEventID, JobEventID: jobEventID,
		CreatedAt: now.UnixMilli(),
	})
	if err != nil {
		return nil, deploymentDestroyError(err)
	}
	if approved.Created {
		m.publish(ctx, "cloud.deployment.changed", deploymentEventID, deploymentPayload(approved.Deployment))
		m.publish(ctx, "cloud.job.changed", jobEventID, jobPayload(approved.Job))
	}
	return map[string]any{"deployment": approved.Deployment, "job": approved.Job}, nil
}

func (m *Module) deploymentDestroyStore() (DeploymentDestroyStore, *actionbase.Error) {
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	store, ok := m.store.(DeploymentDestroyStore)
	if !ok {
		return nil, unavailableError()
	}
	return store, nil
}

func deploymentDestroyError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrIdempotencyConflict):
		return actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud deployment destruction request")
	case errors.Is(err, ErrDeploymentDestroyConflict):
		return actionbase.CodedError(http.StatusConflict, cloudDeploymentDestroyConflictCode, "cloud deployment destruction conflicts with current state")
	case errors.Is(err, ErrDeploymentDestroyExpired):
		return actionbase.CodedError(http.StatusConflict, cloudDeploymentDestroyExpiredCode, "cloud deployment destruction approval has expired")
	case errors.Is(err, ErrDeploymentDestroySignature):
		return actionbase.CodedError(http.StatusUnauthorized, cloudDeploymentDestroySignatureCode, "cloud deployment destruction approval signature is invalid")
	case errors.Is(err, ErrDeploymentDestroyInvalid):
		return actionbase.CodedError(http.StatusBadRequest, cloudDeploymentDestroyInvalidCode, "cloud deployment destruction is invalid")
	default:
		return actionbase.InternalError(err)
	}
}

func decodeDeploymentDestroyApprovalV1(value any) (cloudcontracts.DeploymentDestroyApprovalV1, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return cloudcontracts.DeploymentDestroyApprovalV1{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var approval cloudcontracts.DeploymentDestroyApprovalV1
	if err = decoder.Decode(&approval); err != nil {
		return cloudcontracts.DeploymentDestroyApprovalV1{}, err
	}
	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return cloudcontracts.DeploymentDestroyApprovalV1{}, errors.New("deployment destroy approval contains trailing JSON")
	}
	if err = approval.Validate(); err != nil {
		return cloudcontracts.DeploymentDestroyApprovalV1{}, err
	}
	return approval, nil
}
