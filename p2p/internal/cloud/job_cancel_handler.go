package cloud

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/google/uuid"
)

const (
	actionJobsCancelPlan    = "cloud.jobs.cancel.plan"
	actionJobsCancelApprove = "cloud.jobs.cancel.approve"

	cloudJobCancelInvalidCode        = "cloud_job_cancel_invalid"
	cloudJobCancelConflictCode       = "cloud_job_cancel_conflict"
	cloudJobCancelNotCancellableCode = "cloud_job_cancel_not_cancellable"
	cloudJobCancelExpiredCode        = "cloud_job_cancel_approval_expired"
	cloudJobCancelSignatureCode      = "cloud_job_cancel_approval_signature_invalid"
)

func (m *Module) prepareJobCancel(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "job_id", "expected_revision", "idempotency_key"); err != nil {
		return nil, err
	}
	store, failure := m.jobCancelStore()
	if failure != nil {
		return nil, failure
	}
	v := actionbase.Params(params)
	jobID, key, revision := v.String("job_id"), v.String("idempotency_key"), v.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(jobID) || revision <= 0 || ContainsSensitiveGoalMaterial(key) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudJobCancelInvalidCode, "cloud job cancellation is invalid")
	}
	if _, err := uuid.Parse(key); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	owner := m.ownerMXID()
	if owner == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UTC().Truncate(time.Second)
	prepared, err := store.PrepareCloudJobCancel(ctx, PrepareJobCancelRequest{
		OwnerMXID: owner, JobID: jobID, ExpectedRevision: revision, IdempotencyHash: digest(key),
		ApprovalID: m.newID("job_cancel_approval"), ChallengeID: m.newID("job_cancel_challenge"),
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	})
	if err != nil {
		return nil, jobCancelError(err)
	}
	return map[string]any{"confirmation": prepared.Confirmation}, nil
}

func (m *Module) approveJobCancel(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "job_id", "expected_revision", "idempotency_key", "approval"); err != nil {
		return nil, err
	}
	store, failure := m.jobCancelStore()
	if failure != nil {
		return nil, failure
	}
	v := actionbase.Params(params)
	jobID, key, revision := v.String("job_id"), v.String("idempotency_key"), v.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(jobID) || revision <= 0 || ContainsSensitiveGoalMaterial(key) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudJobCancelInvalidCode, "cloud job cancellation is invalid")
	}
	if _, err := uuid.Parse(key); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	approval, err := decodeJobCancelApprovalV1(params["approval"])
	if err != nil || approval.Signature == "" {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudJobCancelInvalidCode, "cloud job cancellation approval is invalid")
	}
	owner := m.ownerMXID()
	if owner == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UTC().Truncate(time.Second)
	jobEventID, deploymentEventID := m.newID("event"), m.newID("event")
	approved, err := store.ApproveCloudJobCancel(ctx, ApproveJobCancelRequest{
		OwnerMXID: owner, JobID: jobID, ExpectedRevision: revision, IdempotencyHash: digest(key), Approval: approval,
		JobEventID: jobEventID, DeploymentEventID: deploymentEventID, CreatedAt: now.UnixMilli(),
	})
	if err != nil {
		return nil, jobCancelError(err)
	}
	if approved.Created {
		m.publish(ctx, "cloud.job.changed", jobEventID, jobPayload(approved.Job))
		m.publish(ctx, "cloud.deployment.changed", deploymentEventID, deploymentPayload(approved.Deployment))
	}
	return map[string]any{"job": approved.Job, "deployment": approved.Deployment}, nil
}

func (m *Module) jobCancelStore() (JobCancelStore, *actionbase.Error) {
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	store, ok := m.store.(JobCancelStore)
	if !ok {
		return nil, unavailableError()
	}
	return store, nil
}

func jobCancelError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrIdempotencyConflict):
		return actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud job cancellation request")
	case errors.Is(err, ErrJobCancelNotCancellable):
		return actionbase.CodedError(http.StatusConflict, cloudJobCancelNotCancellableCode, "cloud job is not cancellable in its current state")
	case errors.Is(err, ErrJobCancelConflict):
		return actionbase.CodedError(http.StatusConflict, cloudJobCancelConflictCode, "cloud job cancellation conflicts with current state")
	case errors.Is(err, ErrJobCancelExpired):
		return actionbase.CodedError(http.StatusConflict, cloudJobCancelExpiredCode, "cloud job cancellation approval has expired")
	case errors.Is(err, ErrJobCancelSignature):
		return actionbase.CodedError(http.StatusUnauthorized, cloudJobCancelSignatureCode, "cloud job cancellation approval signature is invalid")
	case errors.Is(err, ErrJobCancelInvalid):
		return actionbase.CodedError(http.StatusBadRequest, cloudJobCancelInvalidCode, "cloud job cancellation is invalid")
	default:
		return actionbase.InternalError(err)
	}
}

func decodeJobCancelApprovalV1(value any) (cloudcontracts.JobCancelApprovalV1, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return cloudcontracts.JobCancelApprovalV1{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var approval cloudcontracts.JobCancelApprovalV1
	if err = decoder.Decode(&approval); err != nil {
		return cloudcontracts.JobCancelApprovalV1{}, err
	}
	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return cloudcontracts.JobCancelApprovalV1{}, fmt.Errorf("job cancellation approval contains trailing JSON")
	}
	if err = approval.Validate(); err != nil {
		return cloudcontracts.JobCancelApprovalV1{}, err
	}
	return approval, nil
}
