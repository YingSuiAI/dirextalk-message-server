package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

type cloudJobCancelState struct {
	OwnerMXID  string
	Job        cloudmodule.Job
	Deployment cloudmodule.Deployment
}

type storedCloudJobCancel struct {
	ApprovalID, OwnerMXID, JobID, JobKind, PlanID, DeploymentID, ConnectionID, ResourceStatus, SignerKeyID string
	ApprovalJSON, PrepareJobJSON, PrepareDeploymentJSON, Status, PrepareDigest, ApproveDigest              string
	Signature, ResultJobJSON, ResultDeploymentJSON                                                         string
	JobRevision, DeploymentRevision, ExpiresAt                                                             int64
	SigningPayload                                                                                         []byte
}

func (s *DatabaseStore) PrepareCloudJobCancel(ctx context.Context, r cloudmodule.PrepareJobCancelRequest) (cloudmodule.PrepareJobCancelResult, error) {
	if strings.TrimSpace(r.OwnerMXID) == "" || r.JobID == "" || r.ExpectedRevision <= 0 || r.IdempotencyHash == "" || r.ApprovalID == "" || r.ChallengeID == "" || r.CreatedAt <= 0 || r.ExpiresAt <= r.CreatedAt || r.ExpiresAt-r.CreatedAt > int64((5*time.Minute).Milliseconds()) {
		return cloudmodule.PrepareJobCancelResult{}, cloudmodule.ErrJobCancelInvalid
	}
	r.RequestDigest = jobCancelPrepareDigest(r)
	var result cloudmodule.PrepareJobCancelResult
	err := s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadJobCancelPrepareReplay(ctx, tx, r); err != nil || found {
			result = replay
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_job_cancel_approvals SET status='expired',updated_at=$1 WHERE job_id=$2 AND status='pending' AND expires_at<=$1`, r.CreatedAt, r.JobID); err != nil {
			return err
		}
		state, err := lockCloudJobCancelState(ctx, tx, r.JobID)
		if errors.Is(err, sql.ErrNoRows) || state.OwnerMXID != r.OwnerMXID {
			return cloudmodule.ErrJobCancelInvalid
		}
		if err != nil {
			return err
		}
		if state.Job.Revision != r.ExpectedRevision {
			return cloudmodule.ErrJobCancelConflict
		}
		if err = ensureCloudJobCancellable(ctx, tx, state, r.CreatedAt); err != nil {
			return err
		}
		var pending int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_job_cancel_approvals WHERE job_id=$1 AND status='pending'`, r.JobID).Scan(&pending); err != nil {
			return err
		}
		if pending != 0 {
			return cloudmodule.ErrJobCancelConflict
		}
		keyID, _, err := lockCloudDeviceApprovalKey(ctx, tx, r.OwnerMXID, state.Deployment.ConnectionID)
		if err != nil || keyID == "" {
			return cloudmodule.ErrJobCancelInvalid
		}
		target := cloudJobCancelTarget(state)
		approval, err := cloudcontracts.NewJobCancelApprovalV1(target, r.ApprovalID, r.ChallengeID, keyID, time.UnixMilli(r.CreatedAt), time.UnixMilli(r.ExpiresAt))
		if err != nil {
			return cloudmodule.ErrJobCancelInvalid
		}
		payload, _ := approval.SigningPayload()
		approvalJSON, _ := json.Marshal(approval)
		jobJSON, _ := json.Marshal(state.Job)
		deploymentJSON, _ := json.Marshal(state.Deployment)
		_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_job_cancel_approvals(
			approval_id,challenge_id,owner_mxid,job_id,job_revision,job_kind,plan_id,deployment_id,deployment_revision,
			cloud_connection_id,resource_status,signer_key_id,approval_json,signing_payload_cbor,prepare_job_json,
			prepare_deployment_json,status,prepare_idempotency_hash,prepare_request_digest,expires_at,created_at,updated_at
		)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'pending',$17,$18,$19,$20,$20)`,
			approval.ApprovalID, approval.ChallengeID, r.OwnerMXID, state.Job.JobID, state.Job.Revision, state.Job.Kind,
			state.Job.PlanID, state.Deployment.DeploymentID, state.Deployment.Revision, state.Deployment.ConnectionID,
			state.Deployment.Resource, keyID, string(approvalJSON), payload, string(jobJSON), string(deploymentJSON),
			r.IdempotencyHash, r.RequestDigest, r.ExpiresAt, r.CreatedAt)
		if sqlutil.IsUniqueConstraintViolationErr(err) {
			return cloudmodule.ErrIdempotencyConflict
		}
		if err != nil {
			return err
		}
		result = cloudmodule.PrepareJobCancelResult{Confirmation: cloudmodule.JobCancelConfirmation{Job: state.Job, Deployment: state.Deployment, Approval: approval}, Created: true}
		return nil
	})
	return result, err
}

func (s *DatabaseStore) ApproveCloudJobCancel(ctx context.Context, r cloudmodule.ApproveJobCancelRequest) (cloudmodule.ApproveJobCancelResult, error) {
	if strings.TrimSpace(r.OwnerMXID) == "" || r.JobID == "" || r.ExpectedRevision <= 0 || r.IdempotencyHash == "" || r.Approval.Signature == "" || r.JobEventID == "" || r.DeploymentEventID == "" || r.CreatedAt <= 0 {
		return cloudmodule.ApproveJobCancelResult{}, cloudmodule.ErrJobCancelInvalid
	}
	digest, err := jobCancelApproveDigest(r)
	if err != nil {
		return cloudmodule.ApproveJobCancelResult{}, cloudmodule.ErrJobCancelInvalid
	}
	r.RequestDigest = digest
	var result cloudmodule.ApproveJobCancelResult
	var terminal error
	err = s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadJobCancelApproveReplay(ctx, tx, r); err != nil || found {
			result = replay
			return err
		}
		stored, err := lockStoredCloudJobCancel(ctx, tx, r.Approval.ApprovalID)
		if err != nil {
			return err
		}
		if stored.OwnerMXID != r.OwnerMXID || stored.JobID != r.JobID || stored.JobRevision != r.ExpectedRevision || stored.Status != "pending" {
			return cloudmodule.ErrJobCancelConflict
		}
		if stored.ExpiresAt <= r.CreatedAt {
			_, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_job_cancel_approvals SET status='expired',approve_idempotency_hash=$1,approve_request_digest=$2,updated_at=$3 WHERE approval_id=$4 AND status='pending'`, r.IdempotencyHash, r.RequestDigest, r.CreatedAt, stored.ApprovalID)
			terminal = cloudmodule.ErrJobCancelExpired
			return err
		}
		state, err := lockCloudJobCancelState(ctx, tx, r.JobID)
		if errors.Is(err, sql.ErrNoRows) || state.OwnerMXID != r.OwnerMXID {
			return cloudmodule.ErrJobCancelInvalid
		}
		if err != nil {
			return err
		}
		if state.Job.Revision != stored.JobRevision || state.Deployment.Revision != stored.DeploymentRevision {
			return cloudmodule.ErrJobCancelConflict
		}
		if err = ensureCloudJobCancellable(ctx, tx, state, r.CreatedAt); err != nil {
			return err
		}
		var prepared cloudcontracts.JobCancelApprovalV1
		if decodeCloudContractJSON(stored.ApprovalJSON, &prepared) != nil {
			return cloudmodule.ErrJobCancelInvalid
		}
		incomingPayload, incomingErr := r.Approval.SigningPayload()
		preparedPayload, preparedErr := prepared.SigningPayload()
		target := cloudJobCancelTarget(state)
		if incomingErr != nil || preparedErr != nil || !bytes.Equal(incomingPayload, stored.SigningPayload) || !bytes.Equal(preparedPayload, stored.SigningPayload) || r.Approval.JobCancelTargetV1 != target || prepared.JobCancelTargetV1 != target {
			return cloudmodule.ErrJobCancelInvalid
		}
		keyID, publicSPKI, err := lockCloudDeviceApprovalKey(ctx, tx, r.OwnerMXID, state.Deployment.ConnectionID)
		if err != nil || keyID != stored.SignerKeyID || keyID != r.Approval.SignerKeyID {
			return cloudmodule.ErrJobCancelInvalid
		}
		publicKey, err := parseCloudApprovalPublicKey(publicSPKI)
		if err != nil || r.Approval.Verify(publicKey, time.UnixMilli(r.CreatedAt)) != nil || r.Approval.ValidateAgainst(target, time.UnixMilli(r.CreatedAt)) != nil {
			return cloudmodule.ErrJobCancelSignature
		}

		job := state.Job
		job.Execution, job.Outcome, job.Checkpoint, job.ErrorCode = "finished", "canceled", "job_canceled", ""
		job.Revision++
		job.UpdatedAt = r.CreatedAt
		jobUpdate, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_jobs SET execution_status='finished',outcome_status='canceled',checkpoint='job_canceled',error_code='',revision=$1,updated_at=$2 WHERE job_id=$3 AND revision=$4 AND outcome_status='pending'`, job.Revision, r.CreatedAt, job.JobID, state.Job.Revision)
		if err != nil || !exactlyOneRow(jobUpdate) {
			return cloudmodule.ErrJobCancelConflict
		}
		if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_job_steps SET status='canceled',summary=$1,checkpoint='job_canceled',error_code='',revision=revision+1,updated_at=$2 WHERE job_id=$3 AND status IN('queued','running','waiting_user')`, "Canceled by a device-approved owner request; cloud resources remain retained and billable until separately destroyed.", r.CreatedAt, job.JobID); err != nil {
			return err
		}

		deployment := state.Deployment
		if deployment.Resource == "active" {
			deployment.Resource = "retained_tracked"
			resourceUpdate, updateErr := tx.ExecContext(ctx, `UPDATE p2p_cloud_deployment_resources SET resource_status='retained_tracked',updated_at=$1 WHERE deployment_id=$2 AND resource_status='active'`, r.CreatedAt, deployment.DeploymentID)
			if updateErr != nil || !exactlyOneRow(resourceUpdate) {
				return cloudmodule.ErrJobCancelConflict
			}
		}
		deployment.Execution, deployment.Outcome = "finished", "canceled"
		deployment.Revision++
		deployment.UpdatedAt = r.CreatedAt
		deploymentUpdate, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_deployments SET execution_status='finished',outcome_status='canceled',resource_status=$1,revision=$2,updated_at=$3 WHERE deployment_id=$4 AND revision=$5 AND outcome_status='pending'`, deployment.Resource, deployment.Revision, r.CreatedAt, deployment.DeploymentID, state.Deployment.Revision)
		if err != nil || !exactlyOneRow(deploymentUpdate) {
			return cloudmodule.ErrJobCancelConflict
		}
		if err = isolateCloudJobCancelWork(ctx, tx, state, r.CreatedAt); err != nil {
			return err
		}

		jobJSON, _ := json.Marshal(job)
		deploymentJSON, _ := json.Marshal(deployment)
		approvalUpdate, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_job_cancel_approvals SET status='approved',approve_idempotency_hash=$1,approve_request_digest=$2,signature=$3,result_job_json=$4,result_deployment_json=$5,updated_at=$6 WHERE approval_id=$7 AND status='pending'`, r.IdempotencyHash, r.RequestDigest, r.Approval.Signature, string(jobJSON), string(deploymentJSON), r.CreatedAt, stored.ApprovalID)
		if sqlutil.IsUniqueConstraintViolationErr(err) {
			return cloudmodule.ErrIdempotencyConflict
		}
		if err != nil || !exactlyOneRow(approvalUpdate) {
			return cloudmodule.ErrJobCancelConflict
		}
		if err = writeCloudConfirmationEvent(ctx, tx, r.JobEventID, "cloud.job.changed", "job", job.JobID, job.Revision, job, r.CreatedAt); err != nil {
			return err
		}
		if err = writeCloudConfirmationEvent(ctx, tx, r.DeploymentEventID, "cloud.deployment.changed", "deployment", deployment.DeploymentID, deployment.Revision, deployment, r.CreatedAt); err != nil {
			return err
		}
		result = cloudmodule.ApproveJobCancelResult{Job: job, Deployment: deployment, Created: true}
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, terminal
}

func lockCloudJobCancelState(ctx context.Context, tx *sql.Tx, jobID string) (cloudJobCancelState, error) {
	var state cloudJobCancelState
	err := tx.QueryRowContext(ctx, `SELECT goal.owner_mxid,
		job.job_id,job.plan_id,job.deployment_id,job.kind,job.execution_status,job.outcome_status,job.checkpoint,job.error_code,job.revision,job.created_at,job.updated_at,
		deployment.deployment_id,deployment.plan_id,deployment.cloud_connection_id,deployment.execution_status,deployment.outcome_status,deployment.resource_status,deployment.revision,deployment.created_at,deployment.updated_at
		FROM p2p_cloud_jobs job
		JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=job.deployment_id
		JOIN p2p_cloud_plans plan ON plan.plan_id=job.plan_id AND plan.plan_id=deployment.plan_id
		JOIN p2p_cloud_goals goal ON goal.goal_id=plan.goal_id
		WHERE job.job_id=$1 FOR UPDATE OF job,deployment`, jobID).Scan(
		&state.OwnerMXID,
		&state.Job.JobID, &state.Job.PlanID, &state.Job.DeploymentID, &state.Job.Kind, &state.Job.Execution, &state.Job.Outcome, &state.Job.Checkpoint, &state.Job.ErrorCode, &state.Job.Revision, &state.Job.CreatedAt, &state.Job.UpdatedAt,
		&state.Deployment.DeploymentID, &state.Deployment.PlanID, &state.Deployment.ConnectionID, &state.Deployment.Execution, &state.Deployment.Outcome, &state.Deployment.Resource, &state.Deployment.Revision, &state.Deployment.CreatedAt, &state.Deployment.UpdatedAt)
	return state, err
}

func ensureCloudJobCancellable(ctx context.Context, tx *sql.Tx, state cloudJobCancelState, now int64) error {
	job, deployment := state.Job, state.Deployment
	if job.Revision <= 0 || deployment.Revision <= 0 || job.PlanID != deployment.PlanID || job.DeploymentID != deployment.DeploymentID || job.Outcome != "pending" || deployment.Outcome != "pending" || !jobCancelExecutionAllowed(job.Execution) || !jobCancelExecutionAllowed(deployment.Execution) {
		return cloudmodule.ErrJobCancelNotCancellable
	}
	switch job.Kind {
	case "provision":
		if job.Execution != "queued" || deployment.Execution != "queued" || deployment.Resource != "none" {
			return cloudmodule.ErrJobCancelNotCancellable
		}
		var commands int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_deployment_commands WHERE deployment_id=$1 AND state IN('allocated','signed','indeterminate','accepted')`, deployment.DeploymentID).Scan(&commands); err != nil {
			return err
		}
		var leaseToken string
		var attempts, completedAt int64
		err := tx.QueryRowContext(ctx, `SELECT lease_token,attempts,completed_at FROM p2p_cloud_outbox WHERE kind=$1 AND aggregate_type='deployment' AND aggregate_id=$2 ORDER BY created_at DESC LIMIT 1 FOR UPDATE`, cloudmodule.OutboxKindDeploymentProvisionRequested, deployment.DeploymentID).Scan(&leaseToken, &attempts, &completedAt)
		if errors.Is(err, sql.ErrNoRows) || commands != 0 || leaseToken != "" || attempts != 0 || completedAt != 0 {
			return cloudmodule.ErrJobCancelNotCancellable
		}
		if err != nil {
			return err
		}
		var resources int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_deployment_resources WHERE deployment_id=$1`, deployment.DeploymentID).Scan(&resources); err != nil {
			return err
		}
		if resources != 0 {
			return cloudmodule.ErrJobCancelNotCancellable
		}
	case "install", "verify":
		if deployment.Resource != "active" && deployment.Resource != "retained_tracked" {
			return cloudmodule.ErrJobCancelNotCancellable
		}
		var resourceStatus string
		err := tx.QueryRowContext(ctx, `SELECT resource_status FROM p2p_cloud_deployment_resources WHERE deployment_id=$1 FOR UPDATE`, deployment.DeploymentID).Scan(&resourceStatus)
		if errors.Is(err, sql.ErrNoRows) || resourceStatus != deployment.Resource {
			return cloudmodule.ErrJobCancelNotCancellable
		}
		if err != nil {
			return err
		}
	default:
		return cloudmodule.ErrJobCancelNotCancellable
	}
	_ = now
	return nil
}

func jobCancelExecutionAllowed(value string) bool {
	switch value {
	case "queued", "provisioning", "installing", "waiting_user", "verifying":
		return true
	default:
		return false
	}
}

func cloudJobCancelTarget(state cloudJobCancelState) cloudcontracts.JobCancelTargetV1 {
	return cloudcontracts.JobCancelTargetV1{
		JobID: state.Job.JobID, JobRevision: uint64(state.Job.Revision), JobKind: state.Job.Kind, PlanID: state.Job.PlanID,
		DeploymentID: state.Deployment.DeploymentID, DeploymentRevision: uint64(state.Deployment.Revision),
		CloudConnectionID: state.Deployment.ConnectionID, ResourceStatus: state.Deployment.Resource,
	}
}

func isolateCloudJobCancelWork(ctx context.Context, tx *sql.Tx, state cloudJobCancelState, now int64) error {
	switch state.Job.Kind {
	case "provision":
		return completeCloudJobCancelOutbox(ctx, tx, now, `kind=$2 AND aggregate_type='deployment' AND aggregate_id=$3`, cloudmodule.OutboxKindDeploymentProvisionRequested, state.Deployment.DeploymentID)
	case "install":
		if _, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_tasks SET task_status='interrupted',error_code='job_canceled',available_at=0,lease_owner='',lease_token='',lease_until=0,last_error_code='job_canceled',updated_at=$1 WHERE deployment_id=$2 AND task_status IN('unissued','queued','running')`, now, state.Deployment.DeploymentID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_commands SET state='expired',last_error_code='job_canceled',updated_at=$1 WHERE deployment_id=$2 AND state IN('allocated','signed')`, now, state.Deployment.DeploymentID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_tasks SET task_status='interrupted',error_code='job_canceled',available_at=0,lease_owner='',lease_token='',lease_until=0,last_error_code='job_canceled',updated_at=$1 WHERE deployment_id=$2 AND purpose='install' AND (job_id=$3 OR execution_id IN(SELECT execution_id FROM p2p_cloud_recipe_install_tasks WHERE deployment_id=$2)) AND task_status IN('unissued','queued','running')`, now, state.Deployment.DeploymentID, state.Job.JobID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_commands SET state='expired',last_error_code='job_canceled',updated_at=$1 WHERE deployment_id=$2 AND state IN('allocated','signed') AND task_id IN(SELECT task_id FROM p2p_cloud_service_readiness_tasks WHERE deployment_id=$2 AND purpose='install')`, now, state.Deployment.DeploymentID); err != nil {
			return err
		}
		if err := completeCloudJobCancelOutbox(ctx, tx, now, `(kind=$2 AND aggregate_type='recipe_execution' AND aggregate_id IN(SELECT execution_id FROM p2p_cloud_recipe_install_tasks WHERE deployment_id=$3)) OR (kind=$4 AND aggregate_type='service_readiness_task' AND aggregate_id IN(SELECT task_id FROM p2p_cloud_service_readiness_tasks WHERE deployment_id=$3 AND purpose='install')) OR (kind=$5 AND aggregate_type='deployment' AND aggregate_id=$3)`, cloudmodule.OutboxKindRecipeExecutionInstallRequested, state.Deployment.DeploymentID, cloudmodule.OutboxKindServiceReadinessRequested, cloudmodule.OutboxKindDeploymentPairingResumeRequested); err != nil {
			return err
		}
	case "verify":
		if _, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_execution_probe_tasks SET task_status='interrupted',available_at=0,lease_owner='',lease_token='',lease_until=0,last_error_code='job_canceled',updated_at=$1 WHERE deployment_id=$2 AND task_status IN('unissued','queued','running')`, now, state.Deployment.DeploymentID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_execution_probe_commands SET state='expired',last_error_code='job_canceled',updated_at=$1 WHERE deployment_id=$2 AND state IN('allocated','signed')`, now, state.Deployment.DeploymentID); err != nil {
			return err
		}
		if err := completeCloudJobCancelOutbox(ctx, tx, now, `kind=$2 AND aggregate_type='execution_probe_task' AND aggregate_id IN(SELECT task_id FROM p2p_cloud_execution_probe_tasks WHERE deployment_id=$3)`, cloudmodule.OutboxKindExecutionProbeIssueRequested, state.Deployment.DeploymentID); err != nil {
			return err
		}
	}
	return nil
}

func completeCloudJobCancelOutbox(ctx context.Context, tx *sql.Tx, now int64, predicate string, args ...any) error {
	queryArgs := make([]any, 0, len(args)+1)
	queryArgs = append(queryArgs, now)
	queryArgs = append(queryArgs, args...)
	_, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner='',lease_token='',lease_until=0,completed_at=$1,delivered_at=$1,available_at=$1,last_error_code='job_canceled' WHERE completed_at=0 AND (`+predicate+`)`, queryArgs...)
	return err
}

func jobCancelPrepareDigest(r cloudmodule.PrepareJobCancelRequest) string {
	sum := sha256.Sum256([]byte(r.OwnerMXID + "\x00" + r.JobID + "\x00" + strconv.FormatInt(r.ExpectedRevision, 10)))
	return hex.EncodeToString(sum[:])
}

func jobCancelApproveDigest(r cloudmodule.ApproveJobCancelRequest) (string, error) {
	payload, err := r.Approval.SigningPayload()
	if err != nil {
		return "", err
	}
	sum := sha256.New()
	_, _ = sum.Write([]byte(r.OwnerMXID + "\x00" + r.JobID + "\x00" + strconv.FormatInt(r.ExpectedRevision, 10) + "\x00"))
	_, _ = sum.Write(payload)
	_, _ = sum.Write([]byte("\x00" + r.Approval.Signature))
	return hex.EncodeToString(sum.Sum(nil)), nil
}

func lockStoredCloudJobCancel(ctx context.Context, tx *sql.Tx, approvalID string) (storedCloudJobCancel, error) {
	var v storedCloudJobCancel
	err := tx.QueryRowContext(ctx, `SELECT approval_id,owner_mxid,job_id,job_revision,job_kind,plan_id,deployment_id,deployment_revision,cloud_connection_id,resource_status,signer_key_id,approval_json,signing_payload_cbor,prepare_job_json,prepare_deployment_json,status,prepare_request_digest,COALESCE(approve_request_digest,''),signature,result_job_json,result_deployment_json,expires_at FROM p2p_cloud_job_cancel_approvals WHERE approval_id=$1 FOR UPDATE`, approvalID).Scan(
		&v.ApprovalID, &v.OwnerMXID, &v.JobID, &v.JobRevision, &v.JobKind, &v.PlanID, &v.DeploymentID, &v.DeploymentRevision, &v.ConnectionID, &v.ResourceStatus, &v.SignerKeyID,
		&v.ApprovalJSON, &v.SigningPayload, &v.PrepareJobJSON, &v.PrepareDeploymentJSON, &v.Status, &v.PrepareDigest, &v.ApproveDigest, &v.Signature, &v.ResultJobJSON, &v.ResultDeploymentJSON, &v.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return v, cloudmodule.ErrJobCancelInvalid
	}
	return v, err
}

func loadJobCancelPrepareReplay(ctx context.Context, tx *sql.Tx, r cloudmodule.PrepareJobCancelRequest) (cloudmodule.PrepareJobCancelResult, bool, error) {
	var approvalID string
	err := tx.QueryRowContext(ctx, `SELECT approval_id FROM p2p_cloud_job_cancel_approvals WHERE owner_mxid=$1 AND prepare_idempotency_hash=$2 FOR UPDATE`, r.OwnerMXID, r.IdempotencyHash).Scan(&approvalID)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.PrepareJobCancelResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.PrepareJobCancelResult{}, false, err
	}
	stored, err := lockStoredCloudJobCancel(ctx, tx, approvalID)
	if err != nil || stored.PrepareDigest != r.RequestDigest {
		return cloudmodule.PrepareJobCancelResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	var approval cloudcontracts.JobCancelApprovalV1
	var job cloudmodule.Job
	var deployment cloudmodule.Deployment
	if decodeCloudContractJSON(stored.ApprovalJSON, &approval) != nil || json.Unmarshal([]byte(stored.PrepareJobJSON), &job) != nil || json.Unmarshal([]byte(stored.PrepareDeploymentJSON), &deployment) != nil {
		return cloudmodule.PrepareJobCancelResult{}, true, cloudmodule.ErrJobCancelInvalid
	}
	return cloudmodule.PrepareJobCancelResult{Confirmation: cloudmodule.JobCancelConfirmation{Job: job, Deployment: deployment, Approval: approval}}, true, nil
}

func loadJobCancelApproveReplay(ctx context.Context, tx *sql.Tx, r cloudmodule.ApproveJobCancelRequest) (cloudmodule.ApproveJobCancelResult, bool, error) {
	var requestDigest, jobJSON, deploymentJSON, status string
	err := tx.QueryRowContext(ctx, `SELECT approve_request_digest,result_job_json,result_deployment_json,status FROM p2p_cloud_job_cancel_approvals WHERE owner_mxid=$1 AND approve_idempotency_hash=$2 FOR UPDATE`, r.OwnerMXID, r.IdempotencyHash).Scan(&requestDigest, &jobJSON, &deploymentJSON, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.ApproveJobCancelResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.ApproveJobCancelResult{}, false, err
	}
	if requestDigest != r.RequestDigest {
		return cloudmodule.ApproveJobCancelResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	if status == "expired" {
		return cloudmodule.ApproveJobCancelResult{}, true, cloudmodule.ErrJobCancelExpired
	}
	if status != "approved" {
		return cloudmodule.ApproveJobCancelResult{}, true, cloudmodule.ErrJobCancelConflict
	}
	var job cloudmodule.Job
	var deployment cloudmodule.Deployment
	if json.Unmarshal([]byte(jobJSON), &job) != nil || json.Unmarshal([]byte(deploymentJSON), &deployment) != nil {
		return cloudmodule.ApproveJobCancelResult{}, true, cloudmodule.ErrJobCancelInvalid
	}
	return cloudmodule.ApproveJobCancelResult{Job: job, Deployment: deployment}, true, nil
}
