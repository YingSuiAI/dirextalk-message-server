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

type storedPairingResume struct {
	ApprovalID, OwnerMXID, DeploymentID, PlanID, ConnectionID, ExecutionID, ManifestDigest, JobID, SignerKeyID                                string
	ApprovalJSON, PrepareDeploymentJSON, PrepareJobJSON, Status, PrepareDigest, ApproveDigest, Signature, ResultDeploymentJSON, ResultJobJSON string
	DeploymentRevision, JobRevision, ExpiresAt                                                                                                int64
	SigningPayload                                                                                                                            []byte
}

func (s *DatabaseStore) PrepareCloudPairingResume(ctx context.Context, r cloudmodule.PreparePairingResumeRequest) (cloudmodule.PreparePairingResumeResult, error) {
	if strings.TrimSpace(r.OwnerMXID) == "" || r.DeploymentID == "" || r.ExpectedRevision <= 0 || r.IdempotencyHash == "" || r.ApprovalID == "" || r.ChallengeID == "" || r.CreatedAt <= 0 || r.ExpiresAt <= r.CreatedAt || r.ExpiresAt-r.CreatedAt > int64((5*time.Minute).Milliseconds()) {
		return cloudmodule.PreparePairingResumeResult{}, cloudmodule.ErrPairingResumeInvalid
	}
	r.RequestDigest = pairingPrepareRequestDigest(r)
	var result cloudmodule.PreparePairingResumeResult
	err := s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadPairingPrepareReplay(ctx, tx, r); err != nil || found {
			result = replay
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_pairing_resume_approvals SET status='expired',updated_at=$1 WHERE deployment_id=$2 AND status='pending' AND expires_at<=$1`, r.CreatedAt, r.DeploymentID); err != nil {
			return err
		}
		owner, err := lockCloudRecipeExecutionOwner(ctx, tx, r.DeploymentID)
		if err != nil || owner != r.OwnerMXID {
			return cloudmodule.ErrPairingResumeInvalid
		}
		deployment, job, manifest, err := lockPairingResumeState(ctx, tx, r.DeploymentID)
		if err != nil {
			return err
		}
		if deployment.Revision != r.ExpectedRevision || !pairingResumeWaiting(deployment, job) || manifest.Execution.Status != recipeExecutionStatusApproved {
			return cloudmodule.ErrPairingResumeConflict
		}
		var pending int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_pairing_resume_approvals WHERE deployment_id=$1 AND status='pending'`, r.DeploymentID).Scan(&pending); err != nil {
			return err
		}
		if pending != 0 {
			return cloudmodule.ErrPairingResumeConflict
		}
		keyID, _, err := lockCloudDeviceApprovalKey(ctx, tx, r.OwnerMXID, deployment.ConnectionID)
		if err != nil {
			return cloudmodule.ErrPairingResumeInvalid
		}
		target := pairingResumeTarget(deployment, job, manifest)
		approval, err := cloudcontracts.NewPairingResumeApprovalV1(target, r.ApprovalID, r.ChallengeID, keyID, time.UnixMilli(r.CreatedAt), time.UnixMilli(r.ExpiresAt))
		if err != nil {
			return cloudmodule.ErrPairingResumeInvalid
		}
		payload, _ := approval.SigningPayload()
		approvalJSON, _ := json.Marshal(approval)
		deploymentJSON, _ := json.Marshal(deployment)
		jobJSON, _ := json.Marshal(job)
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_pairing_resume_approvals(approval_id,challenge_id,owner_mxid,deployment_id,deployment_revision,plan_id,cloud_connection_id,execution_id,manifest_digest,job_id,job_revision,signer_key_id,approval_json,signing_payload_cbor,prepare_deployment_json,prepare_job_json,status,prepare_idempotency_hash,prepare_request_digest,expires_at,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'pending',$17,$18,$19,$20,$20)`, approval.ApprovalID, approval.ChallengeID, r.OwnerMXID, deployment.DeploymentID, deployment.Revision, deployment.PlanID, deployment.ConnectionID, manifest.Execution.ExecutionID, manifest.Execution.RecipeExecutionManifestDigest, job.JobID, job.Revision, keyID, string(approvalJSON), payload, string(deploymentJSON), string(jobJSON), r.IdempotencyHash, r.RequestDigest, r.ExpiresAt, r.CreatedAt); err != nil {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrIdempotencyConflict
			}
			return err
		}
		result = cloudmodule.PreparePairingResumeResult{Confirmation: cloudmodule.PairingResumeConfirmation{Deployment: deployment, Job: job, Approval: approval}, Created: true}
		return nil
	})
	return result, err
}

func (s *DatabaseStore) ApproveCloudPairingResume(ctx context.Context, r cloudmodule.ApprovePairingResumeRequest) (cloudmodule.ApprovePairingResumeResult, error) {
	if strings.TrimSpace(r.OwnerMXID) == "" || r.DeploymentID == "" || r.ExpectedRevision <= 0 || r.IdempotencyHash == "" || r.Approval.Signature == "" || r.OutboxID == "" || r.DeploymentEventID == "" || r.JobEventID == "" || r.CreatedAt <= 0 {
		return cloudmodule.ApprovePairingResumeResult{}, cloudmodule.ErrPairingResumeInvalid
	}
	requestDigest, err := pairingApproveRequestDigest(r)
	if err != nil {
		return cloudmodule.ApprovePairingResumeResult{}, cloudmodule.ErrPairingResumeInvalid
	}
	r.RequestDigest = requestDigest
	var result cloudmodule.ApprovePairingResumeResult
	var terminal error
	err = s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadPairingApproveReplay(ctx, tx, r); err != nil || found {
			result = replay
			return err
		}
		stored, err := lockStoredPairingResume(ctx, tx, r.Approval.ApprovalID)
		if err != nil {
			return err
		}
		if stored.OwnerMXID != r.OwnerMXID || stored.DeploymentID != r.DeploymentID || stored.DeploymentRevision != r.ExpectedRevision || stored.Status != "pending" {
			return cloudmodule.ErrPairingResumeConflict
		}
		if stored.ExpiresAt <= r.CreatedAt {
			_, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_pairing_resume_approvals SET status='expired',approve_idempotency_hash=$1,approve_request_digest=$2,updated_at=$3 WHERE approval_id=$4 AND status='pending'`, r.IdempotencyHash, r.RequestDigest, r.CreatedAt, stored.ApprovalID)
			terminal = cloudmodule.ErrPairingResumeExpired
			return err
		}
		deployment, job, manifest, err := lockPairingResumeState(ctx, tx, r.DeploymentID)
		if err != nil {
			return err
		}
		if deployment.Revision != stored.DeploymentRevision || job.Revision != stored.JobRevision || manifest.Execution.ExecutionID != stored.ExecutionID || manifest.Execution.RecipeExecutionManifestDigest != stored.ManifestDigest || !pairingResumeWaiting(deployment, job) {
			return cloudmodule.ErrPairingResumeConflict
		}
		var prepared cloudcontracts.PairingResumeApprovalV1
		if decodeCloudContractJSON(stored.ApprovalJSON, &prepared) != nil {
			return cloudmodule.ErrPairingResumeInvalid
		}
		incomingPayload, err := r.Approval.SigningPayload()
		preparedPayload, preparedErr := prepared.SigningPayload()
		if err != nil || preparedErr != nil || !bytes.Equal(incomingPayload, stored.SigningPayload) || !bytes.Equal(preparedPayload, stored.SigningPayload) || r.Approval.PairingResumeTargetV1 != pairingResumeTarget(deployment, job, manifest) {
			return cloudmodule.ErrPairingResumeInvalid
		}
		keyID, publicSPKI, err := lockCloudDeviceApprovalKey(ctx, tx, r.OwnerMXID, deployment.ConnectionID)
		if err != nil || keyID != stored.SignerKeyID {
			return cloudmodule.ErrPairingResumeInvalid
		}
		publicKey, err := parseCloudApprovalPublicKey(publicSPKI)
		if err != nil || r.Approval.Verify(publicKey, time.UnixMilli(r.CreatedAt)) != nil {
			return cloudmodule.ErrPairingResumeSignature
		}
		deployment.Execution, deployment.Revision, deployment.UpdatedAt = "queued", deployment.Revision+1, r.CreatedAt
		job.Execution, job.Checkpoint, job.ErrorCode, job.Revision, job.UpdatedAt = "queued", "pairing_resume_queued", "", job.Revision+1, r.CreatedAt
		deploymentUpdate, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_deployments SET execution_status='queued',revision=$1,updated_at=$2 WHERE deployment_id=$3 AND revision=$4 AND execution_status IN('waiting_user','waiting_user_pairing') AND outcome_status='pending'`, deployment.Revision, r.CreatedAt, deployment.DeploymentID, deployment.Revision-1)
		if err != nil || !exactlyOneRow(deploymentUpdate) {
			return cloudmodule.ErrPairingResumeConflict
		}
		jobUpdate, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_jobs SET execution_status='queued',checkpoint='pairing_resume_queued',error_code='',revision=$1,updated_at=$2 WHERE job_id=$3 AND revision=$4 AND execution_status='waiting_user' AND outcome_status='pending'`, job.Revision, r.CreatedAt, job.JobID, job.Revision-1)
		if err != nil || !exactlyOneRow(jobUpdate) {
			return cloudmodule.ErrPairingResumeConflict
		}
		payload, _ := json.Marshal(map[string]string{"deployment_id": deployment.DeploymentID, "execution_id": stored.ExecutionID, "job_id": job.JobID})
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,created_at)VALUES($1,$2,'deployment',$3,$4,$5)`, r.OutboxID, cloudmodule.OutboxKindDeploymentPairingResumeRequested, deployment.DeploymentID, string(payload), r.CreatedAt); err != nil {
			return err
		}
		deploymentJSON, _ := json.Marshal(deployment)
		jobJSON, _ := json.Marshal(job)
		approvalUpdate, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_pairing_resume_approvals SET status='approved',approve_idempotency_hash=$1,approve_request_digest=$2,signature=$3,result_deployment_json=$4,result_job_json=$5,updated_at=$6 WHERE approval_id=$7 AND status='pending'`, r.IdempotencyHash, r.RequestDigest, r.Approval.Signature, string(deploymentJSON), string(jobJSON), r.CreatedAt, stored.ApprovalID)
		if err != nil || !exactlyOneRow(approvalUpdate) {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrIdempotencyConflict
			}
			return cloudmodule.ErrPairingResumeConflict
		}
		if err = writeCloudConfirmationEvent(ctx, tx, r.DeploymentEventID, "cloud.deployment.changed", "deployment", deployment.DeploymentID, deployment.Revision, deployment, r.CreatedAt); err != nil {
			return err
		}
		if err = writeCloudConfirmationEvent(ctx, tx, r.JobEventID, "cloud.job.changed", "job", job.JobID, job.Revision, job, r.CreatedAt); err != nil {
			return err
		}
		result = cloudmodule.ApprovePairingResumeResult{Deployment: deployment, Job: job, Created: true}
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, terminal
}

func pairingPrepareRequestDigest(r cloudmodule.PreparePairingResumeRequest) string {
	sum := sha256.Sum256([]byte(r.OwnerMXID + "\x00" + r.DeploymentID + "\x00" + strconv.FormatInt(r.ExpectedRevision, 10)))
	return hex.EncodeToString(sum[:])
}

func pairingApproveRequestDigest(r cloudmodule.ApprovePairingResumeRequest) (string, error) {
	payload, err := r.Approval.SigningPayload()
	if err != nil {
		return "", err
	}
	sum := sha256.New()
	_, _ = sum.Write([]byte(r.OwnerMXID + "\x00" + r.DeploymentID + "\x00" + strconv.FormatInt(r.ExpectedRevision, 10) + "\x00"))
	_, _ = sum.Write(payload)
	_, _ = sum.Write([]byte("\x00" + r.Approval.Signature))
	return hex.EncodeToString(sum.Sum(nil)), nil
}

func lockPairingResumeState(ctx context.Context, tx *sql.Tx, deploymentID string) (cloudmodule.Deployment, cloudmodule.Job, storedRecipeExecutionManifest, error) {
	deployment, err := lockCloudDeploymentForRecipeExecution(ctx, tx, deploymentID)
	if err != nil {
		return cloudmodule.Deployment{}, cloudmodule.Job{}, storedRecipeExecutionManifest{}, cloudmodule.ErrPairingResumeInvalid
	}
	var job cloudmodule.Job
	err = scanCloudJob(tx.QueryRowContext(ctx, `SELECT `+cloudJobColumns+` FROM p2p_cloud_jobs WHERE deployment_id=$1 AND kind='install' ORDER BY created_at DESC LIMIT 1 FOR UPDATE`, deploymentID), &job)
	if err != nil {
		return cloudmodule.Deployment{}, cloudmodule.Job{}, storedRecipeExecutionManifest{}, cloudmodule.ErrPairingResumeInvalid
	}
	manifest, err := lockRecipeExecutionManifestByDeploymentID(ctx, tx, deploymentID)
	if err != nil {
		return cloudmodule.Deployment{}, cloudmodule.Job{}, storedRecipeExecutionManifest{}, cloudmodule.ErrPairingResumeInvalid
	}
	return deployment, job, manifest, nil
}

func pairingResumeWaiting(deployment cloudmodule.Deployment, job cloudmodule.Job) bool {
	return (deployment.Execution == "waiting_user" || deployment.Execution == "waiting_user_pairing") && deployment.Outcome == "pending" && (deployment.Resource == "active" || deployment.Resource == "retained_tracked") && deployment.Revision > 0 && job.DeploymentID == deployment.DeploymentID && job.Kind == "install" && job.Execution == "waiting_user" && job.Outcome == "pending" && job.Revision > 0
}

func pairingResumeTarget(deployment cloudmodule.Deployment, job cloudmodule.Job, manifest storedRecipeExecutionManifest) cloudcontracts.PairingResumeTargetV1 {
	return cloudcontracts.PairingResumeTargetV1{DeploymentID: deployment.DeploymentID, DeploymentRevision: uint64(deployment.Revision), PlanID: deployment.PlanID, CloudConnectionID: deployment.ConnectionID, ExecutionID: manifest.Execution.ExecutionID, RecipeExecutionManifestDigest: manifest.Execution.RecipeExecutionManifestDigest, JobID: job.JobID, JobRevision: uint64(job.Revision)}
}

func lockStoredPairingResume(ctx context.Context, tx *sql.Tx, approvalID string) (storedPairingResume, error) {
	var v storedPairingResume
	err := tx.QueryRowContext(ctx, `SELECT approval_id,owner_mxid,deployment_id,deployment_revision,plan_id,cloud_connection_id,execution_id,manifest_digest,job_id,job_revision,signer_key_id,approval_json,signing_payload_cbor,prepare_deployment_json,prepare_job_json,status,prepare_request_digest,COALESCE(approve_request_digest,''),signature,result_deployment_json,result_job_json,expires_at FROM p2p_cloud_pairing_resume_approvals WHERE approval_id=$1 FOR UPDATE`, approvalID).Scan(&v.ApprovalID, &v.OwnerMXID, &v.DeploymentID, &v.DeploymentRevision, &v.PlanID, &v.ConnectionID, &v.ExecutionID, &v.ManifestDigest, &v.JobID, &v.JobRevision, &v.SignerKeyID, &v.ApprovalJSON, &v.SigningPayload, &v.PrepareDeploymentJSON, &v.PrepareJobJSON, &v.Status, &v.PrepareDigest, &v.ApproveDigest, &v.Signature, &v.ResultDeploymentJSON, &v.ResultJobJSON, &v.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return v, cloudmodule.ErrPairingResumeInvalid
	}
	return v, err
}

func loadPairingPrepareReplay(ctx context.Context, tx *sql.Tx, r cloudmodule.PreparePairingResumeRequest) (cloudmodule.PreparePairingResumeResult, bool, error) {
	var approvalID string
	err := tx.QueryRowContext(ctx, `SELECT approval_id FROM p2p_cloud_pairing_resume_approvals WHERE owner_mxid=$1 AND prepare_idempotency_hash=$2 FOR UPDATE`, r.OwnerMXID, r.IdempotencyHash).Scan(&approvalID)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.PreparePairingResumeResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.PreparePairingResumeResult{}, false, err
	}
	stored, err := lockStoredPairingResume(ctx, tx, approvalID)
	if err != nil || stored.PrepareDigest != r.RequestDigest {
		return cloudmodule.PreparePairingResumeResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	var approval cloudcontracts.PairingResumeApprovalV1
	if decodeCloudContractJSON(stored.ApprovalJSON, &approval) != nil {
		return cloudmodule.PreparePairingResumeResult{}, true, cloudmodule.ErrPairingResumeInvalid
	}
	var deployment cloudmodule.Deployment
	var job cloudmodule.Job
	if json.Unmarshal([]byte(stored.PrepareDeploymentJSON), &deployment) != nil || json.Unmarshal([]byte(stored.PrepareJobJSON), &job) != nil {
		return cloudmodule.PreparePairingResumeResult{}, true, cloudmodule.ErrPairingResumeInvalid
	}
	return cloudmodule.PreparePairingResumeResult{Confirmation: cloudmodule.PairingResumeConfirmation{Deployment: deployment, Job: job, Approval: approval}}, true, nil
}

func loadPairingApproveReplay(ctx context.Context, tx *sql.Tx, r cloudmodule.ApprovePairingResumeRequest) (cloudmodule.ApprovePairingResumeResult, bool, error) {
	var approvalID, requestDigest, deploymentJSON, jobJSON, status string
	err := tx.QueryRowContext(ctx, `SELECT approval_id,approve_request_digest,result_deployment_json,result_job_json,status FROM p2p_cloud_pairing_resume_approvals WHERE owner_mxid=$1 AND approve_idempotency_hash=$2 FOR UPDATE`, r.OwnerMXID, r.IdempotencyHash).Scan(&approvalID, &requestDigest, &deploymentJSON, &jobJSON, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.ApprovePairingResumeResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.ApprovePairingResumeResult{}, false, err
	}
	if requestDigest != r.RequestDigest {
		return cloudmodule.ApprovePairingResumeResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	if status == "expired" {
		return cloudmodule.ApprovePairingResumeResult{}, true, cloudmodule.ErrPairingResumeExpired
	}
	if status != "approved" || deploymentJSON == "" || jobJSON == "" {
		return cloudmodule.ApprovePairingResumeResult{}, true, cloudmodule.ErrPairingResumeInvalid
	}
	var deployment cloudmodule.Deployment
	var job cloudmodule.Job
	if json.Unmarshal([]byte(deploymentJSON), &deployment) != nil || json.Unmarshal([]byte(jobJSON), &job) != nil {
		return cloudmodule.ApprovePairingResumeResult{}, true, cloudmodule.ErrPairingResumeInvalid
	}
	return cloudmodule.ApprovePairingResumeResult{Deployment: deployment, Job: job}, true, nil
}
