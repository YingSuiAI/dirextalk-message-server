package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

var _ cloudmodule.ServiceBackupConfirmationStore = (*DatabaseStore)(nil)

type storedServiceBackupApproval struct {
	ApprovalID, OwnerMXID, BackupID, ServiceID, SignerKeyID, ApprovalJSON, ServiceJSON, DeploymentJSON     string
	Status, PrepareRequestDigest, ApproveRequestDigest, ResultServiceJSON, ResultBackupJSON, ResultJobJSON string
	ServiceRevision, ExpiresAt                                                                             int64
	SigningPayload                                                                                         []byte
}

func (s *DatabaseStore) PrepareCloudServiceBackup(ctx context.Context, request cloudmodule.PrepareServiceBackupRequest) (cloudmodule.PrepareServiceBackupResult, error) {
	if validatePrepareServiceBackupRequest(request) != nil {
		return cloudmodule.PrepareServiceBackupResult{}, cloudmodule.ErrServiceBackupConfirmationInvalid
	}
	var result cloudmodule.PrepareServiceBackupResult
	err := s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadServiceBackupPrepareReplay(ctx, tx, request); err != nil || found {
			result = replay
			return err
		}
		state, err := lockServiceDestroyState(ctx, tx, request.ServiceID)
		if err != nil {
			return err
		}
		if state.OwnerMXID != request.OwnerMXID || state.Service.Revision != request.ExpectedRevision {
			return cloudmodule.ErrServiceBackupConfirmationConflict
		}
		if !serviceDestroyStateReady(state) || serviceHasActiveOperation(ctx, tx, request.ServiceID) || serviceHasActiveBackup(ctx, tx, request.ServiceID) || serviceHasActiveRestore(ctx, tx, request.ServiceID) {
			return cloudmodule.ErrServiceBackupConfirmationInvalid
		}
		keyID, _, err := lockCloudDeviceApprovalKey(ctx, tx, request.OwnerMXID, state.Deployment.ConnectionID)
		if err != nil {
			return cloudmodule.ErrServiceBackupConfirmationInvalid
		}
		target := serviceBackupTarget(state, request.BackupID)
		approval, err := cloudcontracts.NewServiceBackupApprovalV1(target, request.ApprovalID, request.ChallengeID, keyID, time.UnixMilli(request.CreatedAt), time.UnixMilli(request.ExpiresAt))
		if err != nil {
			return cloudmodule.ErrServiceBackupConfirmationInvalid
		}
		payload, err := approval.SigningPayload()
		if err != nil {
			return cloudmodule.ErrServiceBackupConfirmationInvalid
		}
		approvalJSON, _ := json.Marshal(approval)
		serviceJSON, _ := json.Marshal(state.Service)
		deploymentJSON, _ := json.Marshal(state.Deployment)
		volumesJSON, _ := json.Marshal(target.VolumeIDs)
		_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_backup_approvals(approval_id,challenge_id,owner_mxid,backup_id,service_id,service_revision,deployment_id,deployment_revision,cloud_connection_id,recipe_id,recipe_digest,instance_id,volume_ids_json,retention_policy,signer_key_id,approval_json,signing_payload,service_json,deployment_json,status,prepare_idempotency_hash,prepare_request_digest,expires_at,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,'pending',$20,$21,$22,$23,$23)`, approval.ApprovalID, approval.ChallengeID, request.OwnerMXID, target.BackupID, target.ServiceID, target.ServiceRevision, target.DeploymentID, target.DeploymentRevision, target.CloudConnectionID, target.RecipeID, target.RecipeDigest, target.InstanceID, string(volumesJSON), target.RetentionPolicy, keyID, string(approvalJSON), payload, string(serviceJSON), string(deploymentJSON), request.IdempotencyHash, request.RequestDigest, request.ExpiresAt, request.CreatedAt)
		if err != nil {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrIdempotencyConflict
			}
			return err
		}
		result = cloudmodule.PrepareServiceBackupResult{Confirmation: cloudmodule.ServiceBackupConfirmation{Service: state.Service, Deployment: state.Deployment, Approval: approval}, Created: true}
		return nil
	})
	return result, err
}

func (s *DatabaseStore) ApproveCloudServiceBackup(ctx context.Context, request cloudmodule.ApproveServiceBackupRequest) (cloudmodule.ApproveServiceBackupResult, error) {
	digest, err := serviceBackupApprovalRequestDigest(request)
	if err != nil || validateApproveServiceBackupRequest(request) != nil {
		return cloudmodule.ApproveServiceBackupResult{}, cloudmodule.ErrServiceBackupConfirmationInvalid
	}
	var result cloudmodule.ApproveServiceBackupResult
	var terminalErr error
	err = s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, err := loadServiceBackupApproveReplay(ctx, tx, request, digest); err != nil || found {
			result = replay
			return err
		}
		stored, err := lockServiceBackupApproval(ctx, tx, request.Approval.ApprovalID)
		if err != nil {
			return err
		}
		if stored.OwnerMXID != request.OwnerMXID || stored.ServiceID != request.ServiceID || stored.ServiceRevision != request.ExpectedRevision || stored.Status != "pending" {
			return cloudmodule.ErrServiceBackupConfirmationConflict
		}
		if stored.ExpiresAt <= request.CreatedAt {
			_, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_backup_approvals SET status='expired',approve_idempotency_hash=$1,approve_request_digest=$2,updated_at=$3 WHERE approval_id=$4 AND status='pending'`, request.IdempotencyHash, digest, request.CreatedAt, stored.ApprovalID)
			terminalErr = cloudmodule.ErrServiceBackupApprovalExpired
			return err
		}
		state, err := lockServiceDestroyState(ctx, tx, request.ServiceID)
		if err != nil {
			return err
		}
		if state.OwnerMXID != request.OwnerMXID || state.Service.Revision != request.ExpectedRevision || !serviceDestroyStateReady(state) || serviceHasActiveOperation(ctx, tx, request.ServiceID) || serviceHasActiveBackup(ctx, tx, request.ServiceID) || serviceHasActiveRestore(ctx, tx, request.ServiceID) {
			return cloudmodule.ErrServiceBackupConfirmationConflict
		}
		storedApproval, err := decodeStoredServiceBackupApproval(stored.ApprovalJSON)
		if err != nil {
			return cloudmodule.ErrServiceBackupConfirmationInvalid
		}
		incomingPayload, err := request.Approval.SigningPayload()
		if err != nil || !bytes.Equal(incomingPayload, stored.SigningPayload) {
			return cloudmodule.ErrServiceBackupConfirmationInvalid
		}
		storedPayload, err := storedApproval.SigningPayload()
		if err != nil || !bytes.Equal(storedPayload, stored.SigningPayload) {
			return cloudmodule.ErrServiceBackupConfirmationInvalid
		}
		keyID, publicSPKI, err := lockCloudDeviceApprovalKey(ctx, tx, request.OwnerMXID, state.Deployment.ConnectionID)
		if err != nil || keyID != stored.SignerKeyID {
			return cloudmodule.ErrServiceBackupConfirmationInvalid
		}
		publicKey, err := parseCloudApprovalPublicKey(publicSPKI)
		if err != nil || request.Approval.Verify(publicKey, time.UnixMilli(request.CreatedAt)) != nil {
			return cloudmodule.ErrServiceBackupApprovalSignature
		}
		if request.Approval.ValidateAgainst(serviceBackupTarget(state, stored.BackupID), time.UnixMilli(request.CreatedAt)) != nil {
			return cloudmodule.ErrServiceBackupConfirmationInvalid
		}
		// Persist the canonical tracked order, not the caller's JSON array order.
		// Approval signing is set-based for volume IDs, while the resource ledger
		// uses exact JSON equality as an additional drift fence.
		volumesJSON, _ := json.Marshal(serviceBackupTarget(state, stored.BackupID).VolumeIDs)
		backup := cloudmodule.ServiceBackup{BackupID: stored.BackupID, ServiceID: state.Service.ServiceID, DeploymentID: state.Deployment.DeploymentID, Status: "queued", RetentionPolicy: cloudcontracts.ServiceBackupRetentionManual, Revision: 1, CreatedAt: request.CreatedAt, UpdatedAt: request.CreatedAt}
		job := cloudmodule.Job{JobID: request.JobID, PlanID: state.Deployment.PlanID, DeploymentID: state.Deployment.DeploymentID, Kind: "backup", Execution: "queued", Outcome: "pending", Checkpoint: "backup_queued", Revision: 1, CreatedAt: request.CreatedAt, UpdatedAt: request.CreatedAt}
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_backups(backup_id,approval_id,service_id,service_revision,deployment_id,deployment_revision,plan_id,cloud_connection_id,instance_id,volume_ids_json,retention_policy,job_id,backup_status,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'manual',$11,'queued',$12,$12)`, stored.BackupID, stored.ApprovalID, state.Service.ServiceID, state.Service.Revision, state.Deployment.DeploymentID, state.Deployment.Revision, state.Deployment.PlanID, state.Deployment.ConnectionID, state.InstanceID, string(volumesJSON), request.JobID, request.CreatedAt); err != nil {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrServiceBackupConfirmationConflict
			}
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at) VALUES($1,$2,$3,'backup','queued','pending','backup_queued','',1,$4,$4)`, job.JobID, job.PlanID, job.DeploymentID, request.CreatedAt); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_job_steps(job_id,step_id,status,summary,checkpoint,error_code,revision,created_at,updated_at) VALUES($1,'backup','queued','Device-approved encrypted EBS backup is queued; the service and EC2 resources remain active and billable.','backup_queued','',1,$2,$2)`, job.JobID, request.CreatedAt); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]string{"backup_id": stored.BackupID})
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,available_at,created_at) VALUES($1,$2,'service_backup',$3,$4,$5,$5)`, request.OutboxID, cloudmodule.OutboxKindServiceBackupRequested, stored.BackupID, string(payload), request.CreatedAt); err != nil {
			return err
		}
		serviceJSON, _ := json.Marshal(state.Service)
		backupJSON, _ := json.Marshal(backup)
		jobJSON, _ := json.Marshal(job)
		r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_backup_approvals SET status='approved',approve_idempotency_hash=$1,approve_request_digest=$2,signature=$3,job_id=$4,result_service_json=$5,result_backup_json=$6,result_job_json=$7,updated_at=$8 WHERE approval_id=$9 AND status='pending'`, request.IdempotencyHash, digest, request.Approval.Signature, request.JobID, string(serviceJSON), string(backupJSON), string(jobJSON), request.CreatedAt, stored.ApprovalID)
		if err != nil {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrIdempotencyConflict
			}
			return err
		}
		if !exactlyOneRow(r) {
			return cloudmodule.ErrServiceBackupConfirmationConflict
		}
		if err = writeCloudConfirmationEvent(ctx, tx, request.JobEventID, "cloud.job.changed", "job", job.JobID, job.Revision, job, request.CreatedAt); err != nil {
			return err
		}
		result = cloudmodule.ApproveServiceBackupResult{Service: state.Service, Backup: backup, Job: job, Created: true}
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, terminalErr
}

func serviceBackupTarget(state serviceDestroyState, backupID string) cloudcontracts.ServiceBackupTargetV1 {
	return cloudcontracts.ServiceBackupTargetV1{BackupID: backupID, ServiceID: state.Service.ServiceID, ServiceRevision: uint64(state.Service.Revision), DeploymentID: state.Deployment.DeploymentID, DeploymentRevision: uint64(state.Deployment.Revision), CloudConnectionID: state.Deployment.ConnectionID, RecipeID: state.Service.RecipeID, RecipeDigest: state.RecipeDigest, InstanceID: state.InstanceID, VolumeIDs: append([]string(nil), state.VolumeIDs...), RetentionPolicy: cloudcontracts.ServiceBackupRetentionManual}
}
func serviceHasActiveBackup(ctx context.Context, tx *sql.Tx, serviceID string) bool {
	var count int
	return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_service_backups WHERE service_id=$1 AND backup_status IN('queued','running')`, serviceID).Scan(&count) != nil || count != 0
}
func validatePrepareServiceBackupRequest(r cloudmodule.PrepareServiceBackupRequest) error {
	if strings.TrimSpace(r.OwnerMXID) == "" || r.ServiceID == "" || r.ExpectedRevision <= 0 || r.IdempotencyHash == "" || r.RequestDigest == "" || r.BackupID == "" || r.ApprovalID == "" || r.ChallengeID == "" || r.CreatedAt <= 0 || r.ExpiresAt <= r.CreatedAt || r.ExpiresAt-r.CreatedAt > int64((5*time.Minute).Milliseconds()) {
		return cloudmodule.ErrServiceBackupConfirmationInvalid
	}
	return nil
}
func validateApproveServiceBackupRequest(r cloudmodule.ApproveServiceBackupRequest) error {
	if strings.TrimSpace(r.OwnerMXID) == "" || r.ServiceID == "" || r.ExpectedRevision <= 0 || r.IdempotencyHash == "" || r.JobID == "" || r.OutboxID == "" || r.JobEventID == "" || r.CreatedAt <= 0 || r.Approval.Validate() != nil || r.Approval.Signature == "" {
		return cloudmodule.ErrServiceBackupConfirmationInvalid
	}
	return nil
}

func loadServiceBackupPrepareReplay(ctx context.Context, tx *sql.Tx, request cloudmodule.PrepareServiceBackupRequest) (cloudmodule.PrepareServiceBackupResult, bool, error) {
	var approvalJSON, serviceJSON, deploymentJSON, digest string
	err := tx.QueryRowContext(ctx, `SELECT approval_json,service_json,deployment_json,prepare_request_digest FROM p2p_cloud_service_backup_approvals WHERE owner_mxid=$1 AND prepare_idempotency_hash=$2`, request.OwnerMXID, request.IdempotencyHash).Scan(&approvalJSON, &serviceJSON, &deploymentJSON, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.PrepareServiceBackupResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.PrepareServiceBackupResult{}, false, err
	}
	if digest != request.RequestDigest {
		return cloudmodule.PrepareServiceBackupResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	approval, err := decodeStoredServiceBackupApproval(approvalJSON)
	var service cloudmodule.Service
	var deployment cloudmodule.Deployment
	if err != nil || json.Unmarshal([]byte(serviceJSON), &service) != nil || json.Unmarshal([]byte(deploymentJSON), &deployment) != nil {
		return cloudmodule.PrepareServiceBackupResult{}, true, cloudmodule.ErrServiceBackupConfirmationInvalid
	}
	return cloudmodule.PrepareServiceBackupResult{Confirmation: cloudmodule.ServiceBackupConfirmation{Service: service, Deployment: deployment, Approval: approval}}, true, nil
}
func lockServiceBackupApproval(ctx context.Context, tx *sql.Tx, id string) (storedServiceBackupApproval, error) {
	var v storedServiceBackupApproval
	err := tx.QueryRowContext(ctx, `SELECT approval_id,owner_mxid,backup_id,service_id,service_revision,signer_key_id,approval_json,signing_payload,service_json,deployment_json,status,prepare_request_digest,COALESCE(approve_request_digest,''),result_service_json,result_backup_json,result_job_json,expires_at FROM p2p_cloud_service_backup_approvals WHERE approval_id=$1 FOR UPDATE`, id).Scan(&v.ApprovalID, &v.OwnerMXID, &v.BackupID, &v.ServiceID, &v.ServiceRevision, &v.SignerKeyID, &v.ApprovalJSON, &v.SigningPayload, &v.ServiceJSON, &v.DeploymentJSON, &v.Status, &v.PrepareRequestDigest, &v.ApproveRequestDigest, &v.ResultServiceJSON, &v.ResultBackupJSON, &v.ResultJobJSON, &v.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return v, cloudmodule.ErrServiceBackupConfirmationInvalid
	}
	return v, err
}
func decodeStoredServiceBackupApproval(raw string) (cloudcontracts.ServiceBackupApprovalV1, error) {
	var a cloudcontracts.ServiceBackupApprovalV1
	if json.Unmarshal([]byte(raw), &a) != nil || a.Signature != "" || a.Validate() != nil {
		return a, errors.New("stored service backup approval is invalid")
	}
	return a, nil
}
func loadServiceBackupApproveReplay(ctx context.Context, tx *sql.Tx, r cloudmodule.ApproveServiceBackupRequest, digest string) (cloudmodule.ApproveServiceBackupResult, bool, error) {
	var stored, status, sj, bj, jj string
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(approve_request_digest,''),status,result_service_json,result_backup_json,result_job_json FROM p2p_cloud_service_backup_approvals WHERE owner_mxid=$1 AND approve_idempotency_hash=$2`, r.OwnerMXID, r.IdempotencyHash).Scan(&stored, &status, &sj, &bj, &jj)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.ApproveServiceBackupResult{}, false, nil
	}
	if err != nil {
		return cloudmodule.ApproveServiceBackupResult{}, false, err
	}
	if stored != digest {
		return cloudmodule.ApproveServiceBackupResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	if status == "expired" {
		return cloudmodule.ApproveServiceBackupResult{}, true, cloudmodule.ErrServiceBackupApprovalExpired
	}
	var service cloudmodule.Service
	var backup cloudmodule.ServiceBackup
	var job cloudmodule.Job
	if status != "approved" || json.Unmarshal([]byte(sj), &service) != nil || json.Unmarshal([]byte(bj), &backup) != nil || json.Unmarshal([]byte(jj), &job) != nil {
		return cloudmodule.ApproveServiceBackupResult{}, true, cloudmodule.ErrServiceBackupConfirmationInvalid
	}
	return cloudmodule.ApproveServiceBackupResult{Service: service, Backup: backup, Job: job}, true, nil
}
func serviceBackupApprovalRequestDigest(r cloudmodule.ApproveServiceBackupRequest) (string, error) {
	payload, err := r.Approval.SigningPayload()
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(struct {
		ServiceID        string `json:"service_id"`
		ExpectedRevision int64  `json:"expected_revision"`
		ApprovalID       string `json:"approval_id"`
		SigningPayload   []byte `json:"signing_payload"`
		Signature        string `json:"signature"`
	}{r.ServiceID, r.ExpectedRevision, r.Approval.ApprovalID, payload, r.Approval.Signature})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
