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
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
)

var _ cloudmodule.ServiceRestoreConfirmationStore = (*DatabaseStore)(nil)

type storedRestorePlan struct {
	Plan                                                                                                   cloudmodule.ServiceRestorePlan
	Owner, ConnectionID, PlanID, RecipeID, RecipeDigest, InstanceID, Region, AZ, QuoteID, Currency, Status string
	ServiceRevision, DeploymentRevision, BackupRevision, EstimatedHourly, EstimatedThirty, ValidUntil      int64
	Unincluded                                                                                             []string
	Swaps                                                                                                  []broker.ServiceRestoreVolumeSwap
}
type storedRestoreApproval struct {
	ApprovalID, Owner, RestorePlanID, ServiceID, SignerKeyID, ApprovalJSON, ServiceJSON, DeploymentJSON, PlanJSON, Status, PrepareDigest, ApproveDigest, ResultServiceJSON, ResultRestoreJSON, ResultJobJSON string
	ServiceRevision, PlanRevision, ExpiresAt                                                                                                                                                                 int64
	Payload                                                                                                                                                                                                  []byte
}

func (s *DatabaseStore) PrepareCloudServiceRestore(ctx context.Context, r cloudmodule.PrepareServiceRestoreRequest) (cloudmodule.PrepareServiceRestoreResult, error) {
	if validatePrepareRestore(r) != nil {
		return cloudmodule.PrepareServiceRestoreResult{}, cloudmodule.ErrServiceRestoreConfirmationInvalid
	}
	var result cloudmodule.PrepareServiceRestoreResult
	err := s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, e := loadRestorePrepareReplay(ctx, tx, r); e != nil || found {
			result = replay
			return e
		}
		state, e := lockServiceDestroyState(ctx, tx, r.ServiceID)
		if e != nil {
			return e
		}
		plan, e := lockReadyRestorePlan(ctx, tx, r.RestorePlanID)
		if e != nil {
			return e
		}
		if state.OwnerMXID != r.OwnerMXID || plan.Owner != r.OwnerMXID || state.Service.Revision != r.ExpectedRevision || plan.Plan.ServiceID != r.ServiceID || !restorePlanBindsState(plan, state) || serviceHasActiveOperation(ctx, tx, r.ServiceID) || serviceHasActiveBackup(ctx, tx, r.ServiceID) || serviceHasActiveRestore(ctx, tx, r.ServiceID) {
			return cloudmodule.ErrServiceRestoreConfirmationConflict
		}
		now := time.UnixMilli(r.CreatedAt).UTC()
		if plan.ValidUntil <= r.CreatedAt {
			return cloudmodule.ErrServiceRestoreConfirmationInvalid
		}
		keyID, _, e := lockCloudDeviceApprovalKey(ctx, tx, r.OwnerMXID, state.Deployment.ConnectionID)
		if e != nil {
			return cloudmodule.ErrServiceRestoreConfirmationInvalid
		}
		target := restoreTarget(plan, state)
		expires := time.UnixMilli(r.ExpiresAt).UTC()
		if quoteExpiry := time.UnixMilli(plan.ValidUntil).UTC(); expires.After(quoteExpiry) {
			expires = quoteExpiry
		}
		approval, e := cloudcontracts.NewServiceRestoreApprovalV1(target, r.ApprovalID, r.ChallengeID, keyID, now, expires)
		if e != nil {
			return cloudmodule.ErrServiceRestoreConfirmationInvalid
		}
		payload, e := approval.SigningPayload()
		if e != nil {
			return cloudmodule.ErrServiceRestoreConfirmationInvalid
		}
		approvalJSON, _ := json.Marshal(approval)
		serviceJSON, _ := json.Marshal(state.Service)
		deploymentJSON, _ := json.Marshal(state.Deployment)
		planJSON, _ := json.Marshal(plan.Plan)
		_, e = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_restore_approvals(approval_id,challenge_id,owner_mxid,restore_plan_id,restore_plan_revision,service_id,service_revision,deployment_id,deployment_revision,backup_id,backup_revision,cloud_connection_id,signer_key_id,approval_json,signing_payload,service_json,deployment_json,restore_plan_json,status,prepare_idempotency_hash,prepare_request_digest,expires_at,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,'pending',$19,$20,$21,$22,$22)`, approval.ApprovalID, approval.ChallengeID, r.OwnerMXID, plan.Plan.RestorePlanID, plan.Plan.Revision, state.Service.ServiceID, state.Service.Revision, state.Deployment.DeploymentID, state.Deployment.Revision, plan.Plan.BackupID, plan.BackupRevision, state.Deployment.ConnectionID, keyID, string(approvalJSON), payload, string(serviceJSON), string(deploymentJSON), string(planJSON), r.IdempotencyHash, r.RequestDigest, expires.UnixMilli(), r.CreatedAt)
		if e != nil {
			if sqlutil.IsUniqueConstraintViolationErr(e) {
				return cloudmodule.ErrIdempotencyConflict
			}
			return e
		}
		result = cloudmodule.PrepareServiceRestoreResult{Confirmation: cloudmodule.ServiceRestoreConfirmation{Service: state.Service, Deployment: state.Deployment, Plan: plan.Plan, Approval: approval}, Created: true}
		return nil
	})
	return result, err
}

func (s *DatabaseStore) ApproveCloudServiceRestore(ctx context.Context, r cloudmodule.ApproveServiceRestoreRequest) (cloudmodule.ApproveServiceRestoreResult, error) {
	digest, e := restoreApprovalRequestDigest(r)
	if e != nil || validateApproveRestore(r) != nil {
		return cloudmodule.ApproveServiceRestoreResult{}, cloudmodule.ErrServiceRestoreConfirmationInvalid
	}
	var result cloudmodule.ApproveServiceRestoreResult
	var terminal error
	e = s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, e := loadRestoreApproveReplay(ctx, tx, r, digest); e != nil || found {
			result = replay
			return e
		}
		stored, e := lockRestoreApproval(ctx, tx, r.Approval.ApprovalID)
		if e != nil {
			return e
		}
		if stored.Owner != r.OwnerMXID || stored.RestorePlanID != r.RestorePlanID || stored.ServiceID != r.ServiceID || stored.ServiceRevision != r.ExpectedRevision || stored.Status != "pending" {
			return cloudmodule.ErrServiceRestoreConfirmationConflict
		}
		if stored.ExpiresAt <= r.CreatedAt {
			_, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restore_approvals SET status='expired',approve_idempotency_hash=$1,approve_request_digest=$2,updated_at=$3 WHERE approval_id=$4 AND status='pending'`, r.IdempotencyHash, digest, r.CreatedAt, stored.ApprovalID)
			terminal = cloudmodule.ErrServiceRestoreApprovalExpired
			return e
		}
		state, e := lockServiceDestroyState(ctx, tx, r.ServiceID)
		if e != nil {
			return e
		}
		plan, e := lockReadyRestorePlan(ctx, tx, r.RestorePlanID)
		if e != nil {
			return e
		}
		if state.OwnerMXID != r.OwnerMXID || state.Service.Revision != r.ExpectedRevision || plan.Plan.Revision != stored.PlanRevision || !restorePlanBindsState(plan, state) || serviceHasActiveOperation(ctx, tx, r.ServiceID) || serviceHasActiveBackup(ctx, tx, r.ServiceID) || serviceHasActiveRestore(ctx, tx, r.ServiceID) {
			return cloudmodule.ErrServiceRestoreConfirmationConflict
		}
		storedApproval, e := decodeStoredRestoreApproval(stored.ApprovalJSON)
		if e != nil {
			return cloudmodule.ErrServiceRestoreConfirmationInvalid
		}
		incoming, e := r.Approval.SigningPayload()
		if e != nil || !bytes.Equal(incoming, stored.Payload) {
			return cloudmodule.ErrServiceRestoreConfirmationInvalid
		}
		canonical, e := storedApproval.SigningPayload()
		if e != nil || !bytes.Equal(canonical, stored.Payload) {
			return cloudmodule.ErrServiceRestoreConfirmationInvalid
		}
		keyID, spki, e := lockCloudDeviceApprovalKey(ctx, tx, r.OwnerMXID, state.Deployment.ConnectionID)
		if e != nil || keyID != stored.SignerKeyID {
			return cloudmodule.ErrServiceRestoreConfirmationInvalid
		}
		public, e := parseCloudApprovalPublicKey(spki)
		if e != nil || r.Approval.Verify(public, time.UnixMilli(r.CreatedAt)) != nil {
			return cloudmodule.ErrServiceRestoreApprovalSignature
		}
		if r.Approval.ValidateAgainst(restoreTarget(plan, state), time.UnixMilli(r.CreatedAt)) != nil {
			return cloudmodule.ErrServiceRestoreConfirmationInvalid
		}
		swaps, _ := json.Marshal(plan.Swaps)
		restore := cloudmodule.ServiceRestore{RestoreID: plan.Plan.RestorePlanID, RestorePlanID: plan.Plan.RestorePlanID, ServiceID: state.Service.ServiceID, DeploymentID: state.Deployment.DeploymentID, BackupID: plan.Plan.BackupID, Status: "queued", Revision: 1, CreatedAt: r.CreatedAt, UpdatedAt: r.CreatedAt}
		job := cloudmodule.Job{JobID: r.JobID, PlanID: state.Deployment.PlanID, DeploymentID: state.Deployment.DeploymentID, Kind: "restore", Execution: "queued", Outcome: "pending", Checkpoint: "restore_queued", Revision: 1, CreatedAt: r.CreatedAt, UpdatedAt: r.CreatedAt}
		_, e = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_restores(restore_id,restore_plan_id,approval_id,service_id,service_revision,deployment_id,deployment_revision,backup_id,backup_revision,plan_id,cloud_connection_id,instance_id,region,availability_zone,volume_swaps_json,original_volume_retention,failure_policy,job_id,restore_status,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'manual','reattach_original',$16,'queued',$17,$17)`, restore.RestoreID, restore.RestorePlanID, stored.ApprovalID, restore.ServiceID, state.Service.Revision, restore.DeploymentID, state.Deployment.Revision, restore.BackupID, plan.BackupRevision, state.Deployment.PlanID, state.Deployment.ConnectionID, state.InstanceID, plan.Region, plan.AZ, string(swaps), job.JobID, r.CreatedAt)
		if e != nil {
			if sqlutil.IsUniqueConstraintViolationErr(e) {
				return cloudmodule.ErrServiceRestoreConfirmationConflict
			}
			return e
		}
		if _, e = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at)VALUES($1,$2,$3,'restore','queued','pending','restore_queued','',1,$4,$4)`, job.JobID, job.PlanID, job.DeploymentID, r.CreatedAt); e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_job_steps(job_id,step_id,status,summary,checkpoint,error_code,revision,created_at,updated_at)VALUES($1,'restore','queued','Device-approved in-place volume restore is queued. Downtime and replacement-volume charges begin only when the typed Stack executor is enabled.','restore_queued','',1,$2,$2)`, job.JobID, r.CreatedAt); e != nil {
			return e
		}
		payload, _ := json.Marshal(map[string]string{"restore_id": restore.RestoreID})
		if _, e = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,available_at,created_at)VALUES($1,$2,'service_restore',$3,$4,$5,$5)`, r.OutboxID, cloudmodule.OutboxKindServiceRestoreRequested, restore.RestoreID, string(payload), r.CreatedAt); e != nil {
			return e
		}
		serviceJSON, _ := json.Marshal(state.Service)
		restoreJSON, _ := json.Marshal(restore)
		jobJSON, _ := json.Marshal(job)
		updated, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restore_approvals SET status='approved',approve_idempotency_hash=$1,approve_request_digest=$2,signature=$3,job_id=$4,result_service_json=$5,result_restore_json=$6,result_job_json=$7,updated_at=$8 WHERE approval_id=$9 AND status='pending'`, r.IdempotencyHash, digest, r.Approval.Signature, r.JobID, string(serviceJSON), string(restoreJSON), string(jobJSON), r.CreatedAt, stored.ApprovalID)
		if e != nil {
			return e
		}
		if !exactlyOneRow(updated) {
			return cloudmodule.ErrServiceRestoreConfirmationConflict
		}
		updated, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restore_plans SET plan_status='approved',revision=revision+1,updated_at=$1 WHERE restore_plan_id=$2 AND revision=$3 AND plan_status='ready_for_confirmation'`, r.CreatedAt, r.RestorePlanID, stored.PlanRevision)
		if e != nil {
			return e
		}
		if !exactlyOneRow(updated) {
			return cloudmodule.ErrServiceRestoreConfirmationConflict
		}
		if e = writeCloudConfirmationEvent(ctx, tx, r.JobEventID, "cloud.job.changed", "job", job.JobID, job.Revision, job, r.CreatedAt); e != nil {
			return e
		}
		result = cloudmodule.ApproveServiceRestoreResult{Service: state.Service, Restore: restore, Job: job, Created: true}
		return nil
	})
	if e != nil {
		return result, e
	}
	return result, terminal
}

func lockReadyRestorePlan(ctx context.Context, tx *sql.Tx, id string) (p storedRestorePlan, e error) {
	var unincluded, swaps string
	e = tx.QueryRowContext(ctx, `SELECT owner_mxid,restore_plan_id,service_id,deployment_id,backup_id,plan_status,revision,created_at,updated_at,service_revision,deployment_revision,backup_revision,plan_id,cloud_connection_id,recipe_id,recipe_digest,instance_id,region,availability_zone,quote_id,currency,estimated_hourly_minor,estimated_thirty_day_minor,valid_until,unincluded_json,volume_swaps_json FROM p2p_cloud_service_restore_plans WHERE restore_plan_id=$1 FOR UPDATE`, id).Scan(&p.Owner, &p.Plan.RestorePlanID, &p.Plan.ServiceID, &p.Plan.DeploymentID, &p.Plan.BackupID, &p.Status, &p.Plan.Revision, &p.Plan.CreatedAt, &p.Plan.UpdatedAt, &p.ServiceRevision, &p.DeploymentRevision, &p.BackupRevision, &p.PlanID, &p.ConnectionID, &p.RecipeID, &p.RecipeDigest, &p.InstanceID, &p.Region, &p.AZ, &p.QuoteID, &p.Currency, &p.EstimatedHourly, &p.EstimatedThirty, &p.ValidUntil, &unincluded, &swaps)
	if errors.Is(e, sql.ErrNoRows) {
		return p, cloudmodule.ErrServiceRestoreConfirmationInvalid
	}
	if e != nil {
		return p, e
	}
	p.Plan.Status = p.Status
	if p.Status != "ready_for_confirmation" || json.Unmarshal([]byte(unincluded), &p.Unincluded) != nil || json.Unmarshal([]byte(swaps), &p.Swaps) != nil {
		return p, cloudmodule.ErrServiceRestoreConfirmationInvalid
	}
	return p, nil
}
func restorePlanBindsState(p storedRestorePlan, s serviceDestroyState) bool {
	return serviceDestroyStateReady(s) && p.Plan.ServiceID == s.Service.ServiceID && p.ServiceRevision == s.Service.Revision && p.Plan.DeploymentID == s.Deployment.DeploymentID && p.DeploymentRevision == s.Deployment.Revision && p.ConnectionID == s.Deployment.ConnectionID && p.RecipeID == s.Service.RecipeID && p.RecipeDigest == s.RecipeDigest && p.InstanceID == s.InstanceID && p.Status == "ready_for_confirmation" && len(p.Swaps) > 0
}
func restoreTarget(p storedRestorePlan, s serviceDestroyState) cloudcontracts.ServiceRestoreTargetV1 {
	swaps := make([]cloudcontracts.ServiceRestoreVolumeSwapV1, len(p.Swaps))
	for i, x := range p.Swaps {
		swaps[i] = cloudcontracts.ServiceRestoreVolumeSwapV1{OriginalVolumeID: x.OriginalVolumeID, SnapshotID: x.SnapshotID, DeviceName: x.DeviceName, VolumeType: x.VolumeType, SizeGiB: x.SizeGiB, IOPS: x.IOPS, ThroughputMiB: x.ThroughputMiB, Encrypted: x.Encrypted, DeleteOnTermination: x.DeleteOnTermination}
	}
	return cloudcontracts.ServiceRestoreTargetV1{RestoreID: p.Plan.RestorePlanID, ServiceID: s.Service.ServiceID, ServiceRevision: uint64(s.Service.Revision), DeploymentID: s.Deployment.DeploymentID, DeploymentRevision: uint64(s.Deployment.Revision), CloudConnectionID: s.Deployment.ConnectionID, BackupID: p.Plan.BackupID, BackupRevision: uint64(p.BackupRevision), RecipeID: s.Service.RecipeID, RecipeDigest: s.RecipeDigest, InstanceID: s.InstanceID, Region: p.Region, AvailabilityZone: p.AZ, RestoreMode: cloudcontracts.ServiceRestoreModeInPlace, DowntimeRequired: true, OriginalVolumeRetention: cloudcontracts.ServiceRestoreRetentionManual, FailurePolicy: cloudcontracts.ServiceRestoreFailureReattachOriginal, QuoteID: p.QuoteID, Currency: p.Currency, EstimatedHourlyMinor: p.EstimatedHourly, EstimatedThirtyDayMinor: p.EstimatedThirty, QuoteValidUntil: time.UnixMilli(p.ValidUntil).UTC(), Unincluded: append([]string(nil), p.Unincluded...), VolumeSwaps: swaps}
}
func serviceHasActiveRestore(ctx context.Context, tx *sql.Tx, serviceID string) bool {
	var n int
	return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_service_restores WHERE service_id=$1 AND restore_status IN('queued','running','verifying','restore_blocked')`, serviceID).Scan(&n) != nil || n != 0
}
func validatePrepareRestore(r cloudmodule.PrepareServiceRestoreRequest) error {
	if strings.TrimSpace(r.OwnerMXID) == "" || r.ServiceID == "" || r.RestorePlanID == "" || r.ExpectedRevision <= 0 || r.IdempotencyHash == "" || r.RequestDigest == "" || r.ApprovalID == "" || r.ChallengeID == "" || r.CreatedAt <= 0 || r.ExpiresAt <= r.CreatedAt || r.ExpiresAt-r.CreatedAt > int64((5*time.Minute).Milliseconds()) {
		return cloudmodule.ErrServiceRestoreConfirmationInvalid
	}
	return nil
}
func validateApproveRestore(r cloudmodule.ApproveServiceRestoreRequest) error {
	if strings.TrimSpace(r.OwnerMXID) == "" || r.ServiceID == "" || r.RestorePlanID == "" || r.ExpectedRevision <= 0 || r.IdempotencyHash == "" || r.JobID == "" || r.OutboxID == "" || r.JobEventID == "" || r.CreatedAt <= 0 || r.Approval.Validate() != nil || r.Approval.Signature == "" {
		return cloudmodule.ErrServiceRestoreConfirmationInvalid
	}
	return nil
}
func loadRestorePrepareReplay(ctx context.Context, tx *sql.Tx, r cloudmodule.PrepareServiceRestoreRequest) (cloudmodule.PrepareServiceRestoreResult, bool, error) {
	var approvalJSON, serviceJSON, deploymentJSON, planJSON, digest string
	e := tx.QueryRowContext(ctx, `SELECT approval_json,service_json,deployment_json,restore_plan_json,prepare_request_digest FROM p2p_cloud_service_restore_approvals WHERE owner_mxid=$1 AND prepare_idempotency_hash=$2`, r.OwnerMXID, r.IdempotencyHash).Scan(&approvalJSON, &serviceJSON, &deploymentJSON, &planJSON, &digest)
	if errors.Is(e, sql.ErrNoRows) {
		return cloudmodule.PrepareServiceRestoreResult{}, false, nil
	}
	if e != nil {
		return cloudmodule.PrepareServiceRestoreResult{}, false, e
	}
	if digest != r.RequestDigest {
		return cloudmodule.PrepareServiceRestoreResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	approval, e := decodeStoredRestoreApproval(approvalJSON)
	var service cloudmodule.Service
	var deployment cloudmodule.Deployment
	var plan cloudmodule.ServiceRestorePlan
	if e != nil || json.Unmarshal([]byte(serviceJSON), &service) != nil || json.Unmarshal([]byte(deploymentJSON), &deployment) != nil || json.Unmarshal([]byte(planJSON), &plan) != nil {
		return cloudmodule.PrepareServiceRestoreResult{}, true, cloudmodule.ErrServiceRestoreConfirmationInvalid
	}
	return cloudmodule.PrepareServiceRestoreResult{Confirmation: cloudmodule.ServiceRestoreConfirmation{Service: service, Deployment: deployment, Plan: plan, Approval: approval}}, true, nil
}
func lockRestoreApproval(ctx context.Context, tx *sql.Tx, id string) (v storedRestoreApproval, e error) {
	e = tx.QueryRowContext(ctx, `SELECT approval_id,owner_mxid,restore_plan_id,restore_plan_revision,service_id,service_revision,signer_key_id,approval_json,signing_payload,service_json,deployment_json,restore_plan_json,status,prepare_request_digest,COALESCE(approve_request_digest,''),result_service_json,result_restore_json,result_job_json,expires_at FROM p2p_cloud_service_restore_approvals WHERE approval_id=$1 FOR UPDATE`, id).Scan(&v.ApprovalID, &v.Owner, &v.RestorePlanID, &v.PlanRevision, &v.ServiceID, &v.ServiceRevision, &v.SignerKeyID, &v.ApprovalJSON, &v.Payload, &v.ServiceJSON, &v.DeploymentJSON, &v.PlanJSON, &v.Status, &v.PrepareDigest, &v.ApproveDigest, &v.ResultServiceJSON, &v.ResultRestoreJSON, &v.ResultJobJSON, &v.ExpiresAt)
	if errors.Is(e, sql.ErrNoRows) {
		return v, cloudmodule.ErrServiceRestoreConfirmationInvalid
	}
	return v, e
}
func decodeStoredRestoreApproval(raw string) (cloudcontracts.ServiceRestoreApprovalV1, error) {
	var a cloudcontracts.ServiceRestoreApprovalV1
	if json.Unmarshal([]byte(raw), &a) != nil || a.Signature != "" || a.Validate() != nil {
		return a, errors.New("stored service restore approval is invalid")
	}
	return a, nil
}
func loadRestoreApproveReplay(ctx context.Context, tx *sql.Tx, r cloudmodule.ApproveServiceRestoreRequest, digest string) (cloudmodule.ApproveServiceRestoreResult, bool, error) {
	var stored, status, sj, rj, jj string
	e := tx.QueryRowContext(ctx, `SELECT COALESCE(approve_request_digest,''),status,result_service_json,result_restore_json,result_job_json FROM p2p_cloud_service_restore_approvals WHERE owner_mxid=$1 AND approve_idempotency_hash=$2`, r.OwnerMXID, r.IdempotencyHash).Scan(&stored, &status, &sj, &rj, &jj)
	if errors.Is(e, sql.ErrNoRows) {
		return cloudmodule.ApproveServiceRestoreResult{}, false, nil
	}
	if e != nil {
		return cloudmodule.ApproveServiceRestoreResult{}, false, e
	}
	if stored != digest {
		return cloudmodule.ApproveServiceRestoreResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	if status == "expired" {
		return cloudmodule.ApproveServiceRestoreResult{}, true, cloudmodule.ErrServiceRestoreApprovalExpired
	}
	var service cloudmodule.Service
	var restore cloudmodule.ServiceRestore
	var job cloudmodule.Job
	if status != "approved" || json.Unmarshal([]byte(sj), &service) != nil || json.Unmarshal([]byte(rj), &restore) != nil || json.Unmarshal([]byte(jj), &job) != nil {
		return cloudmodule.ApproveServiceRestoreResult{}, true, cloudmodule.ErrServiceRestoreConfirmationInvalid
	}
	return cloudmodule.ApproveServiceRestoreResult{Service: service, Restore: restore, Job: job}, true, nil
}
func restoreApprovalRequestDigest(r cloudmodule.ApproveServiceRestoreRequest) (string, error) {
	payload, e := r.Approval.SigningPayload()
	if e != nil {
		return "", e
	}
	raw, e := json.Marshal(struct {
		ServiceID        string `json:"service_id"`
		RestorePlanID    string `json:"restore_plan_id"`
		ExpectedRevision int64  `json:"expected_revision"`
		ApprovalID       string `json:"approval_id"`
		Payload          []byte `json:"signing_payload"`
		Signature        string `json:"signature"`
	}{r.ServiceID, r.RestorePlanID, r.ExpectedRevision, r.Approval.ApprovalID, payload, r.Approval.Signature})
	if e != nil {
		return "", e
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
