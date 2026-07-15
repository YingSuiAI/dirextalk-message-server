package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
)

var _ cloudmodule.ServiceRestorePlanStore = (*DatabaseStore)(nil)

func (s *DatabaseStore) CreateCloudServiceRestorePlan(ctx context.Context, r cloudmodule.CreateServiceRestorePlanRequest) (cloudmodule.CreateServiceRestorePlanResult, error) {
	if validateCreateServiceRestorePlan(r) != nil {
		return cloudmodule.CreateServiceRestorePlanResult{}, cloudmodule.ErrServiceRestorePlanInvalid
	}
	var result cloudmodule.CreateServiceRestorePlanResult
	err := s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		if replay, found, e := loadServiceRestorePlanReplay(ctx, tx, r); e != nil || found {
			result = replay
			return e
		}
		state, e := lockServiceDestroyState(ctx, tx, r.ServiceID)
		if e != nil {
			return e
		}
		if state.OwnerMXID != r.OwnerMXID || state.Service.Revision != r.ExpectedRevision {
			return cloudmodule.ErrServiceRestorePlanConflict
		}
		if !serviceDestroyStateReady(state) || serviceHasActiveOperation(ctx, tx, r.ServiceID) || serviceHasActiveBackup(ctx, tx, r.ServiceID) || serviceHasActiveRestorePlan(ctx, tx, r.ServiceID) {
			return cloudmodule.ErrServiceRestorePlanInvalid
		}
		var backupService, backupDeployment, imageID, snapshotsJSON, status string
		var backupRevision int64
		e = tx.QueryRowContext(ctx, `SELECT service_id,deployment_id,backup_status,image_id,snapshots_json,revision FROM p2p_cloud_service_backups WHERE backup_id=$1 FOR SHARE`, r.BackupID).Scan(&backupService, &backupDeployment, &status, &imageID, &snapshotsJSON, &backupRevision)
		if errors.Is(e, sql.ErrNoRows) {
			return cloudmodule.ErrServiceRestorePlanInvalid
		}
		if e != nil {
			return e
		}
		if backupService != r.ServiceID || backupDeployment != state.Deployment.DeploymentID || status != "available" || imageID == "" {
			return cloudmodule.ErrServiceRestorePlanInvalid
		}
		var snapshots []struct {
			VolumeID   string `json:"volume_id"`
			SnapshotID string `json:"snapshot_id"`
			State      string `json:"state"`
			Encrypted  bool   `json:"encrypted"`
		}
		if json.Unmarshal([]byte(snapshotsJSON), &snapshots) != nil || len(snapshots) != len(state.VolumeIDs) {
			return cloudmodule.ErrServiceRestorePlanInvalid
		}
		byVolume := map[string]string{}
		for _, x := range snapshots {
			if x.State != "completed" || !x.Encrypted || x.VolumeID == "" || x.SnapshotID == "" || byVolume[x.VolumeID] != "" {
				return cloudmodule.ErrServiceRestorePlanInvalid
			}
			byVolume[x.VolumeID] = x.SnapshotID
		}
		refs := make([]broker.ServiceRestoreSnapshotRef, 0, len(state.VolumeIDs))
		for _, v := range state.VolumeIDs {
			snap := byVolume[v]
			if snap == "" {
				return cloudmodule.ErrServiceRestorePlanInvalid
			}
			refs = append(refs, broker.ServiceRestoreSnapshotRef{OriginalVolumeID: v, SnapshotID: snap})
		}
		sort.Slice(refs, func(i, j int) bool { return refs[i].OriginalVolumeID < refs[j].OriginalVolumeID })
		refsJSON, _ := json.Marshal(refs)
		var region string
		if e = tx.QueryRowContext(ctx, `SELECT region FROM p2p_cloud_connections WHERE cloud_connection_id=$1 AND status='active'`, state.Deployment.ConnectionID).Scan(&region); e != nil {
			return cloudmodule.ErrServiceRestorePlanInvalid
		}
		plan := cloudmodule.ServiceRestorePlan{RestorePlanID: r.RestorePlanID, ServiceID: r.ServiceID, DeploymentID: state.Deployment.DeploymentID, BackupID: r.BackupID, Status: "planning", Revision: 1, CreatedAt: r.CreatedAt, UpdatedAt: r.CreatedAt}
		job := cloudmodule.Job{JobID: r.JobID, PlanID: state.Deployment.PlanID, DeploymentID: state.Deployment.DeploymentID, Kind: "restore_plan", Execution: "queued", Outcome: "pending", Checkpoint: "restore_plan_queued", Revision: 1, CreatedAt: r.CreatedAt, UpdatedAt: r.CreatedAt}
		_, e = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_restore_plans(restore_plan_id,owner_mxid,service_id,service_revision,deployment_id,deployment_revision,plan_id,cloud_connection_id,backup_id,backup_revision,recipe_id,recipe_digest,instance_id,region,image_id,snapshot_refs_json,plan_status,job_id,idempotency_hash,request_digest,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'planning',$17,$18,$19,$20,$20)`, r.RestorePlanID, r.OwnerMXID, r.ServiceID, state.Service.Revision, state.Deployment.DeploymentID, state.Deployment.Revision, state.Deployment.PlanID, state.Deployment.ConnectionID, r.BackupID, backupRevision, state.Service.RecipeID, state.RecipeDigest, state.InstanceID, region, imageID, string(refsJSON), r.JobID, r.IdempotencyHash, r.RequestDigest, r.CreatedAt)
		if e != nil {
			if sqlutil.IsUniqueConstraintViolationErr(e) {
				return cloudmodule.ErrServiceRestorePlanConflict
			}
			return e
		}
		if _, e = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at)VALUES($1,$2,$3,'restore_plan','queued','pending','restore_plan_queued','',1,$4,$4)`, job.JobID, job.PlanID, job.DeploymentID, r.CreatedAt); e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_job_steps(job_id,step_id,status,summary,checkpoint,error_code,revision,created_at,updated_at)VALUES($1,'restore_plan','queued','Independent AWS read-back and restore cost estimation are queued; no resource mutation is authorized.','restore_plan_queued','',1,$2,$2)`, job.JobID, r.CreatedAt); e != nil {
			return e
		}
		payload, _ := json.Marshal(map[string]string{"restore_plan_id": r.RestorePlanID})
		if _, e = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,available_at,created_at)VALUES($1,$2,'service_restore_plan',$3,$4,$5,$5)`, r.OutboxID, cloudmodule.OutboxKindServiceRestorePlanRequested, r.RestorePlanID, string(payload), r.CreatedAt); e != nil {
			return e
		}
		if e = writeCloudConfirmationEvent(ctx, tx, r.JobEventID, "cloud.job.changed", "job", job.JobID, job.Revision, job, r.CreatedAt); e != nil {
			return e
		}
		result = cloudmodule.CreateServiceRestorePlanResult{Plan: plan, Job: job, Created: true}
		return nil
	})
	return result, err
}

func validateCreateServiceRestorePlan(r cloudmodule.CreateServiceRestorePlanRequest) error {
	if strings.TrimSpace(r.OwnerMXID) == "" || r.ServiceID == "" || r.BackupID == "" || r.ExpectedRevision <= 0 || r.IdempotencyHash == "" || r.RequestDigest == "" || r.RestorePlanID == "" || r.JobID == "" || r.OutboxID == "" || r.JobEventID == "" || r.CreatedAt <= 0 {
		return cloudmodule.ErrServiceRestorePlanInvalid
	}
	return nil
}
func serviceHasActiveRestorePlan(ctx context.Context, tx *sql.Tx, serviceID string) bool {
	var n int
	return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_service_restore_plans WHERE service_id=$1 AND plan_status IN('planning','ready_for_confirmation','approved')`, serviceID).Scan(&n) != nil || n != 0
}
func loadServiceRestorePlanReplay(ctx context.Context, tx *sql.Tx, r cloudmodule.CreateServiceRestorePlanRequest) (cloudmodule.CreateServiceRestorePlanResult, bool, error) {
	var digest string
	var p cloudmodule.ServiceRestorePlan
	var jobID, jobPlan, jobDeployment, kind, execution, outcome, checkpoint, errorCode string
	var jobRevision, jobCreated, jobUpdated int64
	e := tx.QueryRowContext(ctx, `SELECT restore.restore_plan_id,restore.service_id,restore.deployment_id,restore.backup_id,restore.plan_status,restore.revision,restore.created_at,restore.updated_at,restore.request_digest,job.job_id,job.plan_id,job.deployment_id,job.kind,job.execution_status,job.outcome_status,job.checkpoint,job.error_code,job.revision,job.created_at,job.updated_at FROM p2p_cloud_service_restore_plans restore JOIN p2p_cloud_jobs job ON job.job_id=restore.job_id WHERE restore.owner_mxid=$1 AND restore.idempotency_hash=$2`, r.OwnerMXID, r.IdempotencyHash).Scan(&p.RestorePlanID, &p.ServiceID, &p.DeploymentID, &p.BackupID, &p.Status, &p.Revision, &p.CreatedAt, &p.UpdatedAt, &digest, &jobID, &jobPlan, &jobDeployment, &kind, &execution, &outcome, &checkpoint, &errorCode, &jobRevision, &jobCreated, &jobUpdated)
	if errors.Is(e, sql.ErrNoRows) {
		return cloudmodule.CreateServiceRestorePlanResult{}, false, nil
	}
	if e != nil {
		return cloudmodule.CreateServiceRestorePlanResult{}, false, e
	}
	if digest != r.RequestDigest {
		return cloudmodule.CreateServiceRestorePlanResult{}, true, cloudmodule.ErrIdempotencyConflict
	}
	job := cloudmodule.Job{JobID: jobID, PlanID: jobPlan, DeploymentID: jobDeployment, Kind: kind, Execution: execution, Outcome: outcome, Checkpoint: checkpoint, ErrorCode: errorCode, Revision: jobRevision, CreatedAt: jobCreated, UpdatedAt: jobUpdated}
	return cloudmodule.CreateServiceRestorePlanResult{Plan: p, Job: job}, true, nil
}
