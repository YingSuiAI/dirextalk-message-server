package storepg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var _ runtime.ServiceOperationStore = (*Store)(nil)

func (s *Store) ClaimServiceOperation(ctx context.Context, workerID string, lease time.Duration) (runtime.RecipeInstallClaim, bool, error) {
	if s == nil || s.db == nil || strings.TrimSpace(workerID) == "" || lease <= 0 || lease > 5*time.Minute {
		return runtime.RecipeInstallClaim{}, false, errors.New("service operation claim configuration is invalid")
	}
	if claim, found, err := s.claimServiceOperationIssue(ctx, workerID, lease); err != nil || found {
		return claim, found, err
	}
	return s.claimServiceOperationObserve(ctx, workerID, lease)
}

func (s *Store) claimServiceOperationIssue(ctx context.Context, workerID string, lease time.Duration) (claim runtime.RecipeInstallClaim, found bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return claim, false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	now := s.now().UnixMilli()
	token := strings.TrimSpace(s.cfg.NewLeaseToken())
	if token == "" {
		return claim, false, errors.New("service operation lease token is invalid")
	}
	var manifestJSON, operation string
	err = tx.QueryRowContext(ctx, `SELECT outbox.outbox_id,outbox.kind,outbox.aggregate_type,outbox.aggregate_id,
		task.execution_id,task.deployment_id,task.plan_id,task.cloud_connection_id,connection.region,task.instance_id,task.task_id,task.task_attempt,task.operation,task.manifest_digest,task.input_digest,task.manifest_json,
		broker.broker_command_url,broker.node_key_id,broker.connection_generation,task.job_id
		FROM p2p_cloud_outbox outbox JOIN p2p_cloud_service_operation_tasks task ON task.operation_id=outbox.aggregate_id
		JOIN p2p_cloud_services service ON service.service_id=task.service_id
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=task.cloud_connection_id JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=task.cloud_connection_id
		JOIN p2p_cloud_worker_bootstrap_observations observation ON observation.deployment_id=task.deployment_id JOIN p2p_cloud_jobs job ON job.job_id=task.job_id
		WHERE outbox.kind=$1 AND outbox.completed_at=0 AND outbox.available_at<=$2 AND outbox.lease_until<=$2 AND task.task_status='queued' AND task.lease_until<=$2
		AND task.service_revision=service.revision AND task.expected_service_status=service.service_status
		AND connection.status='active' AND observation.worker_session_state='active' AND observation.worker_lease_expires_at>$2 AND job.outcome_status='pending'
		ORDER BY outbox.available_at,outbox.created_at,outbox.outbox_id FOR UPDATE OF outbox,task SKIP LOCKED LIMIT 1`, cloudmodule.OutboxKindServiceOperationRequested, now).Scan(
		&claim.OutboxID, &claim.Kind, &claim.AggregateType, &claim.AggregateID, &claim.ExecutionID, &claim.DeploymentID, &claim.PlanID, &claim.ConnectionID, &claim.Region, &claim.InstanceID, &claim.TaskID, &claim.TaskAttempt, &operation, &claim.ManifestDigest, &claim.InputDigest, &manifestJSON, &claim.BrokerEndpoint, &claim.NodeKeyID, &claim.ExpectedGeneration, &claim.JobID)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return runtime.RecipeInstallClaim{}, false, nil
	}
	if err != nil {
		return claim, false, err
	}
	if err = decodeRecipeInstallManifest(manifestJSON, &claim.Manifest); err != nil || claim.Manifest.VerifyDigest(claim.ManifestDigest) != nil {
		return claim, false, errors.New("service operation manifest is invalid")
	}
	claim.Phase = runtime.RecipeInstallPhaseIssue
	claim.LeaseToken = token
	claim.IssueRequest = runtime.RecipeInstallIssueRequest{Schema: runtime.RecipeInstallIssueSchema, ExecutionID: claim.ExecutionID, DeploymentID: claim.DeploymentID, TaskID: claim.TaskID, TaskKind: "recipe_execution", RecipeExecutionManifestDigest: claim.ManifestDigest, InputDigest: claim.InputDigest, CheckpointSequence: append([]string(nil), claim.Manifest.CheckpointSequence...), Manifest: claim.Manifest}
	if err = validateServiceOperationManifest(claim.Manifest, operation); err != nil {
		return claim, false, err
	}
	r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner=$1,lease_token=$2,lease_until=$3,attempts=attempts+1,last_error_code='' WHERE outbox_id=$4 AND completed_at=0 AND lease_until<=$5`, workerID, token, now+lease.Milliseconds(), claim.OutboxID, now)
	if err != nil {
		return claim, false, err
	}
	if requireOneAffected(r) != nil {
		return claim, false, ErrLeaseLost
	}
	r, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_operation_tasks SET lease_owner=$1,lease_token=$2,lease_until=$3,attempts=attempts+1,last_error_code='',updated_at=$4 WHERE operation_id=$5 AND task_status='queued' AND lease_until<=$4`, workerID, token, now+lease.Milliseconds(), now, claim.ExecutionID)
	if err != nil {
		return claim, false, err
	}
	if requireOneAffected(r) != nil {
		return claim, false, ErrLeaseLost
	}
	if claim.Command, err = prepareRecipeInstallCommand(ctx, tx, claim, now); err != nil {
		return claim, false, err
	}
	if err = tx.Commit(); err != nil {
		return claim, false, err
	}
	return claim, true, nil
}

func (s *Store) claimServiceOperationObserve(ctx context.Context, workerID string, lease time.Duration) (claim runtime.RecipeInstallClaim, found bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return claim, false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	now := s.now().UnixMilli()
	token := strings.TrimSpace(s.cfg.NewLeaseToken())
	if token == "" {
		return claim, false, errors.New("service operation lease token is invalid")
	}
	var manifestJSON, operation string
	err = tx.QueryRowContext(ctx, `SELECT task.execution_id,task.deployment_id,task.plan_id,task.cloud_connection_id,connection.region,task.instance_id,task.task_id,task.task_attempt,task.operation,task.manifest_digest,task.input_digest,task.manifest_json,broker.broker_command_url,broker.node_key_id,broker.connection_generation,task.job_id
		FROM p2p_cloud_service_operation_tasks task JOIN p2p_cloud_services service ON service.service_id=task.service_id
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=task.cloud_connection_id JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=task.cloud_connection_id
		JOIN p2p_cloud_worker_bootstrap_observations observation ON observation.deployment_id=task.deployment_id JOIN p2p_cloud_jobs job ON job.job_id=task.job_id
		WHERE task.task_status='running' AND task.available_at<=$1 AND task.lease_until<=$1 AND task.service_revision=service.revision AND task.expected_service_status=service.service_status
		AND connection.status='active' AND observation.worker_session_state='active' AND observation.worker_lease_expires_at>$1 AND job.outcome_status='pending'
		ORDER BY task.available_at,task.updated_at,task.operation_id FOR UPDATE OF task SKIP LOCKED LIMIT 1`, now).Scan(&claim.ExecutionID, &claim.DeploymentID, &claim.PlanID, &claim.ConnectionID, &claim.Region, &claim.InstanceID, &claim.TaskID, &claim.TaskAttempt, &operation, &claim.ManifestDigest, &claim.InputDigest, &manifestJSON, &claim.BrokerEndpoint, &claim.NodeKeyID, &claim.ExpectedGeneration, &claim.JobID)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return runtime.RecipeInstallClaim{}, false, nil
	}
	if err != nil {
		return claim, false, err
	}
	if err = decodeRecipeInstallManifest(manifestJSON, &claim.Manifest); err != nil || claim.Manifest.VerifyDigest(claim.ManifestDigest) != nil || validateServiceOperationManifest(claim.Manifest, operation) != nil {
		return claim, false, errors.New("service operation manifest is invalid")
	}
	claim.Phase = runtime.RecipeInstallPhaseObserve
	claim.LeaseToken = token
	claim.ObserveRequest = runtime.RecipeInstallObserveRequest{DeploymentID: claim.DeploymentID, TaskID: claim.TaskID}
	r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_operation_tasks SET lease_owner=$1,lease_token=$2,lease_until=$3,attempts=attempts+1,last_error_code='',updated_at=$4 WHERE operation_id=$5 AND task_status='running' AND lease_until<=$4`, workerID, token, now+lease.Milliseconds(), now, claim.ExecutionID)
	if err != nil {
		return claim, false, err
	}
	if requireOneAffected(r) != nil {
		return claim, false, ErrLeaseLost
	}
	if claim.Command, err = prepareRecipeInstallCommand(ctx, tx, claim, now); err != nil {
		return claim, false, err
	}
	if err = tx.Commit(); err != nil {
		return claim, false, err
	}
	return claim, true, nil
}

func validateServiceOperationManifest(manifest cloudcontracts.RecipeExecutionManifestV1, operation string) error {
	if manifest.ArtifactDigest != cloudcontracts.FixedProbeManagedArtifactDigest || !manifest.RootRequired {
		return errors.New("service operation artifact is not managed")
	}
	want := map[string]string{"start": cloudcontracts.FixedProbeStartActionID, "stop": cloudcontracts.FixedProbeStopActionID, "restart": cloudcontracts.FixedProbeRestartActionID}[operation]
	if want != "" && manifest.ActionID == want {
		return nil
	}
	return errors.New("service operation action is not managed")
}

func (s *Store) PersistServiceOperationCommand(ctx context.Context, claim runtime.RecipeInstallClaim, signed runtime.SignedRecipeInstallCommand) error {
	return s.withServiceOperationClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_commands SET canonical_payload_json=$1,payload_sha256=$2,request_sha256=$3,signed_envelope_json=$4,issued_at=$5,expires_at=$6,state='signed',updated_at=$7 WHERE command_id=$8 AND state='allocated'`, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256, signed.EnvelopeJSON, signed.IssuedAt.UnixMilli(), signed.ExpiresAt.UnixMilli(), now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		return requireOneAffected(r)
	})
}

func (s *Store) MarkServiceOperationStarted(ctx context.Context, claim runtime.RecipeInstallClaim) error {
	return s.withServiceOperationClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		if claim.Phase == runtime.RecipeInstallPhaseObserve {
			return nil
		}
		r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_jobs SET execution_status='installing',checkpoint='service_operation_issuing',revision=revision+1,updated_at=$1 WHERE job_id=$2 AND outcome_status='pending'`, now, claim.JobID)
		if err != nil {
			return err
		}
		if requireOneAffected(r) != nil {
			return ErrLeaseLost
		}
		_, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_job_steps SET status='running',checkpoint='service_operation_running',summary='The sealed managed service operation is running on the dedicated Worker.',revision=revision+1,updated_at=$1 WHERE job_id=$2 AND step_id='service_operation'`, now, claim.JobID)
		return err
	})
}

func (s *Store) CommitServiceOperation(ctx context.Context, claim runtime.RecipeInstallClaim, result runtime.RecipeInstallResult) error {
	if result.ExecutionID != claim.ExecutionID || result.DeploymentID != claim.DeploymentID || result.TaskID != claim.TaskID || result.Attempt != claim.TaskAttempt || result.LastSequence < 0 {
		return s.FailServiceOperation(ctx, claim, "service_operation_result_binding_invalid")
	}
	if result.Status == "queued" || result.Status == "running" {
		return s.commitRunningServiceOperation(ctx, claim, result)
	}
	if result.Status != "succeeded" {
		code := derefRecipeError(result.ErrorCode)
		if code == "" {
			code = "service_operation_failed"
		}
		return s.finishServiceOperation(ctx, claim, result, code, false)
	}
	return s.finishServiceOperation(ctx, claim, result, "", true)
}

func (s *Store) commitRunningServiceOperation(ctx context.Context, claim runtime.RecipeInstallClaim, result runtime.RecipeInstallResult) error {
	return s.withServiceOperationClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_commands SET state='accepted',last_error_code='',updated_at=$1 WHERE command_id=$2 AND state IN('signed','indeterminate')`, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if requireOneAffected(r) != nil {
			return ErrLeaseLost
		}
		r, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_operation_tasks SET task_status='running',task_attempt=$1,last_sequence=$2,last_checkpoint=$3,error_code='',available_at=$4,lease_owner='',lease_token='',lease_until=0,last_error_code='',updated_at=$4 WHERE operation_id=$5 AND task_status IN('queued','running')`, result.Attempt, result.LastSequence, result.LastCheckpoint, now, claim.ExecutionID)
		if err != nil {
			return err
		}
		if requireOneAffected(r) != nil {
			return ErrLeaseLost
		}
		if claim.Phase == runtime.RecipeInstallPhaseIssue {
			r, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET completed_at=$1,lease_owner='',lease_token='',lease_until=0,last_error_code='' WHERE outbox_id=$2 AND completed_at=0`, now, claim.OutboxID)
			if err != nil {
				return err
			}
			if requireOneAffected(r) != nil {
				return ErrLeaseLost
			}
		}
		r, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_jobs SET execution_status='installing',checkpoint='service_operation_running',error_code='',revision=revision+1,updated_at=$1 WHERE job_id=$2 AND outcome_status='pending'`, now, claim.JobID)
		if err != nil {
			return err
		}
		if requireOneAffected(r) != nil {
			return ErrLeaseLost
		}
		var kind string
		var revision, createdAt int64
		if err = tx.QueryRowContext(ctx, `SELECT kind,revision,created_at FROM p2p_cloud_jobs WHERE job_id=$1`, claim.JobID).Scan(&kind, &revision, &createdAt); err != nil {
			return err
		}
		return writeEventAndProjection(ctx, tx, stableID("cloud_event_", claim.JobID, fmt.Sprint(revision), "service_operation_running"), "cloud.job.changed", "job", claim.JobID, revision, map[string]any{"job_id": claim.JobID, "plan_id": claim.PlanID, "deployment_id": claim.DeploymentID, "kind": kind, "execution_status": "installing", "outcome_status": "pending", "checkpoint": "service_operation_running", "error_code": "", "revision": revision, "created_at": createdAt, "updated_at": now}, now)
	})
}

func (s *Store) finishServiceOperation(ctx context.Context, claim runtime.RecipeInstallClaim, result runtime.RecipeInstallResult, code string, succeeded bool) error {
	return s.withServiceOperationClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		var serviceID, operation, status, name, recipeID, integration string
		var revision, createdAt int64
		if err := tx.QueryRowContext(ctx, `SELECT task.service_id,task.operation,service.service_status,service.name,service.recipe_id,service.integration_status,service.revision,service.created_at FROM p2p_cloud_service_operation_tasks task JOIN p2p_cloud_services service ON service.service_id=task.service_id WHERE task.operation_id=$1 FOR UPDATE OF task,service`, claim.ExecutionID).Scan(&serviceID, &operation, &status, &name, &recipeID, &integration, &revision, &createdAt); err != nil {
			return err
		}
		taskStatus, outcome, checkpoint, stepStatus, summary := "succeeded", "succeeded", "service_operation_succeeded", "finished", "The managed service operation completed and its checkpoint sequence was verified."
		newStatus := "active"
		if operation == string(cloudcontracts.ServiceOperationStop) {
			newStatus = "stopped"
		}
		if !succeeded {
			taskStatus = "failed"
			outcome = "failed"
			checkpoint = "service_operation_failed"
			stepStatus = "failed"
			summary = "The managed service operation failed; the resource remains active, tracked and billable."
			newStatus = "degraded"
		}
		r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_operation_tasks SET task_status=$1,last_sequence=$2,last_checkpoint=$3,error_code=$4,lease_owner='',lease_token='',lease_until=0,last_error_code=$4,updated_at=$5 WHERE operation_id=$6 AND task_status IN('queued','running')`, taskStatus, result.LastSequence, result.LastCheckpoint, code, now, claim.ExecutionID)
		if err != nil {
			return err
		}
		if requireOneAffected(r) != nil {
			return ErrLeaseLost
		}
		r, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_commands SET state=$1,last_error_code=$2,updated_at=$3 WHERE command_id=$4 AND state IN('allocated','signed','indeterminate')`, map[bool]string{true: "accepted", false: "failed"}[succeeded], code, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if requireOneAffected(r) != nil {
			return ErrLeaseLost
		}
		r, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_services SET service_status=$1,revision=revision+1,updated_at=$2 WHERE service_id=$3 AND revision=$4 AND service_status=$5`, newStatus, now, serviceID, revision, status)
		if err != nil {
			return err
		}
		if requireOneAffected(r) != nil {
			return ErrLeaseLost
		}
		revision++
		if claim.Phase == runtime.RecipeInstallPhaseIssue {
			r, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET completed_at=$1,lease_owner='',lease_token='',lease_until=0,last_error_code=$2 WHERE outbox_id=$3 AND completed_at=0`, now, code, claim.OutboxID)
			if err != nil {
				return err
			}
			if requireOneAffected(r) != nil {
				return ErrLeaseLost
			}
		}
		r, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_jobs SET execution_status='finished',outcome_status=$1,checkpoint=$2,error_code=$3,revision=revision+1,updated_at=$4 WHERE job_id=$5 AND outcome_status='pending'`, outcome, checkpoint, code, now, claim.JobID)
		if err != nil {
			return err
		}
		if requireOneAffected(r) != nil {
			return ErrLeaseLost
		}
		_, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_job_steps SET status=$1,checkpoint=$2,error_code=$3,summary=$4,revision=revision+1,updated_at=$5 WHERE job_id=$6 AND step_id='service_operation'`, stepStatus, checkpoint, code, summary, now, claim.JobID)
		if err != nil {
			return err
		}
		servicePayload := map[string]any{"service_id": serviceID, "deployment_id": claim.DeploymentID, "recipe_id": recipeID, "name": name, "service_status": newStatus, "integration_status": integration, "revision": revision, "created_at": createdAt, "updated_at": now}
		if err = writeEventAndProjection(ctx, tx, stableID("cloud_event_", serviceID, fmt.Sprint(revision), newStatus), "cloud.service.changed", "service", serviceID, revision, servicePayload, now); err != nil {
			return err
		}
		var jobKind string
		var jobRevision, jobCreated int64
		if err = tx.QueryRowContext(ctx, `SELECT kind,revision,created_at FROM p2p_cloud_jobs WHERE job_id=$1`, claim.JobID).Scan(&jobKind, &jobRevision, &jobCreated); err != nil {
			return err
		}
		return writeEventAndProjection(ctx, tx, stableID("cloud_event_", claim.JobID, fmt.Sprint(jobRevision), checkpoint), "cloud.job.changed", "job", claim.JobID, jobRevision, map[string]any{"job_id": claim.JobID, "plan_id": claim.PlanID, "deployment_id": claim.DeploymentID, "kind": jobKind, "execution_status": "finished", "outcome_status": outcome, "checkpoint": checkpoint, "error_code": code, "revision": jobRevision, "created_at": jobCreated, "updated_at": now}, now)
	})
}

func (s *Store) DeferServiceOperation(ctx context.Context, claim runtime.RecipeInstallClaim, code string, available time.Time) error {
	return s.releaseServiceOperation(ctx, claim, code, available)
}
func (s *Store) ExpireServiceOperationCommand(ctx context.Context, claim runtime.RecipeInstallClaim) error {
	return s.withServiceOperationClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_commands SET state='expired',last_error_code='service_operation_command_expired',updated_at=$1 WHERE command_id=$2 AND state IN('allocated','signed','indeterminate')`, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if requireOneAffected(r) != nil {
			return ErrLeaseLost
		}
		return releaseServiceOperationTx(ctx, tx, claim, "service_operation_command_expired", now, now)
	})
}
func (s *Store) FailServiceOperation(ctx context.Context, claim runtime.RecipeInstallClaim, code string) error {
	return s.finishServiceOperation(ctx, claim, runtime.RecipeInstallResult{ExecutionID: claim.ExecutionID, DeploymentID: claim.DeploymentID, TaskID: claim.TaskID, Status: "failed", Attempt: claim.TaskAttempt}, code, false)
}
func (s *Store) releaseServiceOperation(ctx context.Context, claim runtime.RecipeInstallClaim, code string, available time.Time) error {
	return s.withServiceOperationClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		return releaseServiceOperationTx(ctx, tx, claim, code, available.UnixMilli(), now)
	})
}
func releaseServiceOperationTx(ctx context.Context, tx *sql.Tx, claim runtime.RecipeInstallClaim, code string, available, now int64) error {
	r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_operation_tasks SET available_at=$1,lease_owner='',lease_token='',lease_until=0,last_error_code=$2,updated_at=$3 WHERE operation_id=$4 AND task_status IN('queued','running')`, available, code, now, claim.ExecutionID)
	if err != nil {
		return err
	}
	if requireOneAffected(r) != nil {
		return ErrLeaseLost
	}
	if claim.OutboxID != "" {
		_, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET available_at=$1,lease_owner='',lease_token='',lease_until=0,last_error_code=$2 WHERE outbox_id=$3 AND completed_at=0`, available, code, claim.OutboxID)
	}
	return err
}

func (s *Store) withServiceOperationClaim(ctx context.Context, claim runtime.RecipeInstallClaim, run func(*sql.Tx, int64) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	now := s.now().UnixMilli()
	var leaseToken string
	err = tx.QueryRowContext(ctx, `SELECT task.lease_token FROM p2p_cloud_service_operation_tasks task
		WHERE task.operation_id=$1 AND task.execution_id=$1 AND task.deployment_id=$2 AND task.plan_id=$3 AND task.cloud_connection_id=$4
		AND task.instance_id=$5 AND task.task_id=$6 AND task.task_attempt=$7 AND task.manifest_digest=$8 AND task.input_digest=$9 AND task.job_id=$10
		AND EXISTS(SELECT 1 FROM p2p_cloud_recipe_install_commands command WHERE command.command_id=$11 AND command.execution_id=task.execution_id
			AND command.deployment_id=task.deployment_id AND command.task_id=task.task_id AND command.cloud_connection_id=task.cloud_connection_id
			AND command.action=$12 AND command.request_digest=$13 AND command.node_key_id=$14 AND command.expected_generation=$15 AND command.node_counter=$16)
		FOR UPDATE OF task`, claim.ExecutionID, claim.DeploymentID, claim.PlanID, claim.ConnectionID, claim.InstanceID, claim.TaskID, claim.TaskAttempt,
		claim.ManifestDigest, claim.InputDigest, claim.JobID, claim.Command.CommandID, claim.Command.Action, claim.Command.RequestDigest,
		claim.Command.NodeKeyID, claim.Command.ExpectedGeneration, claim.Command.NodeCounter).Scan(&leaseToken)
	if err != nil {
		return err
	}
	if leaseToken != claim.LeaseToken {
		return ErrLeaseLost
	}
	if err = run(tx, now); err != nil {
		return err
	}
	return tx.Commit()
}
