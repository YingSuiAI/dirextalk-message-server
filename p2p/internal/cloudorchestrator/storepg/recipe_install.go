package storepg

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var _ runtime.RecipeInstallStore = (*Store)(nil)

func (s *Store) ClaimRecipeInstall(ctx context.Context, workerID string, lease time.Duration) (runtime.RecipeInstallClaim, bool, error) {
	if s == nil || s.db == nil || strings.TrimSpace(workerID) == "" || lease <= 0 || lease > 5*time.Minute {
		return runtime.RecipeInstallClaim{}, false, errors.New("recipe install claim configuration is invalid")
	}
	if claim, found, err := s.claimRecipeInstallIssue(ctx, workerID, lease); err != nil || found {
		return claim, found, err
	}
	return s.claimRecipeInstallObserve(ctx, workerID, lease)
}

func (s *Store) claimRecipeInstallIssue(ctx context.Context, workerID string, lease time.Duration) (claim runtime.RecipeInstallClaim, found bool, err error) {
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
		return claim, false, errors.New("recipe install lease token is invalid")
	}
	var manifestJSON string
	err = tx.QueryRowContext(ctx, `
		SELECT outbox.outbox_id, outbox.kind, outbox.aggregate_type, outbox.aggregate_id,
			manifest.execution_id, manifest.deployment_id, manifest.plan_id, manifest.cloud_connection_id, manifest.manifest_digest, manifest.manifest_json,
			deployment.cloud_connection_id, connection.region, resource.instance_id, broker.broker_command_url, broker.node_key_id, broker.connection_generation, job.job_id
		FROM p2p_cloud_outbox outbox
		JOIN p2p_cloud_recipe_execution_manifests manifest ON manifest.execution_id = outbox.aggregate_id
		JOIN p2p_cloud_recipe_execution_approvals approval ON approval.execution_id = manifest.execution_id AND approval.status = 'approved'
		JOIN p2p_cloud_deployments deployment ON deployment.deployment_id = manifest.deployment_id
		JOIN p2p_cloud_plans plan ON plan.plan_id = manifest.plan_id
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id = manifest.cloud_connection_id
		JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id = manifest.cloud_connection_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id = manifest.deployment_id
		JOIN p2p_cloud_worker_bootstrap_observations observation ON observation.deployment_id = manifest.deployment_id
		JOIN p2p_cloud_jobs job ON job.job_id = approval.job_id AND job.kind = 'install'
		WHERE outbox.kind = $1 AND outbox.aggregate_type = 'recipe_execution' AND outbox.completed_at = 0 AND outbox.available_at <= $2 AND outbox.lease_until <= $2
			AND manifest.status = 'approved' AND deployment.execution_status = 'verifying' AND deployment.outcome_status = 'pending' AND deployment.resource_status = 'active'
			AND plan.status = 'approved' AND connection.status = 'active' AND connection.region = broker.broker_region
			AND resource.cloud_connection_id = manifest.cloud_connection_id AND resource.resource_status = 'active'
			AND observation.worker_session_state = 'active' AND observation.worker_lease_expires_at > $2
			AND job.execution_status = 'queued' AND job.outcome_status = 'pending' AND job.checkpoint = 'install_queued'
		ORDER BY outbox.available_at, outbox.created_at, outbox.outbox_id FOR UPDATE OF outbox SKIP LOCKED LIMIT 1
	`, runtime.RecipeInstallRequested, now).Scan(&claim.OutboxID, &claim.Kind, &claim.AggregateType, &claim.AggregateID,
		&claim.ExecutionID, &claim.DeploymentID, &claim.PlanID, &claim.ConnectionID, &claim.ManifestDigest, &manifestJSON,
		&claim.ConnectionID, &claim.Region, &claim.InstanceID, &claim.BrokerEndpoint, &claim.NodeKeyID, &claim.ExpectedGeneration, &claim.JobID)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return runtime.RecipeInstallClaim{}, false, nil
	}
	if err != nil {
		return claim, false, err
	}
	if err = decodeRecipeInstallManifest(manifestJSON, &claim.Manifest); err != nil || claim.Manifest.VerifyDigest(claim.ManifestDigest) != nil {
		return claim, false, errors.New("approved recipe manifest is invalid")
	}
	claim.InputDigest = recipeInstallInputDigest(claim.ManifestDigest)
	claim.TaskID = stableID("cloud_recipe_install_task_", claim.ExecutionID, claim.DeploymentID, claim.ManifestDigest, claim.InputDigest)
	claim.TaskAttempt = 1
	claim.Phase = runtime.RecipeInstallPhaseIssue
	claim.LeaseToken = token
	checkpoints, _ := json.Marshal(claim.Manifest.CheckpointSequence)
	_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_recipe_install_tasks
		(execution_id,task_id,deployment_id,plan_id,cloud_connection_id,instance_id,manifest_digest,input_digest,checkpoint_sequence_json,task_status,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'unissued',$10,$10) ON CONFLICT (execution_id) DO NOTHING`,
		claim.ExecutionID, claim.TaskID, claim.DeploymentID, claim.PlanID, claim.ConnectionID, claim.InstanceID, claim.ManifestDigest, claim.InputDigest, string(checkpoints), now)
	if err != nil {
		return claim, false, err
	}
	var taskID, manifestDigest, inputDigest, status string
	if err = tx.QueryRowContext(ctx, `SELECT task_id,manifest_digest,input_digest,task_status FROM p2p_cloud_recipe_install_tasks WHERE execution_id=$1 FOR UPDATE`, claim.ExecutionID).Scan(&taskID, &manifestDigest, &inputDigest, &status); err != nil || taskID != claim.TaskID || manifestDigest != claim.ManifestDigest || inputDigest != claim.InputDigest || status != "unissued" {
		return claim, false, errors.New("recipe install task binding conflict")
	}
	result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner=$1,lease_token=$2,lease_until=$3,attempts=attempts+1,last_error_code='' WHERE outbox_id=$4 AND completed_at=0 AND lease_until <= $5`, workerID, token, now+lease.Milliseconds(), claim.OutboxID, now)
	if err != nil {
		return claim, false, err
	}
	if err = requireOneAffected(result); err != nil {
		return claim, false, ErrLeaseLost
	}
	claim.IssueRequest = runtime.RecipeInstallIssueRequest{Schema: runtime.RecipeInstallIssueSchema, ExecutionID: claim.ExecutionID, DeploymentID: claim.DeploymentID, TaskID: claim.TaskID, TaskKind: "recipe_execution", RecipeExecutionManifestDigest: claim.ManifestDigest, InputDigest: claim.InputDigest, CheckpointSequence: append([]string(nil), claim.Manifest.CheckpointSequence...), Manifest: claim.Manifest}
	if claim.Command, err = prepareRecipeInstallCommand(ctx, tx, claim, now); err != nil {
		return claim, false, err
	}
	if err = tx.Commit(); err != nil {
		return claim, false, err
	}
	return claim, true, nil
}

func (s *Store) claimRecipeInstallObserve(ctx context.Context, workerID string, lease time.Duration) (claim runtime.RecipeInstallClaim, found bool, err error) {
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
		return claim, false, errors.New("recipe install lease token is invalid")
	}
	var manifestJSON string
	err = tx.QueryRowContext(ctx, `SELECT task.execution_id,task.deployment_id,task.plan_id,task.cloud_connection_id,connection.region,task.instance_id,task.task_id,task.task_attempt,task.manifest_digest,task.input_digest,
		manifest.manifest_json,broker.broker_command_url,broker.node_key_id,broker.connection_generation,job.job_id
		FROM p2p_cloud_recipe_install_tasks task JOIN p2p_cloud_recipe_execution_manifests manifest ON manifest.execution_id=task.execution_id
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=task.cloud_connection_id JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=task.cloud_connection_id
		JOIN p2p_cloud_worker_bootstrap_observations observation ON observation.deployment_id=task.deployment_id JOIN p2p_cloud_jobs job ON job.deployment_id=task.deployment_id AND job.kind='install'
		WHERE task.task_status IN('queued','running') AND task.available_at <= $1 AND task.lease_until <= $1 AND manifest.status='approved' AND connection.status='active' AND observation.worker_session_state='active' AND observation.worker_lease_expires_at > $1 AND job.outcome_status='pending'
		ORDER BY task.available_at,task.updated_at,task.execution_id FOR UPDATE OF task SKIP LOCKED LIMIT 1`, now).Scan(&claim.ExecutionID, &claim.DeploymentID, &claim.PlanID, &claim.ConnectionID, &claim.Region, &claim.InstanceID, &claim.TaskID, &claim.TaskAttempt, &claim.ManifestDigest, &claim.InputDigest, &manifestJSON, &claim.BrokerEndpoint, &claim.NodeKeyID, &claim.ExpectedGeneration, &claim.JobID)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return runtime.RecipeInstallClaim{}, false, nil
	}
	if err != nil {
		return claim, false, err
	}
	if err = decodeRecipeInstallManifest(manifestJSON, &claim.Manifest); err != nil || claim.Manifest.VerifyDigest(claim.ManifestDigest) != nil {
		return claim, false, errors.New("approved recipe manifest is invalid")
	}
	claim.Phase = runtime.RecipeInstallPhaseObserve
	claim.LeaseToken = token
	claim.ObserveRequest = runtime.RecipeInstallObserveRequest{DeploymentID: claim.DeploymentID, TaskID: claim.TaskID}
	result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_tasks SET lease_owner=$1,lease_token=$2,lease_until=$3,attempts=attempts+1,last_error_code='',updated_at=$4 WHERE execution_id=$5 AND lease_until <= $4`, workerID, token, now+lease.Milliseconds(), now, claim.ExecutionID)
	if err != nil {
		return claim, false, err
	}
	if err = requireOneAffected(result); err != nil {
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

func prepareRecipeInstallCommand(ctx context.Context, tx *sql.Tx, claim runtime.RecipeInstallClaim, now int64) (runtime.RecipeInstallCommand, error) {
	var requestDigest string
	var err error
	action := runtime.RecipeInstallIssueAction
	if claim.Phase == runtime.RecipeInstallPhaseIssue {
		requestDigest, err = claim.IssueRequest.Digest()
	} else {
		action = runtime.RecipeInstallObserveAction
		requestDigest, err = claim.ObserveRequest.Digest()
	}
	if err != nil {
		return runtime.RecipeInstallCommand{}, err
	}
	var c runtime.RecipeInstallCommand
	var issued, expires int64
	err = tx.QueryRowContext(ctx, `SELECT command_id,command_attempt,node_counter,canonical_payload_json,payload_sha256,request_sha256,signed_envelope_json,issued_at,expires_at,state FROM p2p_cloud_recipe_install_commands WHERE execution_id=$1 AND action=$2 AND request_digest=$3 AND state IN('allocated','signed','indeterminate') ORDER BY command_attempt DESC LIMIT 1`, claim.ExecutionID, action, requestDigest).Scan(&c.CommandID, &c.Attempt, &c.NodeCounter, &c.PayloadJSON, &c.PayloadSHA256, &c.RequestSHA256, &c.SignedEnvelope, &issued, &expires, &c.State)
	if err == nil {
		c.ExecutionID = claim.ExecutionID
		c.DeploymentID = claim.DeploymentID
		c.TaskID = claim.TaskID
		c.ConnectionID = claim.ConnectionID
		c.NodeKeyID = claim.NodeKeyID
		c.ExpectedGeneration = claim.ExpectedGeneration
		c.Action = action
		c.RequestDigest = requestDigest
		c.IssuedAt = time.UnixMilli(issued).UTC()
		c.ExpiresAt = time.UnixMilli(expires).UTC()
		return c, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return c, err
	}
	var attempt int
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(command_attempt),0)+1 FROM p2p_cloud_recipe_install_commands WHERE execution_id=$1 AND action=$2 AND request_digest=$3`, claim.ExecutionID, action, requestDigest).Scan(&attempt); err != nil {
		return c, err
	}
	var counter int64
	if err = tx.QueryRowContext(ctx, `UPDATE p2p_cloud_connection_brokers SET next_node_counter=next_node_counter+1,updated_at=$1 WHERE cloud_connection_id=$2 RETURNING next_node_counter`, now, claim.ConnectionID).Scan(&counter); err != nil {
		return c, err
	}
	c = runtime.RecipeInstallCommand{CommandID: stableID("cloud_recipe_install_command_", claim.ExecutionID, action, fmt.Sprint(attempt)), ExecutionID: claim.ExecutionID, DeploymentID: claim.DeploymentID, TaskID: claim.TaskID, ConnectionID: claim.ConnectionID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: counter, Attempt: attempt, Action: action, RequestDigest: requestDigest, State: "allocated"}
	_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_recipe_install_commands(command_id,execution_id,deployment_id,task_id,cloud_connection_id,request_digest,command_attempt,action,node_key_id,expected_generation,node_counter,state,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'allocated',$12,$12)`, c.CommandID, c.ExecutionID, c.DeploymentID, c.TaskID, c.ConnectionID, c.RequestDigest, c.Attempt, c.Action, c.NodeKeyID, c.ExpectedGeneration, c.NodeCounter, now)
	return c, err
}

func (s *Store) PersistRecipeInstallCommand(ctx context.Context, claim runtime.RecipeInstallClaim, signed runtime.SignedRecipeInstallCommand) error {
	return s.withRecipeInstallClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_commands SET canonical_payload_json=$1,payload_sha256=$2,request_sha256=$3,signed_envelope_json=$4,issued_at=$5,expires_at=$6,state='signed',updated_at=$7 WHERE command_id=$8 AND state='allocated'`, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256, signed.EnvelopeJSON, signed.IssuedAt.UnixMilli(), signed.ExpiresAt.UnixMilli(), now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		return requireOneAffected(result)
	})
}
func (s *Store) MarkRecipeInstallStarted(ctx context.Context, claim runtime.RecipeInstallClaim) error {
	return s.withRecipeInstallClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		if claim.Phase == runtime.RecipeInstallPhaseObserve {
			return nil
		}
		r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_jobs SET execution_status='installing',checkpoint='install_issuing',revision=revision+1,updated_at=$1 WHERE job_id=$2 AND outcome_status='pending'`, now, claim.JobID)
		if e != nil {
			return e
		}
		return requireOneAffected(r)
	})
}
func (s *Store) CommitRecipeInstall(ctx context.Context, claim runtime.RecipeInstallClaim, result runtime.RecipeInstallResult) error {
	return s.withRecipeInstallClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_commands SET state='accepted',updated_at=$1 WHERE command_id=$2 AND state IN('signed','indeterminate')`, now, claim.Command.CommandID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(r); e != nil {
			return e
		}
		r, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_tasks SET task_status=$1,task_attempt=$2,last_sequence=$3,last_checkpoint=$4,error_code=$5,available_at=$6,lease_owner='',lease_token='',lease_until=0,updated_at=$6 WHERE execution_id=$7`, result.Status, result.Attempt, result.LastSequence, result.LastCheckpoint, derefRecipeError(result.ErrorCode), now, claim.ExecutionID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(r); e != nil {
			return e
		}
		if claim.Phase == runtime.RecipeInstallPhaseIssue {
			r, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET completed_at=$1,lease_owner='',lease_token='',lease_until=0,last_error_code='' WHERE outbox_id=$2 AND completed_at=0`, now, claim.OutboxID)
			if e != nil {
				return e
			}
			if e = requireOneAffected(r); e != nil {
				return e
			}
		}
		if result.Status == "succeeded" {
			if e = ensureServiceReadinessTask(ctx, tx, claim, now); e != nil {
				return e
			}
		}
		execution, outcome, checkpoint, errorCode := "installing", "pending", "install_running", ""
		stepStatus := "running"
		switch result.Status {
		case "queued":
			checkpoint = "install_issued"
		case "running":
		case "succeeded":
			execution, outcome, checkpoint, stepStatus = "verifying", "pending", "readiness_queued", "running"
		case "failed":
			execution, outcome, checkpoint, stepStatus, errorCode = "finished", "failed", "install_failed", "failed", derefRecipeError(result.ErrorCode)
		case "interrupted":
			execution, outcome, checkpoint, stepStatus, errorCode = "finished", "interrupted", "install_interrupted", "interrupted", derefRecipeError(result.ErrorCode)
		}
		r, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_jobs SET execution_status=$1,outcome_status=$2,checkpoint=$3,error_code=$4,revision=revision+1,updated_at=$5 WHERE job_id=$6 AND outcome_status='pending'`, execution, outcome, checkpoint, errorCode, now, claim.JobID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(r); e != nil {
			return e
		}
		summary := "The sealed Recipe install task is tracked by digest and checkpoint only; service readiness remains unverified."
		if result.Status == "succeeded" {
			summary = "Recipe installation succeeded; a separate Stack-witnessed fixed readiness challenge is queued."
		}
		r, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_job_steps SET status=$1,checkpoint=$2,error_code=$3,summary=$4,revision=revision+1,updated_at=$5 WHERE job_id=$6 AND step_id='install'`, stepStatus, checkpoint, errorCode, summary, now, claim.JobID)
		if e != nil {
			return e
		}
		return requireOneAffected(r)
	})
}
func (s *Store) DeferRecipeInstall(ctx context.Context, claim runtime.RecipeInstallClaim, code string, available time.Time) error {
	return s.releaseRecipeInstall(ctx, claim, code, available, false)
}
func (s *Store) ExpireRecipeInstallCommand(ctx context.Context, claim runtime.RecipeInstallClaim) error {
	return s.withRecipeInstallClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_commands
			SET state='expired',last_error_code='recipe_install_command_expired',updated_at=$1
			WHERE command_id=$2 AND execution_id=$3 AND deployment_id=$4 AND task_id=$5 AND cloud_connection_id=$6
				AND action=$7 AND request_digest=$8 AND command_attempt=$9 AND state IN('allocated','signed','indeterminate')`,
			now, claim.Command.CommandID, claim.ExecutionID, claim.DeploymentID, claim.TaskID, claim.ConnectionID,
			claim.Command.Action, claim.Command.RequestDigest, claim.Command.Attempt)
		if err != nil {
			return err
		}
		if err = requireOneAffected(result); err != nil {
			return err
		}
		table, key, id := "p2p_cloud_recipe_install_tasks", "execution_id", claim.ExecutionID
		if claim.Phase == runtime.RecipeInstallPhaseIssue {
			table, key, id = "p2p_cloud_outbox", "outbox_id", claim.OutboxID
		}
		query := fmt.Sprintf("UPDATE %s SET available_at=$1,lease_owner='',lease_token='',lease_until=0,last_error_code='recipe_install_command_expired' WHERE %s=$2", table, key)
		result, err = tx.ExecContext(ctx, query, now, id)
		if err != nil {
			return err
		}
		return requireOneAffected(result)
	})
}
func (s *Store) FailRecipeInstall(ctx context.Context, claim runtime.RecipeInstallClaim, code string) error {
	return s.withRecipeInstallClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		r, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_install_tasks SET task_status='failed',error_code=$1,lease_owner='',lease_token='',lease_until=0,last_error_code=$1,updated_at=$2 WHERE execution_id=$3`, code, now, claim.ExecutionID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(r); err != nil {
			return err
		}
		if claim.Phase == runtime.RecipeInstallPhaseIssue {
			r, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET completed_at=$1,lease_owner='',lease_token='',lease_until=0,last_error_code=$2 WHERE outbox_id=$3 AND completed_at=0`, now, code, claim.OutboxID)
			if err != nil {
				return err
			}
			if err = requireOneAffected(r); err != nil {
				return err
			}
		}
		r, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_jobs SET execution_status='finished',outcome_status='failed',checkpoint='install_failed',error_code=$1,revision=revision+1,updated_at=$2 WHERE job_id=$3 AND outcome_status='pending'`, code, now, claim.JobID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(r); err != nil {
			return err
		}
		r, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_job_steps SET status='failed',checkpoint='install_failed',error_code=$1,summary=$2,revision=revision+1,updated_at=$3 WHERE job_id=$4 AND step_id='install'`, code, "The sealed Recipe install task was rejected before service readiness could be verified.", now, claim.JobID)
		if err != nil {
			return err
		}
		return requireOneAffected(r)
	})
}
func (s *Store) releaseRecipeInstall(ctx context.Context, claim runtime.RecipeInstallClaim, code string, available time.Time, failed bool) error {
	return s.withRecipeInstallClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		table, key := "p2p_cloud_recipe_install_tasks", "execution_id"
		id := claim.ExecutionID
		if claim.Phase == runtime.RecipeInstallPhaseIssue {
			table = "p2p_cloud_outbox"
			key = "outbox_id"
			id = claim.OutboxID
		}
		query := fmt.Sprintf("UPDATE %s SET available_at=$1,lease_owner='',lease_token='',lease_until=0,last_error_code=$2 WHERE %s=$3", table, key)
		if failed {
			query = fmt.Sprintf("UPDATE %s SET lease_owner='',lease_token='',lease_until=0,last_error_code=$1 WHERE %s=$2", table, key)
			_, e := tx.ExecContext(ctx, query, code, id)
			return e
		}
		_, e := tx.ExecContext(ctx, query, available.UnixMilli(), code, id)
		return e
	})
}

func (s *Store) withRecipeInstallClaim(ctx context.Context, claim runtime.RecipeInstallClaim, run func(*sql.Tx, int64) error) (err error) {
	if runtime.ValidateRecipeInstallClaim(claim) != nil {
		return ErrLeaseLost
	}
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
	var token string
	var until int64
	if claim.Phase == runtime.RecipeInstallPhaseIssue {
		err = tx.QueryRowContext(ctx, `SELECT lease_token,lease_until FROM p2p_cloud_outbox WHERE outbox_id=$1 FOR UPDATE`, claim.OutboxID).Scan(&token, &until)
	} else {
		err = tx.QueryRowContext(ctx, `SELECT lease_token,lease_until FROM p2p_cloud_recipe_install_tasks WHERE execution_id=$1 FOR UPDATE`, claim.ExecutionID).Scan(&token, &until)
	}
	if err != nil || token != claim.LeaseToken || until <= now {
		return ErrLeaseLost
	}
	if err = verifyRecipeInstallBindings(ctx, tx, claim, now); err != nil {
		return err
	}
	if err = run(tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

func verifyRecipeInstallBindings(ctx context.Context, tx *sql.Tx, claim runtime.RecipeInstallClaim, now int64) error {
	var manifestDeployment, manifestPlan, manifestConnection, manifestDigest, manifestStatus string
	var deploymentPlan, deploymentConnection, deploymentExecution, deploymentOutcome, deploymentResource string
	var connectionStatus, region, endpoint, nodeKey string
	var generation int64
	var resourceConnection, resourceStatus, instanceID, workerState string
	var workerLease int64
	var jobKind, jobOutcome string
	err := tx.QueryRowContext(ctx, `SELECT manifest.deployment_id,manifest.plan_id,manifest.cloud_connection_id,manifest.manifest_digest,manifest.status,
		deployment.plan_id,deployment.cloud_connection_id,deployment.execution_status,deployment.outcome_status,deployment.resource_status,
		connection.status,connection.region,broker.broker_command_url,broker.node_key_id,broker.connection_generation,
		resource.cloud_connection_id,resource.resource_status,resource.instance_id,observation.worker_session_state,observation.worker_lease_expires_at,job.kind,job.outcome_status
		FROM p2p_cloud_recipe_execution_manifests manifest JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=manifest.deployment_id
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=manifest.cloud_connection_id JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=manifest.cloud_connection_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=manifest.deployment_id JOIN p2p_cloud_worker_bootstrap_observations observation ON observation.deployment_id=manifest.deployment_id
		JOIN p2p_cloud_jobs job ON job.job_id=$2 WHERE manifest.execution_id=$1 FOR UPDATE OF manifest,deployment,connection,broker,resource,observation,job`, claim.ExecutionID, claim.JobID).Scan(
		&manifestDeployment, &manifestPlan, &manifestConnection, &manifestDigest, &manifestStatus, &deploymentPlan, &deploymentConnection, &deploymentExecution, &deploymentOutcome, &deploymentResource,
		&connectionStatus, &region, &endpoint, &nodeKey, &generation, &resourceConnection, &resourceStatus, &instanceID, &workerState, &workerLease, &jobKind, &jobOutcome)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrLeaseLost
		}
		return err
	}
	if manifestDeployment != claim.DeploymentID || manifestPlan != claim.PlanID || manifestConnection != claim.ConnectionID || manifestDigest != claim.ManifestDigest || manifestStatus != "approved" ||
		deploymentPlan != claim.PlanID || deploymentConnection != claim.ConnectionID || deploymentExecution != "verifying" || deploymentOutcome != "pending" || deploymentResource != "active" ||
		connectionStatus != "active" || region != claim.Region || endpoint != claim.BrokerEndpoint || nodeKey != claim.NodeKeyID || generation != claim.ExpectedGeneration ||
		resourceConnection != claim.ConnectionID || resourceStatus != "active" || instanceID != claim.InstanceID || workerState != "active" || workerLease <= now || jobKind != "install" || jobOutcome != "pending" {
		return ErrLeaseLost
	}
	return nil
}
func decodeRecipeInstallManifest(raw string, target *cloudcontracts.RecipeExecutionManifestV1) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("recipe manifest contains trailing JSON")
	}
	return target.Validate()
}
func recipeInstallInputDigest(manifestDigest string) string {
	sum := sha256.Sum256([]byte("dirextalk.recipe-install-input/v1\x00" + manifestDigest))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ensureServiceReadinessTask(ctx context.Context, tx *sql.Tx, claim runtime.RecipeInstallClaim, now int64) error {
	serviceID := stableID("cloud_service_", claim.DeploymentID, claim.ExecutionID, claim.ManifestDigest)
	taskID := stableID("cloud_service_readiness_task_", serviceID, claim.ManifestDigest)
	semanticDigest := cloudcontracts.FixedReadinessEvidenceDigestV1
	_, err := tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_readiness_tasks
		(task_id,execution_id,deployment_id,service_id,cloud_connection_id,instance_id,recipe_execution_manifest_digest,install_evidence_digest,semantic_expectation_digest,task_status,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$7,$8,'unissued',$9,$9) ON CONFLICT (task_id) DO NOTHING`,
		taskID, claim.ExecutionID, claim.DeploymentID, serviceID, claim.ConnectionID, claim.InstanceID, claim.ManifestDigest, semanticDigest, now)
	if err != nil {
		return err
	}
	var existingExecution, existingDeployment, existingService, existingManifest, existingInstall, existingSemantic, status string
	if err = tx.QueryRowContext(ctx, `SELECT execution_id,deployment_id,service_id,recipe_execution_manifest_digest,install_evidence_digest,semantic_expectation_digest,task_status FROM p2p_cloud_service_readiness_tasks WHERE task_id=$1 FOR UPDATE`, taskID).Scan(&existingExecution, &existingDeployment, &existingService, &existingManifest, &existingInstall, &existingSemantic, &status); err != nil {
		return err
	}
	if existingExecution != claim.ExecutionID || existingDeployment != claim.DeploymentID || existingService != serviceID || existingManifest != claim.ManifestDigest || existingInstall != claim.ManifestDigest || existingSemantic != semanticDigest ||
		(status != "unissued" && status != "queued" && status != "running" && status != "succeeded" && status != "failed" && status != "interrupted") {
		return errors.New("service readiness task binding conflict")
	}
	outboxID := stableID("cloud_service_readiness_outbox_", taskID)
	payload, _ := json.Marshal(map[string]string{"task_id": taskID})
	_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,available_at,created_at)
		VALUES($1,$2,'service_readiness_task',$3,$4,$5,$5) ON CONFLICT (outbox_id) DO NOTHING`,
		outboxID, "cloud.service_readiness.requested", taskID, string(payload), now)
	return err
}
func derefRecipeError(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
