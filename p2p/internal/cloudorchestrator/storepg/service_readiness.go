package storepg

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var _ runtime.ServiceReadinessStore = (*Store)(nil)

const serviceReadinessObserveDelay = 5 * time.Second

func (s *Store) ClaimServiceReadiness(ctx context.Context, workerID string, lease time.Duration) (runtime.ServiceReadinessClaim, bool, error) {
	if s == nil || s.db == nil || strings.TrimSpace(workerID) == "" || lease <= 0 || lease > 5*time.Minute {
		return runtime.ServiceReadinessClaim{}, false, errors.New("service readiness claim configuration is invalid")
	}
	if claim, found, err := s.claimServiceReadinessIssue(ctx, strings.TrimSpace(workerID), lease); err != nil || found {
		return claim, found, err
	}
	return s.claimServiceReadinessObserve(ctx, strings.TrimSpace(workerID), lease)
}

func (s *Store) claimServiceReadinessIssue(ctx context.Context, workerID string, lease time.Duration) (claim runtime.ServiceReadinessClaim, found bool, err error) {
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
		return claim, false, errors.New("service readiness lease token is invalid")
	}
	var semanticProbeJSON string
	err = tx.QueryRowContext(ctx, `SELECT outbox.outbox_id,outbox.kind,outbox.aggregate_type,outbox.aggregate_id,
		task.execution_id,task.deployment_id,task.service_id,task.task_id,task.cloud_connection_id,connection.region,task.instance_id,
		task.recipe_execution_manifest_digest,task.install_evidence_digest,task.artifact_digest,task.semantic_probe_json,task.semantic_expectation_digest,task.task_attempt,task.purpose,task.restore_id,task.job_id,
		task.monitor_generation,task.monitor_service_revision,task.monitor_deployment_revision,task.monitor_resource_status,task.worker_lease_epoch,
		broker.broker_command_url,broker.node_key_id,broker.connection_generation
		FROM p2p_cloud_outbox outbox JOIN p2p_cloud_service_readiness_tasks task ON task.task_id=outbox.aggregate_id
		JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=task.deployment_id
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=task.cloud_connection_id
		JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=task.cloud_connection_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=task.deployment_id
		JOIN p2p_cloud_worker_bootstrap_observations observation ON observation.deployment_id=task.deployment_id
		JOIN p2p_cloud_recipe_install_tasks install ON install.execution_id=task.execution_id
		LEFT JOIN p2p_cloud_jobs job ON job.job_id=task.job_id
		LEFT JOIN p2p_cloud_service_restores restore ON restore.restore_id=task.restore_id
		LEFT JOIN p2p_cloud_services service ON service.service_id=task.service_id
		LEFT JOIN p2p_cloud_service_monitors monitor ON monitor.service_id=task.service_id
		WHERE outbox.kind=$1 AND outbox.aggregate_type='service_readiness_task' AND outbox.completed_at=0 AND outbox.available_at<=$2 AND outbox.lease_until<=$2
		AND task.task_status='unissued'
		AND ((task.purpose='install' AND task.restore_id='' AND deployment.execution_status='verifying' AND deployment.outcome_status='pending' AND deployment.resource_status='active' AND job.kind='install' AND job.execution_status='verifying' AND job.outcome_status='pending' AND job.checkpoint IN('readiness_queued','readiness_issuing'))
		 OR (task.purpose='restore' AND restore.restore_id=task.restore_id AND restore.restore_status='verifying' AND job.kind='restore' AND job.execution_status='verifying' AND job.outcome_status='pending' AND job.checkpoint IN('restore_readiness_queued','restore_readiness_issuing'))
		 OR (task.purpose='monitor' AND task.restore_id='' AND task.job_id='' AND service.service_id=task.service_id AND service.deployment_id=task.deployment_id AND service.service_status IN('active','experimental','degraded') AND service.revision=task.monitor_service_revision
			AND monitor.current_task_id=task.task_id AND monitor.monitor_status='checking' AND monitor.generation=task.monitor_generation
			AND deployment.execution_status='finished' AND deployment.outcome_status='succeeded' AND deployment.resource_status IN('active','retained_tracked') AND deployment.revision=task.monitor_deployment_revision
			AND resource.resource_status=task.monitor_resource_status AND observation.worker_lease_epoch=task.worker_lease_epoch))
		AND connection.status='active' AND connection.region=broker.broker_region AND resource.cloud_connection_id=task.cloud_connection_id AND resource.resource_status IN('active','retained_tracked')
		AND observation.worker_session_state='active' AND observation.worker_lease_expires_at>$2 AND install.task_status='succeeded'
		ORDER BY outbox.available_at,outbox.created_at,outbox.outbox_id FOR UPDATE OF outbox SKIP LOCKED LIMIT 1`, cloudmodule.OutboxKindServiceReadinessRequested, now).Scan(
		&claim.OutboxID, &claim.Kind, &claim.AggregateType, &claim.AggregateID, &claim.ExecutionID, &claim.DeploymentID, &claim.ServiceID, &claim.TaskID, &claim.ConnectionID, &claim.Region, &claim.InstanceID,
		&claim.RecipeExecutionManifestDigest, &claim.InstallEvidenceDigest, &claim.ArtifactDigest, &semanticProbeJSON, &claim.SemanticExpectationDigest, &claim.TaskAttempt, &claim.Purpose, &claim.RestoreID, &claim.JobID,
		&claim.MonitorGeneration, &claim.MonitorServiceRevision, &claim.MonitorDeploymentRevision, &claim.MonitorResourceStatus, &claim.WorkerLeaseEpoch,
		&claim.BrokerEndpoint, &claim.NodeKeyID, &claim.ExpectedGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return runtime.ServiceReadinessClaim{}, false, nil
	}
	if err != nil {
		return claim, false, err
	}
	if claim.SemanticProbe, err = decodeServiceReadinessProbe(semanticProbeJSON); err != nil {
		return claim, false, err
	}
	claim.Phase = runtime.ServiceReadinessPhaseIssue
	claim.LeaseToken = token
	claim.IssueRequest.Schema = runtime.ServiceReadinessIssueSchema
	claim.IssueRequest.ExecutionID = claim.ExecutionID
	claim.IssueRequest.DeploymentID = claim.DeploymentID
	claim.IssueRequest.ServiceID = claim.ServiceID
	claim.IssueRequest.TaskID = claim.TaskID
	claim.IssueRequest.ProbeKind = runtime.ServiceReadinessProbeKind
	claim.IssueRequest.RecipeExecutionManifestDigest = claim.RecipeExecutionManifestDigest
	claim.IssueRequest.InstallEvidenceDigest = claim.InstallEvidenceDigest
	claim.IssueRequest.ArtifactDigest = claim.ArtifactDigest
	claim.IssueRequest.SemanticProbe = claim.SemanticProbe
	claim.IssueRequest.SemanticExpectationDigest = claim.SemanticExpectationDigest
	result, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET lease_owner=$1,lease_token=$2,lease_until=$3,attempts=attempts+1,last_error_code='' WHERE outbox_id=$4 AND completed_at=0 AND lease_until<=$5`, workerID, token, now+lease.Milliseconds(), claim.OutboxID, now)
	if e != nil {
		return claim, false, e
	}
	if e = requireOneAffected(result); e != nil {
		return claim, false, ErrLeaseLost
	}
	if claim.Command, err = prepareServiceReadinessCommand(ctx, tx, claim, now); err != nil {
		return claim, false, err
	}
	if err = tx.Commit(); err != nil {
		return claim, false, err
	}
	return claim, true, nil
}

func (s *Store) claimServiceReadinessObserve(ctx context.Context, workerID string, lease time.Duration) (claim runtime.ServiceReadinessClaim, found bool, err error) {
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
		return claim, false, errors.New("service readiness lease token is invalid")
	}
	var semanticProbeJSON string
	err = tx.QueryRowContext(ctx, `SELECT task.execution_id,task.deployment_id,task.service_id,task.task_id,task.cloud_connection_id,connection.region,task.instance_id,
		task.recipe_execution_manifest_digest,task.install_evidence_digest,task.artifact_digest,task.semantic_probe_json,task.semantic_expectation_digest,task.task_attempt,task.purpose,task.restore_id,task.job_id,
		task.monitor_generation,task.monitor_service_revision,task.monitor_deployment_revision,task.monitor_resource_status,task.worker_lease_epoch,
		broker.broker_command_url,broker.node_key_id,broker.connection_generation
		FROM p2p_cloud_service_readiness_tasks task JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=task.deployment_id
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=task.cloud_connection_id JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=task.cloud_connection_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=task.deployment_id JOIN p2p_cloud_worker_bootstrap_observations observation ON observation.deployment_id=task.deployment_id
		JOIN p2p_cloud_recipe_install_tasks install ON install.execution_id=task.execution_id
		LEFT JOIN p2p_cloud_jobs job ON job.job_id=task.job_id LEFT JOIN p2p_cloud_service_restores restore ON restore.restore_id=task.restore_id
		LEFT JOIN p2p_cloud_services service ON service.service_id=task.service_id LEFT JOIN p2p_cloud_service_monitors monitor ON monitor.service_id=task.service_id
		WHERE task.task_status IN('queued','running') AND task.available_at<=$1 AND task.lease_until<=$1
		AND ((task.purpose='install' AND task.restore_id='' AND deployment.execution_status='verifying' AND deployment.outcome_status='pending' AND deployment.resource_status='active' AND job.kind='install' AND job.execution_status='verifying' AND job.outcome_status='pending' AND job.checkpoint IN('readiness_queued','readiness_issuing','readiness_issued','readiness_running'))
		 OR (task.purpose='restore' AND restore.restore_id=task.restore_id AND restore.restore_status='verifying' AND job.kind='restore' AND job.execution_status='verifying' AND job.outcome_status='pending' AND job.checkpoint IN('restore_readiness_queued','restore_readiness_issuing','restore_readiness_issued','restore_readiness_running'))
		 OR (task.purpose='monitor' AND task.restore_id='' AND task.job_id='' AND service.service_id=task.service_id AND service.deployment_id=task.deployment_id AND service.service_status IN('active','experimental','degraded') AND service.revision=task.monitor_service_revision
			AND monitor.current_task_id=task.task_id AND monitor.monitor_status='checking' AND monitor.generation=task.monitor_generation
			AND deployment.execution_status='finished' AND deployment.outcome_status='succeeded' AND deployment.resource_status IN('active','retained_tracked') AND deployment.revision=task.monitor_deployment_revision
			AND resource.resource_status=task.monitor_resource_status AND observation.worker_lease_epoch=task.worker_lease_epoch))
		AND connection.status='active' AND connection.region=broker.broker_region
		AND resource.cloud_connection_id=task.cloud_connection_id AND resource.instance_id=task.instance_id AND resource.resource_status IN('active','retained_tracked')
		AND observation.worker_session_state='active' AND observation.worker_lease_expires_at>$1 AND install.task_status='succeeded'
		ORDER BY task.available_at,task.updated_at,task.task_id FOR UPDATE OF task SKIP LOCKED LIMIT 1`, now).Scan(&claim.ExecutionID, &claim.DeploymentID, &claim.ServiceID, &claim.TaskID, &claim.ConnectionID, &claim.Region, &claim.InstanceID, &claim.RecipeExecutionManifestDigest, &claim.InstallEvidenceDigest, &claim.ArtifactDigest, &semanticProbeJSON, &claim.SemanticExpectationDigest, &claim.TaskAttempt, &claim.Purpose, &claim.RestoreID, &claim.JobID,
		&claim.MonitorGeneration, &claim.MonitorServiceRevision, &claim.MonitorDeploymentRevision, &claim.MonitorResourceStatus, &claim.WorkerLeaseEpoch,
		&claim.BrokerEndpoint, &claim.NodeKeyID, &claim.ExpectedGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		_ = tx.Rollback()
		return runtime.ServiceReadinessClaim{}, false, nil
	}
	if err != nil {
		return claim, false, err
	}
	if claim.SemanticProbe, err = decodeServiceReadinessProbe(semanticProbeJSON); err != nil {
		return claim, false, err
	}
	claim.IssueRequest = runtime.ServiceReadinessIssueRequest{}
	claim.Phase = runtime.ServiceReadinessPhaseObserve
	claim.LeaseToken = token
	claim.ObserveRequest = runtime.ServiceReadinessObserveRequest{DeploymentID: claim.DeploymentID, ServiceID: claim.ServiceID, TaskID: claim.TaskID}
	result, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_tasks SET lease_owner=$1,lease_token=$2,lease_until=$3,attempts=attempts+1,last_error_code='',updated_at=$4 WHERE task_id=$5 AND lease_until<=$4`, workerID, token, now+lease.Milliseconds(), now, claim.TaskID)
	if e != nil {
		return claim, false, e
	}
	if e = requireOneAffected(result); e != nil {
		return claim, false, ErrLeaseLost
	}
	if claim.Command, err = prepareServiceReadinessCommand(ctx, tx, claim, now); err != nil {
		return claim, false, err
	}
	if err = tx.Commit(); err != nil {
		return claim, false, err
	}
	return claim, true, nil
}

func decodeServiceReadinessProbe(raw string) (cloudcontracts.OCIServiceLoopbackProbeV1, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var probe cloudcontracts.OCIServiceLoopbackProbeV1
	if err := decoder.Decode(&probe); err != nil || probe.Validate() != nil {
		return cloudcontracts.OCIServiceLoopbackProbeV1{}, errors.New("service readiness semantic probe is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return cloudcontracts.OCIServiceLoopbackProbeV1{}, errors.New("service readiness semantic probe is invalid")
	}
	return probe, nil
}

func prepareServiceReadinessCommand(ctx context.Context, tx *sql.Tx, claim runtime.ServiceReadinessClaim, now int64) (runtime.ServiceReadinessCommand, error) {
	action := runtime.ServiceReadinessIssueAction
	digest, err := claim.IssueRequest.Digest()
	if claim.Phase == runtime.ServiceReadinessPhaseObserve {
		action = runtime.ServiceReadinessObserveAction
		digest, err = claim.ObserveRequest.Digest()
	}
	if err != nil {
		return runtime.ServiceReadinessCommand{}, err
	}
	var c runtime.ServiceReadinessCommand
	var issued, expires int64
	err = tx.QueryRowContext(ctx, `SELECT command_id,command_attempt,node_counter,canonical_payload_json,payload_sha256,request_sha256,signed_envelope_json,issued_at,expires_at,state FROM p2p_cloud_service_readiness_commands WHERE task_id=$1 AND action=$2 AND request_digest=$3 AND state IN('allocated','signed','indeterminate') ORDER BY command_attempt DESC LIMIT 1`, claim.TaskID, action, digest).Scan(&c.CommandID, &c.Attempt, &c.NodeCounter, &c.PayloadJSON, &c.PayloadSHA256, &c.RequestSHA256, &c.SignedEnvelope, &issued, &expires, &c.State)
	if err == nil {
		c.ExecutionID = claim.ExecutionID
		c.DeploymentID = claim.DeploymentID
		c.ServiceID = claim.ServiceID
		c.TaskID = claim.TaskID
		c.ConnectionID = claim.ConnectionID
		c.NodeKeyID = claim.NodeKeyID
		c.ExpectedGeneration = claim.ExpectedGeneration
		c.Action = action
		c.RequestDigest = digest
		c.IssuedAt = time.UnixMilli(issued).UTC()
		c.ExpiresAt = time.UnixMilli(expires).UTC()
		return c, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return c, err
	}
	var attempt int
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(command_attempt),0)+1 FROM p2p_cloud_service_readiness_commands WHERE task_id=$1 AND action=$2 AND request_digest=$3`, claim.TaskID, action, digest).Scan(&attempt); err != nil {
		return c, err
	}
	var counter int64
	if err = tx.QueryRowContext(ctx, `UPDATE p2p_cloud_connection_brokers SET next_node_counter=next_node_counter+1,updated_at=$1 WHERE cloud_connection_id=$2 RETURNING next_node_counter`, now, claim.ConnectionID).Scan(&counter); err != nil {
		return c, err
	}
	c = runtime.ServiceReadinessCommand{CommandID: stableID("cloud_service_readiness_command_", claim.TaskID, action, fmt.Sprint(attempt)), ExecutionID: claim.ExecutionID, DeploymentID: claim.DeploymentID, ServiceID: claim.ServiceID, TaskID: claim.TaskID, ConnectionID: claim.ConnectionID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: counter, Attempt: attempt, Action: action, RequestDigest: digest, State: "allocated"}
	_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_readiness_commands(command_id,task_id,execution_id,deployment_id,service_id,cloud_connection_id,request_digest,command_attempt,action,node_key_id,expected_generation,node_counter,state,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'allocated',$13,$13)`, c.CommandID, c.TaskID, c.ExecutionID, c.DeploymentID, c.ServiceID, c.ConnectionID, c.RequestDigest, c.Attempt, c.Action, c.NodeKeyID, c.ExpectedGeneration, c.NodeCounter, now)
	return c, err
}

func (s *Store) PersistServiceReadinessCommand(ctx context.Context, claim runtime.ServiceReadinessClaim, signed runtime.SignedServiceReadinessCommand) error {
	if err := validatePersistedServiceReadinessCommand(claim, signed); err != nil {
		return err
	}
	return s.withServiceReadinessClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_commands SET canonical_payload_json=$1,payload_sha256=$2,request_sha256=$3,signed_envelope_json=$4,issued_at=$5,expires_at=$6,state='signed',updated_at=$7 WHERE command_id=$8 AND state='allocated'`, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256, signed.EnvelopeJSON, signed.IssuedAt.UnixMilli(), signed.ExpiresAt.UnixMilli(), now, claim.Command.CommandID)
		if e != nil {
			return e
		}
		return requireOneAffected(r)
	})
}
func (s *Store) MarkServiceReadinessStarted(ctx context.Context, claim runtime.ServiceReadinessClaim) error {
	return s.withServiceReadinessClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		if claim.Phase == runtime.ServiceReadinessPhaseObserve || claim.Purpose == "monitor" {
			return nil
		}
		var planID, kind string
		if err := tx.QueryRowContext(ctx, `SELECT plan_id,kind FROM p2p_cloud_jobs WHERE job_id=$1 AND deployment_id=$2 AND execution_status='verifying' AND outcome_status='pending'`, claim.JobID, claim.DeploymentID).Scan(&planID, &kind); err != nil {
			return err
		}
		checkpoint, summary := "readiness_issuing", "A separate Stack-witnessed fixed readiness challenge is being issued; no Service is active yet."
		if claim.Purpose == "restore" {
			checkpoint, summary = "restore_readiness_issuing", "A separate Stack-witnessed semantic readiness challenge is verifying the restored service."
		}
		if kind != claim.Purpose {
			return ErrLeaseLost
		}
		_, err := transitionCloudJob(ctx, tx, claim.JobID, planID, claim.DeploymentID, kind, kind, now, researchJobTransition{execution: "verifying", outcome: "pending", checkpoint: checkpoint, stepStatus: "running", stepSummary: summary})
		return err
	})
}

func (s *Store) CommitServiceReadiness(ctx context.Context, claim runtime.ServiceReadinessClaim, result runtime.ServiceReadinessResult) (err error) {
	err = s.withServiceReadinessClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		if runtime.ValidateServiceReadinessResult(claim, result, time.UnixMilli(now).UTC()) != nil {
			return errors.New("invalid service readiness result")
		}
		var signed runtime.SignedServiceReadinessCommand
		var issued, expires int64
		var commandState string
		if e := tx.QueryRowContext(ctx, `SELECT canonical_payload_json,payload_sha256,request_sha256,signed_envelope_json,issued_at,expires_at,state FROM p2p_cloud_service_readiness_commands WHERE command_id=$1 FOR UPDATE`, claim.Command.CommandID).Scan(&signed.PayloadJSON, &signed.PayloadSHA256, &signed.RequestSHA256, &signed.EnvelopeJSON, &issued, &expires, &commandState); e != nil {
			return e
		}
		signed.IssuedAt = time.UnixMilli(issued).UTC()
		signed.ExpiresAt = time.UnixMilli(expires).UTC()
		if (commandState != "signed" && commandState != "indeterminate" && commandState != "accepted") || validatePersistedServiceReadinessCommand(claim, signed) != nil {
			return errors.New("persisted service readiness command is invalid")
		}
		var status string
		var attempt, sequence int64
		var checkpoint, challenge, semantic, stack, errorCode string
		if e := tx.QueryRowContext(ctx, `SELECT task_status,task_attempt,last_sequence,checkpoint,challenge_digest,semantic_evidence_digest,stack_observation_digest,error_code FROM p2p_cloud_service_readiness_tasks WHERE task_id=$1 FOR UPDATE`, claim.TaskID).Scan(&status, &attempt, &sequence, &checkpoint, &challenge, &semantic, &stack, &errorCode); e != nil {
			return e
		}
		if !serviceReadinessTransition(status, attempt, sequence, checkpoint, challenge, semantic, stack, errorCode, result) {
			return errors.New("service readiness result is stale")
		}
		r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_commands SET state='accepted',last_error_code='',updated_at=$1 WHERE command_id=$2 AND state IN('signed','indeterminate','accepted')`, now, claim.Command.CommandID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(r); e != nil {
			return e
		}
		next := int64(0)
		if result.Status == "queued" || result.Status == "running" {
			next = time.UnixMilli(now).Add(serviceReadinessObserveDelay).UnixMilli()
		}
		r, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_tasks SET task_status=$1,task_attempt=$2,last_sequence=$3,checkpoint=$4,challenge_digest=$5,semantic_evidence_digest=$6,stack_observation_digest=$7,error_code=$8,available_at=$9,lease_owner='',lease_token='',lease_until=0,last_error_code='',updated_at=$10 WHERE task_id=$11`, result.Status, result.Attempt, result.LastSequence, result.Checkpoint, derefReadiness(result.ChallengeDigest), derefReadiness(result.SemanticEvidenceDigest), derefReadiness(result.StackObservationDigest), derefReadiness(result.ErrorCode), next, now, claim.TaskID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(r); e != nil {
			return e
		}
		if claim.Phase == runtime.ServiceReadinessPhaseIssue {
			r, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET completed_at=$1,lease_owner='',lease_token='',lease_until=0,last_error_code='' WHERE outbox_id=$2 AND completed_at=0`, now, claim.OutboxID)
			if e != nil {
				return e
			}
			if e = requireOneAffected(r); e != nil {
				return e
			}
		}
		if claim.Purpose == "restore" {
			return commitRestoreReadinessResult(ctx, tx, claim, result, now)
		}
		if claim.Purpose == "monitor" {
			return commitServiceMonitorReadinessResult(ctx, tx, claim, result, now)
		}
		jobExecution, jobOutcome, jobCheckpoint, stepStatus, summary := "verifying", "pending", "readiness_running", "running", "The Stack-witnessed fixed readiness challenge is pending; no experimental Service is active yet."
		if result.Status == "queued" {
			jobCheckpoint = "readiness_issued"
		}
		if result.Status == "succeeded" {
			jobExecution, jobOutcome, jobCheckpoint, stepStatus, summary = "finished", "succeeded", "readiness_verified", "finished", "Stack-witnessed fixed readiness passed; the Service is experimental and has not been accepted as managed."
			var recipeID, recipeName string
			if e = tx.QueryRowContext(ctx, `SELECT recipe.recipe_id,recipe.name FROM p2p_cloud_service_readiness_tasks task
				JOIN p2p_cloud_recipe_execution_manifests manifest ON manifest.execution_id=task.execution_id
				JOIN p2p_cloud_plans plan ON plan.plan_id=manifest.plan_id
				JOIN p2p_cloud_recipes recipe ON recipe.digest=plan.recipe_digest WHERE task.task_id=$1`, claim.TaskID).Scan(&recipeID, &recipeName); e != nil {
				return e
			}
			r, e = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_services(service_id,deployment_id,recipe_id,name,service_status,integration_status,revision,created_at,updated_at) VALUES($1,$2,$3,$4,'experimental','not_requested',1,$5,$5)`, claim.ServiceID, claim.DeploymentID, recipeID, recipeName, now)
			if e != nil {
				return e
			}
			if e = requireOneAffected(r); e != nil {
				return e
			}
			if e = writeEventAndProjection(ctx, tx, stableID("cloud_event_", claim.ServiceID, "1", "experimental"), "cloud.service.changed", "service", claim.ServiceID, 1,
				map[string]any{"service_id": claim.ServiceID, "deployment_id": claim.DeploymentID, "recipe_id": recipeID, "name": recipeName, "service_status": "experimental", "integration_status": "not_requested", "revision": int64(1), "created_at": now, "updated_at": now}, now); e != nil {
				return e
			}
			var planID, connectionID string
			if e = tx.QueryRowContext(ctx, `SELECT plan_id,cloud_connection_id FROM p2p_cloud_deployments WHERE deployment_id=$1`, claim.DeploymentID).Scan(&planID, &connectionID); e != nil {
				return e
			}
			r, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_deployment_resources SET resource_status='retained_tracked',updated_at=$1 WHERE deployment_id=$2 AND cloud_connection_id=$3 AND instance_id=$4 AND resource_status='active'`, now, claim.DeploymentID, claim.ConnectionID, claim.InstanceID)
			if e != nil {
				return e
			}
			if e = requireOneAffected(r); e != nil {
				return e
			}
			if _, e = transitionDeployment(ctx, tx, claim.DeploymentID, planID, connectionID, now, "finished", "succeeded", "retained_tracked"); e != nil {
				return e
			}
		} else if result.Status == "failed" || result.Status == "interrupted" {
			jobExecution, jobOutcome, jobCheckpoint, stepStatus, summary = "finished", result.Status, "readiness_"+result.Status, result.Status, "Service readiness did not pass; resources remain retained and no Service was activated."
			r, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_deployment_resources SET resource_status='retained_tracked',updated_at=$1 WHERE deployment_id=$2 AND cloud_connection_id=$3 AND instance_id=$4 AND resource_status='active'`, now, claim.DeploymentID, claim.ConnectionID, claim.InstanceID)
			if e != nil {
				return e
			}
			if e = requireOneAffected(r); e != nil {
				return e
			}
			var planID string
			if e = tx.QueryRowContext(ctx, `SELECT plan_id FROM p2p_cloud_deployments WHERE deployment_id=$1`, claim.DeploymentID).Scan(&planID); e != nil {
				return e
			}
			if _, e = transitionDeployment(ctx, tx, claim.DeploymentID, planID, claim.ConnectionID, now, "finished", result.Status, "retained_tracked"); e != nil {
				return e
			}
		}
		var jobID, planID string
		if e = tx.QueryRowContext(ctx, `SELECT job_id,plan_id FROM p2p_cloud_jobs WHERE deployment_id=$1 AND kind='install'`, claim.DeploymentID).Scan(&jobID, &planID); e != nil {
			return e
		}
		_, e = transitionCloudJob(ctx, tx, jobID, planID, claim.DeploymentID, "install", "install", now, researchJobTransition{
			execution: jobExecution, outcome: jobOutcome, checkpoint: jobCheckpoint, errorCode: derefReadiness(result.ErrorCode), stepStatus: stepStatus, stepSummary: summary,
		})
		return e
	})
	if errors.Is(err, ErrLeaseLost) {
		same, e := s.sameServiceReadinessResult(ctx, claim, result)
		if e != nil {
			return e
		}
		if same {
			return nil
		}
	}
	return err
}

func commitRestoreReadinessResult(ctx context.Context, tx *sql.Tx, claim runtime.ServiceReadinessClaim, result runtime.ServiceReadinessResult, now int64) error {
	var planID string
	if err := tx.QueryRowContext(ctx, `SELECT plan_id FROM p2p_cloud_service_restores WHERE restore_id=$1 AND service_id=$2 AND deployment_id=$3 AND job_id=$4 AND restore_status='verifying' FOR UPDATE`, claim.RestoreID, claim.ServiceID, claim.DeploymentID, claim.JobID).Scan(&planID); err != nil {
		return err
	}
	transition := researchJobTransition{execution: "verifying", outcome: "pending", checkpoint: "restore_readiness_running", stepStatus: "running", stepSummary: "The restored service is completing its Stack-witnessed semantic readiness verification."}
	if result.Status == "queued" {
		transition.checkpoint = "restore_readiness_issued"
	}
	if result.Status == "succeeded" {
		restoreResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restores SET restore_status='succeeded',revision=revision+1,last_error_code='',updated_at=$1 WHERE restore_id=$2 AND restore_status='verifying'`, now, claim.RestoreID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(restoreResult); err != nil {
			return err
		}
		serviceResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_services SET revision=revision+1,updated_at=$1 WHERE service_id=$2`, now, claim.ServiceID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(serviceResult); err != nil {
			return err
		}
		transition = researchJobTransition{execution: "finished", outcome: "succeeded", checkpoint: "restore_readiness_verified", stepStatus: "finished", stepSummary: "AWS volume mapping and Worker semantic readiness were independently verified. The original volumes remain retained and billable until separately destroyed."}
		if err = publishRestoreReadinessProjection(ctx, tx, claim, "succeeded", now); err != nil {
			return err
		}
	} else if result.Status == "failed" || result.Status == "interrupted" {
		code := derefReadiness(result.ErrorCode)
		if code == "" {
			code = "restore_readiness_" + result.Status
		}
		restoreResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_restores SET restore_status='failed',revision=revision+1,last_error_code=$1,updated_at=$2 WHERE restore_id=$3 AND restore_status='verifying'`, code, now, claim.RestoreID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(restoreResult); err != nil {
			return err
		}
		serviceResult, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_services SET service_status='degraded',revision=revision+1,updated_at=$1 WHERE service_id=$2`, now, claim.ServiceID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(serviceResult); err != nil {
			return err
		}
		summary := "AWS verified the restored volume mapping, but semantic readiness failed. Resources remain active and billable for diagnosis or an explicit rollback."
		transition = researchJobTransition{execution: "finished", outcome: "failed", checkpoint: "restore_readiness_failed", errorCode: code, stepStatus: "failed", stepSummary: summary}
		alertID := stableID("cloud_alert_", claim.RestoreID, code)
		if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_alerts(alert_id,deployment_id,service_id,severity,code,message,acknowledged,revision,created_at,updated_at) VALUES($1,$2,$3,'critical',$4,$5,FALSE,1,$6,$6) ON CONFLICT(alert_id) DO NOTHING`, alertID, claim.DeploymentID, claim.ServiceID, code, summary, now); err != nil {
			return err
		}
		if err = writeEventAndProjection(ctx, tx, stableID("cloud_event_", alertID, "1"), "cloud.alert.raised", "alert", alertID, 1, map[string]any{"alert_id": alertID, "deployment_id": claim.DeploymentID, "service_id": claim.ServiceID, "severity": "critical", "code": code, "message": summary, "acknowledged": false, "revision": int64(1), "created_at": now, "updated_at": now}, now); err != nil {
			return err
		}
		if err = publishRestoreReadinessProjection(ctx, tx, claim, "failed", now); err != nil {
			return err
		}
	}
	_, err := transitionCloudJob(ctx, tx, claim.JobID, planID, claim.DeploymentID, "restore", "restore", now, transition)
	return err
}

func publishRestoreReadinessProjection(ctx context.Context, tx *sql.Tx, claim runtime.ServiceReadinessClaim, restoreStatus string, now int64) error {
	var service cloudmodule.Service
	if err := tx.QueryRowContext(ctx, `SELECT service_id,deployment_id,recipe_id,name,service_status,integration_status,revision,created_at,updated_at FROM p2p_cloud_services WHERE service_id=$1`, claim.ServiceID).Scan(&service.ServiceID, &service.DeploymentID, &service.RecipeID, &service.Name, &service.Status, &service.Integration, &service.Revision, &service.CreatedAt, &service.UpdatedAt); err != nil {
		return err
	}
	backups, restores, err := loadServiceRestoreProjectionCollections(ctx, tx, claim.ServiceID)
	if err != nil {
		return err
	}
	payload := map[string]any{"service_id": service.ServiceID, "deployment_id": service.DeploymentID, "recipe_id": service.RecipeID, "name": service.Name, "service_status": service.Status, "integration_status": service.Integration, "revision": service.Revision, "created_at": service.CreatedAt, "updated_at": service.UpdatedAt, "backups": backups, "restores": restores}
	return writeEventAndProjection(ctx, tx, stableID("cloud_event_", service.ServiceID, fmt.Sprint(service.Revision), "restore", restoreStatus), "cloud.service.changed", "service", service.ServiceID, service.Revision, payload, now)
}

func validatePersistedServiceReadinessCommand(claim runtime.ServiceReadinessClaim, signed runtime.SignedServiceReadinessCommand) error {
	if runtime.ValidateServiceReadinessClaim(claim) != nil || runtime.ValidateSignedServiceReadinessCommand(signed) != nil {
		return errors.New("service readiness command is invalid")
	}
	command, err := broker.ParseServiceReadinessCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return err
	}
	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil {
		return err
	}
	if command.CommandID != claim.Command.CommandID || command.ConnectionID != claim.ConnectionID || command.NodeKeyID != claim.NodeKeyID || command.ExpectedGeneration != claim.ExpectedGeneration || command.NodeCounter != claim.Command.NodeCounter || command.Action != claim.Command.Action || command.PayloadSHA256 != signed.PayloadSHA256 || command.RequestSHA256() != signed.RequestSHA256 || string(payload) != signed.PayloadJSON {
		return errors.New("service readiness signed command does not bind claim")
	}
	return nil
}

func (s *Store) DeferServiceReadiness(ctx context.Context, claim runtime.ServiceReadinessClaim, code string, available time.Time) error {
	return s.releaseServiceReadiness(ctx, claim, code, available, false)
}
func (s *Store) FailServiceReadiness(ctx context.Context, claim runtime.ServiceReadinessClaim, code string) error {
	return s.releaseServiceReadiness(ctx, claim, code, s.now().Add(time.Minute), false)
}
func (s *Store) ExpireServiceReadinessCommand(ctx context.Context, claim runtime.ServiceReadinessClaim) error {
	return s.withServiceReadinessClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_commands SET state='expired',last_error_code='service_readiness_command_expired',updated_at=$1 WHERE command_id=$2 AND state IN('allocated','signed','indeterminate')`, now, claim.Command.CommandID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(r); e != nil {
			return e
		}
		return releaseServiceReadinessClaim(ctx, tx, claim, now, "service_readiness_command_expired")
	})
}
func (s *Store) releaseServiceReadiness(ctx context.Context, claim runtime.ServiceReadinessClaim, code string, available time.Time, _ bool) error {
	return s.withServiceReadinessClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_commands SET state=CASE WHEN state='signed' THEN 'indeterminate' ELSE state END,last_error_code=$1,updated_at=$2 WHERE command_id=$3`, durableErrorCode(code, "service_readiness_retryable"), now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(result); err != nil {
			return err
		}
		return releaseServiceReadinessClaim(ctx, tx, claim, available.UnixMilli(), durableErrorCode(code, "service_readiness_retryable"))
	})
}
func releaseServiceReadinessClaim(ctx context.Context, tx *sql.Tx, claim runtime.ServiceReadinessClaim, available int64, code string) error {
	var r sql.Result
	var e error
	if claim.Phase == runtime.ServiceReadinessPhaseIssue {
		r, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_outbox SET available_at=$1,lease_owner='',lease_token='',lease_until=0,last_error_code=$2 WHERE outbox_id=$3`, available, code, claim.OutboxID)
	} else {
		r, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_tasks SET available_at=$1,lease_owner='',lease_token='',lease_until=0,last_error_code=$2 WHERE task_id=$3`, available, code, claim.TaskID)
	}
	if e != nil {
		return e
	}
	return requireOneAffected(r)
}

func (s *Store) withServiceReadinessClaim(ctx context.Context, claim runtime.ServiceReadinessClaim, run func(*sql.Tx, int64) error) (err error) {
	if runtime.ValidateServiceReadinessClaim(claim) != nil {
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
	if claim.Phase == runtime.ServiceReadinessPhaseIssue {
		err = tx.QueryRowContext(ctx, `SELECT lease_token,lease_until FROM p2p_cloud_outbox WHERE outbox_id=$1 FOR UPDATE`, claim.OutboxID).Scan(&token, &until)
	} else {
		err = tx.QueryRowContext(ctx, `SELECT lease_token,lease_until FROM p2p_cloud_service_readiness_tasks WHERE task_id=$1 FOR UPDATE`, claim.TaskID).Scan(&token, &until)
	}
	if err != nil || token != claim.LeaseToken || until <= now {
		return ErrLeaseLost
	}
	if err = verifyServiceReadinessBindings(ctx, tx, claim, now); err != nil {
		return err
	}
	if err = run(tx, now); err != nil {
		return err
	}
	return tx.Commit()
}
func verifyServiceReadinessBindings(ctx context.Context, tx *sql.Tx, claim runtime.ServiceReadinessClaim, now int64) error {
	if claim.Purpose == "monitor" {
		return verifyServiceMonitorReadinessBindings(ctx, tx, claim, now)
	}
	var execution, deployment, service, connection, instance, manifest, installEvidence, semantic, status, purpose, restoreID, jobID string
	var manifestDeployment, manifestPlan, manifestConnection, approvedManifestDigest, manifestStatus string
	var deploymentPlan, deploymentConnection, deploymentExecution, deploymentOutcome, deploymentResource string
	var connectionStatus, connectionRegion, brokerRegion, brokerEndpoint, nodeKey string
	var resourceConnection, resourceInstance, resourceStatus, workerState, installStatus, jobKind, jobExecution, jobOutcome, jobCheckpoint, restoreStatus string
	var workerLease, generation int64
	err := tx.QueryRowContext(ctx, `SELECT task.execution_id,task.deployment_id,task.service_id,task.cloud_connection_id,task.instance_id,
		task.recipe_execution_manifest_digest,task.install_evidence_digest,task.semantic_expectation_digest,task.task_status,task.purpose,task.restore_id,task.job_id,
		manifest.deployment_id,manifest.plan_id,manifest.cloud_connection_id,manifest.manifest_digest,manifest.status,
		deployment.plan_id,deployment.cloud_connection_id,deployment.execution_status,deployment.outcome_status,deployment.resource_status,
		connection.status,connection.region,broker.broker_region,broker.broker_command_url,broker.node_key_id,broker.connection_generation,
		resource.cloud_connection_id,resource.instance_id,resource.resource_status,observation.worker_session_state,observation.worker_lease_expires_at,
		install.task_status,job.kind,job.execution_status,job.outcome_status,job.checkpoint,COALESCE(restore.restore_status,'')
		FROM p2p_cloud_service_readiness_tasks task JOIN p2p_cloud_recipe_execution_manifests manifest ON manifest.execution_id=task.execution_id
		JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=task.deployment_id
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=task.cloud_connection_id JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=task.cloud_connection_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=task.deployment_id JOIN p2p_cloud_worker_bootstrap_observations observation ON observation.deployment_id=task.deployment_id
		JOIN p2p_cloud_recipe_install_tasks install ON install.execution_id=task.execution_id JOIN p2p_cloud_jobs job ON job.job_id=task.job_id
		LEFT JOIN p2p_cloud_service_restores restore ON restore.restore_id=task.restore_id
		WHERE task.task_id=$1 FOR UPDATE OF task,manifest,deployment,connection,broker,resource,observation,install,job`, claim.TaskID).Scan(
		&execution, &deployment, &service, &connection, &instance, &manifest, &installEvidence, &semantic, &status, &purpose, &restoreID, &jobID,
		&manifestDeployment, &manifestPlan, &manifestConnection, &approvedManifestDigest, &manifestStatus,
		&deploymentPlan, &deploymentConnection, &deploymentExecution, &deploymentOutcome, &deploymentResource,
		&connectionStatus, &connectionRegion, &brokerRegion, &brokerEndpoint, &nodeKey, &generation,
		&resourceConnection, &resourceInstance, &resourceStatus, &workerState, &workerLease, &installStatus, &jobKind, &jobExecution, &jobOutcome, &jobCheckpoint, &restoreStatus)
	if err != nil || execution != claim.ExecutionID || deployment != claim.DeploymentID || service != claim.ServiceID || connection != claim.ConnectionID || instance != claim.InstanceID || semantic != claim.SemanticExpectationDigest ||
		purpose != claim.Purpose || restoreID != claim.RestoreID || jobID != claim.JobID || jobKind != claim.Purpose ||
		manifestDeployment != deployment || manifestPlan != deploymentPlan || manifestConnection != connection || approvedManifestDigest != manifest || manifestStatus != "approved" ||
		deploymentConnection != connection || brokerEndpoint != claim.BrokerEndpoint || nodeKey != claim.NodeKeyID || generation != claim.ExpectedGeneration ||
		connectionStatus != "active" || connectionRegion != brokerRegion || resourceConnection != connection || resourceInstance != instance || (resourceStatus != "active" && resourceStatus != "retained_tracked") || workerState != "active" || workerLease <= now || installStatus != "succeeded" || jobExecution != "verifying" || jobOutcome != "pending" {
		return ErrLeaseLost
	}
	installBinding := purpose == "install" && restoreID == "" && deploymentExecution == "verifying" && deploymentOutcome == "pending" && deploymentResource == "active" && (jobCheckpoint == "readiness_queued" || jobCheckpoint == "readiness_issuing" || jobCheckpoint == "readiness_issued" || jobCheckpoint == "readiness_running")
	restoreBinding := purpose == "restore" && restoreID != "" && restoreStatus == "verifying" && (jobCheckpoint == "restore_readiness_queued" || jobCheckpoint == "restore_readiness_issuing" || jobCheckpoint == "restore_readiness_issued" || jobCheckpoint == "restore_readiness_running")
	if !installBinding && !restoreBinding {
		return ErrLeaseLost
	}
	if claim.Phase == runtime.ServiceReadinessPhaseIssue && (manifest != claim.IssueRequest.RecipeExecutionManifestDigest || installEvidence != claim.IssueRequest.InstallEvidenceDigest || status != "unissued") {
		return ErrLeaseLost
	}
	if claim.Phase == runtime.ServiceReadinessPhaseObserve && status != "queued" && status != "running" {
		return ErrLeaseLost
	}
	return nil
}
func serviceReadinessTransition(status string, attempt, sequence int64, checkpoint, challenge, semantic, stack, errorCode string, next runtime.ServiceReadinessResult) bool {
	if next.Attempt < attempt {
		return false
	}
	// A Worker session renewal may rotate the readiness challenge under a new
	// task attempt while the Orchestrator is offline. Stack is the attempt
	// authority; a newer attempt may replace only a nonterminal sequence-zero
	// projection. It may already be terminal when the next signed observation
	// arrives, so no intermediate running observation is required.
	if next.Attempt > attempt {
		if sequence != 0 || (status != "queued" && status != "running") {
			return false
		}
		return (next.Status == "running" && next.LastSequence == 0 && next.Checkpoint == runtime.ServiceReadinessChallengeIssued) ||
			((next.Status == "succeeded" || next.Status == "failed" || next.Status == "interrupted") && next.LastSequence > 0)
	}
	if status == "unissued" && next.Status == "queued" && sequence == 0 && next.LastSequence == 0 {
		return true
	}
	if status == "queued" && next.Status == "running" && next.LastSequence == sequence && sequence == 0 && next.Checkpoint == runtime.ServiceReadinessChallengeIssued {
		return true
	}
	if sequence == next.LastSequence {
		return status == next.Status && checkpoint == next.Checkpoint && challenge == derefReadiness(next.ChallengeDigest) && semantic == derefReadiness(next.SemanticEvidenceDigest) && stack == derefReadiness(next.StackObservationDigest) && errorCode == derefReadiness(next.ErrorCode)
	}
	if next.LastSequence <= sequence {
		return false
	}
	if status == "unissued" {
		return next.Status == "queued"
	}
	if status == "queued" {
		return next.Status == "running" || next.Status == "succeeded" || next.Status == "failed" || next.Status == "interrupted"
	}
	return status == "running" && (next.Status == "running" || next.Status == "succeeded" || next.Status == "failed" || next.Status == "interrupted")
}
func derefReadiness(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func (s *Store) sameServiceReadinessResult(ctx context.Context, claim runtime.ServiceReadinessClaim, result runtime.ServiceReadinessResult) (bool, error) {
	var status, checkpoint, challenge, semantic, stack, errorCode string
	var attempt, sequence int64
	err := s.db.QueryRowContext(ctx, `SELECT task_status,task_attempt,last_sequence,checkpoint,challenge_digest,semantic_evidence_digest,stack_observation_digest,error_code FROM p2p_cloud_service_readiness_tasks WHERE task_id=$1`, claim.TaskID).Scan(&status, &attempt, &sequence, &checkpoint, &challenge, &semantic, &stack, &errorCode)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return status == result.Status && attempt == result.Attempt && sequence == result.LastSequence && checkpoint == result.Checkpoint && challenge == derefReadiness(result.ChallengeDigest) && semantic == derefReadiness(result.SemanticEvidenceDigest) && stack == derefReadiness(result.StackObservationDigest) && errorCode == derefReadiness(result.ErrorCode), nil
}
