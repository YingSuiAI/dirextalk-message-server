package storepg

import (
	"bytes"
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

var _ runtime.ExecutionProbeStore = (*Store)(nil)

// ErrExecutionProbeStale is an internal ordering failure. It is deliberately
// not projected: the Connection Stack summary is only one input to the
// durable state machine, never a user-visible task log.
var ErrExecutionProbeStale = errors.New("cloud execution probe result is stale")

const executionProbeObserveDelay = 5 * time.Second

// ClaimExecutionProbe leases either the one sealed issue outbox or a later
// read-only observation. Both paths require independently recorded bootstrap
// evidence and never contain recipe instructions, Worker bearer material, or
// a cloud control action.
func (s *Store) ClaimExecutionProbe(ctx context.Context, workerID string, lease time.Duration) (runtime.ExecutionProbeClaim, bool, error) {
	if s == nil || s.db == nil {
		return runtime.ExecutionProbeClaim{}, false, errors.New("cloud orchestrator database is unavailable")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || len(workerID) > 128 || strings.ContainsAny(workerID, "\r\n\t") {
		return runtime.ExecutionProbeClaim{}, false, errors.New("cloud orchestrator worker id is invalid")
	}
	if lease <= 0 || lease > 5*time.Minute {
		return runtime.ExecutionProbeClaim{}, false, errors.New("cloud execution probe lease must be between 1ns and 5m")
	}
	if claim, found, err := s.claimExecutionProbeIssue(ctx, workerID, lease); err != nil || found {
		return claim, found, err
	}
	return s.claimExecutionProbeObservation(ctx, workerID, lease)
}

func (s *Store) claimExecutionProbeIssue(ctx context.Context, workerID string, lease time.Duration) (runtime.ExecutionProbeClaim, bool, error) {
	now := s.now().UnixMilli()
	token := strings.TrimSpace(s.cfg.NewLeaseToken())
	if token == "" || len(token) > 128 {
		return runtime.ExecutionProbeClaim{}, false, errors.New("cloud orchestrator lease token is invalid")
	}
	row := s.db.QueryRowContext(ctx, `
		WITH selected AS (
			SELECT outbox.outbox_id
			FROM p2p_cloud_outbox AS outbox
			JOIN p2p_cloud_execution_probe_tasks AS task ON task.task_id = outbox.aggregate_id
			JOIN p2p_cloud_deployments AS deployment ON deployment.deployment_id = task.deployment_id
			JOIN p2p_cloud_plans AS plan ON plan.plan_id = deployment.plan_id
			JOIN p2p_cloud_connections AS connection ON connection.cloud_connection_id = deployment.cloud_connection_id
			JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = deployment.cloud_connection_id
			JOIN p2p_cloud_deployment_resources AS resource ON resource.deployment_id = deployment.deployment_id
			JOIN p2p_cloud_worker_bootstrap_observations AS observation ON observation.deployment_id = deployment.deployment_id
			JOIN p2p_cloud_jobs AS provision_job ON provision_job.deployment_id = deployment.deployment_id AND provision_job.kind = 'provision'
			JOIN p2p_cloud_jobs AS verify_job ON verify_job.deployment_id = deployment.deployment_id AND verify_job.kind = 'verify'
			WHERE outbox.kind = $1
				AND outbox.aggregate_type = 'execution_probe_task'
				AND outbox.completed_at = 0 AND outbox.available_at <= $2 AND outbox.lease_until <= $2
				AND task.task_status = 'unissued'
				AND deployment.execution_status = 'verifying' AND deployment.outcome_status = 'pending' AND deployment.resource_status = 'active'
				AND plan.status = 'approved'
				AND connection.status = 'active' AND connection.region = broker.broker_region
				AND resource.cloud_connection_id = deployment.cloud_connection_id AND resource.resource_status = 'active'
				AND observation.worker_session_state = 'active' AND observation.worker_lease_expires_at > $2
				AND provision_job.execution_status = 'verifying' AND provision_job.outcome_status = 'pending' AND provision_job.checkpoint = 'worker_bootstrap_verified'
				AND verify_job.execution_status IN ('queued', 'verifying') AND verify_job.outcome_status = 'pending'
				AND verify_job.checkpoint IN ('execution_probe_queued', 'execution_probe_issuing')
			ORDER BY outbox.available_at ASC, outbox.created_at ASC, outbox.outbox_id ASC
			FOR UPDATE OF outbox SKIP LOCKED
			LIMIT 1
		), claimed AS (
			UPDATE p2p_cloud_outbox AS outbox
			SET lease_owner = $3, lease_token = $4, lease_until = $5,
				attempts = outbox.attempts + 1, last_error_code = ''
			FROM selected
			WHERE outbox.outbox_id = selected.outbox_id
			RETURNING outbox.outbox_id, outbox.kind, outbox.aggregate_type, outbox.aggregate_id, outbox.lease_token
		)
		SELECT claimed.outbox_id, claimed.kind, claimed.aggregate_type, claimed.aggregate_id, claimed.lease_token,
			deployment.deployment_id, deployment.plan_id, deployment.cloud_connection_id, connection.region, resource.instance_id,
			task.task_id, task.task_attempt, task.execution_manifest_digest, task.input_digest,
			broker.broker_command_url, broker.node_key_id, broker.connection_generation, verify_job.job_id
		FROM claimed
		JOIN p2p_cloud_execution_probe_tasks AS task ON task.task_id = claimed.aggregate_id
		JOIN p2p_cloud_deployments AS deployment ON deployment.deployment_id = task.deployment_id
		JOIN p2p_cloud_connections AS connection ON connection.cloud_connection_id = deployment.cloud_connection_id
		JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = deployment.cloud_connection_id
		JOIN p2p_cloud_deployment_resources AS resource ON resource.deployment_id = deployment.deployment_id
		JOIN p2p_cloud_jobs AS verify_job ON verify_job.deployment_id = deployment.deployment_id AND verify_job.kind = 'verify'
	`, runtime.ExecutionProbeIssueRequested, now, workerID, token, now+lease.Milliseconds())
	var claim runtime.ExecutionProbeClaim
	if err := row.Scan(
		&claim.OutboxID, &claim.Kind, &claim.AggregateType, &claim.AggregateID, &claim.LeaseToken,
		&claim.DeploymentID, &claim.PlanID, &claim.ConnectionID, &claim.Region, &claim.InstanceID,
		&claim.TaskID, &claim.TaskAttempt, &claim.ExecutionManifestDigest, &claim.InputDigest,
		&claim.BrokerEndpoint, &claim.NodeKeyID, &claim.ExpectedGeneration, &claim.JobID,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.ExecutionProbeClaim{}, false, nil
		}
		return runtime.ExecutionProbeClaim{}, false, fmt.Errorf("claim cloud execution probe issue: %w", err)
	}
	claim.Phase = runtime.ExecutionProbePhaseIssue
	claim.IssueRequest = runtime.ExecutionProbeIssueRequest{
		Schema: runtime.ExecutionProbeIssueSchema, DeploymentID: claim.DeploymentID, TaskID: claim.TaskID,
		TaskKind: runtime.ExecutionProbeTaskKind, ExecutionManifestDigest: claim.ExecutionManifestDigest, InputDigest: claim.InputDigest,
	}
	command, err := s.prepareExecutionProbeCommand(ctx, claim)
	if err != nil {
		return runtime.ExecutionProbeClaim{}, false, err
	}
	claim.Command = command
	return claim, true, nil
}

func (s *Store) claimExecutionProbeObservation(ctx context.Context, workerID string, lease time.Duration) (runtime.ExecutionProbeClaim, bool, error) {
	now := s.now().UnixMilli()
	token := strings.TrimSpace(s.cfg.NewLeaseToken())
	if token == "" || len(token) > 128 {
		return runtime.ExecutionProbeClaim{}, false, errors.New("cloud orchestrator lease token is invalid")
	}
	row := s.db.QueryRowContext(ctx, `
		WITH selected AS (
			SELECT task.deployment_id
			FROM p2p_cloud_execution_probe_tasks AS task
			JOIN p2p_cloud_deployments AS deployment ON deployment.deployment_id = task.deployment_id
			JOIN p2p_cloud_plans AS plan ON plan.plan_id = deployment.plan_id
			JOIN p2p_cloud_connections AS connection ON connection.cloud_connection_id = deployment.cloud_connection_id
			JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = deployment.cloud_connection_id
			JOIN p2p_cloud_deployment_resources AS resource ON resource.deployment_id = deployment.deployment_id
			JOIN p2p_cloud_worker_bootstrap_observations AS observation ON observation.deployment_id = deployment.deployment_id
			JOIN p2p_cloud_jobs AS provision_job ON provision_job.deployment_id = deployment.deployment_id AND provision_job.kind = 'provision'
			JOIN p2p_cloud_jobs AS verify_job ON verify_job.deployment_id = deployment.deployment_id AND verify_job.kind = 'verify'
			WHERE task.task_status IN ('queued', 'running') AND task.available_at <= $1 AND task.lease_until <= $1
				AND deployment.execution_status = 'verifying' AND deployment.outcome_status = 'pending' AND deployment.resource_status = 'active'
				AND plan.status = 'approved'
				AND connection.status = 'active' AND connection.region = broker.broker_region
				AND resource.cloud_connection_id = deployment.cloud_connection_id AND resource.resource_status = 'active'
				AND observation.worker_session_state = 'active' AND observation.worker_lease_expires_at > $1
				AND provision_job.execution_status = 'verifying' AND provision_job.outcome_status = 'pending' AND provision_job.checkpoint = 'worker_bootstrap_verified'
				AND verify_job.outcome_status = 'pending'
			ORDER BY task.available_at ASC, task.updated_at ASC, task.deployment_id ASC
			FOR UPDATE OF task SKIP LOCKED
			LIMIT 1
		), claimed AS (
			UPDATE p2p_cloud_execution_probe_tasks AS task
			SET lease_owner = $2, lease_token = $3, lease_until = $4,
				attempts = task.attempts + 1, last_error_code = '', updated_at = $1
			FROM selected
			WHERE task.deployment_id = selected.deployment_id
			RETURNING task.deployment_id, task.task_id, task.task_attempt, task.execution_manifest_digest, task.input_digest, task.lease_token
		)
		SELECT claimed.deployment_id, deployment.plan_id, deployment.cloud_connection_id, connection.region, resource.instance_id,
			claimed.task_id, claimed.task_attempt, claimed.execution_manifest_digest, claimed.input_digest, claimed.lease_token,
			broker.broker_command_url, broker.node_key_id, broker.connection_generation, verify_job.job_id
		FROM claimed
		JOIN p2p_cloud_deployments AS deployment ON deployment.deployment_id = claimed.deployment_id
		JOIN p2p_cloud_connections AS connection ON connection.cloud_connection_id = deployment.cloud_connection_id
		JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = deployment.cloud_connection_id
		JOIN p2p_cloud_deployment_resources AS resource ON resource.deployment_id = deployment.deployment_id
		JOIN p2p_cloud_jobs AS verify_job ON verify_job.deployment_id = deployment.deployment_id AND verify_job.kind = 'verify'
	`, now, workerID, token, now+lease.Milliseconds())
	var claim runtime.ExecutionProbeClaim
	if err := row.Scan(
		&claim.DeploymentID, &claim.PlanID, &claim.ConnectionID, &claim.Region, &claim.InstanceID,
		&claim.TaskID, &claim.TaskAttempt, &claim.ExecutionManifestDigest, &claim.InputDigest, &claim.LeaseToken,
		&claim.BrokerEndpoint, &claim.NodeKeyID, &claim.ExpectedGeneration, &claim.JobID,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.ExecutionProbeClaim{}, false, nil
		}
		return runtime.ExecutionProbeClaim{}, false, fmt.Errorf("claim cloud execution probe observation: %w", err)
	}
	claim.Phase = runtime.ExecutionProbePhaseObserve
	claim.ObserveRequest = runtime.ExecutionProbeObserveRequest{DeploymentID: claim.DeploymentID, TaskID: claim.TaskID}
	command, err := s.prepareExecutionProbeCommand(ctx, claim)
	if err != nil {
		return runtime.ExecutionProbeClaim{}, false, err
	}
	claim.Command = command
	return claim, true, nil
}

func (s *Store) prepareExecutionProbeCommand(ctx context.Context, claim runtime.ExecutionProbeClaim) (runtime.ExecutionProbeCommand, error) {
	requestDigest, action, err := executionProbeRequestIdentity(claim)
	if err != nil {
		return runtime.ExecutionProbeCommand{}, err
	}
	var command runtime.ExecutionProbeCommand
	err = s.withExecutionProbeClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		latest, found, err := selectLatestExecutionProbeCommand(ctx, tx, claim.TaskID, action, requestDigest)
		if err != nil {
			return err
		}
		if found {
			candidate := claim
			candidate.Command = latest
			if !validExecutionProbeCommandClaim(candidate) {
				return errors.New("persisted execution probe command does not bind the claim")
			}
		}
		if found && (latest.State == "allocated" || latest.State == "signed" || latest.State == "indeterminate") {
			command = latest
			return nil
		}
		if found && latest.State != "accepted" && latest.State != "expired" {
			return errors.New("execution probe command is not eligible for another attempt")
		}
		if found && claim.Phase == runtime.ExecutionProbePhaseIssue && latest.State == "accepted" {
			return errors.New("execution probe issue command was already accepted")
		}
		attempt := 1
		if found {
			attempt = latest.Attempt + 1
		}
		var nodeCounter int64
		if err := tx.QueryRowContext(ctx, `
			UPDATE p2p_cloud_connection_brokers
			SET next_node_counter = next_node_counter + 1, updated_at = $1
			WHERE cloud_connection_id = $2
			RETURNING next_node_counter
		`, now, claim.ConnectionID).Scan(&nodeCounter); err != nil {
			return err
		}
		prefix := "cloud_broker_execution_probe_issue_"
		if claim.Phase == runtime.ExecutionProbePhaseObserve {
			prefix = "cloud_broker_execution_probe_observe_"
		}
		command = runtime.ExecutionProbeCommand{
			CommandID:    stableID(prefix, claim.ConnectionID, claim.TaskID, requestDigest, fmt.Sprint(attempt)),
			DeploymentID: claim.DeploymentID, TaskID: claim.TaskID, ConnectionID: claim.ConnectionID,
			NodeKeyID: claim.NodeKeyID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: nodeCounter,
			Attempt: attempt, Action: action, RequestDigest: requestDigest, State: "allocated",
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_execution_probe_commands (
				command_id, task_id, deployment_id, cloud_connection_id, request_digest, command_attempt,
				action, node_key_id, expected_generation, node_counter, state, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'allocated', $11, $11)
		`, command.CommandID, command.TaskID, command.DeploymentID, command.ConnectionID, command.RequestDigest, command.Attempt,
			command.Action, command.NodeKeyID, command.ExpectedGeneration, command.NodeCounter, now)
		return err
	})
	return command, err
}

// PersistExecutionProbeCommand journals the exact signature envelope before
// HTTP I/O. A retry can therefore replay the same node counter and request
// digest after a process crash or lost Broker response.
func (s *Store) PersistExecutionProbeCommand(ctx context.Context, claim runtime.ExecutionProbeClaim, signed runtime.SignedExecutionProbeCommand) error {
	if err := validPersistedExecutionProbeCommand(claim, signed); err != nil {
		return err
	}
	return s.withExecutionProbeClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		stored, err := selectExecutionProbeCommandByID(ctx, tx, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if !sameExecutionProbeCommandIdentity(stored, claim.Command) {
			return errors.New("persisted execution probe command does not match the claim")
		}
		if stored.State == "signed" || stored.State == "indeterminate" || stored.State == "accepted" {
			if sameSignedExecutionProbeCommand(stored, signed) {
				return nil
			}
			return errors.New("execution probe command already has a different signed envelope")
		}
		if stored.State != "allocated" {
			return errors.New("execution probe command cannot be signed in its current state")
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_execution_probe_commands
			SET canonical_payload_json = $1, payload_sha256 = $2, request_sha256 = $3, signed_envelope_json = $4,
				issued_at = $5, expires_at = $6, state = 'signed', updated_at = $7
			WHERE command_id = $8 AND state = 'allocated'
		`, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256, signed.EnvelopeJSON,
			signed.IssuedAt.UTC().UnixMilli(), signed.ExpiresAt.UTC().UnixMilli(), now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		return requireOneAffected(result)
	})
}

func (s *Store) MarkExecutionProbeStarted(ctx context.Context, claim runtime.ExecutionProbeClaim) error {
	if !validExecutionProbeCommandClaim(claim) {
		return ErrLeaseLost
	}
	return s.withExecutionProbeClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if claim.Phase == runtime.ExecutionProbePhaseObserve {
			result, err := tx.ExecContext(ctx, `
				UPDATE p2p_cloud_execution_probe_tasks
				SET last_error_code = '', updated_at = $1
				WHERE deployment_id = $2 AND lease_token = $3
			`, now, claim.DeploymentID, claim.LeaseToken)
			if err != nil {
				return err
			}
			return requireOneAffected(result)
		}
		return ensureExecutionProbeJobTransition(ctx, tx, claim, now, researchJobTransition{
			execution: "verifying", outcome: "pending", checkpoint: "execution_probe_issuing", errorCode: "",
			stepStatus: "running", stepSummary: "The sealed Worker task transport is being issued; this does not execute the Recipe or establish service readiness.",
		})
	})
}

func (s *Store) CommitExecutionProbe(ctx context.Context, claim runtime.ExecutionProbeClaim, taskResult runtime.ExecutionProbeTaskResult) error {
	if !validExecutionProbeCommandClaim(claim) {
		return ErrLeaseLost
	}
	err := s.withExecutionProbeClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		command, err := selectExecutionProbeCommandByID(ctx, tx, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if !sameExecutionProbeCommandIdentity(command, claim.Command) || (command.State != "signed" && command.State != "indeterminate" && command.State != "accepted") {
			return errors.New("execution probe command is not eligible for a result")
		}
		if err := validPersistedExecutionProbeCommand(claim, signedExecutionProbeCommand(command)); err != nil {
			return err
		}
		if err := runtime.ValidateExecutionProbeResult(claim, signedExecutionProbeCommand(command), taskResult, time.UnixMilli(now).UTC()); err != nil {
			return err
		}
		current, err := selectExecutionProbeTask(ctx, tx, claim.DeploymentID)
		if err != nil {
			return err
		}
		if current.TaskID != claim.TaskID || current.TaskAttempt != claim.TaskAttempt || current.ManifestDigest != claim.ExecutionManifestDigest || current.InputDigest != claim.InputDigest {
			return ErrLeaseLost
		}
		if !executionProbeTransitionAllowed(current, taskResult) {
			return ErrExecutionProbeStale
		}
		commandResult, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_execution_probe_commands
			SET state = 'accepted', attempts = attempts + 1, last_error_code = '', updated_at = $1
			WHERE command_id = $2 AND state IN ('signed', 'indeterminate', 'accepted')
		`, now, command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(commandResult); err != nil {
			return err
		}
		nextAvailable := int64(0)
		if !executionProbeTerminal(taskResult.Status) {
			nextAvailable = time.UnixMilli(now).Add(executionProbeObserveDelay).UnixMilli()
		}
		checkpoint, errorCode, evidenceDigest := dereferenceExecutionProbe(taskResult.Checkpoint), dereferenceExecutionProbe(taskResult.ErrorCode), dereferenceExecutionProbe(taskResult.EvidenceDigest)
		query := `
			UPDATE p2p_cloud_execution_probe_tasks
			SET task_status = $1, task_attempt = $2, last_sequence = $3, checkpoint = $4, error_code = $5,
				evidence_digest = $6, observed_at = $7, available_at = $8, lease_owner = '', lease_token = '', lease_until = 0,
				last_error_code = '', updated_at = $9
			WHERE deployment_id = $10 AND task_id = $11`
		args := []any{taskResult.Status, taskResult.Attempt, taskResult.LastSequence, checkpoint, errorCode, evidenceDigest,
			taskResult.UpdatedAt.UTC().UnixMilli(), nextAvailable, now, claim.DeploymentID, claim.TaskID}
		if claim.Phase == runtime.ExecutionProbePhaseIssue {
			query += ` AND task_status = 'unissued'`
		} else {
			query += ` AND lease_token = $12`
			args = append(args, claim.LeaseToken)
		}
		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}
		if err := requireOneAffected(result); err != nil {
			return err
		}
		if claim.Phase == runtime.ExecutionProbePhaseIssue {
			if err := completeExecutionProbeIssueOutbox(ctx, tx, claim, now); err != nil {
				return err
			}
		}
		if current.matches(taskResult) {
			return nil
		}
		return ensureExecutionProbeJobTransition(ctx, tx, claim, now, executionProbeJobTransition(taskResult))
	})
	if errors.Is(err, ErrLeaseLost) {
		duplicate, duplicateErr := s.sameCommittedExecutionProbeResult(ctx, claim, taskResult)
		if duplicateErr != nil {
			return duplicateErr
		}
		if duplicate {
			return nil
		}
	}
	return err
}

// DeferExecutionProbe only records a transport retry. It never turns a
// retained Worker, VM, or service into a terminal failure.
func (s *Store) DeferExecutionProbe(ctx context.Context, claim runtime.ExecutionProbeClaim, code string, availableAt time.Time) error {
	if !validExecutionProbeCommandClaim(claim) {
		return ErrLeaseLost
	}
	code = durableErrorCode(code, "execution_probe_retryable")
	return s.withExecutionProbeClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if err := updateExecutionProbeCommandAttempt(ctx, tx, claim, code, now); err != nil {
			return err
		}
		available := availableAt.UTC().UnixMilli()
		if available < now {
			available = now
		}
		return releaseExecutionProbeClaim(ctx, tx, claim, available, code, now)
	})
}

func (s *Store) ExpireExecutionProbeCommand(ctx context.Context, claim runtime.ExecutionProbeClaim) error {
	if !validExecutionProbeCommandClaim(claim) {
		return ErrLeaseLost
	}
	return s.withExecutionProbeClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		requestDigest, action, err := executionProbeRequestIdentity(claim)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_execution_probe_commands
			SET state = 'expired', attempts = attempts + 1, last_error_code = 'execution_probe_command_expired', updated_at = $1
			WHERE command_id = $2 AND task_id = $3 AND deployment_id = $4 AND cloud_connection_id = $5
				AND action = $6 AND request_digest = $7 AND command_attempt = $8
				AND state IN ('allocated', 'signed', 'indeterminate')
		`, now, claim.Command.CommandID, claim.TaskID, claim.DeploymentID, claim.ConnectionID, action, requestDigest, claim.Command.Attempt)
		if err != nil {
			return err
		}
		if err := requireOneAffected(result); err != nil {
			return err
		}
		return releaseExecutionProbeClaim(ctx, tx, claim, now, "execution_probe_command_expired", now)
	})
}

func (s *Store) FailExecutionProbe(ctx context.Context, claim runtime.ExecutionProbeClaim, code string) error {
	return s.DeferExecutionProbe(ctx, claim, code, s.now().Add(time.Minute))
}

// ensureExecutionProbeTask seals the fixed, no-input transport artifact only
// after the Connection Stack independently confirmed the dedicated Worker.
// The artifacts live exclusively in the orchestrator database; public events
// contain only the ordinary verify Job summary.
func ensureExecutionProbeTask(ctx context.Context, tx *sql.Tx, claim runtime.WorkerBootstrapObservationClaim, now int64) error {
	var planHash, recipeDigest, connectionID, planStatus, workerManifestDigest string
	var planRevision int64
	if err := tx.QueryRowContext(ctx, `
		SELECT plan.plan_hash, plan.recipe_digest, plan.cloud_connection_id, plan.status, plan.revision,
			broker.worker_resource_manifest_digest
		FROM p2p_cloud_plans AS plan
		JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = plan.cloud_connection_id
		WHERE plan.plan_id = $1 FOR UPDATE OF plan, broker
	`, claim.PlanID).Scan(&planHash, &recipeDigest, &connectionID, &planStatus, &planRevision, &workerManifestDigest); err != nil {
		return err
	}
	if connectionID != claim.ConnectionID || planStatus != "approved" || planRevision <= 0 ||
		!executionProbeDigest(planHash) || !executionProbeDigest(recipeDigest) || !executionProbeDigest(workerManifestDigest) {
		return ErrLeaseLost
	}
	manifest := cloudcontracts.ExecutionProbeManifestV1{
		SchemaVersion: cloudcontracts.ExecutionProbeManifestV1Schema,
		DeploymentID:  claim.DeploymentID, PlanID: claim.PlanID, PlanHash: planHash, PlanRevision: uint64(planRevision),
		RecipeDigest: recipeDigest, WorkerResourceManifestDigest: workerManifestDigest, TaskKind: cloudcontracts.ExecutionProbeTaskKind,
	}
	input := cloudcontracts.NoInputV1{
		SchemaVersion: cloudcontracts.NoInputV1Schema, DeploymentID: claim.DeploymentID,
		TaskKind: cloudcontracts.ExecutionProbeTaskKind, NoInput: true,
	}
	if err := input.ValidateForManifest(manifest); err != nil {
		return err
	}
	manifestCBOR, err := manifest.CanonicalExecutionProbeManifestCBOR()
	if err != nil {
		return err
	}
	manifestDigest, err := manifest.Digest()
	if err != nil {
		return err
	}
	inputCBOR, err := input.CanonicalNoInputCBOR()
	if err != nil {
		return err
	}
	inputDigest, err := input.Digest()
	if err != nil {
		return err
	}
	taskID := stableID("cloud_execution_probe_task_", claim.DeploymentID, planHash, manifestDigest, inputDigest)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO p2p_cloud_execution_probe_tasks (
			deployment_id, task_id, plan_id, cloud_connection_id, instance_id, execution_manifest_cbor, execution_manifest_digest,
			input_cbor, input_digest, task_status, task_attempt, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'unissued', 1, $10, $10)
		ON CONFLICT (deployment_id) DO NOTHING
	`, claim.DeploymentID, taskID, claim.PlanID, claim.ConnectionID, claim.InstanceID, manifestCBOR, manifestDigest, inputCBOR, inputDigest, now)
	if err != nil {
		return err
	}
	var existingTaskID, existingPlanID, existingConnectionID, existingInstanceID, existingManifestDigest, existingInputDigest string
	var existingManifestCBOR, existingInputCBOR []byte
	if err := tx.QueryRowContext(ctx, `
		SELECT task_id, plan_id, cloud_connection_id, instance_id, execution_manifest_cbor, execution_manifest_digest, input_cbor, input_digest
		FROM p2p_cloud_execution_probe_tasks WHERE deployment_id = $1 FOR UPDATE
	`, claim.DeploymentID).Scan(
		&existingTaskID, &existingPlanID, &existingConnectionID, &existingInstanceID, &existingManifestCBOR, &existingManifestDigest, &existingInputCBOR, &existingInputDigest,
	); err != nil {
		return err
	}
	if existingTaskID != taskID || existingPlanID != claim.PlanID || existingConnectionID != claim.ConnectionID || existingInstanceID != claim.InstanceID ||
		existingManifestDigest != manifestDigest || existingInputDigest != inputDigest || !bytes.Equal(existingManifestCBOR, manifestCBOR) || !bytes.Equal(existingInputCBOR, inputCBOR) {
		return errors.New("execution probe task is already bound to different private artifacts")
	}
	payload, err := json.Marshal(struct {
		DeploymentID string `json:"deployment_id"`
		TaskID       string `json:"task_id"`
	}{DeploymentID: claim.DeploymentID, TaskID: taskID})
	if err != nil {
		return err
	}
	outboxID := stableID("cloud_outbox_execution_probe_", taskID)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO p2p_cloud_outbox (outbox_id, kind, aggregate_type, aggregate_id, payload_json, created_at)
		VALUES ($1, $2, 'execution_probe_task', $3, $4, $5)
		ON CONFLICT (outbox_id) DO NOTHING
	`, outboxID, runtime.ExecutionProbeIssueRequested, taskID, string(payload), now)
	if err != nil {
		return err
	}
	var kind, aggregateType, aggregateID, existingPayload string
	if err := tx.QueryRowContext(ctx, `
		SELECT kind, aggregate_type, aggregate_id, payload_json FROM p2p_cloud_outbox WHERE outbox_id = $1 FOR UPDATE
	`, outboxID).Scan(&kind, &aggregateType, &aggregateID, &existingPayload); err != nil {
		return err
	}
	if kind != runtime.ExecutionProbeIssueRequested || aggregateType != "execution_probe_task" || aggregateID != taskID || existingPayload != string(payload) {
		return errors.New("execution probe issue outbox is already bound to another task")
	}
	jobID := executionProbeJobID(taskID)
	return ensureExecutionProbeJobTransitionByID(ctx, tx, jobID, claim.PlanID, claim.DeploymentID, now, researchJobTransition{
		execution: "queued", outcome: "pending", checkpoint: "execution_probe_queued", errorCode: "",
		stepStatus: "queued", stepSummary: "The dedicated Worker transport probe is queued; no Recipe execution or service readiness has been verified.",
	})
}

func executionProbeJobID(taskID string) string {
	return stableID("cloud_job_execution_probe_", taskID)
}

func ensureExecutionProbeJobTransition(ctx context.Context, tx *sql.Tx, claim runtime.ExecutionProbeClaim, now int64, transition researchJobTransition) error {
	return ensureExecutionProbeJobTransitionByID(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, now, transition)
}

func ensureExecutionProbeJobTransitionByID(ctx context.Context, tx *sql.Tx, jobID, planID, deploymentID string, now int64, transition researchJobTransition) error {
	var execution, outcome, checkpoint, errorCode string
	err := tx.QueryRowContext(ctx, `
		SELECT execution_status, outcome_status, checkpoint, error_code
		FROM p2p_cloud_jobs WHERE job_id = $1 FOR UPDATE
	`, jobID).Scan(&execution, &outcome, &checkpoint, &errorCode)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = transitionCloudJob(ctx, tx, jobID, planID, deploymentID, "verify", "execution_probe", now, transition)
		return err
	}
	if err != nil {
		return err
	}
	if execution == transition.execution && outcome == transition.outcome && checkpoint == transition.checkpoint && errorCode == durableErrorCode(transition.errorCode, "") {
		return nil
	}
	_, err = transitionCloudJob(ctx, tx, jobID, planID, deploymentID, "verify", "execution_probe", now, transition)
	return err
}

func executionProbeJobTransition(taskResult runtime.ExecutionProbeTaskResult) researchJobTransition {
	switch taskResult.Status {
	case "queued":
		return researchJobTransition{execution: "verifying", outcome: "pending", checkpoint: "execution_probe_issued", stepStatus: "running", stepSummary: "The sealed Worker task transport was accepted; no Recipe execution or service readiness has been verified."}
	case "running":
		return researchJobTransition{execution: "verifying", outcome: "pending", checkpoint: runtime.ExecutionProbeReceived, stepStatus: "running", stepSummary: "The Worker confirmed receipt of the sealed transport manifest; this is not service readiness."}
	case "succeeded":
		return researchJobTransition{execution: "finished", outcome: "succeeded", checkpoint: runtime.ExecutionProbeTransportPassed, stepStatus: "finished", stepSummary: "The sealed Worker task transport was verified. No Recipe execution or service readiness is implied."}
	case "failed":
		return researchJobTransition{execution: "finished", outcome: "failed", checkpoint: "execution_probe_failed", errorCode: dereferenceExecutionProbe(taskResult.ErrorCode), stepStatus: "failed", stepSummary: "The Worker task transport reported a failure; the retained deployment remains independently tracked."}
	default:
		return researchJobTransition{execution: "finished", outcome: "interrupted", checkpoint: "execution_probe_interrupted", errorCode: dereferenceExecutionProbe(taskResult.ErrorCode), stepStatus: "interrupted", stepSummary: "The Worker task transport was interrupted; the retained deployment remains independently tracked."}
	}
}

type executionProbeTask struct {
	TaskID         string
	TaskAttempt    int64
	Status         string
	LastSequence   int64
	Checkpoint     string
	ErrorCode      string
	EvidenceDigest string
	ObservedAt     int64
	ManifestDigest string
	InputDigest    string
}

func selectExecutionProbeTask(ctx context.Context, tx *sql.Tx, deploymentID string) (executionProbeTask, error) {
	var task executionProbeTask
	err := tx.QueryRowContext(ctx, `
		SELECT task_id, task_attempt, task_status, last_sequence, checkpoint, error_code, evidence_digest, observed_at,
			execution_manifest_digest, input_digest
		FROM p2p_cloud_execution_probe_tasks WHERE deployment_id = $1 FOR UPDATE
	`, deploymentID).Scan(
		&task.TaskID, &task.TaskAttempt, &task.Status, &task.LastSequence, &task.Checkpoint, &task.ErrorCode, &task.EvidenceDigest, &task.ObservedAt,
		&task.ManifestDigest, &task.InputDigest,
	)
	return task, err
}

func (task executionProbeTask) matches(result runtime.ExecutionProbeTaskResult) bool {
	return task.Status == result.Status && task.TaskAttempt == result.Attempt && task.LastSequence == result.LastSequence &&
		task.Checkpoint == dereferenceExecutionProbe(result.Checkpoint) && task.ErrorCode == dereferenceExecutionProbe(result.ErrorCode) &&
		task.EvidenceDigest == dereferenceExecutionProbe(result.EvidenceDigest)
}

func executionProbeTransitionAllowed(current executionProbeTask, result runtime.ExecutionProbeTaskResult) bool {
	if result.LastSequence < current.LastSequence {
		return false
	}
	if result.LastSequence == current.LastSequence && !current.matches(result) {
		// The Stack's first accepted issue response is the one legal sequence-0
		// state change: a locally unissued task becomes durable queued work.
		// Every later same-sequence response must be byte-for-byte equivalent.
		if current.Status != "unissued" || result.Status != "queued" || current.LastSequence != 0 {
			return false
		}
	}
	switch current.Status {
	case "unissued", "queued":
		return true
	case "running":
		return result.Status == "running" || executionProbeTerminal(result.Status)
	case "succeeded", "failed", "interrupted":
		return current.matches(result)
	default:
		return false
	}
}

func executionProbeTerminal(status string) bool {
	return status == "succeeded" || status == "failed" || status == "interrupted"
}

func selectLatestExecutionProbeCommand(ctx context.Context, tx *sql.Tx, taskID, action, requestDigest string) (runtime.ExecutionProbeCommand, bool, error) {
	command, err := scanExecutionProbeCommand(tx.QueryRowContext(ctx, `
		SELECT command_id, task_id, deployment_id, cloud_connection_id, request_digest, command_attempt, action,
			node_key_id, expected_generation, node_counter, canonical_payload_json, payload_sha256, request_sha256,
			signed_envelope_json, issued_at, expires_at, state
		FROM p2p_cloud_execution_probe_commands
		WHERE task_id = $1 AND action = $2 AND request_digest = $3
		ORDER BY command_attempt DESC LIMIT 1 FOR UPDATE
	`, taskID, action, requestDigest))
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ExecutionProbeCommand{}, false, nil
	}
	if err != nil {
		return runtime.ExecutionProbeCommand{}, false, err
	}
	return command, true, nil
}

func selectExecutionProbeCommandByID(ctx context.Context, tx *sql.Tx, commandID string) (runtime.ExecutionProbeCommand, error) {
	return scanExecutionProbeCommand(tx.QueryRowContext(ctx, `
		SELECT command_id, task_id, deployment_id, cloud_connection_id, request_digest, command_attempt, action,
			node_key_id, expected_generation, node_counter, canonical_payload_json, payload_sha256, request_sha256,
			signed_envelope_json, issued_at, expires_at, state
		FROM p2p_cloud_execution_probe_commands WHERE command_id = $1 FOR UPDATE
	`, commandID))
}

func scanExecutionProbeCommand(row interface{ Scan(...any) error }) (runtime.ExecutionProbeCommand, error) {
	var command runtime.ExecutionProbeCommand
	var issuedAt, expiresAt int64
	err := row.Scan(
		&command.CommandID, &command.TaskID, &command.DeploymentID, &command.ConnectionID, &command.RequestDigest, &command.Attempt, &command.Action,
		&command.NodeKeyID, &command.ExpectedGeneration, &command.NodeCounter, &command.PayloadJSON, &command.PayloadSHA256,
		&command.RequestSHA256, &command.SignedEnvelope, &issuedAt, &expiresAt, &command.State,
	)
	if err != nil {
		return runtime.ExecutionProbeCommand{}, err
	}
	if issuedAt != 0 {
		command.IssuedAt = time.UnixMilli(issuedAt).UTC()
	}
	if expiresAt != 0 {
		command.ExpiresAt = time.UnixMilli(expiresAt).UTC()
	}
	return command, nil
}

func sameExecutionProbeCommandIdentity(left, right runtime.ExecutionProbeCommand) bool {
	return left.CommandID == right.CommandID && left.TaskID == right.TaskID && left.DeploymentID == right.DeploymentID &&
		left.ConnectionID == right.ConnectionID && left.NodeKeyID == right.NodeKeyID && left.ExpectedGeneration == right.ExpectedGeneration &&
		left.NodeCounter == right.NodeCounter && left.Attempt == right.Attempt && left.Action == right.Action && left.RequestDigest == right.RequestDigest
}

func sameSignedExecutionProbeCommand(command runtime.ExecutionProbeCommand, signed runtime.SignedExecutionProbeCommand) bool {
	return command.PayloadJSON == signed.PayloadJSON && command.PayloadSHA256 == signed.PayloadSHA256 && command.RequestSHA256 == signed.RequestSHA256 &&
		command.SignedEnvelope == signed.EnvelopeJSON && command.IssuedAt.UTC().Equal(signed.IssuedAt.UTC()) && command.ExpiresAt.UTC().Equal(signed.ExpiresAt.UTC())
}

func signedExecutionProbeCommand(command runtime.ExecutionProbeCommand) runtime.SignedExecutionProbeCommand {
	return runtime.SignedExecutionProbeCommand{
		EnvelopeJSON: command.SignedEnvelope, PayloadJSON: command.PayloadJSON, PayloadSHA256: command.PayloadSHA256,
		RequestSHA256: command.RequestSHA256, IssuedAt: command.IssuedAt, ExpiresAt: command.ExpiresAt,
	}
}

func validPersistedExecutionProbeCommand(claim runtime.ExecutionProbeClaim, signed runtime.SignedExecutionProbeCommand) error {
	if !validExecutionProbeCommandClaim(claim) || strings.TrimSpace(signed.EnvelopeJSON) != signed.EnvelopeJSON || signed.EnvelopeJSON == "" ||
		strings.TrimSpace(signed.PayloadJSON) != signed.PayloadJSON || signed.PayloadJSON == "" || !lowerHexSHA256ForObservation(signed.PayloadSHA256) ||
		!lowerHexSHA256ForObservation(signed.RequestSHA256) || len(signed.EnvelopeJSON) > 256*1024 || len(signed.PayloadJSON) > 8*1024 ||
		signed.IssuedAt.IsZero() || signed.ExpiresAt.IsZero() || !signed.ExpiresAt.After(signed.IssuedAt) || signed.ExpiresAt.Sub(signed.IssuedAt) > 5*time.Minute {
		return errors.New("signed execution probe command is invalid")
	}
	switch claim.Phase {
	case runtime.ExecutionProbePhaseIssue:
		command, err := broker.ParseWorkerTaskIssueCommand([]byte(signed.EnvelopeJSON))
		if err != nil || command.ValidateBinding(broker.WorkerTaskIssueCommandBinding{
			ConnectionID: claim.ConnectionID, CommandID: claim.Command.CommandID, NodeKeyID: claim.NodeKeyID,
			ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter,
			IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, Request: brokerExecutionProbeIssueRequest(claim.IssueRequest),
		}) != nil || command.PayloadSHA256 != signed.PayloadSHA256 || command.RequestSHA256() != signed.RequestSHA256 {
			return errors.New("signed execution probe issue command is invalid")
		}
		payload, decodeErr := base64.StdEncoding.DecodeString(command.PayloadB64)
		if decodeErr != nil || string(payload) != signed.PayloadJSON {
			return errors.New("signed execution probe issue command payload is invalid")
		}
	case runtime.ExecutionProbePhaseObserve:
		command, err := broker.ParseWorkerTaskObserveCommand([]byte(signed.EnvelopeJSON))
		if err != nil || command.ValidateBinding(broker.WorkerTaskObserveCommandBinding{
			ConnectionID: claim.ConnectionID, CommandID: claim.Command.CommandID, NodeKeyID: claim.NodeKeyID,
			ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter,
			IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, Request: brokerExecutionProbeObserveRequest(claim.ObserveRequest),
		}) != nil || command.PayloadSHA256 != signed.PayloadSHA256 || command.RequestSHA256() != signed.RequestSHA256 {
			return errors.New("signed execution probe observe command is invalid")
		}
		payload, decodeErr := base64.StdEncoding.DecodeString(command.PayloadB64)
		if decodeErr != nil || string(payload) != signed.PayloadJSON {
			return errors.New("signed execution probe observe command payload is invalid")
		}
	default:
		return errors.New("execution probe phase is invalid")
	}
	return nil
}

func brokerExecutionProbeIssueRequest(request runtime.ExecutionProbeIssueRequest) broker.WorkerTaskIssueRequest {
	return broker.WorkerTaskIssueRequest{
		Schema: request.Schema, DeploymentID: request.DeploymentID, TaskID: request.TaskID, TaskKind: request.TaskKind,
		ExecutionManifestDigest: request.ExecutionManifestDigest, InputDigest: request.InputDigest,
	}
}

func brokerExecutionProbeObserveRequest(request runtime.ExecutionProbeObserveRequest) broker.WorkerTaskObserveRequest {
	return broker.WorkerTaskObserveRequest{DeploymentID: request.DeploymentID, TaskID: request.TaskID}
}

func validExecutionProbeClaimForStore(claim runtime.ExecutionProbeClaim) bool {
	if claim.DeploymentID == "" || claim.PlanID == "" || claim.ConnectionID == "" || claim.Region == "" || claim.InstanceID == "" || claim.TaskID == "" ||
		claim.TaskAttempt <= 0 || claim.ExecutionManifestDigest == "" || claim.InputDigest == "" || claim.BrokerEndpoint == "" || claim.NodeKeyID == "" ||
		claim.ExpectedGeneration <= 0 || claim.JobID == "" || claim.LeaseToken == "" || strings.TrimSpace(claim.DeploymentID) != claim.DeploymentID ||
		strings.TrimSpace(claim.PlanID) != claim.PlanID || strings.TrimSpace(claim.ConnectionID) != claim.ConnectionID || strings.TrimSpace(claim.InstanceID) != claim.InstanceID ||
		strings.TrimSpace(claim.TaskID) != claim.TaskID || strings.TrimSpace(claim.NodeKeyID) != claim.NodeKeyID || strings.TrimSpace(claim.JobID) != claim.JobID ||
		strings.TrimSpace(claim.LeaseToken) != claim.LeaseToken || !executionProbeDigest(claim.ExecutionManifestDigest) || !executionProbeDigest(claim.InputDigest) {
		return false
	}
	if cloudmodule.ContainsSensitiveGoalMaterial(claim.DeploymentID) || cloudmodule.ContainsSensitiveGoalMaterial(claim.PlanID) ||
		cloudmodule.ContainsSensitiveGoalMaterial(claim.ConnectionID) || cloudmodule.ContainsSensitiveGoalMaterial(claim.InstanceID) ||
		cloudmodule.ContainsSensitiveGoalMaterial(claim.TaskID) || cloudmodule.ContainsSensitiveGoalMaterial(claim.NodeKeyID) ||
		cloudmodule.ContainsSensitiveGoalMaterial(claim.JobID) || cloudmodule.ContainsSensitiveGoalMaterial(claim.LeaseToken) {
		return false
	}
	if err := cloudmodule.ValidateConnectionRegistrationEndpoint(claim.BrokerEndpoint, claim.Region); err != nil {
		return false
	}
	switch claim.Phase {
	case runtime.ExecutionProbePhaseIssue:
		return claim.OutboxID != "" && claim.Kind == runtime.ExecutionProbeIssueRequested && claim.AggregateType == "execution_probe_task" && claim.AggregateID == claim.TaskID &&
			claim.IssueRequest.Validate() == nil && claim.IssueRequest.DeploymentID == claim.DeploymentID && claim.IssueRequest.TaskID == claim.TaskID &&
			claim.IssueRequest.ExecutionManifestDigest == claim.ExecutionManifestDigest && claim.IssueRequest.InputDigest == claim.InputDigest
	case runtime.ExecutionProbePhaseObserve:
		return claim.OutboxID == "" && claim.Kind == "" && claim.AggregateType == "" && claim.AggregateID == "" &&
			claim.ObserveRequest.Validate() == nil && claim.ObserveRequest.DeploymentID == claim.DeploymentID && claim.ObserveRequest.TaskID == claim.TaskID
	default:
		return false
	}
}

func validExecutionProbeCommandClaim(claim runtime.ExecutionProbeClaim) bool {
	if !validExecutionProbeClaimForStore(claim) || claim.Command.CommandID == "" || strings.TrimSpace(claim.Command.CommandID) != claim.Command.CommandID ||
		claim.Command.DeploymentID != claim.DeploymentID || claim.Command.TaskID != claim.TaskID || claim.Command.ConnectionID != claim.ConnectionID ||
		claim.Command.NodeKeyID != claim.NodeKeyID || claim.Command.ExpectedGeneration != claim.ExpectedGeneration || claim.Command.NodeCounter <= 0 ||
		claim.Command.Attempt <= 0 {
		return false
	}
	requestDigest, action, err := executionProbeRequestIdentity(claim)
	return err == nil && claim.Command.Action == action && claim.Command.RequestDigest == requestDigest
}

func executionProbeRequestIdentity(claim runtime.ExecutionProbeClaim) (string, string, error) {
	switch claim.Phase {
	case runtime.ExecutionProbePhaseIssue:
		digest, err := claim.IssueRequest.Digest()
		return digest, runtime.ExecutionProbeIssueAction, err
	case runtime.ExecutionProbePhaseObserve:
		digest, err := claim.ObserveRequest.Digest()
		return digest, runtime.ExecutionProbeObserveAction, err
	default:
		return "", "", errors.New("execution probe phase is invalid")
	}
}

func executionProbeDigest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	return lowerHexSHA256ForObservation(strings.TrimPrefix(value, "sha256:"))
}

func updateExecutionProbeCommandAttempt(ctx context.Context, tx *sql.Tx, claim runtime.ExecutionProbeClaim, code string, now int64) error {
	requestDigest, action, err := executionProbeRequestIdentity(claim)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_execution_probe_commands
		SET state = CASE WHEN state IN ('signed', 'indeterminate') THEN 'indeterminate' ELSE state END,
			attempts = attempts + 1, last_error_code = $1, updated_at = $2
		WHERE command_id = $3 AND task_id = $4 AND deployment_id = $5 AND cloud_connection_id = $6
			AND action = $7 AND request_digest = $8 AND command_attempt = $9
	`, code, now, claim.Command.CommandID, claim.TaskID, claim.DeploymentID, claim.ConnectionID, action, requestDigest, claim.Command.Attempt)
	if err != nil {
		return err
	}
	return requireOneAffected(result)
}

func releaseExecutionProbeClaim(ctx context.Context, tx *sql.Tx, claim runtime.ExecutionProbeClaim, available int64, code string, now int64) error {
	if claim.Phase == runtime.ExecutionProbePhaseIssue {
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_outbox
			SET lease_owner = '', lease_token = '', lease_until = 0, available_at = $1, last_error_code = $2
			WHERE outbox_id = $3 AND lease_token = $4 AND completed_at = 0
		`, available, code, claim.OutboxID, claim.LeaseToken)
		if err != nil {
			return err
		}
		return requireOneAffected(result)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_execution_probe_tasks
		SET lease_owner = '', lease_token = '', lease_until = 0, available_at = $1,
			last_error_code = $2, updated_at = $3
		WHERE deployment_id = $4 AND lease_token = $5
	`, available, code, now, claim.DeploymentID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return requireOneAffected(result)
}

func completeExecutionProbeIssueOutbox(ctx context.Context, tx *sql.Tx, claim runtime.ExecutionProbeClaim, now int64) error {
	result, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_outbox
		SET lease_owner = '', lease_token = '', lease_until = 0, completed_at = $1,
			delivered_at = $1, available_at = $1, last_error_code = ''
		WHERE outbox_id = $2 AND lease_token = $3 AND completed_at = 0
	`, now, claim.OutboxID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return requireOneAffected(result)
}

func dereferenceExecutionProbe(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (s *Store) withExecutionProbeClaimTransaction(ctx context.Context, claim runtime.ExecutionProbeClaim, run func(*sql.Tx, int64) error) (err error) {
	if s == nil || s.db == nil {
		return errors.New("cloud orchestrator database is unavailable")
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
	if err = verifyExecutionProbeClaimFence(ctx, tx, claim, now); err != nil {
		return err
	}
	if err = run(tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

func verifyExecutionProbeClaimFence(ctx context.Context, tx *sql.Tx, claim runtime.ExecutionProbeClaim, now int64) error {
	if !validExecutionProbeClaimForStore(claim) {
		return ErrLeaseLost
	}
	var taskID, planID, connectionID, instanceID, manifestDigest, inputDigest, taskStatus, taskLeaseToken string
	var taskAttempt, taskLeaseUntil int64
	var deploymentPlanID, deploymentConnectionID, deploymentExecution, deploymentOutcome, deploymentResource string
	var planStatus, connectionStatus, connectionRegion, endpoint, brokerRegion, nodeKeyID string
	var generation int64
	var resourceConnectionID, resourceStatus, resourceInstanceID string
	var observationState string
	var observationLeaseExpiresAt int64
	var provisionJobID, provisionPlanID, provisionExecution, provisionOutcome, provisionCheckpoint, provisionKind string
	var verifyJobID, verifyPlanID, verifyDeploymentID, verifyKind string
	err := tx.QueryRowContext(ctx, `
		SELECT task.task_id, task.plan_id, task.cloud_connection_id, task.instance_id, task.execution_manifest_digest, task.input_digest,
			task.task_status, task.task_attempt, task.lease_token, task.lease_until,
			deployment.plan_id, deployment.cloud_connection_id, deployment.execution_status, deployment.outcome_status, deployment.resource_status,
			plan.status, connection.status, connection.region, broker.broker_command_url, broker.broker_region, broker.node_key_id, broker.connection_generation,
			resource.cloud_connection_id, resource.resource_status, resource.instance_id,
			observation.worker_session_state, observation.worker_lease_expires_at,
			provision_job.job_id, provision_job.plan_id, provision_job.execution_status, provision_job.outcome_status, provision_job.checkpoint, provision_job.kind,
			verify_job.job_id, verify_job.plan_id, verify_job.deployment_id, verify_job.kind
		FROM p2p_cloud_execution_probe_tasks AS task
		JOIN p2p_cloud_deployments AS deployment ON deployment.deployment_id = task.deployment_id
		JOIN p2p_cloud_plans AS plan ON plan.plan_id = deployment.plan_id
		JOIN p2p_cloud_connections AS connection ON connection.cloud_connection_id = deployment.cloud_connection_id
		JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = deployment.cloud_connection_id
		JOIN p2p_cloud_deployment_resources AS resource ON resource.deployment_id = deployment.deployment_id
		JOIN p2p_cloud_worker_bootstrap_observations AS observation ON observation.deployment_id = deployment.deployment_id
		JOIN p2p_cloud_jobs AS provision_job ON provision_job.deployment_id = deployment.deployment_id AND provision_job.kind = 'provision'
		JOIN p2p_cloud_jobs AS verify_job ON verify_job.deployment_id = deployment.deployment_id AND verify_job.kind = 'verify'
		WHERE task.deployment_id = $1
		FOR UPDATE OF task, deployment, plan, connection, broker, resource, observation, provision_job, verify_job
	`, claim.DeploymentID).Scan(
		&taskID, &planID, &connectionID, &instanceID, &manifestDigest, &inputDigest, &taskStatus, &taskAttempt, &taskLeaseToken, &taskLeaseUntil,
		&deploymentPlanID, &deploymentConnectionID, &deploymentExecution, &deploymentOutcome, &deploymentResource,
		&planStatus, &connectionStatus, &connectionRegion, &endpoint, &brokerRegion, &nodeKeyID, &generation,
		&resourceConnectionID, &resourceStatus, &resourceInstanceID, &observationState, &observationLeaseExpiresAt,
		&provisionJobID, &provisionPlanID, &provisionExecution, &provisionOutcome, &provisionCheckpoint, &provisionKind,
		&verifyJobID, &verifyPlanID, &verifyDeploymentID, &verifyKind,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if taskID != claim.TaskID || planID != claim.PlanID || connectionID != claim.ConnectionID || instanceID != claim.InstanceID ||
		manifestDigest != claim.ExecutionManifestDigest || inputDigest != claim.InputDigest || taskAttempt != claim.TaskAttempt ||
		deploymentPlanID != claim.PlanID || deploymentConnectionID != claim.ConnectionID || deploymentExecution != "verifying" || deploymentOutcome != "pending" || deploymentResource != "active" ||
		planStatus != "approved" || connectionStatus != "active" || connectionRegion != claim.Region || endpoint != claim.BrokerEndpoint || brokerRegion != claim.Region ||
		nodeKeyID != claim.NodeKeyID || generation != claim.ExpectedGeneration || resourceConnectionID != claim.ConnectionID || resourceStatus != "active" || resourceInstanceID != claim.InstanceID ||
		observationState != "active" || observationLeaseExpiresAt <= now || provisionJobID == "" || provisionPlanID != claim.PlanID || provisionKind != "provision" ||
		provisionExecution != "verifying" || provisionOutcome != "pending" || provisionCheckpoint != "worker_bootstrap_verified" ||
		verifyJobID != claim.JobID || verifyPlanID != claim.PlanID || verifyDeploymentID != claim.DeploymentID || verifyKind != "verify" {
		return ErrLeaseLost
	}
	switch claim.Phase {
	case runtime.ExecutionProbePhaseIssue:
		var outboxKind, aggregateType, aggregateID, leaseToken, payload string
		var leaseUntil, completedAt int64
		err := tx.QueryRowContext(ctx, `
			SELECT kind, aggregate_type, aggregate_id, payload_json, lease_token, lease_until, completed_at
			FROM p2p_cloud_outbox WHERE outbox_id = $1 FOR UPDATE
		`, claim.OutboxID).Scan(&outboxKind, &aggregateType, &aggregateID, &payload, &leaseToken, &leaseUntil, &completedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return err
		}
		payloadDeploymentID, payloadTaskID, payloadErr := decodeExecutionProbeIssueOutbox(payload)
		if taskStatus != "unissued" || outboxKind != runtime.ExecutionProbeIssueRequested || aggregateType != "execution_probe_task" || aggregateID != claim.TaskID ||
			payloadErr != nil || payloadDeploymentID != claim.DeploymentID || payloadTaskID != claim.TaskID ||
			leaseToken != claim.LeaseToken || leaseUntil <= now || completedAt != 0 {
			return ErrLeaseLost
		}
	case runtime.ExecutionProbePhaseObserve:
		if (taskStatus != "queued" && taskStatus != "running") || taskLeaseToken != claim.LeaseToken || taskLeaseUntil <= now {
			return ErrLeaseLost
		}
	default:
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) sameCommittedExecutionProbeResult(ctx context.Context, claim runtime.ExecutionProbeClaim, result runtime.ExecutionProbeTaskResult) (bool, error) {
	if s == nil || s.db == nil || !validExecutionProbeClaimForStore(claim) {
		return false, nil
	}
	var task executionProbeTask
	var planID, connectionID, jobID, jobExecution, jobOutcome, jobCheckpoint string
	err := s.db.QueryRowContext(ctx, `
		SELECT task.task_id, task.task_attempt, task.task_status, task.last_sequence, task.checkpoint, task.error_code, task.evidence_digest, task.observed_at,
			task.execution_manifest_digest, task.input_digest, deployment.plan_id, deployment.cloud_connection_id,
			verify_job.job_id, verify_job.execution_status, verify_job.outcome_status, verify_job.checkpoint
		FROM p2p_cloud_execution_probe_tasks AS task
		JOIN p2p_cloud_deployments AS deployment ON deployment.deployment_id = task.deployment_id
		JOIN p2p_cloud_jobs AS verify_job ON verify_job.deployment_id = deployment.deployment_id AND verify_job.kind = 'verify'
		WHERE task.deployment_id = $1
	`, claim.DeploymentID).Scan(
		&task.TaskID, &task.TaskAttempt, &task.Status, &task.LastSequence, &task.Checkpoint, &task.ErrorCode, &task.EvidenceDigest, &task.ObservedAt,
		&task.ManifestDigest, &task.InputDigest, &planID, &connectionID, &jobID, &jobExecution, &jobOutcome, &jobCheckpoint,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	transition := executionProbeJobTransition(result)
	return task.TaskID == claim.TaskID && task.TaskAttempt == result.Attempt && task.ManifestDigest == claim.ExecutionManifestDigest && task.InputDigest == claim.InputDigest &&
		planID == claim.PlanID && connectionID == claim.ConnectionID && jobID == claim.JobID && task.matches(result) &&
		jobExecution == transition.execution && jobOutcome == transition.outcome && jobCheckpoint == transition.checkpoint, nil
}

func decodeExecutionProbeIssueOutbox(payload string) (deploymentID, taskID string, err error) {
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.DisallowUnknownFields()
	var value struct {
		DeploymentID string `json:"deployment_id"`
		TaskID       string `json:"task_id"`
	}
	if err := decoder.Decode(&value); err != nil {
		return "", "", errors.New("execution probe issue outbox payload is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return "", "", errors.New("execution probe issue outbox payload contains trailing JSON")
	}
	if strings.TrimSpace(value.DeploymentID) != value.DeploymentID || strings.TrimSpace(value.TaskID) != value.TaskID || value.DeploymentID == "" || value.TaskID == "" {
		return "", "", errors.New("execution probe issue outbox payload is invalid")
	}
	return value.DeploymentID, value.TaskID, nil
}
