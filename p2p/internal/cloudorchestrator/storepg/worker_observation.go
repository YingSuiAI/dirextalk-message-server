package storepg

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var _ runtime.WorkerBootstrapObservationStore = (*Store)(nil)

// ErrWorkerBootstrapObservationStale means the Stack observation is older
// than private evidence already committed for this deployment. It is never
// projected to ProductCore because it is an internal read/retry fact.
var ErrWorkerBootstrapObservationStale = errors.New("cloud worker bootstrap observation is stale")

// ClaimWorkerBootstrapObservation leases one private, signed read after the
// create receipt has been recorded. Unlike a provision outbox it uses a
// dedicated observation lease: polling never becomes an AWS mutation and a
// restart cannot duplicate a command counter.
func (s *Store) ClaimWorkerBootstrapObservation(ctx context.Context, workerID string, lease time.Duration) (runtime.WorkerBootstrapObservationClaim, bool, error) {
	if s == nil || s.db == nil {
		return runtime.WorkerBootstrapObservationClaim{}, false, errors.New("cloud orchestrator database is unavailable")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || len(workerID) > 128 || strings.ContainsAny(workerID, "\r\n\t") {
		return runtime.WorkerBootstrapObservationClaim{}, false, errors.New("cloud orchestrator worker id is invalid")
	}
	if lease <= 0 || lease > 5*time.Minute {
		return runtime.WorkerBootstrapObservationClaim{}, false, errors.New("cloud worker bootstrap observation lease must be between 1ns and 5m")
	}
	now := s.now().UnixMilli()
	if err := s.ensureWorkerBootstrapObservationRows(ctx, now); err != nil {
		return runtime.WorkerBootstrapObservationClaim{}, false, err
	}
	token := strings.TrimSpace(s.cfg.NewLeaseToken())
	if token == "" || len(token) > 128 {
		return runtime.WorkerBootstrapObservationClaim{}, false, errors.New("cloud orchestrator lease token is invalid")
	}
	row := s.db.QueryRowContext(ctx, `
		WITH selected AS (
			SELECT observation.deployment_id
			FROM p2p_cloud_worker_bootstrap_observations AS observation
			JOIN p2p_cloud_deployments AS deployment ON deployment.deployment_id = observation.deployment_id
			JOIN p2p_cloud_jobs AS job ON job.deployment_id = deployment.deployment_id AND job.kind = 'provision'
			JOIN p2p_cloud_connections AS connection ON connection.cloud_connection_id = deployment.cloud_connection_id
			JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = deployment.cloud_connection_id
			JOIN p2p_cloud_deployment_resources AS resource ON resource.deployment_id = deployment.deployment_id
			WHERE observation.available_at <= $1
				AND observation.lease_until <= $1
				AND deployment.execution_status = 'provisioning'
				AND deployment.outcome_status = 'pending'
				AND deployment.resource_status = 'active'
				AND job.execution_status = 'provisioning'
				AND job.outcome_status = 'pending'
				AND job.checkpoint = 'worker_bootstrap_pending'
				AND connection.status = 'active'
				AND connection.region = broker.broker_region
				AND resource.cloud_connection_id = deployment.cloud_connection_id
				AND resource.resource_status = 'active'
			ORDER BY observation.available_at ASC, observation.created_at ASC, observation.deployment_id ASC
			FOR UPDATE OF observation SKIP LOCKED
			LIMIT 1
		), claimed AS (
			UPDATE p2p_cloud_worker_bootstrap_observations AS observation
			SET lease_owner = $2, lease_token = $3, lease_until = $4,
				attempts = observation.attempts + 1, last_error_code = '', updated_at = $1
			FROM selected
			WHERE observation.deployment_id = selected.deployment_id
			RETURNING observation.deployment_id, observation.cloud_connection_id, observation.instance_id,
				observation.lease_token, observation.attempts
		)
		SELECT claimed.deployment_id, deployment.plan_id, claimed.cloud_connection_id, connection.region,
			claimed.instance_id, broker.broker_command_url, broker.node_key_id, broker.connection_generation,
			job.job_id, claimed.lease_token, claimed.attempts
		FROM claimed
		JOIN p2p_cloud_deployments AS deployment ON deployment.deployment_id = claimed.deployment_id
		JOIN p2p_cloud_connections AS connection ON connection.cloud_connection_id = claimed.cloud_connection_id
		JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = claimed.cloud_connection_id
		JOIN p2p_cloud_jobs AS job ON job.deployment_id = claimed.deployment_id AND job.kind = 'provision'
	`, now, workerID, token, now+lease.Milliseconds())
	var claim runtime.WorkerBootstrapObservationClaim
	if err := row.Scan(
		&claim.DeploymentID, &claim.PlanID, &claim.ConnectionID, &claim.Region, &claim.InstanceID,
		&claim.BrokerEndpoint, &claim.NodeKeyID, &claim.ExpectedGeneration, &claim.JobID, &claim.LeaseToken, &claim.Attempt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.WorkerBootstrapObservationClaim{}, false, nil
		}
		return runtime.WorkerBootstrapObservationClaim{}, false, fmt.Errorf("claim cloud worker bootstrap observation: %w", err)
	}
	claim.Request = runtime.WorkerBootstrapObservationRequest{DeploymentID: claim.DeploymentID}
	command, err := s.prepareWorkerBootstrapObservationCommand(ctx, claim)
	if err != nil {
		return runtime.WorkerBootstrapObservationClaim{}, false, err
	}
	claim.Command = command
	return claim, true, nil
}

func (s *Store) ensureWorkerBootstrapObservationRows(ctx context.Context, now int64) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO p2p_cloud_worker_bootstrap_observations (
			deployment_id, cloud_connection_id, instance_id, created_at, updated_at
		)
		SELECT deployment.deployment_id, deployment.cloud_connection_id, resource.instance_id, $1, $1
		FROM p2p_cloud_deployments AS deployment
		JOIN p2p_cloud_jobs AS job ON job.deployment_id = deployment.deployment_id AND job.kind = 'provision'
		JOIN p2p_cloud_deployment_resources AS resource ON resource.deployment_id = deployment.deployment_id
		WHERE deployment.execution_status = 'provisioning'
			AND deployment.outcome_status = 'pending'
			AND deployment.resource_status = 'active'
			AND job.execution_status = 'provisioning'
			AND job.outcome_status = 'pending'
			AND job.checkpoint = 'worker_bootstrap_pending'
			AND resource.cloud_connection_id = deployment.cloud_connection_id
			AND resource.resource_status = 'active'
		ON CONFLICT (deployment_id) DO NOTHING
	`, now)
	if err != nil {
		return fmt.Errorf("initialize cloud worker bootstrap observations: %w", err)
	}
	return nil
}

func (s *Store) prepareWorkerBootstrapObservationCommand(ctx context.Context, claim runtime.WorkerBootstrapObservationClaim) (runtime.WorkerBootstrapObservationCommand, error) {
	digest, err := claim.Request.Digest()
	if err != nil {
		return runtime.WorkerBootstrapObservationCommand{}, fmt.Errorf("worker bootstrap observation request digest: %w", err)
	}
	var command runtime.WorkerBootstrapObservationCommand
	err = s.withWorkerBootstrapObservationClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		latest, found, err := selectLatestWorkerBootstrapObservationCommand(ctx, tx, claim, digest)
		if err != nil {
			return err
		}
		if found && latest.State != "expired" {
			command = latest
			return nil
		}
		attempt := 1
		if found {
			attempt = latest.Attempt + 1
		}
		var counter int64
		if err := tx.QueryRowContext(ctx, `
			UPDATE p2p_cloud_connection_brokers
			SET next_node_counter = next_node_counter + 1, updated_at = $1
			WHERE cloud_connection_id = $2
			RETURNING next_node_counter
		`, now, claim.ConnectionID).Scan(&counter); err != nil {
			return err
		}
		command = runtime.WorkerBootstrapObservationCommand{
			CommandID:          stableID("cloud_broker_deployment_observe_", claim.ConnectionID, claim.DeploymentID, digest, fmt.Sprint(attempt)),
			DeploymentID:       claim.DeploymentID,
			ConnectionID:       claim.ConnectionID,
			NodeKeyID:          claim.NodeKeyID,
			ExpectedGeneration: claim.ExpectedGeneration,
			NodeCounter:        counter,
			Attempt:            attempt,
			RequestDigest:      digest,
			State:              "allocated",
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_deployment_observation_commands (
				command_id, deployment_id, cloud_connection_id, request_digest, command_attempt,
				action, node_key_id, expected_generation, node_counter, state, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, 'deployment.observe', $6, $7, $8, 'allocated', $9, $9)
		`, command.CommandID, command.DeploymentID, command.ConnectionID, command.RequestDigest, command.Attempt,
			command.NodeKeyID, command.ExpectedGeneration, command.NodeCounter, now)
		return err
	})
	return command, err
}

func (s *Store) PersistWorkerBootstrapObservationCommand(ctx context.Context, claim runtime.WorkerBootstrapObservationClaim, signed runtime.SignedWorkerBootstrapObservationCommand) error {
	if err := validPersistedWorkerBootstrapObservationCommand(claim, signed); err != nil {
		return err
	}
	return s.withWorkerBootstrapObservationClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		existing, err := selectWorkerBootstrapObservationCommandByID(ctx, tx, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if !sameWorkerBootstrapObservationCommandIdentity(existing, claim.Command) {
			return errors.New("persisted worker bootstrap observation command does not match the claim")
		}
		if existing.State == "signed" || existing.State == "indeterminate" || existing.State == "accepted" {
			if existing.PayloadJSON == signed.PayloadJSON && existing.PayloadSHA256 == signed.PayloadSHA256 &&
				existing.RequestSHA256 == signed.RequestSHA256 && existing.SignedEnvelope == signed.EnvelopeJSON &&
				existing.IssuedAt.UTC().Equal(signed.IssuedAt.UTC()) && existing.ExpiresAt.UTC().Equal(signed.ExpiresAt.UTC()) {
				return nil
			}
			return errors.New("worker bootstrap observation command already has a different signed envelope")
		}
		if existing.State != "allocated" {
			return errors.New("worker bootstrap observation command cannot be signed in its current state")
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_deployment_observation_commands
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

func (s *Store) MarkWorkerBootstrapObservationStarted(ctx context.Context, claim runtime.WorkerBootstrapObservationClaim) error {
	return s.withWorkerBootstrapObservationClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_worker_bootstrap_observations
			SET last_error_code = '', updated_at = $1
			WHERE deployment_id = $2 AND lease_token = $3
		`, now, claim.DeploymentID, claim.LeaseToken)
		if err != nil {
			return err
		}
		return requireOneAffected(result)
	})
}

// DeferWorkerBootstrapObservation preserves the exact signed read for retry.
// A not-yet-active Worker is expected during first boot and does not fail the
// deployment or alter public Job state.
func (s *Store) DeferWorkerBootstrapObservation(ctx context.Context, claim runtime.WorkerBootstrapObservationClaim, code string, availableAt time.Time) error {
	code = durableErrorCode(code, "worker_bootstrap_observation_retryable")
	return s.withWorkerBootstrapObservationClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if err := updateWorkerBootstrapObservationCommandAttempt(ctx, tx, claim.Command.CommandID, code, now); err != nil {
			return err
		}
		available := availableAt.UTC().UnixMilli()
		if available < now {
			available = now
		}
		return releaseWorkerBootstrapObservationClaim(ctx, tx, claim, available, code, now)
	})
}

func (s *Store) ExpireWorkerBootstrapObservationCommand(ctx context.Context, claim runtime.WorkerBootstrapObservationClaim) error {
	return s.withWorkerBootstrapObservationClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_deployment_observation_commands
			SET state = 'expired', attempts = attempts + 1, last_error_code = 'worker_bootstrap_observation_command_expired', updated_at = $1
			WHERE command_id = $2 AND state IN ('allocated', 'signed', 'indeterminate')
		`, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(result); err != nil {
			return err
		}
		return releaseWorkerBootstrapObservationClaim(ctx, tx, claim, now, "worker_bootstrap_observation_command_expired", now)
	})
}

// FailWorkerBootstrapObservation intentionally remains retryable. A failed
// signed read cannot prove the Worker or EC2 instance failed, so it must never
// turn a retained resource into a terminal deployment outcome.
func (s *Store) FailWorkerBootstrapObservation(ctx context.Context, claim runtime.WorkerBootstrapObservationClaim, code string) error {
	return s.DeferWorkerBootstrapObservation(ctx, claim, code, s.now().Add(time.Minute))
}

// CommitWorkerBootstrapObservation atomically records minimum private
// evidence and advances only the existing provision Job checkpoint. The
// Stack has already bound hidden session identity; this Store independently
// binds the returned deployment/instance and rejects stale epochs.
func (s *Store) CommitWorkerBootstrapObservation(ctx context.Context, claim runtime.WorkerBootstrapObservationClaim, observation runtime.WorkerBootstrapObservation) error {
	if err := runtime.ValidateWorkerBootstrapObservation(claim, observation, s.now()); err != nil {
		duplicate, duplicateErr := s.sameVerifiedWorkerBootstrapObservation(ctx, claim, observation)
		if duplicateErr != nil {
			return duplicateErr
		}
		if duplicate {
			return nil
		}
		return err
	}
	err := s.withWorkerBootstrapObservationClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		command, err := selectWorkerBootstrapObservationCommandByID(ctx, tx, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if !sameWorkerBootstrapObservationCommandIdentity(command, claim.Command) ||
			(command.State != "signed" && command.State != "indeterminate" && command.State != "accepted") {
			return errors.New("worker bootstrap observation command is not eligible for a result")
		}
		signed := runtime.SignedWorkerBootstrapObservationCommand{
			EnvelopeJSON: command.SignedEnvelope, PayloadJSON: command.PayloadJSON, PayloadSHA256: command.PayloadSHA256,
			RequestSHA256: command.RequestSHA256, IssuedAt: command.IssuedAt, ExpiresAt: command.ExpiresAt,
		}
		if err := validPersistedWorkerBootstrapObservationCommand(claim, signed); err != nil {
			return err
		}
		if err := runtime.ValidateWorkerBootstrapObservation(claim, observation, time.UnixMilli(now).UTC()); err != nil {
			return err
		}
		current, err := selectWorkerBootstrapEvidence(ctx, tx, claim.DeploymentID)
		if err != nil {
			return err
		}
		if current.WorkerSessionState == "active" {
			if observation.LeaseEpoch < current.LeaseEpoch {
				return ErrWorkerBootstrapObservationStale
			}
			if observation.LeaseEpoch == current.LeaseEpoch && !sameWorkerBootstrapEvidence(current, observation) {
				return ErrWorkerBootstrapObservationStale
			}
		}
		commandUpdate, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_deployment_observation_commands
			SET state = 'accepted', attempts = attempts + 1, last_error_code = '', updated_at = $1
			WHERE command_id = $2 AND state IN ('signed', 'indeterminate', 'accepted')
		`, now, command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(commandUpdate); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_worker_bootstrap_observations
			SET worker_session_state = 'active', worker_lease_epoch = $1, worker_lease_expires_at = $2,
				worker_last_sequence = $3, worker_last_event_at = $4, observed_at = $5,
				available_at = $6, lease_owner = '', lease_token = '', lease_until = 0,
				last_error_code = '', updated_at = $6
			WHERE deployment_id = $7 AND cloud_connection_id = $8 AND instance_id = $9 AND lease_token = $10
		`, observation.LeaseEpoch, observation.LeaseExpiresAt.UTC().UnixMilli(), observation.LastSequence,
			zeroTimeUnixMilli(observation.LastEventAt), observation.ObservedAt.UTC().UnixMilli(), now,
			claim.DeploymentID, claim.ConnectionID, claim.InstanceID, claim.LeaseToken)
		if err != nil {
			return err
		}
		if err := requireOneAffected(result); err != nil {
			return err
		}
		if _, err := transitionDeployment(ctx, tx, claim.DeploymentID, claim.PlanID, claim.ConnectionID, now, "verifying", "pending", "active"); err != nil {
			return err
		}
		_, err = transitionCloudJob(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, "provision", "provision", now, researchJobTransition{
			execution: "verifying", outcome: "pending", checkpoint: "worker_bootstrap_verified", errorCode: "",
			stepStatus: "finished", stepSummary: "The dedicated Worker identity and active bootstrap lease were independently verified by the Connection Stack.",
		})
		if err != nil {
			return err
		}
		return ensureExecutionProbeTask(ctx, tx, claim, now)
	})
	if errors.Is(err, ErrLeaseLost) {
		duplicate, duplicateErr := s.sameVerifiedWorkerBootstrapObservation(ctx, claim, observation)
		if duplicateErr != nil {
			return duplicateErr
		}
		if duplicate {
			return nil
		}
	}
	return err
}

func (s *Store) withWorkerBootstrapObservationClaimTransaction(ctx context.Context, claim runtime.WorkerBootstrapObservationClaim, run func(*sql.Tx, int64) error) (err error) {
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
	if err = verifyWorkerBootstrapObservationClaimFence(ctx, tx, claim, now); err != nil {
		return err
	}
	if err = run(tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

func verifyWorkerBootstrapObservationClaimFence(ctx context.Context, tx *sql.Tx, claim runtime.WorkerBootstrapObservationClaim, now int64) error {
	var leaseToken, observationConnectionID, observationInstanceID string
	var deploymentID, planID, deploymentConnectionID, deploymentExecution, deploymentOutcome, deploymentResource string
	var jobID, jobPlanID, jobExecution, jobOutcome, jobCheckpoint, jobKind string
	var connectionStatus, connectionRegion, endpoint, brokerRegion, nodeKeyID string
	var resourceConnectionID, resourceStatus, resourceInstanceID, planStatus string
	var leaseUntil, generation int64
	err := tx.QueryRowContext(ctx, `
		SELECT observation.lease_token, observation.lease_until, observation.cloud_connection_id, observation.instance_id,
			deployment.deployment_id, deployment.plan_id, deployment.cloud_connection_id,
			deployment.execution_status, deployment.outcome_status, deployment.resource_status,
			job.job_id, job.plan_id, job.execution_status, job.outcome_status, job.checkpoint, job.kind,
			connection.status, connection.region, broker.broker_command_url, broker.broker_region,
			broker.node_key_id, broker.connection_generation,
			resource.cloud_connection_id, resource.resource_status, resource.instance_id, plan.status
		FROM p2p_cloud_worker_bootstrap_observations AS observation
		JOIN p2p_cloud_deployments AS deployment ON deployment.deployment_id = observation.deployment_id
		JOIN p2p_cloud_jobs AS job ON job.deployment_id = deployment.deployment_id AND job.kind = 'provision'
		JOIN p2p_cloud_connections AS connection ON connection.cloud_connection_id = deployment.cloud_connection_id
		JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = deployment.cloud_connection_id
		JOIN p2p_cloud_deployment_resources AS resource ON resource.deployment_id = deployment.deployment_id
		JOIN p2p_cloud_plans AS plan ON plan.plan_id = deployment.plan_id
		WHERE observation.deployment_id = $1
		FOR UPDATE OF observation, deployment, job, connection, broker, resource, plan
	`, claim.DeploymentID).Scan(
		&leaseToken, &leaseUntil, &observationConnectionID, &observationInstanceID,
		&deploymentID, &planID, &deploymentConnectionID, &deploymentExecution, &deploymentOutcome, &deploymentResource,
		&jobID, &jobPlanID, &jobExecution, &jobOutcome, &jobCheckpoint, &jobKind,
		&connectionStatus, &connectionRegion, &endpoint, &brokerRegion, &nodeKeyID, &generation,
		&resourceConnectionID, &resourceStatus, &resourceInstanceID, &planStatus,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if !validWorkerBootstrapObservationClaimForStore(claim) {
		return ErrLeaseLost
	}
	if leaseToken != claim.LeaseToken || leaseUntil <= now || observationConnectionID != claim.ConnectionID || observationInstanceID != claim.InstanceID ||
		deploymentID != claim.DeploymentID || planID != claim.PlanID || deploymentConnectionID != claim.ConnectionID ||
		deploymentExecution != "provisioning" || deploymentOutcome != "pending" || deploymentResource != "active" ||
		jobID != claim.JobID || jobPlanID != claim.PlanID || jobKind != "provision" || jobExecution != "provisioning" || jobOutcome != "pending" || jobCheckpoint != "worker_bootstrap_pending" ||
		connectionStatus != "active" || connectionRegion != claim.Region || endpoint != claim.BrokerEndpoint || brokerRegion != claim.Region ||
		nodeKeyID != claim.NodeKeyID || generation != claim.ExpectedGeneration ||
		resourceConnectionID != claim.ConnectionID || resourceStatus != "active" || resourceInstanceID != claim.InstanceID || planStatus != "approved" {
		return ErrLeaseLost
	}
	return nil
}

func selectLatestWorkerBootstrapObservationCommand(ctx context.Context, tx *sql.Tx, claim runtime.WorkerBootstrapObservationClaim, digest string) (runtime.WorkerBootstrapObservationCommand, bool, error) {
	command, err := scanWorkerBootstrapObservationCommand(tx.QueryRowContext(ctx, `
		SELECT command_id, deployment_id, cloud_connection_id, request_digest, command_attempt, node_key_id,
			expected_generation, node_counter, canonical_payload_json, payload_sha256, request_sha256,
			signed_envelope_json, issued_at, expires_at, state
		FROM p2p_cloud_deployment_observation_commands
		WHERE deployment_id = $1 AND request_digest = $2
		ORDER BY command_attempt DESC LIMIT 1 FOR UPDATE
	`, claim.DeploymentID, digest))
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.WorkerBootstrapObservationCommand{}, false, nil
	}
	if err != nil {
		return runtime.WorkerBootstrapObservationCommand{}, false, err
	}
	return command, true, nil
}

func selectWorkerBootstrapObservationCommandByID(ctx context.Context, tx *sql.Tx, commandID string) (runtime.WorkerBootstrapObservationCommand, error) {
	return scanWorkerBootstrapObservationCommand(tx.QueryRowContext(ctx, `
		SELECT command_id, deployment_id, cloud_connection_id, request_digest, command_attempt, node_key_id,
			expected_generation, node_counter, canonical_payload_json, payload_sha256, request_sha256,
			signed_envelope_json, issued_at, expires_at, state
		FROM p2p_cloud_deployment_observation_commands WHERE command_id = $1 FOR UPDATE
	`, commandID))
}

func scanWorkerBootstrapObservationCommand(row interface{ Scan(...any) error }) (runtime.WorkerBootstrapObservationCommand, error) {
	var command runtime.WorkerBootstrapObservationCommand
	var issuedAt, expiresAt int64
	err := row.Scan(
		&command.CommandID, &command.DeploymentID, &command.ConnectionID, &command.RequestDigest, &command.Attempt,
		&command.NodeKeyID, &command.ExpectedGeneration, &command.NodeCounter, &command.PayloadJSON,
		&command.PayloadSHA256, &command.RequestSHA256, &command.SignedEnvelope, &issuedAt, &expiresAt, &command.State,
	)
	if err != nil {
		return runtime.WorkerBootstrapObservationCommand{}, err
	}
	command.IssuedAt = time.UnixMilli(issuedAt).UTC()
	command.ExpiresAt = time.UnixMilli(expiresAt).UTC()
	return command, nil
}

func sameWorkerBootstrapObservationCommandIdentity(left, right runtime.WorkerBootstrapObservationCommand) bool {
	return left.CommandID == right.CommandID && left.DeploymentID == right.DeploymentID && left.ConnectionID == right.ConnectionID &&
		left.NodeKeyID == right.NodeKeyID && left.ExpectedGeneration == right.ExpectedGeneration && left.NodeCounter == right.NodeCounter &&
		left.Attempt == right.Attempt && left.RequestDigest == right.RequestDigest
}

func validPersistedWorkerBootstrapObservationCommand(claim runtime.WorkerBootstrapObservationClaim, signed runtime.SignedWorkerBootstrapObservationCommand) error {
	if err := validateWorkerBootstrapObservationClaimForCommand(claim); err != nil {
		return err
	}
	if claim.Command.CommandID == "" || strings.TrimSpace(signed.EnvelopeJSON) != signed.EnvelopeJSON || signed.EnvelopeJSON == "" ||
		strings.TrimSpace(signed.PayloadJSON) != signed.PayloadJSON || signed.PayloadJSON == "" ||
		!lowerHexSHA256ForObservation(signed.PayloadSHA256) || !lowerHexSHA256ForObservation(signed.RequestSHA256) ||
		len(signed.EnvelopeJSON) > 256*1024 || len(signed.PayloadJSON) > 8*1024 || signed.IssuedAt.IsZero() || signed.ExpiresAt.IsZero() ||
		!signed.ExpiresAt.After(signed.IssuedAt) || signed.ExpiresAt.Sub(signed.IssuedAt) > 5*time.Minute {
		return errors.New("signed worker bootstrap observation command is invalid")
	}
	command, err := broker.ParseDeploymentObserveCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return errors.New("signed worker bootstrap observation command is invalid")
	}
	binding := broker.DeploymentObserveCommandBinding{
		ConnectionID: claim.ConnectionID, CommandID: claim.Command.CommandID, NodeKeyID: claim.NodeKeyID,
		ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter,
		IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt,
		Request: broker.DeploymentObserveRequest{DeploymentID: claim.DeploymentID},
	}
	if command.ValidateBinding(binding) != nil || command.PayloadSHA256 != signed.PayloadSHA256 || command.RequestSHA256() != signed.RequestSHA256 {
		return errors.New("signed worker bootstrap observation command is invalid")
	}
	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return errors.New("signed worker bootstrap observation command is invalid")
	}
	return nil
}

func validateWorkerBootstrapObservationClaimForCommand(claim runtime.WorkerBootstrapObservationClaim) error {
	if !validWorkerBootstrapObservationClaimForStore(claim) {
		return errors.New("worker bootstrap observation claim is invalid")
	}
	digest, err := claim.Request.Digest()
	if err != nil || claim.Command.CommandID == "" || claim.Command.DeploymentID != claim.DeploymentID ||
		claim.Command.ConnectionID != claim.ConnectionID || claim.Command.NodeKeyID != claim.NodeKeyID ||
		claim.Command.ExpectedGeneration != claim.ExpectedGeneration || claim.Command.NodeCounter <= 0 || claim.Command.Attempt <= 0 ||
		claim.Command.RequestDigest != digest {
		return errors.New("worker bootstrap observation command does not bind the claim")
	}
	return nil
}

func validWorkerBootstrapObservationClaimForStore(claim runtime.WorkerBootstrapObservationClaim) bool {
	if claim.DeploymentID == "" || claim.PlanID == "" || claim.ConnectionID == "" || claim.Region == "" || claim.InstanceID == "" ||
		claim.BrokerEndpoint == "" || claim.NodeKeyID == "" || claim.ExpectedGeneration <= 0 || claim.JobID == "" || claim.LeaseToken == "" {
		return false
	}
	if claim.ExpectedGeneration > 9007199254740991 || strings.TrimSpace(claim.DeploymentID) != claim.DeploymentID ||
		strings.TrimSpace(claim.PlanID) != claim.PlanID || strings.TrimSpace(claim.ConnectionID) != claim.ConnectionID ||
		strings.TrimSpace(claim.InstanceID) != claim.InstanceID || strings.TrimSpace(claim.NodeKeyID) != claim.NodeKeyID ||
		strings.TrimSpace(claim.JobID) != claim.JobID || strings.TrimSpace(claim.LeaseToken) != claim.LeaseToken ||
		cloudmodule.ContainsSensitiveGoalMaterial(claim.DeploymentID) || cloudmodule.ContainsSensitiveGoalMaterial(claim.PlanID) ||
		cloudmodule.ContainsSensitiveGoalMaterial(claim.ConnectionID) || cloudmodule.ContainsSensitiveGoalMaterial(claim.InstanceID) ||
		cloudmodule.ContainsSensitiveGoalMaterial(claim.NodeKeyID) || cloudmodule.ContainsSensitiveGoalMaterial(claim.JobID) {
		return false
	}
	if err := cloudmodule.ValidateConnectionRegistrationEndpoint(claim.BrokerEndpoint, claim.Region); err != nil {
		return false
	}
	return claim.Request.Validate() == nil && claim.Request.DeploymentID == claim.DeploymentID
}

func updateWorkerBootstrapObservationCommandAttempt(ctx context.Context, tx *sql.Tx, commandID, code string, now int64) error {
	result, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_deployment_observation_commands
		SET state = CASE WHEN state IN ('signed', 'indeterminate') THEN 'indeterminate' ELSE state END,
			attempts = attempts + 1, last_error_code = $1, updated_at = $2
		WHERE command_id = $3
	`, code, now, commandID)
	if err != nil {
		return err
	}
	return requireOneAffected(result)
}

func releaseWorkerBootstrapObservationClaim(ctx context.Context, tx *sql.Tx, claim runtime.WorkerBootstrapObservationClaim, available int64, code string, now int64) error {
	result, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_worker_bootstrap_observations
		SET lease_owner = '', lease_token = '', lease_until = 0, available_at = $1,
			last_error_code = $2, updated_at = $3
		WHERE deployment_id = $4 AND lease_token = $5
	`, available, code, now, claim.DeploymentID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return requireOneAffected(result)
}

type workerBootstrapEvidence struct {
	ConnectionID       string
	InstanceID         string
	WorkerSessionState string
	LeaseEpoch         int64
	LeaseExpiresAt     int64
	LastSequence       int64
	LastEventAt        int64
	ObservedAt         int64
}

func selectWorkerBootstrapEvidence(ctx context.Context, tx *sql.Tx, deploymentID string) (workerBootstrapEvidence, error) {
	var evidence workerBootstrapEvidence
	err := tx.QueryRowContext(ctx, `
		SELECT cloud_connection_id, instance_id, worker_session_state, worker_lease_epoch,
			worker_lease_expires_at, worker_last_sequence, worker_last_event_at, observed_at
		FROM p2p_cloud_worker_bootstrap_observations
		WHERE deployment_id = $1 FOR UPDATE
	`, deploymentID).Scan(
		&evidence.ConnectionID, &evidence.InstanceID, &evidence.WorkerSessionState, &evidence.LeaseEpoch,
		&evidence.LeaseExpiresAt, &evidence.LastSequence, &evidence.LastEventAt, &evidence.ObservedAt,
	)
	return evidence, err
}

func sameWorkerBootstrapEvidence(evidence workerBootstrapEvidence, observation runtime.WorkerBootstrapObservation) bool {
	return evidence.InstanceID == observation.InstanceID && evidence.WorkerSessionState == observation.WorkerSessionState &&
		evidence.LeaseEpoch == observation.LeaseEpoch && evidence.LeaseExpiresAt == observation.LeaseExpiresAt.UTC().UnixMilli() &&
		evidence.LastSequence == observation.LastSequence && evidence.LastEventAt == zeroTimeUnixMilli(observation.LastEventAt) &&
		evidence.ObservedAt == observation.ObservedAt.UTC().UnixMilli()
}

func (s *Store) sameVerifiedWorkerBootstrapObservation(ctx context.Context, claim runtime.WorkerBootstrapObservationClaim, observation runtime.WorkerBootstrapObservation) (bool, error) {
	if s == nil || s.db == nil {
		return false, errors.New("cloud orchestrator database is unavailable")
	}
	if err := validateWorkerBootstrapObservationClaimForCommand(claim); err != nil {
		return false, nil
	}
	var evidence workerBootstrapEvidence
	var planID, connectionID, checkpoint, execution, outcome, nodeKeyID string
	var generation int64
	err := s.db.QueryRowContext(ctx, `
		SELECT observation.cloud_connection_id, observation.instance_id, observation.worker_session_state,
			observation.worker_lease_epoch, observation.worker_lease_expires_at, observation.worker_last_sequence,
			observation.worker_last_event_at, observation.observed_at,
			deployment.plan_id, deployment.cloud_connection_id, job.checkpoint, job.execution_status, job.outcome_status,
			broker.node_key_id, broker.connection_generation
		FROM p2p_cloud_worker_bootstrap_observations AS observation
		JOIN p2p_cloud_deployments AS deployment ON deployment.deployment_id = observation.deployment_id
		JOIN p2p_cloud_jobs AS job ON job.deployment_id = deployment.deployment_id AND job.kind = 'provision'
		JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = deployment.cloud_connection_id
		WHERE observation.deployment_id = $1
	`, claim.DeploymentID).Scan(
		&evidence.ConnectionID, &evidence.InstanceID, &evidence.WorkerSessionState, &evidence.LeaseEpoch,
		&evidence.LeaseExpiresAt, &evidence.LastSequence, &evidence.LastEventAt, &evidence.ObservedAt,
		&planID, &connectionID, &checkpoint, &execution, &outcome, &nodeKeyID, &generation,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return planID == claim.PlanID && connectionID == claim.ConnectionID && evidence.ConnectionID == claim.ConnectionID &&
		evidence.InstanceID == claim.InstanceID && nodeKeyID == claim.NodeKeyID && generation == claim.ExpectedGeneration &&
		checkpoint == "worker_bootstrap_verified" && execution == "verifying" && outcome == "pending" &&
		sameWorkerBootstrapEvidence(evidence, observation), nil
}

func zeroTimeUnixMilli(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UTC().UnixMilli()
}

func lowerHexSHA256ForObservation(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
