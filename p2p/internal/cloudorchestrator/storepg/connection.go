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
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

const connectionRegistrationGeneration int64 = 1

var _ runtime.ConnectionRegistrationStore = (*Store)(nil)

// ClaimConnectionRegistration leases the user-submitted Stack completion
// without making it public. The Stack endpoint and ARN remain private until a
// signed Broker response proves they match the immutable bootstrap.
func (s *Store) ClaimConnectionRegistration(ctx context.Context, workerID string, lease time.Duration) (runtime.ConnectionRegistrationClaim, bool, error) {
	if s == nil || s.db == nil {
		return runtime.ConnectionRegistrationClaim{}, false, errors.New("cloud orchestrator database is unavailable")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || len(workerID) > 128 || strings.ContainsAny(workerID, "\r\n\t") {
		return runtime.ConnectionRegistrationClaim{}, false, errors.New("cloud orchestrator worker id is invalid")
	}
	if lease <= 0 || lease > 5*time.Minute {
		return runtime.ConnectionRegistrationClaim{}, false, errors.New("cloud connection registration lease must be between 1ns and 5m")
	}
	now := s.now().UnixMilli()
	token := strings.TrimSpace(s.cfg.NewLeaseToken())
	if token == "" || len(token) > 128 {
		return runtime.ConnectionRegistrationClaim{}, false, errors.New("cloud orchestrator lease token is invalid")
	}
	row := s.db.QueryRowContext(ctx, `
		WITH selected AS (
			SELECT outbox.outbox_id
			FROM p2p_cloud_outbox AS outbox
			JOIN p2p_cloud_connection_bootstraps AS bootstrap ON bootstrap.bootstrap_id = outbox.aggregate_id
			WHERE outbox.kind = $1
				AND outbox.aggregate_type = 'connection_bootstrap'
				AND outbox.completed_at = 0
				AND outbox.available_at <= $2
				AND outbox.lease_until <= $2
				AND bootstrap.status IN ('verification_queued', 'verifying')
				AND bootstrap.job_id <> ''
				AND bootstrap.candidate_broker_url <> ''
				AND bootstrap.stack_arn <> ''
			ORDER BY outbox.created_at ASC, outbox.outbox_id ASC
			FOR UPDATE OF outbox SKIP LOCKED
			LIMIT 1
		), claimed AS (
			UPDATE p2p_cloud_outbox AS outbox
			SET lease_owner = $3,
				lease_token = $4,
				lease_until = $5,
				attempts = outbox.attempts + 1,
				last_error_code = ''
			FROM selected
			WHERE outbox.outbox_id = selected.outbox_id
			RETURNING outbox.outbox_id, outbox.kind, outbox.aggregate_type, outbox.aggregate_id,
				outbox.payload_json, outbox.lease_token, outbox.attempts
		)
		SELECT claimed.outbox_id, claimed.kind, claimed.aggregate_type, claimed.aggregate_id,
			bootstrap.bootstrap_id, bootstrap.cloud_connection_id, bootstrap.requested_region,
			bootstrap.candidate_broker_url, bootstrap.stack_arn, bootstrap.node_key_id, bootstrap.job_id,
			claimed.payload_json, claimed.lease_token, claimed.attempts
		FROM claimed
		JOIN p2p_cloud_connection_bootstraps AS bootstrap ON bootstrap.bootstrap_id = claimed.aggregate_id
	`, runtime.ConnectionRegistrationRequested, now, workerID, token, now+lease.Milliseconds())
	var claim runtime.ConnectionRegistrationClaim
	var payloadJSON string
	if err := row.Scan(
		&claim.OutboxID, &claim.Kind, &claim.AggregateType, &claim.AggregateID,
		&claim.BootstrapID, &claim.ConnectionID, &claim.RequestedRegion,
		&claim.BrokerEndpoint, &claim.StackARN, &claim.NodeKeyID, &claim.JobID,
		&payloadJSON, &claim.LeaseToken, &claim.Attempt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.ConnectionRegistrationClaim{}, false, nil
		}
		return runtime.ConnectionRegistrationClaim{}, false, fmt.Errorf("claim cloud connection registration outbox: %w", err)
	}
	bootstrapID, err := decodeConnectionRegistrationOutbox(payloadJSON)
	if err != nil || bootstrapID != claim.BootstrapID {
		return runtime.ConnectionRegistrationClaim{}, false, errors.New("connection registration outbox payload does not bind the claimed bootstrap")
	}
	claim.ExpectedGeneration = connectionRegistrationGeneration
	claim.Request = runtime.ConnectionRegistrationRequest{
		BootstrapID: claim.BootstrapID, RequestedRegion: claim.RequestedRegion, StackARN: claim.StackARN,
	}
	command, err := s.prepareConnectionRegistrationCommand(ctx, claim)
	if err != nil {
		return runtime.ConnectionRegistrationClaim{}, false, err
	}
	claim.Command = command
	return claim, true, nil
}

// prepareConnectionRegistrationCommand allocates a new counter only after the
// prior exact envelope is known expired. Network ambiguity deliberately
// replays the durable command byte-for-byte.
func (s *Store) prepareConnectionRegistrationCommand(ctx context.Context, claim runtime.ConnectionRegistrationClaim) (runtime.ConnectionRegistrationCommand, error) {
	digest, err := claim.Request.Digest()
	if err != nil {
		return runtime.ConnectionRegistrationCommand{}, fmt.Errorf("connection registration request digest: %w", err)
	}
	var command runtime.ConnectionRegistrationCommand
	err = s.withConnectionRegistrationClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		latest, found, err := selectLatestConnectionRegistrationCommand(ctx, tx, claim, digest)
		if err != nil {
			return err
		}
		if found && latest.State != "expired" {
			if latest.State == "failed" {
				return errors.New("connection registration command is terminal while its outbox remains pending")
			}
			command = latest
			return nil
		}
		attempt := 1
		if found {
			attempt = latest.Attempt + 1
		}
		var counter int64
		if err := tx.QueryRowContext(ctx, `
			UPDATE p2p_cloud_connection_bootstraps
			SET next_node_counter = next_node_counter + 1, updated_at = $1
			WHERE bootstrap_id = $2
			RETURNING next_node_counter
		`, now, claim.BootstrapID).Scan(&counter); err != nil {
			return err
		}
		command = runtime.ConnectionRegistrationCommand{
			CommandID:          stableID("cloud_broker_registration_", claim.BootstrapID, claim.ConnectionID, digest, fmt.Sprint(attempt)),
			BootstrapID:        claim.BootstrapID,
			ConnectionID:       claim.ConnectionID,
			NodeKeyID:          claim.NodeKeyID,
			ExpectedGeneration: claim.ExpectedGeneration,
			NodeCounter:        counter,
			Attempt:            attempt,
			RequestDigest:      digest,
			State:              "allocated",
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_connection_registration_commands (
				command_id, bootstrap_id, cloud_connection_id, command_attempt, action,
				node_key_id, expected_generation, node_counter, state, created_at, updated_at
			) VALUES ($1, $2, $3, $4, 'connection.registration.verify', $5, $6, $7, 'allocated', $8, $8)
		`, command.CommandID, command.BootstrapID, command.ConnectionID, command.Attempt,
			command.NodeKeyID, command.ExpectedGeneration, command.NodeCounter, now)
		return err
	})
	return command, err
}

func (s *Store) PersistConnectionRegistrationCommand(ctx context.Context, claim runtime.ConnectionRegistrationClaim, signed runtime.SignedConnectionRegistrationCommand) error {
	if err := validPersistedConnectionRegistrationCommand(claim, signed); err != nil {
		return err
	}
	return s.withConnectionRegistrationClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		existing, err := selectConnectionRegistrationCommandByID(ctx, tx, claim.Command.CommandID, claim.Command.RequestDigest)
		if err != nil {
			return err
		}
		if existing.BootstrapID != claim.BootstrapID || existing.ConnectionID != claim.ConnectionID || existing.NodeKeyID != claim.NodeKeyID ||
			existing.ExpectedGeneration != claim.ExpectedGeneration || existing.NodeCounter != claim.Command.NodeCounter || existing.Attempt != claim.Command.Attempt {
			return errors.New("persisted connection registration command does not match the claim")
		}
		if existing.State == "signed" || existing.State == "indeterminate" || existing.State == "accepted" {
			if existing.PayloadJSON == signed.PayloadJSON && existing.PayloadSHA256 == signed.PayloadSHA256 && existing.RequestSHA256 == signed.RequestSHA256 && existing.SignedEnvelope == signed.EnvelopeJSON &&
				existing.IssuedAt.UTC().Equal(signed.IssuedAt.UTC()) && existing.ExpiresAt.UTC().Equal(signed.ExpiresAt.UTC()) {
				return nil
			}
			return errors.New("connection registration command already has a different signed envelope")
		}
		if existing.State != "allocated" {
			return errors.New("connection registration command cannot be signed in its current state")
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_connection_registration_commands
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

func (s *Store) MarkConnectionRegistrationStarted(ctx context.Context, claim runtime.ConnectionRegistrationClaim) error {
	return s.withConnectionRegistrationClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_connection_bootstraps
			SET status = 'verifying', revision = revision + 1, updated_at = $1
			WHERE bootstrap_id = $2 AND status = 'verification_queued'
		`, now, claim.BootstrapID)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			var status string
			if err := tx.QueryRowContext(ctx, `SELECT status FROM p2p_cloud_connection_bootstraps WHERE bootstrap_id = $1`, claim.BootstrapID).Scan(&status); err != nil || status != "verifying" {
				return ErrLeaseLost
			}
		}
		_, err = transitionConnectionRegistrationJob(ctx, tx, claim, now, researchJobTransition{
			execution: "verifying", outcome: "pending", checkpoint: "connection_verification_started", errorCode: "",
			stepStatus: "running", stepSummary: "The submitted AWS Connection Stack is being verified with a signed Broker command.",
		})
		return err
	})
}

func (s *Store) DeferConnectionRegistration(ctx context.Context, claim runtime.ConnectionRegistrationClaim, code string, availableAt time.Time) error {
	code = durableErrorCode(code, "connection_registration_retryable")
	return s.withConnectionRegistrationClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if _, err := transitionConnectionRegistrationJob(ctx, tx, claim, now, researchJobTransition{
			execution: "queued", outcome: "pending", checkpoint: "connection_verification_retry_scheduled", errorCode: code,
			stepStatus: "queued", stepSummary: "The signed AWS Connection Stack verification is waiting to retry.",
		}); err != nil {
			return err
		}
		if err := setConnectionRegistrationBootstrapStatus(ctx, tx, claim.BootstrapID, "verification_queued", now); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_connection_registration_commands
			SET state = CASE WHEN state = 'accepted' THEN 'accepted' ELSE 'indeterminate' END,
				attempts = attempts + 1, last_error_code = $1, updated_at = $2
			WHERE command_id = $3
		`, code, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(result); err != nil {
			return err
		}
		available := availableAt.UTC().UnixMilli()
		if available < now {
			available = now
		}
		return releaseConnectionRegistrationOutbox(ctx, tx, claim, available, code)
	})
}

func (s *Store) ExpireConnectionRegistrationCommand(ctx context.Context, claim runtime.ConnectionRegistrationClaim) error {
	return s.withConnectionRegistrationClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if _, err := transitionConnectionRegistrationJob(ctx, tx, claim, now, researchJobTransition{
			execution: "queued", outcome: "pending", checkpoint: "connection_verification_command_expired", errorCode: "connection_registration_command_expired",
			stepStatus: "queued", stepSummary: "The signed AWS Connection Stack verification command expired before a receipt was recorded; a new command will be prepared.",
		}); err != nil {
			return err
		}
		if err := setConnectionRegistrationBootstrapStatus(ctx, tx, claim.BootstrapID, "verification_queued", now); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_connection_registration_commands
			SET state = 'expired', attempts = attempts + 1, last_error_code = 'connection_registration_command_expired', updated_at = $1
			WHERE command_id = $2 AND state IN ('allocated', 'signed', 'indeterminate')
		`, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(result); err != nil {
			return err
		}
		return releaseConnectionRegistrationOutbox(ctx, tx, claim, now, "connection_registration_command_expired")
	})
}

func (s *Store) FailConnectionRegistration(ctx context.Context, claim runtime.ConnectionRegistrationClaim, code string) error {
	code = durableErrorCode(code, "connection_registration_transport_failed")
	return s.withConnectionRegistrationClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if _, err := transitionConnectionRegistrationJob(ctx, tx, claim, now, researchJobTransition{
			execution: "finished", outcome: "failed", checkpoint: "connection_verification_failed", errorCode: code,
			stepStatus: "failed", stepSummary: "The submitted AWS Connection Stack could not be verified by its signed Broker.",
		}); err != nil {
			return err
		}
		if err := setConnectionRegistrationBootstrapStatus(ctx, tx, claim.BootstrapID, "verification_failed", now); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_connection_registration_commands
			SET state = 'failed', attempts = attempts + 1, last_error_code = $1, updated_at = $2
			WHERE command_id = $3 AND state IN ('allocated', 'signed', 'indeterminate')
		`, code, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(result); err != nil {
			return err
		}
		return completeConnectionRegistrationOutbox(ctx, tx, claim, now)
	})
}

// CommitConnectionRegistration is the only point that makes a Connection
// owner-visible. It repeats all receipt binding checks inside the fenced
// transaction, writes only a sanitized receipt, and then inserts the public
// Connection/Broker identity atomically with the outbox completion.
func (s *Store) CommitConnectionRegistration(ctx context.Context, claim runtime.ConnectionRegistrationClaim, registration runtime.BrokerRegistration) error {
	return s.withConnectionRegistrationClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		command, err := selectConnectionRegistrationCommandByID(ctx, tx, claim.Command.CommandID, claim.Command.RequestDigest)
		if err != nil {
			return err
		}
		if command.State != "signed" && command.State != "indeterminate" && command.State != "accepted" {
			return errors.New("connection registration command is not eligible for a receipt")
		}
		validatedClaim := claim
		validatedClaim.Command = command
		signed := runtime.SignedConnectionRegistrationCommand{
			EnvelopeJSON: command.SignedEnvelope, PayloadJSON: command.PayloadJSON, PayloadSHA256: command.PayloadSHA256,
			RequestSHA256: command.RequestSHA256, IssuedAt: command.IssuedAt, ExpiresAt: command.ExpiresAt,
		}
		if err := validPersistedConnectionRegistrationCommand(validatedClaim, signed); err != nil {
			return err
		}
		if err := runtime.ValidateBrokerRegistration(validatedClaim, signed, registration); err != nil {
			return err
		}
		safeReceipt, err := connectionRegistrationReceiptJSON(registration)
		if err != nil {
			return err
		}
		commandUpdate, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_connection_registration_commands
			SET state = 'accepted', attempts = attempts + 1, last_error_code = '', receipt_json = $1, updated_at = $2
			WHERE command_id = $3
		`, safeReceipt, now, command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(commandUpdate); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_connections (
				cloud_connection_id, provider, account_id, region, mode, status, revision, created_at, updated_at
			) VALUES ($1, 'aws', $2, $3, 'connection_stack_v2', 'active', 1, $4, $4)
		`, claim.ConnectionID, registration.AccountID, registration.Region, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_connection_brokers (
				cloud_connection_id, broker_command_url, broker_region, connection_generation, node_key_id,
				next_node_counter, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
		`, claim.ConnectionID, registration.BrokerCommandURL, registration.Region, registration.ConnectionGeneration,
			registration.NodeKeyID, command.NodeCounter, now); err != nil {
			return err
		}
		updatedBootstrap, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_connection_bootstraps
			SET status = 'active', revision = revision + 1, updated_at = $1
			WHERE bootstrap_id = $2 AND status IN ('verification_queued', 'verifying')
		`, now, claim.BootstrapID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(updatedBootstrap); err != nil {
			return err
		}
		connectionSummary := map[string]any{
			"cloud_connection_id": claim.ConnectionID, "provider": "aws", "account_id": registration.AccountID,
			"region": registration.Region, "mode": "connection_stack_v2", "status": "active",
			"revision": int64(1), "created_at": now, "updated_at": now,
		}
		if err := writeEventAndProjection(ctx, tx,
			stableID("cloud_event_", claim.ConnectionID, "1", "connection_active"),
			"cloud.connection.changed", "connection", claim.ConnectionID, 1, connectionSummary, now); err != nil {
			return err
		}
		if _, err := transitionConnectionRegistrationJob(ctx, tx, claim, now, researchJobTransition{
			execution: "finished", outcome: "succeeded", checkpoint: "connection_verified", errorCode: "",
			stepStatus: "finished", stepSummary: "The AWS Connection Stack has been verified and is ready for read-only price requests.",
		}); err != nil {
			return err
		}
		return completeConnectionRegistrationOutbox(ctx, tx, claim, now)
	})
}

func decodeConnectionRegistrationOutbox(payload string) (string, error) {
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.DisallowUnknownFields()
	var value struct {
		BootstrapID string `json:"bootstrap_id"`
	}
	if err := decoder.Decode(&value); err != nil {
		return "", errors.New("connection registration outbox payload is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return "", errors.New("connection registration outbox payload contains trailing JSON")
	}
	if strings.TrimSpace(value.BootstrapID) != value.BootstrapID || value.BootstrapID == "" {
		return "", errors.New("connection registration outbox payload is invalid")
	}
	return value.BootstrapID, nil
}

func selectLatestConnectionRegistrationCommand(ctx context.Context, tx *sql.Tx, claim runtime.ConnectionRegistrationClaim, requestDigest string) (runtime.ConnectionRegistrationCommand, bool, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT command_id, bootstrap_id, cloud_connection_id, node_key_id, expected_generation, node_counter, command_attempt,
			canonical_payload_json, payload_sha256, request_sha256, signed_envelope_json, issued_at, expires_at, state
		FROM p2p_cloud_connection_registration_commands
		WHERE bootstrap_id = $1
		ORDER BY command_attempt DESC LIMIT 1 FOR UPDATE
	`, claim.BootstrapID)
	command, err := scanConnectionRegistrationCommand(row, requestDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ConnectionRegistrationCommand{}, false, nil
	}
	if err != nil {
		return runtime.ConnectionRegistrationCommand{}, false, err
	}
	return command, true, nil
}

func selectConnectionRegistrationCommandByID(ctx context.Context, tx *sql.Tx, commandID, requestDigest string) (runtime.ConnectionRegistrationCommand, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT command_id, bootstrap_id, cloud_connection_id, node_key_id, expected_generation, node_counter, command_attempt,
			canonical_payload_json, payload_sha256, request_sha256, signed_envelope_json, issued_at, expires_at, state
		FROM p2p_cloud_connection_registration_commands WHERE command_id = $1 FOR UPDATE
	`, commandID)
	return scanConnectionRegistrationCommand(row, requestDigest)
}

func scanConnectionRegistrationCommand(row interface{ Scan(...any) error }, requestDigest string) (runtime.ConnectionRegistrationCommand, error) {
	var command runtime.ConnectionRegistrationCommand
	var issuedAt, expiresAt int64
	if err := row.Scan(
		&command.CommandID, &command.BootstrapID, &command.ConnectionID, &command.NodeKeyID, &command.ExpectedGeneration,
		&command.NodeCounter, &command.Attempt, &command.PayloadJSON, &command.PayloadSHA256, &command.RequestSHA256,
		&command.SignedEnvelope, &issuedAt, &expiresAt, &command.State,
	); err != nil {
		return runtime.ConnectionRegistrationCommand{}, err
	}
	command.RequestDigest = requestDigest
	command.IssuedAt = time.UnixMilli(issuedAt).UTC()
	command.ExpiresAt = time.UnixMilli(expiresAt).UTC()
	return command, nil
}

func validPersistedConnectionRegistrationCommand(claim runtime.ConnectionRegistrationClaim, signed runtime.SignedConnectionRegistrationCommand) error {
	if claim.Command.CommandID == "" || strings.TrimSpace(signed.EnvelopeJSON) != signed.EnvelopeJSON || signed.EnvelopeJSON == "" || len(signed.EnvelopeJSON) > 256*1024 ||
		strings.TrimSpace(signed.PayloadJSON) != signed.PayloadJSON || signed.PayloadJSON == "" || len(signed.PayloadJSON) > 8*1024 ||
		signed.IssuedAt.IsZero() || signed.ExpiresAt.IsZero() || !signed.ExpiresAt.After(signed.IssuedAt) || signed.ExpiresAt.Sub(signed.IssuedAt) > 5*time.Minute {
		return errors.New("signed connection registration command is invalid")
	}
	command, err := broker.ParseRegistrationCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return errors.New("signed connection registration command is invalid")
	}
	binding, err := connectionRegistrationCommandBinding(claim, signed)
	if err != nil || command.ValidateBinding(binding) != nil || command.PayloadSHA256 != signed.PayloadSHA256 || command.RequestSHA256() != signed.RequestSHA256 {
		return errors.New("signed connection registration command is invalid")
	}
	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return errors.New("signed connection registration command is invalid")
	}
	return nil
}

func connectionRegistrationCommandBinding(claim runtime.ConnectionRegistrationClaim, signed runtime.SignedConnectionRegistrationCommand) (broker.RegistrationCommandBinding, error) {
	if claim.BootstrapID == "" || claim.ConnectionID == "" || claim.RequestedRegion == "" || claim.StackARN == "" ||
		claim.Command.BootstrapID != claim.BootstrapID || claim.Command.ConnectionID != claim.ConnectionID || claim.Command.NodeKeyID == "" ||
		claim.Command.NodeKeyID != claim.NodeKeyID || claim.Command.ExpectedGeneration != claim.ExpectedGeneration || claim.Command.NodeCounter <= 0 {
		return broker.RegistrationCommandBinding{}, errors.New("connection registration command does not bind the claim")
	}
	if err := claim.Request.Validate(); err != nil || claim.Request.BootstrapID != claim.BootstrapID || claim.Request.RequestedRegion != claim.RequestedRegion || claim.Request.StackARN != claim.StackARN {
		return broker.RegistrationCommandBinding{}, errors.New("connection registration command does not bind the request")
	}
	digest, err := claim.Request.Digest()
	if err != nil || claim.Command.RequestDigest != digest {
		return broker.RegistrationCommandBinding{}, errors.New("connection registration command does not bind the request")
	}
	return broker.RegistrationCommandBinding{
		ConnectionID: claim.ConnectionID, CommandID: claim.Command.CommandID, NodeKeyID: claim.Command.NodeKeyID,
		ExpectedGeneration: claim.Command.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter,
		IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt,
		Request: broker.RegistrationRequest{
			BootstrapID: claim.Request.BootstrapID, RequestedRegion: claim.Request.RequestedRegion, StackARN: claim.Request.StackARN,
		},
	}, nil
}

func (s *Store) withConnectionRegistrationClaimTransaction(ctx context.Context, claim runtime.ConnectionRegistrationClaim, run func(*sql.Tx, int64) error) (err error) {
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
	if err = verifyConnectionRegistrationClaimFence(ctx, tx, claim, now); err != nil {
		return err
	}
	if err = run(tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

func verifyConnectionRegistrationClaimFence(ctx context.Context, tx *sql.Tx, claim runtime.ConnectionRegistrationClaim, now int64) error {
	var leaseToken, aggregateType, aggregateID string
	var leaseUntil, completedAt int64
	var bootstrapID, connectionID, region, endpoint, stackARN, nodeKeyID, status, jobID string
	err := tx.QueryRowContext(ctx, `
		SELECT outbox.lease_token, outbox.lease_until, outbox.completed_at, outbox.aggregate_type, outbox.aggregate_id,
			bootstrap.bootstrap_id, bootstrap.cloud_connection_id, bootstrap.requested_region, bootstrap.candidate_broker_url,
			bootstrap.stack_arn, bootstrap.node_key_id, bootstrap.status, bootstrap.job_id
		FROM p2p_cloud_outbox AS outbox
		JOIN p2p_cloud_connection_bootstraps AS bootstrap ON bootstrap.bootstrap_id = outbox.aggregate_id
		WHERE outbox.outbox_id = $1
		FOR UPDATE OF outbox, bootstrap
	`, claim.OutboxID).Scan(&leaseToken, &leaseUntil, &completedAt, &aggregateType, &aggregateID,
		&bootstrapID, &connectionID, &region, &endpoint, &stackARN, &nodeKeyID, &status, &jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if claim.Kind != runtime.ConnectionRegistrationRequested || claim.LeaseToken == "" || leaseToken != claim.LeaseToken || leaseUntil <= now || completedAt != 0 ||
		aggregateType != "connection_bootstrap" || aggregateID != claim.AggregateID || bootstrapID != claim.BootstrapID || connectionID != claim.ConnectionID ||
		region != claim.RequestedRegion || endpoint != claim.BrokerEndpoint || stackARN != claim.StackARN || nodeKeyID != claim.NodeKeyID || jobID != claim.JobID ||
		(status != "verification_queued" && status != "verifying") || claim.ExpectedGeneration != connectionRegistrationGeneration {
		return ErrLeaseLost
	}
	return nil
}

func transitionConnectionRegistrationJob(ctx context.Context, tx *sql.Tx, claim runtime.ConnectionRegistrationClaim, now int64, transition researchJobTransition) (cloudmodule.Job, error) {
	return transitionCloudJob(ctx, tx, claim.JobID, "", "connection_registration", "connection_registration", now, transition)
}

func setConnectionRegistrationBootstrapStatus(ctx context.Context, tx *sql.Tx, bootstrapID, status string, now int64) error {
	result, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_connection_bootstraps
		SET status = $1, revision = revision + 1, updated_at = $2
		WHERE bootstrap_id = $3 AND status IN ('verification_queued', 'verifying')
	`, status, now, bootstrapID)
	if err != nil {
		return err
	}
	return requireOneAffected(result)
}

func releaseConnectionRegistrationOutbox(ctx context.Context, tx *sql.Tx, claim runtime.ConnectionRegistrationClaim, available int64, code string) error {
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

func completeConnectionRegistrationOutbox(ctx context.Context, tx *sql.Tx, claim runtime.ConnectionRegistrationClaim, now int64) error {
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

func connectionRegistrationReceiptJSON(registration runtime.BrokerRegistration) (string, error) {
	// Do not persist the raw Broker body. Endpoint and Stack ARN stay private
	// bootstrap facts; this durable audit record only preserves the exact
	// accepted command identity.
	value := struct {
		Schema        string `json:"schema"`
		ConnectionID  string `json:"connection_id"`
		CommandID     string `json:"command_id"`
		RequestSHA256 string `json:"request_sha256"`
		Action        string `json:"action"`
	}{
		Schema: "dirextalk.aws.command-receipt/v2", ConnectionID: registration.ConnectionID,
		CommandID: registration.CommandID, RequestSHA256: registration.RequestSHA256,
		Action: broker.RegistrationAction,
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}
