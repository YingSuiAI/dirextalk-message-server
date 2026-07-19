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

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var _ runtime.QuoteStore = (*Store)(nil)

// ClaimQuoteRequest leases one strictly typed, pre-price request only after a
// Connection Stack bootstrap has registered its private endpoint metadata.
// Missing broker metadata intentionally leaves the owner-visible quote job
// queued; the Message Server never guesses an endpoint or accepts AWS keys.
func (s *Store) ClaimQuoteRequest(ctx context.Context, workerID string, lease time.Duration) (runtime.QuoteClaim, bool, error) {
	if s == nil || s.db == nil {
		return runtime.QuoteClaim{}, false, errors.New("cloud orchestrator database is unavailable")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || len(workerID) > 128 || strings.ContainsAny(workerID, "\r\n\t") {
		return runtime.QuoteClaim{}, false, errors.New("cloud orchestrator worker id is invalid")
	}
	if lease <= 0 || lease > 5*time.Minute {
		return runtime.QuoteClaim{}, false, errors.New("cloud quote lease must be between 1ns and 5m")
	}
	now := s.now().UnixMilli()
	token := strings.TrimSpace(s.cfg.NewLeaseToken())
	if token == "" || len(token) > 128 {
		return runtime.QuoteClaim{}, false, errors.New("cloud orchestrator lease token is invalid")
	}
	row := s.db.QueryRowContext(ctx, `
		WITH selected AS (
			SELECT outbox.outbox_id
			FROM p2p_cloud_outbox AS outbox
			JOIN p2p_cloud_plans AS plan ON plan.plan_id = outbox.aggregate_id
			JOIN p2p_cloud_connections AS connection ON connection.cloud_connection_id = plan.cloud_connection_id
			JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = plan.cloud_connection_id
			WHERE outbox.kind = $1
				AND outbox.aggregate_type = 'plan'
				AND outbox.completed_at = 0
				AND outbox.available_at <= $2
				AND outbox.lease_until <= $2
				AND plan.status = 'quoting'
				AND plan.quote_id = ''
				AND plan.cloud_connection_id <> ''
				AND connection.region = broker.broker_region
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
			plan.plan_id, plan.cloud_connection_id, plan.revision,
			claimed.payload_json, claimed.lease_token, claimed.attempts,
			broker.broker_command_url, broker.connection_generation, broker.node_key_id
		FROM claimed
		JOIN p2p_cloud_plans AS plan ON plan.plan_id = claimed.aggregate_id
		JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = plan.cloud_connection_id
	`, runtime.QuotePlanRequested, now, workerID, token, now+lease.Milliseconds())
	var claim runtime.QuoteClaim
	var payloadJSON string
	if err := row.Scan(
		&claim.OutboxID, &claim.Kind, &claim.AggregateType, &claim.AggregateID,
		&claim.PlanID, &claim.ConnectionID, &claim.PlanRevision,
		&payloadJSON, &claim.LeaseToken, &claim.Attempt,
		&claim.BrokerEndpoint, &claim.ExpectedGeneration, &claim.NodeKeyID,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.QuoteClaim{}, false, nil
		}
		return runtime.QuoteClaim{}, false, fmt.Errorf("claim cloud quote outbox: %w", err)
	}
	request, err := decodeQuoteRequest(payloadJSON)
	if err != nil {
		return runtime.QuoteClaim{}, false, err
	}
	if request.PlanID != claim.PlanID || request.PlanRevision != uint64(claim.PlanRevision) || request.CloudConnectionID != claim.ConnectionID {
		return runtime.QuoteClaim{}, false, errors.New("quote outbox payload does not bind the claimed plan")
	}
	claim.Request = request
	command, err := s.prepareQuoteCommand(ctx, claim)
	if err != nil {
		return runtime.QuoteClaim{}, false, err
	}
	claim.Command = command
	return claim, true, nil
}

// prepareQuoteCommand allocates a monotonically increasing counter while the
// leased outbox is fenced. Existing allocated, signed, or indeterminate
// commands are returned unchanged; only a known expired command gets a new
// counter and command identity.
func (s *Store) prepareQuoteCommand(ctx context.Context, claim runtime.QuoteClaim) (runtime.QuoteCommand, error) {
	digest, err := claim.Request.Digest()
	if err != nil {
		return runtime.QuoteCommand{}, fmt.Errorf("quote request digest: %w", err)
	}
	var command runtime.QuoteCommand
	err = s.withQuoteClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		latest, found, err := selectLatestQuoteCommand(ctx, tx, claim, digest)
		if err != nil {
			return err
		}
		if found && latest.State != "expired" {
			if latest.State == "failed" {
				return errors.New("quote command is terminal while its outbox remains pending")
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
			UPDATE p2p_cloud_connection_brokers
			SET next_node_counter = next_node_counter + 1, updated_at = $1
			WHERE cloud_connection_id = $2
			RETURNING next_node_counter
		`, now, claim.ConnectionID).Scan(&counter); err != nil {
			return err
		}
		command = runtime.QuoteCommand{
			CommandID:          stableID("cloud_broker_quote_", claim.ConnectionID, claim.PlanID, fmt.Sprint(claim.PlanRevision), digest, fmt.Sprint(attempt)),
			ConnectionID:       claim.ConnectionID,
			NodeKeyID:          claim.NodeKeyID,
			ExpectedGeneration: claim.ExpectedGeneration,
			NodeCounter:        counter,
			Attempt:            attempt,
			RequestDigest:      digest,
			State:              "allocated",
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_broker_commands (
				command_id, cloud_connection_id, plan_id, plan_revision, quote_request_id, quote_request_digest,
				command_attempt, action, node_key_id, expected_generation, node_counter, state, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, 'quote.request', $8, $9, $10, 'allocated', $11, $11)
		`, command.CommandID, command.ConnectionID, claim.PlanID, claim.PlanRevision, claim.Request.QuoteRequestID,
			digest, command.Attempt, command.NodeKeyID, command.ExpectedGeneration, command.NodeCounter, now)
		return err
	})
	return command, err
}

func (s *Store) PersistQuoteCommand(ctx context.Context, claim runtime.QuoteClaim, signed runtime.SignedQuoteCommand) error {
	if err := validPersistedQuoteCommand(claim, signed); err != nil {
		return err
	}
	return s.withQuoteClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		var existing runtime.QuoteCommand
		var issuedAt, expiresAt int64
		if err := tx.QueryRowContext(ctx, `
			SELECT command_id, cloud_connection_id, node_key_id, expected_generation, node_counter, command_attempt,
				quote_request_digest, canonical_payload_json, payload_sha256, request_sha256, signed_envelope_json,
				issued_at, expires_at, state
			FROM p2p_cloud_broker_commands WHERE command_id = $1 FOR UPDATE
		`, claim.Command.CommandID).Scan(
			&existing.CommandID, &existing.ConnectionID, &existing.NodeKeyID, &existing.ExpectedGeneration,
			&existing.NodeCounter, &existing.Attempt, &existing.RequestDigest, &existing.PayloadJSON,
			&existing.PayloadSHA256, &existing.RequestSHA256, &existing.SignedEnvelope, &issuedAt, &expiresAt, &existing.State,
		); err != nil {
			return err
		}
		existing.IssuedAt = time.UnixMilli(issuedAt).UTC()
		existing.ExpiresAt = time.UnixMilli(expiresAt).UTC()
		if existing.ConnectionID != claim.ConnectionID || existing.NodeKeyID != claim.NodeKeyID || existing.ExpectedGeneration != claim.ExpectedGeneration ||
			existing.NodeCounter != claim.Command.NodeCounter || existing.RequestDigest != claim.Command.RequestDigest {
			return errors.New("persisted quote command does not match the claim")
		}
		if existing.State == "signed" || existing.State == "indeterminate" || existing.State == "accepted" {
			if existing.PayloadJSON == signed.PayloadJSON && existing.PayloadSHA256 == signed.PayloadSHA256 && existing.RequestSHA256 == signed.RequestSHA256 && existing.SignedEnvelope == signed.EnvelopeJSON &&
				existing.IssuedAt.UTC().Equal(signed.IssuedAt.UTC()) && existing.ExpiresAt.UTC().Equal(signed.ExpiresAt.UTC()) {
				return nil
			}
			return errors.New("quote command already has a different signed envelope")
		}
		if existing.State != "allocated" {
			return errors.New("quote command cannot be signed in its current state")
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_broker_commands
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

func (s *Store) MarkQuoteStarted(ctx context.Context, claim runtime.QuoteClaim) error {
	return s.withQuoteClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		_, err := transitionQuoteJob(ctx, tx, claim, now, researchJobTransition{
			execution: "queued", outcome: "pending", checkpoint: "quote_leased", errorCode: "",
			stepStatus: "running", stepSummary: "A signed read-only AWS quote request is being verified.",
		})
		return err
	})
}

func (s *Store) DeferQuote(ctx context.Context, claim runtime.QuoteClaim, code string, availableAt time.Time) error {
	code = durableErrorCode(code, "quote_retryable")
	return s.withQuoteClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if _, err := transitionQuoteJob(ctx, tx, claim, now, researchJobTransition{
			execution: "queued", outcome: "pending", checkpoint: "quote_retry_scheduled", errorCode: code,
			stepStatus: "queued", stepSummary: "The verified AWS quote request is waiting to retry.",
		}); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_broker_commands
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
		return releaseQuoteOutbox(ctx, tx, claim, now, available, code)
	})
}

func (s *Store) ExpireQuoteCommand(ctx context.Context, claim runtime.QuoteClaim) error {
	return s.withQuoteClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if _, err := transitionQuoteJob(ctx, tx, claim, now, researchJobTransition{
			execution: "queued", outcome: "pending", checkpoint: "quote_command_expired", errorCode: "quote_command_expired",
			stepStatus: "queued", stepSummary: "The AWS quote command expired before a receipt was recorded; a new signed request will be prepared.",
		}); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_broker_commands
			SET state = 'expired', attempts = attempts + 1, last_error_code = 'quote_command_expired', updated_at = $1
			WHERE command_id = $2 AND state IN ('allocated', 'signed', 'indeterminate')
		`, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(result); err != nil {
			return err
		}
		return releaseQuoteOutbox(ctx, tx, claim, now, now, "quote_command_expired")
	})
}

func (s *Store) FailQuote(ctx context.Context, claim runtime.QuoteClaim, code string) error {
	code = durableErrorCode(code, "quote_transport_failed")
	return s.withQuoteClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if _, err := transitionQuoteJob(ctx, tx, claim, now, researchJobTransition{
			execution: "finished", outcome: "failed", checkpoint: "quote_failed", errorCode: code,
			stepStatus: "failed", stepSummary: "The verified AWS quote request did not produce an acceptable receipt.",
		}); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_broker_commands
			SET state = 'failed', attempts = attempts + 1, last_error_code = $1, updated_at = $2
			WHERE command_id = $3
		`, code, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(result); err != nil {
			return err
		}
		return completeQuoteOutbox(ctx, tx, claim, now)
	})
}

func (s *Store) CommitQuote(ctx context.Context, claim runtime.QuoteClaim, result runtime.BrokerQuote) error {
	return s.withQuoteClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		var command runtime.QuoteCommand
		var issuedAt, expiresAt int64
		if err := tx.QueryRowContext(ctx, `
			SELECT command_id, cloud_connection_id, node_key_id, expected_generation, node_counter, command_attempt,
				quote_request_digest, canonical_payload_json, payload_sha256, request_sha256, signed_envelope_json,
				issued_at, expires_at, state
			FROM p2p_cloud_broker_commands WHERE command_id = $1 FOR UPDATE
		`, claim.Command.CommandID).Scan(
			&command.CommandID, &command.ConnectionID, &command.NodeKeyID, &command.ExpectedGeneration,
			&command.NodeCounter, &command.Attempt, &command.RequestDigest, &command.PayloadJSON,
			&command.PayloadSHA256, &command.RequestSHA256, &command.SignedEnvelope, &issuedAt, &expiresAt, &command.State,
		); err != nil {
			return err
		}
		command.IssuedAt = time.UnixMilli(issuedAt).UTC()
		command.ExpiresAt = time.UnixMilli(expiresAt).UTC()
		if command.State != "signed" && command.State != "indeterminate" && command.State != "accepted" {
			return errors.New("quote command is not eligible for a receipt")
		}
		validatedClaim := claim
		validatedClaim.Command = command
		signed := runtime.SignedQuoteCommand{
			EnvelopeJSON: command.SignedEnvelope, PayloadJSON: command.PayloadJSON, PayloadSHA256: command.PayloadSHA256, RequestSHA256: command.RequestSHA256,
			IssuedAt: command.IssuedAt, ExpiresAt: command.ExpiresAt,
		}
		if err := validPersistedQuoteCommand(validatedClaim, signed); err != nil {
			return err
		}
		if err := runtime.ValidateBrokerQuote(validatedClaim, signed, result); err != nil {
			return err
		}
		quote := cloudcontracts.QuoteV1{
			SchemaVersion:     cloudcontracts.SchemaVersionV1,
			QuoteID:           result.QuoteID,
			CloudConnectionID: result.ConnectionID,
			Region:            result.Region,
			Currency:          result.Currency,
			QuotedAt:          result.QuotedAt,
			ValidUntil:        result.ValidUntil,
			Candidates:        append([]cloudcontracts.QuoteCandidateV1(nil), result.Candidates...),
			IncludedItems:     append([]string(nil), result.IncludedItems...),
			UnincludedItems:   append([]string(nil), result.UnincludedItems...),
		}
		quoteDigest, err := quote.Digest()
		if err != nil {
			return err
		}
		quoteCBOR, err := quote.CanonicalQuoteCBOR()
		if err != nil {
			return err
		}
		quoteJSON, err := json.Marshal(quote)
		if err != nil {
			return err
		}
		if err := ensureQuote(ctx, tx, quote, quoteDigest, quoteCBOR, quoteJSON, now); err != nil {
			return err
		}
		safeReceipt, err := brokerReceiptJSON(result)
		if err != nil {
			return err
		}
		commandUpdate, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_broker_commands
			SET state = 'accepted', attempts = attempts + 1, last_error_code = '',
				receipt_json = $1, quote_json = $2, updated_at = $3
			WHERE command_id = $4
		`, safeReceipt, string(quoteJSON), now, command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(commandUpdate); err != nil {
			return err
		}
		resultUpdate, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_plans
			SET quote_id = $1, plan_hash = '', revision = $2, updated_at = $3
			WHERE plan_id = $4 AND revision = $5 AND status = 'quoting' AND quote_id = '' AND recipe_digest = $6
		`, quote.QuoteID, claim.PlanRevision+1, now, claim.PlanID, claim.PlanRevision, claim.Request.RecipeDigest)
		if err != nil {
			return err
		}
		if err := requireOneAffected(resultUpdate); err != nil {
			return err
		}
		var createdAt int64
		var title, summary string
		if err := tx.QueryRowContext(ctx, `
			SELECT created_at, title, summary FROM p2p_cloud_plans WHERE plan_id = $1
		`, claim.PlanID).Scan(&createdAt, &title, &summary); err != nil {
			return err
		}
		planSummary := map[string]any{
			"plan_id": claim.PlanID, "goal_id": "", "cloud_connection_id": claim.ConnectionID,
			"status": string(cloudcontracts.PlanQuoting), "title": title, "summary": summary,
			"recipe_digest": claim.Request.RecipeDigest, "quote_id": quote.QuoteID, "plan_hash": "",
			"revision": claim.PlanRevision + 1, "created_at": createdAt, "updated_at": now,
		}
		var goalID string
		if err := tx.QueryRowContext(ctx, `SELECT goal_id FROM p2p_cloud_plans WHERE plan_id = $1`, claim.PlanID).Scan(&goalID); err != nil {
			return err
		}
		planSummary["goal_id"] = goalID
		if err := writeEventAndProjection(ctx, tx,
			stableID("cloud_event_", claim.PlanID, fmt.Sprint(claim.PlanRevision+1), "quote_ready"),
			"cloud.plan.changed", "plan", claim.PlanID, claim.PlanRevision+1, planSummary, now); err != nil {
			return err
		}
		if _, err := transitionQuoteJob(ctx, tx, claim, now, researchJobTransition{
			execution: "finished", outcome: "succeeded", checkpoint: "quote_ready", errorCode: "",
			stepStatus: "finished", stepSummary: "A verified AWS price estimate is ready for review; no billable resource has been created.",
		}); err != nil {
			return err
		}
		return completeQuoteOutbox(ctx, tx, claim, now)
	})
}

func decodeQuoteRequest(payload string) (cloudcontracts.QuoteRequestV1, error) {
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.DisallowUnknownFields()
	var request cloudcontracts.QuoteRequestV1
	if err := decoder.Decode(&request); err != nil {
		return cloudcontracts.QuoteRequestV1{}, errors.New("quote outbox payload is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return cloudcontracts.QuoteRequestV1{}, errors.New("quote outbox payload contains trailing JSON")
	}
	if err := request.Validate(); err != nil {
		return cloudcontracts.QuoteRequestV1{}, errors.New("quote outbox payload is invalid")
	}
	return request, nil
}

func selectLatestQuoteCommand(ctx context.Context, tx *sql.Tx, claim runtime.QuoteClaim, digest string) (runtime.QuoteCommand, bool, error) {
	var command runtime.QuoteCommand
	var issuedAt, expiresAt int64
	err := tx.QueryRowContext(ctx, `
		SELECT command_id, cloud_connection_id, node_key_id, expected_generation, node_counter, command_attempt,
			quote_request_digest, canonical_payload_json, payload_sha256, request_sha256, signed_envelope_json,
			issued_at, expires_at, state
		FROM p2p_cloud_broker_commands
		WHERE plan_id = $1 AND plan_revision = $2 AND quote_request_digest = $3
		ORDER BY command_attempt DESC LIMIT 1 FOR UPDATE
	`, claim.PlanID, claim.PlanRevision, digest).Scan(
		&command.CommandID, &command.ConnectionID, &command.NodeKeyID, &command.ExpectedGeneration,
		&command.NodeCounter, &command.Attempt, &command.RequestDigest, &command.PayloadJSON,
		&command.PayloadSHA256, &command.RequestSHA256, &command.SignedEnvelope, &issuedAt, &expiresAt, &command.State,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.QuoteCommand{}, false, nil
	}
	if err != nil {
		return runtime.QuoteCommand{}, false, err
	}
	command.IssuedAt = time.UnixMilli(issuedAt).UTC()
	command.ExpiresAt = time.UnixMilli(expiresAt).UTC()
	return command, true, nil
}

func validPersistedQuoteCommand(claim runtime.QuoteClaim, signed runtime.SignedQuoteCommand) error {
	if claim.Command.CommandID == "" || strings.TrimSpace(signed.EnvelopeJSON) != signed.EnvelopeJSON || signed.EnvelopeJSON == "" || len(signed.EnvelopeJSON) > 256*1024 ||
		strings.TrimSpace(signed.PayloadJSON) != signed.PayloadJSON || signed.PayloadJSON == "" || len(signed.PayloadJSON) > 192*1024 ||
		signed.IssuedAt.IsZero() || signed.ExpiresAt.IsZero() || !signed.ExpiresAt.After(signed.IssuedAt) || signed.ExpiresAt.Sub(signed.IssuedAt) > 5*time.Minute {
		return errors.New("signed quote command is invalid")
	}
	command, err := broker.ParseQuoteCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return errors.New("signed quote command is invalid")
	}
	binding, err := quoteCommandBinding(claim, signed)
	if err != nil || command.ValidateBinding(binding) != nil || command.PayloadSHA256 != signed.PayloadSHA256 || command.RequestSHA256() != signed.RequestSHA256 {
		return errors.New("signed quote command is invalid")
	}
	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return errors.New("signed quote command is invalid")
	}
	return nil
}

func quoteCommandBinding(claim runtime.QuoteClaim, signed runtime.SignedQuoteCommand) (broker.QuoteCommandBinding, error) {
	if err := claim.Request.Validate(); err != nil || claim.PlanID == "" || claim.ConnectionID == "" || claim.PlanRevision <= 0 ||
		claim.Request.PlanID != claim.PlanID || claim.Request.PlanRevision != uint64(claim.PlanRevision) || claim.Request.CloudConnectionID != claim.ConnectionID ||
		claim.Command.ConnectionID != claim.ConnectionID || claim.Command.NodeKeyID == "" || claim.Command.NodeKeyID != claim.NodeKeyID ||
		claim.Command.ExpectedGeneration != claim.ExpectedGeneration || claim.Command.NodeCounter <= 0 {
		return broker.QuoteCommandBinding{}, errors.New("quote command does not bind the claim")
	}
	digest, err := claim.Request.Digest()
	if err != nil || claim.Command.RequestDigest != digest {
		return broker.QuoteCommandBinding{}, errors.New("quote command does not bind the quote request")
	}
	candidates := make([]broker.QuoteCandidate, len(claim.Request.Candidates))
	for index, candidate := range claim.Request.Candidates {
		candidates[index] = broker.QuoteCandidate{
			CandidateID: candidate.CandidateID, Tier: string(candidate.Tier), InstanceType: candidate.InstanceType,
			PurchaseOption: string(candidate.PurchaseOption), EstimatedDiskGiB: int64(candidate.EstimatedDiskGiB),
		}
	}
	return broker.QuoteCommandBinding{
		ConnectionID: claim.ConnectionID, CommandID: claim.Command.CommandID, NodeKeyID: claim.Command.NodeKeyID,
		ExpectedGeneration: claim.Command.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter,
		IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt,
		Request: broker.QuoteRequest{
			QuoteRequestID: claim.Request.QuoteRequestID, PlanDigest: digest, Region: claim.Request.Region, Candidates: candidates,
		},
	}, nil
}

func (s *Store) withQuoteClaimTransaction(ctx context.Context, claim runtime.QuoteClaim, run func(*sql.Tx, int64) error) (err error) {
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
	if err = verifyQuoteClaimFence(ctx, tx, claim, now); err != nil {
		return err
	}
	if err = run(tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

func verifyQuoteClaimFence(ctx context.Context, tx *sql.Tx, claim runtime.QuoteClaim, now int64) error {
	var leaseToken, aggregateType, aggregateID, planID, connectionID, planStatus, quoteID string
	var leaseUntil, completedAt, revision, generation int64
	var endpoint, nodeKeyID, connectionRegion, brokerRegion string
	err := tx.QueryRowContext(ctx, `
		SELECT outbox.lease_token, outbox.lease_until, outbox.completed_at, outbox.aggregate_type, outbox.aggregate_id,
			plan.plan_id, plan.cloud_connection_id, plan.status, plan.quote_id, plan.revision,
			broker.broker_command_url, broker.broker_region, broker.connection_generation, broker.node_key_id,
			connection.region
		FROM p2p_cloud_outbox AS outbox
		JOIN p2p_cloud_plans AS plan ON plan.plan_id = outbox.aggregate_id
		JOIN p2p_cloud_connections AS connection ON connection.cloud_connection_id = plan.cloud_connection_id
		JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = plan.cloud_connection_id
		WHERE outbox.outbox_id = $1
		FOR UPDATE OF outbox, plan, connection, broker
	`, claim.OutboxID).Scan(&leaseToken, &leaseUntil, &completedAt, &aggregateType, &aggregateID,
		&planID, &connectionID, &planStatus, &quoteID, &revision, &endpoint, &brokerRegion, &generation, &nodeKeyID, &connectionRegion)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if claim.Kind != runtime.QuotePlanRequested || claim.LeaseToken == "" || leaseToken != claim.LeaseToken || leaseUntil <= now || completedAt != 0 ||
		aggregateType != "plan" || aggregateID != claim.AggregateID || planID != claim.PlanID || connectionID != claim.ConnectionID ||
		planStatus != string(cloudcontracts.PlanQuoting) || quoteID != "" || revision != claim.PlanRevision ||
		endpoint != claim.BrokerEndpoint || brokerRegion != connectionRegion || generation != claim.ExpectedGeneration || nodeKeyID != claim.NodeKeyID {
		return ErrLeaseLost
	}
	return nil
}

func releaseQuoteOutbox(ctx context.Context, tx *sql.Tx, claim runtime.QuoteClaim, now, available int64, code string) error {
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

func completeQuoteOutbox(ctx context.Context, tx *sql.Tx, claim runtime.QuoteClaim, now int64) error {
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

func brokerReceiptJSON(quote runtime.BrokerQuote) (string, error) {
	// Keep the durable receipt audit-safe even if a future Broker response adds
	// optional data. No raw HTTP body, endpoint, signature, or external error is
	// stored or projected from this control-plane record.
	value := struct {
		Schema         string `json:"schema"`
		ConnectionID   string `json:"connection_id"`
		CommandID      string `json:"command_id"`
		RequestSHA256  string `json:"request_sha256"`
		QuoteRequestID string `json:"quote_request_id"`
	}{
		Schema: "dirextalk.aws.command-receipt/v2", ConnectionID: quote.ConnectionID,
		CommandID: quote.CommandID, RequestSHA256: quote.RequestSHA256, QuoteRequestID: quote.QuoteRequestID,
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}
