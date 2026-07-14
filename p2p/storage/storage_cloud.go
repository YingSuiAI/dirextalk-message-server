package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

const cloudGoalColumns = `
	goal_id, owner_mxid, prompt, cloud_connection_id, plan_id, status,
	idempotency_hash, request_digest, revision, created_at, updated_at
`

const cloudPlanColumns = `
	plan_id, goal_id, cloud_connection_id, status, title, summary,
	recipe_digest, quote_id, plan_hash, revision, created_at, updated_at
`

const cloudJobColumns = `
	job_id, plan_id, deployment_id, kind, execution_status, outcome_status,
	checkpoint, error_code, revision, created_at, updated_at
`

const cloudConnectionBootstrapColumns = `
	bootstrap_id, owner_mxid, cloud_connection_id, provider, requested_region,
	template_url, template_digest, source_tree_digest, stack_name, node_key_id, node_public_key_spki_base64,
	device_approval_key_id, device_approval_public_key_spki_base64,
	candidate_broker_url, stack_arn, status, revision, idempotency_hash, request_digest,
	completion_idempotency_hash, completion_request_digest, job_id, next_node_counter,
	expires_at, created_at, updated_at
`

var errCloudQuoteDisplayInvalid = errors.New("cloud quote display record is invalid")

// cloudQuoteDisplay is the only persisted JSON shape that may be projected to
// ProductCore. It deliberately recognizes a small immutable quote contract;
// broker envelopes, receipt data, keys, endpoints, and any future unreviewed
// field make decoding fail closed.
type cloudQuoteDisplay struct {
	SchemaVersion     string                       `json:"schema_version"`
	QuoteID           string                       `json:"quote_id"`
	CloudConnectionID string                       `json:"cloud_connection_id"`
	Region            string                       `json:"region"`
	Currency          string                       `json:"currency"`
	QuotedAt          time.Time                    `json:"quoted_at"`
	ValidUntil        time.Time                    `json:"valid_until"`
	Candidates        []cloudQuoteDisplayCandidate `json:"candidates"`
	IncludedItems     []string                     `json:"included_items"`
	UnincludedItems   []string                     `json:"unincluded_items"`
}

type cloudQuoteDisplayCandidate struct {
	CandidateID       string   `json:"candidate_id"`
	Tier              string   `json:"tier"`
	InstanceType      string   `json:"instance_type"`
	PurchaseOption    string   `json:"purchase_option"`
	HourlyMinor       int64    `json:"hourly_minor"`
	ThirtyDayMinor    int64    `json:"thirty_day_minor"`
	StartupUpperMinor int64    `json:"startup_upper_minor"`
	EstimatedDiskGiB  uint32   `json:"estimated_disk_gib"`
	AvailabilityZones []string `json:"availability_zones"`
}

func (s *DatabaseStore) CreateCloudGoal(ctx context.Context, request cloudmodule.CreateGoalRequest) (cloudmodule.CreateGoalResult, error) {
	result := cloudmodule.CreateGoalResult{}
	err := s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		goal := request.Goal
		var insertedGoalID string
		err := txn.QueryRowContext(ctx, `
			INSERT INTO p2p_cloud_goals (
				goal_id, owner_mxid, prompt, cloud_connection_id, plan_id, status,
				idempotency_hash, request_digest, revision, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT (owner_mxid, idempotency_hash) DO NOTHING
			RETURNING goal_id
		`, goal.GoalID, goal.OwnerMXID, goal.Prompt, goal.ConnectionID, goal.PlanID, goal.Status,
			goal.IdempotencyHash, goal.RequestDigest, goal.Revision, goal.CreatedAt, goal.UpdatedAt).Scan(&insertedGoalID)
		switch {
		case err == nil:
			// This transaction owns the idempotency slot and can atomically add
			// its dependent plan, durable events and private outbox entry.
		case err == sql.ErrNoRows:
			// PostgreSQL waits for a conflicting in-flight INSERT before the
			// DO NOTHING result is visible. Read the now-committed winner inside
			// this transaction so concurrent retries replay instead of leaking a
			// unique-key error or emitting a second event/outbox item.
			row := txn.QueryRowContext(ctx, `SELECT `+cloudGoalColumns+`
				FROM p2p_cloud_goals WHERE owner_mxid = $1 AND idempotency_hash = $2`,
				goal.OwnerMXID, goal.IdempotencyHash)
			var existing cloudmodule.Goal
			if err := scanCloudGoal(row, &existing); err != nil {
				return err
			}
			if existing.RequestDigest != goal.RequestDigest {
				return cloudmodule.ErrIdempotencyConflict
			}
			planRow := txn.QueryRowContext(ctx, `SELECT `+cloudPlanColumns+` FROM p2p_cloud_plans WHERE plan_id = $1`, existing.PlanID)
			var plan cloudmodule.Plan
			if err := scanCloudPlan(planRow, &plan); err != nil {
				return err
			}
			result = cloudmodule.CreateGoalResult{Goal: existing, Plan: plan, Created: false}
			return nil
		default:
			return err
		}
		plan := request.Plan
		if _, err := txn.ExecContext(ctx, `
			INSERT INTO p2p_cloud_plans (
				plan_id, goal_id, cloud_connection_id, status, title, summary,
				recipe_digest, quote_id, plan_hash, revision, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		`, plan.PlanID, plan.GoalID, plan.ConnectionID, plan.Status, plan.Title, plan.Summary,
			plan.RecipeDigest, plan.QuoteID, plan.PlanHash, plan.Revision, plan.CreatedAt, plan.UpdatedAt); err != nil {
			return err
		}
		job := request.Job
		if job.JobID != "" {
			if _, err := txn.ExecContext(ctx, `
				INSERT INTO p2p_cloud_jobs (
					job_id, plan_id, deployment_id, kind, execution_status, outcome_status,
					checkpoint, error_code, revision, created_at, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			`, job.JobID, job.PlanID, job.DeploymentID, job.Kind, job.Execution, job.Outcome,
				job.Checkpoint, job.ErrorCode, job.Revision, job.CreatedAt, job.UpdatedAt); err != nil {
				return err
			}
			if _, err := txn.ExecContext(ctx, `
				INSERT INTO p2p_cloud_job_steps (
					job_id, step_id, status, summary, checkpoint, error_code, revision, created_at, updated_at
				) VALUES ($1, 'research', 'queued', 'Cloud research is queued for the selected connection.', $2, '', 1, $3, $3)
			`, job.JobID, job.Checkpoint, job.CreatedAt); err != nil {
				return err
			}
		}
		for _, event := range request.Events {
			if _, err := txn.ExecContext(ctx, `
				INSERT INTO p2p_cloud_events (
					event_id, type, aggregate_type, aggregate_id, revision, summary_json, created_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7)
			`, event.EventID, event.Type, event.AggregateType, event.AggregateID, event.Revision, event.SummaryJSON, event.CreatedAt); err != nil {
				return err
			}
			if _, err := txn.ExecContext(ctx, `
				INSERT INTO p2p_cloud_projection_outbox (
					projection_id, cloud_event_id, type, payload_json, available_at, created_at
				) VALUES ($1, $2, $3, $4, $5, $5)
			`, cloudProjectionID(event.EventID), event.EventID, event.Type, event.SummaryJSON, event.CreatedAt); err != nil {
				return err
			}
		}
		outbox := request.Outbox
		if _, err := txn.ExecContext(ctx, `
			INSERT INTO p2p_cloud_outbox (
				outbox_id, kind, aggregate_type, aggregate_id, payload_json, created_at
			) VALUES ($1, $2, $3, $4, $5, $6)
		`, outbox.OutboxID, outbox.Kind, outbox.AggregateType, outbox.AggregateID, outbox.PayloadJSON, outbox.CreatedAt); err != nil {
			return err
		}
		result = cloudmodule.CreateGoalResult{Goal: goal, Plan: plan, Created: true}
		return nil
	})
	return result, err
}

func (s *DatabaseStore) CreateCloudConnectionBootstrap(ctx context.Context, request cloudmodule.CreateConnectionBootstrapRequest) (cloudmodule.CreateConnectionBootstrapResult, error) {
	result := cloudmodule.CreateConnectionBootstrapResult{}
	err := s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		bootstrap := request.Bootstrap
		var inserted string
		err := txn.QueryRowContext(ctx, `
			INSERT INTO p2p_cloud_connection_bootstraps (
				bootstrap_id, owner_mxid, cloud_connection_id, provider, requested_region,
				template_url, template_digest, source_tree_digest, stack_name, node_key_id, node_public_key_spki_base64,
				device_approval_key_id, device_approval_public_key_spki_base64,
				candidate_broker_url, stack_arn, status, revision, idempotency_hash, request_digest,
				completion_idempotency_hash, completion_request_digest, job_id, next_node_counter,
				expires_at, created_at, updated_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, '', '', $14, $15, $16, $17, '', '', '', 0, $18, $19, $19
			)
			ON CONFLICT (owner_mxid, idempotency_hash) DO NOTHING
			RETURNING bootstrap_id
		`, bootstrap.BootstrapID, bootstrap.OwnerMXID, bootstrap.ConnectionID, bootstrap.Provider, bootstrap.RequestedRegion,
			bootstrap.TemplateURL, bootstrap.TemplateDigest, bootstrap.SourceTreeDigest, bootstrap.StackName, bootstrap.NodeKeyID, bootstrap.NodePublicKeySPKIBase64,
			bootstrap.DeviceApprovalKeyID, bootstrap.DeviceApprovalPublicKeySPKIBase64, bootstrap.Status, bootstrap.Revision,
			bootstrap.IdempotencyHash, bootstrap.RequestDigest, bootstrap.ExpiresAt, bootstrap.CreatedAt).Scan(&inserted)
		switch {
		case err == nil:
			result = cloudmodule.CreateConnectionBootstrapResult{Bootstrap: bootstrap, Created: true}
			return nil
		case errors.Is(err, sql.ErrNoRows):
			row := txn.QueryRowContext(ctx, `SELECT `+cloudConnectionBootstrapColumns+`
				FROM p2p_cloud_connection_bootstraps WHERE owner_mxid = $1 AND idempotency_hash = $2`, bootstrap.OwnerMXID, bootstrap.IdempotencyHash)
			var existing cloudmodule.ConnectionBootstrap
			if err := scanCloudConnectionBootstrap(row, &existing); err != nil {
				return err
			}
			if existing.RequestDigest != bootstrap.RequestDigest {
				return cloudmodule.ErrIdempotencyConflict
			}
			result = cloudmodule.CreateConnectionBootstrapResult{Bootstrap: existing}
			return nil
		default:
			return err
		}
	})
	return result, err
}

func (s *DatabaseStore) CompleteCloudConnectionBootstrap(ctx context.Context, request cloudmodule.CompleteConnectionBootstrapRequest) (cloudmodule.CompleteConnectionBootstrapResult, error) {
	result := cloudmodule.CompleteConnectionBootstrapResult{}
	err := s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		row := txn.QueryRowContext(ctx, `SELECT `+cloudConnectionBootstrapColumns+`
			FROM p2p_cloud_connection_bootstraps WHERE bootstrap_id = $1 AND owner_mxid = $2 FOR UPDATE`, request.BootstrapID, request.OwnerMXID)
		var bootstrap cloudmodule.ConnectionBootstrap
		if err := scanCloudConnectionBootstrap(row, &bootstrap); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return cloudmodule.ErrConnectionBootstrapInvalid
			}
			return err
		}
		if bootstrap.CompletionIdempotencyHash != "" {
			if bootstrap.CompletionIdempotencyHash != request.IdempotencyHash || bootstrap.CompletionRequestDigest != request.RequestDigest {
				return cloudmodule.ErrIdempotencyConflict
			}
			result = cloudmodule.CompleteConnectionBootstrapResult{Bootstrap: bootstrap}
			return nil
		}
		if bootstrap.Revision != request.ExpectedRevision {
			return cloudmodule.ErrConnectionBootstrapConflict
		}
		now := request.Job.CreatedAt
		if now <= 0 {
			return cloudmodule.ErrConnectionBootstrapInvalid
		}
		if now >= bootstrap.ExpiresAt {
			updated, err := txn.ExecContext(ctx, `
				UPDATE p2p_cloud_connection_bootstraps
				SET status = 'expired', revision = revision + 1, updated_at = $1
				WHERE bootstrap_id = $2 AND revision = $3 AND status = 'awaiting_stack'
			`, now, bootstrap.BootstrapID, bootstrap.Revision)
			if err != nil {
				return err
			}
			if err := requireCloudBootstrapMutation(updated); err != nil {
				return err
			}
			return cloudmodule.ErrConnectionBootstrapExpired
		}
		if bootstrap.Status != cloudmodule.ConnectionBootstrapAwaitingStack {
			return cloudmodule.ErrConnectionBootstrapInvalid
		}
		if err := cloudmodule.ValidateConnectionRegistrationEndpoint(request.BrokerCommandURL, bootstrap.RequestedRegion); err != nil {
			return cloudmodule.ErrConnectionBootstrapInputInvalid
		}
		if err := cloudmodule.ValidateConnectionRegistrationStackARN(request.StackARN, bootstrap.RequestedRegion); err != nil {
			return cloudmodule.ErrConnectionBootstrapInputInvalid
		}
		job := request.Job
		if job.JobID == "" || job.PlanID != "" || job.DeploymentID != "" || job.Kind != "connection_registration" || job.Execution != "queued" || job.Outcome != "pending" || job.Checkpoint != "connection_verification_queued" || job.Revision != 1 || job.CreatedAt != now || job.UpdatedAt != now ||
			request.Event.Type != "cloud.job.changed" || request.Event.AggregateType != "job" || request.Event.AggregateID != job.JobID || request.Event.Revision != job.Revision || request.Event.CreatedAt != now || request.Outbox.Kind != cloudmodule.OutboxKindConnectionRegistrationRequested || request.Outbox.AggregateType != "connection_bootstrap" || request.Outbox.AggregateID != bootstrap.BootstrapID || request.Outbox.CreatedAt != now {
			return cloudmodule.ErrConnectionBootstrapInvalid
		}
		updated, err := txn.ExecContext(ctx, `
			UPDATE p2p_cloud_connection_bootstraps
			SET candidate_broker_url = $1, stack_arn = $2, status = 'verification_queued',
				revision = revision + 1, completion_idempotency_hash = $3, completion_request_digest = $4,
				job_id = $5, updated_at = $6
			WHERE bootstrap_id = $7 AND revision = $8 AND status = 'awaiting_stack'
		`, request.BrokerCommandURL, request.StackARN, request.IdempotencyHash, request.RequestDigest, job.JobID, now, bootstrap.BootstrapID, bootstrap.Revision)
		if err != nil {
			return err
		}
		if err := requireCloudBootstrapMutation(updated); err != nil {
			return err
		}
		if _, err := txn.ExecContext(ctx, `
			INSERT INTO p2p_cloud_jobs (
				job_id, plan_id, deployment_id, kind, execution_status, outcome_status,
				checkpoint, error_code, revision, created_at, updated_at
			) VALUES ($1, '', '', 'connection_registration', 'queued', 'pending', 'connection_verification_queued', '', 1, $2, $2)
		`, job.JobID, now); err != nil {
			return err
		}
		if _, err := txn.ExecContext(ctx, `
			INSERT INTO p2p_cloud_job_steps (
				job_id, step_id, status, summary, checkpoint, error_code, revision, created_at, updated_at
			) VALUES ($1, 'connection_registration', 'queued', 'The submitted AWS Connection Stack is waiting for signed Broker verification.', 'connection_verification_queued', '', 1, $2, $2)
		`, job.JobID, now); err != nil {
			return err
		}
		if _, err := txn.ExecContext(ctx, `
			INSERT INTO p2p_cloud_events (event_id, type, aggregate_type, aggregate_id, revision, summary_json, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, request.Event.EventID, request.Event.Type, request.Event.AggregateType, request.Event.AggregateID, request.Event.Revision, request.Event.SummaryJSON, now); err != nil {
			return err
		}
		if _, err := txn.ExecContext(ctx, `
			INSERT INTO p2p_cloud_projection_outbox (projection_id, cloud_event_id, type, payload_json, available_at, created_at)
			VALUES ($1, $2, $3, $4, $5, $5)
		`, cloudProjectionID(request.Event.EventID), request.Event.EventID, request.Event.Type, request.Event.SummaryJSON, now); err != nil {
			return err
		}
		if _, err := txn.ExecContext(ctx, `
			INSERT INTO p2p_cloud_outbox (outbox_id, kind, aggregate_type, aggregate_id, payload_json, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, request.Outbox.OutboxID, request.Outbox.Kind, request.Outbox.AggregateType, request.Outbox.AggregateID, request.Outbox.PayloadJSON, now); err != nil {
			return err
		}
		bootstrap.CandidateBrokerURL = request.BrokerCommandURL
		bootstrap.StackARN = request.StackARN
		bootstrap.Status = cloudmodule.ConnectionBootstrapVerificationQueued
		bootstrap.Revision++
		bootstrap.CompletionIdempotencyHash = request.IdempotencyHash
		bootstrap.CompletionRequestDigest = request.RequestDigest
		bootstrap.JobID = job.JobID
		bootstrap.UpdatedAt = now
		result = cloudmodule.CompleteConnectionBootstrapResult{Bootstrap: bootstrap, Created: true}
		return nil
	})
	return result, err
}

// ClaimCloudProjection returns an authoritative Cloud event summary. The
// outbox payload remains a redundant delivery record; it is not trusted as a
// source for ProductCore projection content.
func (s *DatabaseStore) ClaimCloudProjection(ctx context.Context, workerID string, lease time.Duration, leaseToken string) (cloudmodule.ProjectionClaim, bool, error) {
	workerID = strings.TrimSpace(workerID)
	leaseToken = strings.TrimSpace(leaseToken)
	if workerID == "" || len(workerID) > 128 || strings.ContainsAny(workerID, "\r\n\t\x00") || leaseToken == "" || len(leaseToken) > 128 || strings.ContainsAny(leaseToken, "\r\n\t\x00") {
		return cloudmodule.ProjectionClaim{}, false, errors.New("cloud projection claim identity is invalid")
	}
	if lease <= 0 || lease > 5*time.Minute {
		return cloudmodule.ProjectionClaim{}, false, errors.New("cloud projection claim lease is invalid")
	}
	now := time.Now().UTC().UnixMilli()
	row := s.db.QueryRowContext(ctx, `
		WITH selected AS (
			SELECT outbox.projection_id
			FROM p2p_cloud_projection_outbox AS outbox
			WHERE outbox.completed_at = 0
				AND outbox.available_at <= $1
				AND outbox.lease_until <= $1
			ORDER BY outbox.created_at ASC, outbox.projection_id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		), claimed AS (
			UPDATE p2p_cloud_projection_outbox AS outbox
			SET lease_owner = $2, lease_token = $3, lease_until = $4,
				attempts = outbox.attempts + 1, last_error_code = ''
			FROM selected
			WHERE outbox.projection_id = selected.projection_id
			RETURNING outbox.projection_id, outbox.cloud_event_id, outbox.lease_token, outbox.attempts
		)
		SELECT claimed.projection_id, claimed.cloud_event_id, event.type, event.summary_json,
			claimed.lease_token, claimed.attempts
		FROM claimed
		JOIN p2p_cloud_events AS event ON event.event_id = claimed.cloud_event_id
	`, now, workerID, leaseToken, now+lease.Milliseconds())
	var claim cloudmodule.ProjectionClaim
	if err := row.Scan(&claim.ProjectionID, &claim.CloudEventID, &claim.Type, &claim.PayloadJSON, &claim.LeaseToken, &claim.Attempt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return cloudmodule.ProjectionClaim{}, false, nil
		}
		return cloudmodule.ProjectionClaim{}, false, fmt.Errorf("claim cloud projection: %w", err)
	}
	return claim, true, nil
}

func (s *DatabaseStore) CompleteCloudProjection(ctx context.Context, claim cloudmodule.ProjectionClaim) error {
	now := time.Now().UTC().UnixMilli()
	result, err := s.db.ExecContext(ctx, `
		UPDATE p2p_cloud_projection_outbox
		SET lease_owner = '', lease_token = '', lease_until = 0, completed_at = $1,
			available_at = $1, last_error_code = ''
		WHERE projection_id = $2 AND lease_token = $3 AND completed_at = 0 AND lease_until > $1
	`, now, claim.ProjectionID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return requireCloudProjectionMutation(result)
}

func (s *DatabaseStore) DeferCloudProjection(ctx context.Context, claim cloudmodule.ProjectionClaim, code string, availableAt time.Time) error {
	now := time.Now().UTC().UnixMilli()
	available := availableAt.UTC().UnixMilli()
	if available < now {
		available = now
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE p2p_cloud_projection_outbox
		SET lease_owner = '', lease_token = '', lease_until = 0,
			available_at = $1, last_error_code = $2
		WHERE projection_id = $3 AND lease_token = $4 AND completed_at = 0 AND lease_until > $5
	`, available, durableCloudProjectionCode(code, cloudmodule.ProjectionPublishFailureCode), claim.ProjectionID, claim.LeaseToken, now)
	if err != nil {
		return err
	}
	return requireCloudProjectionMutation(result)
}

func (s *DatabaseStore) RejectCloudProjection(ctx context.Context, claim cloudmodule.ProjectionClaim, code string) error {
	now := time.Now().UTC().UnixMilli()
	result, err := s.db.ExecContext(ctx, `
		UPDATE p2p_cloud_projection_outbox
		SET lease_owner = '', lease_token = '', lease_until = 0, completed_at = $1,
			available_at = $1, last_error_code = $2
		WHERE projection_id = $3 AND lease_token = $4 AND completed_at = 0 AND lease_until > $1
	`, now, durableCloudProjectionCode(code, cloudmodule.InvalidCloudProjectionCode), claim.ProjectionID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return requireCloudProjectionMutation(result)
}

func cloudProjectionID(eventID string) string {
	return "cloud_projection_" + eventID
}

func requireCloudProjectionMutation(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return cloudmodule.ErrProjectionLeaseLost
	}
	return nil
}

func durableCloudProjectionCode(value, fallback string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || len(value) > 96 {
		return fallback
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' && character != '-' {
			return fallback
		}
	}
	return value
}

func (s *DatabaseStore) ListCloudGoals(ctx context.Context) ([]cloudmodule.Goal, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+cloudGoalColumns+` FROM p2p_cloud_goals ORDER BY updated_at DESC, goal_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []cloudmodule.Goal{}
	for rows.Next() {
		var item cloudmodule.Goal
		if err := scanCloudGoal(rows, &item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *DatabaseStore) ListCloudPlans(ctx context.Context) ([]cloudmodule.Plan, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+cloudPlanColumns+` FROM p2p_cloud_plans ORDER BY updated_at DESC, plan_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []cloudmodule.Plan{}
	for rows.Next() {
		var item cloudmodule.Plan
		if err := scanCloudPlan(rows, &item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *DatabaseStore) GetCloudPlan(ctx context.Context, id string) (cloudmodule.Plan, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+cloudPlanColumns+` FROM p2p_cloud_plans WHERE plan_id = $1`, id)
	var item cloudmodule.Plan
	if err := scanCloudPlan(row, &item); err == sql.ErrNoRows {
		return cloudmodule.Plan{}, false, nil
	} else if err != nil {
		return cloudmodule.Plan{}, false, err
	}
	return item, true, nil
}

// GetCloudQuote reads the persisted display form through a fail-closed
// allowlist before returning it to ProductCore. Decode errors intentionally do
// not include the stored JSON because it may contain malformed private data.
func (s *DatabaseStore) GetCloudQuote(ctx context.Context, id string) (cloudmodule.QuoteView, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT display_json FROM p2p_cloud_quotes WHERE quote_id = $1`, id)
	var displayJSON string
	if err := row.Scan(&displayJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return cloudmodule.QuoteView{}, false, nil
		}
		return cloudmodule.QuoteView{}, false, fmt.Errorf("load cloud quote: %w", err)
	}
	view, err := decodeCloudQuoteView(displayJSON, id)
	if err != nil {
		return cloudmodule.QuoteView{}, false, err
	}
	return view, true, nil
}

func decodeCloudQuoteView(displayJSON, expectedQuoteID string) (cloudmodule.QuoteView, error) {
	decoder := json.NewDecoder(strings.NewReader(displayJSON))
	decoder.DisallowUnknownFields()
	var display cloudQuoteDisplay
	if err := decoder.Decode(&display); err != nil {
		return cloudmodule.QuoteView{}, errCloudQuoteDisplayInvalid
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return cloudmodule.QuoteView{}, errCloudQuoteDisplayInvalid
	}
	if !validCloudQuoteDisplay(display, expectedQuoteID) {
		return cloudmodule.QuoteView{}, errCloudQuoteDisplayInvalid
	}
	view := cloudmodule.QuoteView{
		QuoteID:         display.QuoteID,
		ConnectionID:    display.CloudConnectionID,
		Region:          display.Region,
		Currency:        display.Currency,
		QuotedAt:        display.QuotedAt,
		ValidUntil:      display.ValidUntil,
		Candidates:      make([]cloudmodule.QuoteCandidateView, 0, len(display.Candidates)),
		IncludedItems:   append([]string(nil), display.IncludedItems...),
		UnincludedItems: append([]string(nil), display.UnincludedItems...),
	}
	for _, candidate := range display.Candidates {
		view.Candidates = append(view.Candidates, cloudmodule.QuoteCandidateView{
			Tier:              candidate.Tier,
			InstanceType:      candidate.InstanceType,
			PurchaseOption:    candidate.PurchaseOption,
			HourlyMinor:       candidate.HourlyMinor,
			ThirtyDayMinor:    candidate.ThirtyDayMinor,
			StartupUpperMinor: candidate.StartupUpperMinor,
			EstimatedDiskGiB:  candidate.EstimatedDiskGiB,
			AvailabilityZones: append([]string(nil), candidate.AvailabilityZones...),
		})
	}
	return view, nil
}

func validCloudQuoteDisplay(display cloudQuoteDisplay, expectedQuoteID string) bool {
	if strings.TrimSpace(display.SchemaVersion) == "" || display.QuoteID != expectedQuoteID ||
		strings.TrimSpace(display.CloudConnectionID) == "" || strings.TrimSpace(display.Region) == "" ||
		strings.TrimSpace(display.Currency) == "" || display.QuotedAt.IsZero() || display.ValidUntil.IsZero() ||
		!display.ValidUntil.After(display.QuotedAt) || len(display.Candidates) == 0 || len(display.Candidates) > 3 {
		return false
	}
	candidateIDs := make(map[string]struct{}, len(display.Candidates))
	tiers := make(map[string]struct{}, len(display.Candidates))
	for _, candidate := range display.Candidates {
		if strings.TrimSpace(candidate.CandidateID) == "" || strings.TrimSpace(candidate.InstanceType) == "" ||
			candidate.HourlyMinor < 0 || candidate.ThirtyDayMinor < 0 || candidate.StartupUpperMinor < 0 ||
			candidate.EstimatedDiskGiB == 0 {
			return false
		}
		if _, exists := candidateIDs[candidate.CandidateID]; exists {
			return false
		}
		candidateIDs[candidate.CandidateID] = struct{}{}
		if _, exists := tiers[candidate.Tier]; exists {
			return false
		}
		tiers[candidate.Tier] = struct{}{}
		if candidate.Tier != "economy" && candidate.Tier != "recommended" && candidate.Tier != "performance" {
			return false
		}
		if candidate.PurchaseOption != "on_demand" && candidate.PurchaseOption != "spot" {
			return false
		}
	}
	return validCloudQuoteItems(display.IncludedItems) && validCloudQuoteItems(display.UnincludedItems)
}

func validCloudQuoteItems(items []string) bool {
	for _, item := range items {
		if item != strings.TrimSpace(item) || item == "" || len(item) > 128 || strings.IndexByte(item, 0) >= 0 {
			return false
		}
	}
	return true
}

func (s *DatabaseStore) ListCloudJobs(ctx context.Context) ([]cloudmodule.Job, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+cloudJobColumns+` FROM p2p_cloud_jobs ORDER BY updated_at DESC, job_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []cloudmodule.Job{}
	for rows.Next() {
		var item cloudmodule.Job
		if err := scanCloudJob(rows, &item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *DatabaseStore) ListCloudConnections(ctx context.Context) ([]cloudmodule.Connection, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT cloud_connection_id, provider, account_id, region, mode, status, revision, created_at, updated_at
		FROM p2p_cloud_connections ORDER BY updated_at DESC, cloud_connection_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []cloudmodule.Connection{}
	for rows.Next() {
		var item cloudmodule.Connection
		if err := scanCloudConnection(rows, &item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *DatabaseStore) GetCloudConnection(ctx context.Context, id string) (cloudmodule.Connection, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT cloud_connection_id, provider, account_id, region, mode, status, revision, created_at, updated_at
		FROM p2p_cloud_connections WHERE cloud_connection_id = $1`, id)
	var item cloudmodule.Connection
	if err := scanCloudConnection(row, &item); err == sql.ErrNoRows {
		return cloudmodule.Connection{}, false, nil
	} else if err != nil {
		return cloudmodule.Connection{}, false, err
	}
	return item, true, nil
}

func (s *DatabaseStore) ListCloudDeployments(ctx context.Context) ([]cloudmodule.Deployment, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT deployment_id, plan_id, cloud_connection_id, execution_status, outcome_status, resource_status, revision, created_at, updated_at
		FROM p2p_cloud_deployments ORDER BY updated_at DESC, deployment_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []cloudmodule.Deployment{}
	for rows.Next() {
		var item cloudmodule.Deployment
		if err := scanCloudDeployment(rows, &item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *DatabaseStore) GetCloudDeployment(ctx context.Context, id string) (cloudmodule.Deployment, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT deployment_id, plan_id, cloud_connection_id, execution_status, outcome_status, resource_status, revision, created_at, updated_at
		FROM p2p_cloud_deployments WHERE deployment_id = $1`, id)
	var item cloudmodule.Deployment
	if err := scanCloudDeployment(row, &item); err == sql.ErrNoRows {
		return cloudmodule.Deployment{}, false, nil
	} else if err != nil {
		return cloudmodule.Deployment{}, false, err
	}
	return item, true, nil
}

func (s *DatabaseStore) ListCloudServices(ctx context.Context) ([]cloudmodule.Service, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT service_id, deployment_id, recipe_id, name, service_status, integration_status, revision, created_at, updated_at
		FROM p2p_cloud_services ORDER BY updated_at DESC, service_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []cloudmodule.Service{}
	for rows.Next() {
		var item cloudmodule.Service
		if err := scanCloudService(rows, &item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *DatabaseStore) GetCloudService(ctx context.Context, id string) (cloudmodule.Service, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT service_id, deployment_id, recipe_id, name, service_status, integration_status, revision, created_at, updated_at
		FROM p2p_cloud_services WHERE service_id = $1`, id)
	var item cloudmodule.Service
	if err := scanCloudService(row, &item); err == sql.ErrNoRows {
		return cloudmodule.Service{}, false, nil
	} else if err != nil {
		return cloudmodule.Service{}, false, err
	}
	return item, true, nil
}

func (s *DatabaseStore) ListCloudRecipes(ctx context.Context) ([]cloudmodule.Recipe, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT recipe_id, name, version, digest, maturity, revision, created_at, updated_at
		FROM p2p_cloud_recipes ORDER BY updated_at DESC, recipe_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []cloudmodule.Recipe{}
	for rows.Next() {
		var item cloudmodule.Recipe
		if err := scanCloudRecipe(rows, &item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *DatabaseStore) GetCloudRecipe(ctx context.Context, id string) (cloudmodule.Recipe, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT recipe_id, name, version, digest, maturity, revision, created_at, updated_at
		FROM p2p_cloud_recipes WHERE recipe_id = $1`, id)
	var item cloudmodule.Recipe
	if err := scanCloudRecipe(row, &item); err == sql.ErrNoRows {
		return cloudmodule.Recipe{}, false, nil
	} else if err != nil {
		return cloudmodule.Recipe{}, false, err
	}
	return item, true, nil
}

func (s *DatabaseStore) ListCloudAlerts(ctx context.Context) ([]cloudmodule.Alert, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT alert_id, deployment_id, service_id, severity, code, message, acknowledged, revision, created_at, updated_at
		FROM p2p_cloud_alerts ORDER BY updated_at DESC, alert_id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []cloudmodule.Alert{}
	for rows.Next() {
		var item cloudmodule.Alert
		if err := scanCloudAlert(rows, &item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *DatabaseStore) ListCloudEvents(ctx context.Context, limit int) ([]cloudmodule.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT event_id, type, aggregate_type, aggregate_id, revision, summary_json, created_at
		FROM p2p_cloud_events ORDER BY created_at DESC, event_id ASC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []cloudmodule.Event{}
	for rows.Next() {
		var item cloudmodule.Event
		if err := scanCloudEvent(rows, &item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

type cloudScanner interface{ Scan(...any) error }

func scanCloudGoal(row cloudScanner, item *cloudmodule.Goal) error {
	return row.Scan(&item.GoalID, &item.OwnerMXID, &item.Prompt, &item.ConnectionID, &item.PlanID, &item.Status,
		&item.IdempotencyHash, &item.RequestDigest, &item.Revision, &item.CreatedAt, &item.UpdatedAt)
}

func scanCloudPlan(row cloudScanner, item *cloudmodule.Plan) error {
	return row.Scan(&item.PlanID, &item.GoalID, &item.ConnectionID, &item.Status, &item.Title, &item.Summary,
		&item.RecipeDigest, &item.QuoteID, &item.PlanHash, &item.Revision, &item.CreatedAt, &item.UpdatedAt)
}

func scanCloudJob(row cloudScanner, item *cloudmodule.Job) error {
	return row.Scan(
		&item.JobID, &item.PlanID, &item.DeploymentID, &item.Kind, &item.Execution, &item.Outcome,
		&item.Checkpoint, &item.ErrorCode, &item.Revision, &item.CreatedAt, &item.UpdatedAt)
}

func scanCloudConnection(row cloudScanner, item *cloudmodule.Connection) error {
	return row.Scan(&item.ConnectionID, &item.Provider, &item.AccountID, &item.Region, &item.Mode, &item.Status, &item.Revision, &item.CreatedAt, &item.UpdatedAt)
}

func scanCloudConnectionBootstrap(row cloudScanner, item *cloudmodule.ConnectionBootstrap) error {
	return row.Scan(
		&item.BootstrapID, &item.OwnerMXID, &item.ConnectionID, &item.Provider, &item.RequestedRegion,
		&item.TemplateURL, &item.TemplateDigest, &item.SourceTreeDigest, &item.StackName, &item.NodeKeyID, &item.NodePublicKeySPKIBase64,
		&item.DeviceApprovalKeyID, &item.DeviceApprovalPublicKeySPKIBase64,
		&item.CandidateBrokerURL, &item.StackARN, &item.Status, &item.Revision, &item.IdempotencyHash, &item.RequestDigest,
		&item.CompletionIdempotencyHash, &item.CompletionRequestDigest, &item.JobID, &item.NextNodeCounter,
		&item.ExpiresAt, &item.CreatedAt, &item.UpdatedAt,
	)
}

func requireCloudBootstrapMutation(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return cloudmodule.ErrConnectionBootstrapConflict
	}
	return nil
}

func scanCloudDeployment(row cloudScanner, item *cloudmodule.Deployment) error {
	return row.Scan(&item.DeploymentID, &item.PlanID, &item.ConnectionID, &item.Execution, &item.Outcome, &item.Resource, &item.Revision, &item.CreatedAt, &item.UpdatedAt)
}

func scanCloudService(row cloudScanner, item *cloudmodule.Service) error {
	return row.Scan(&item.ServiceID, &item.DeploymentID, &item.RecipeID, &item.Name, &item.Status, &item.Integration, &item.Revision, &item.CreatedAt, &item.UpdatedAt)
}

func scanCloudRecipe(row cloudScanner, item *cloudmodule.Recipe) error {
	return row.Scan(&item.RecipeID, &item.Name, &item.Version, &item.Digest, &item.Maturity, &item.Revision, &item.CreatedAt, &item.UpdatedAt)
}

func scanCloudAlert(row cloudScanner, item *cloudmodule.Alert) error {
	return row.Scan(&item.AlertID, &item.DeploymentID, &item.ServiceID, &item.Severity, &item.Code, &item.Message, &item.Acknowledged, &item.Revision, &item.CreatedAt, &item.UpdatedAt)
}

func scanCloudEvent(row cloudScanner, item *cloudmodule.Event) error {
	if err := row.Scan(&item.EventID, &item.Type, &item.AggregateType, &item.AggregateID, &item.Revision, &item.SummaryJSON, &item.CreatedAt); err != nil {
		return err
	}
	item.HydrateSummary()
	return nil
}
