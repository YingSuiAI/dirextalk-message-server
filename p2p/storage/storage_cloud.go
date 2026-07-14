package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
