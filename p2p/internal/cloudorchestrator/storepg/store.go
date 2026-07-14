// Package storepg implements the independent Cloud Orchestrator's narrowly
// scoped PostgreSQL repository. It deliberately does not run product
// migrations: the Message Server owns schema migration and this process must
// run with a database role that can access only Cloud tables.
package storepg

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var ErrLeaseLost = errors.New("cloud orchestrator claim lease was lost")

type Config struct {
	Now             func() time.Time
	NewLeaseToken   func() string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

type Store struct {
	db  *sql.DB
	cfg Config
}

var _ runtime.Store = (*Store)(nil)

func Open(ctx context.Context, databaseURL string, cfg Config) (*Store, error) {
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(databaseURL)), "postgres") {
		return nil, errors.New("cloud orchestrator requires a PostgreSQL database URL")
	}
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open cloud orchestrator database: %w", err)
	}
	configurePool(db, cfg)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping cloud orchestrator database: %w", err)
	}
	return New(db, cfg), nil
}

func configurePool(db *sql.DB, cfg Config) {
	if db == nil {
		return
	}
	maxOpen := cfg.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 2
	}
	maxIdle := cfg.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = 1
	}
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	maxLifetime := cfg.ConnMaxLifetime
	if maxLifetime <= 0 {
		maxLifetime = 5 * time.Minute
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(maxLifetime)
}

func New(db *sql.DB, cfg Config) *Store {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.NewLeaseToken == nil {
		cfg.NewLeaseToken = uuid.NewString
	}
	return &Store{db: db, cfg: cfg}
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) ClaimResearchGoal(ctx context.Context, workerID string, lease time.Duration) (runtime.Claim, bool, error) {
	if s == nil || s.db == nil {
		return runtime.Claim{}, false, errors.New("cloud orchestrator database is unavailable")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || len(workerID) > 128 || strings.ContainsAny(workerID, "\r\n\t") {
		return runtime.Claim{}, false, errors.New("cloud orchestrator worker id is invalid")
	}
	if lease <= 0 || lease > 5*time.Minute {
		return runtime.Claim{}, false, errors.New("cloud orchestrator lease must be between 1ns and 5m")
	}
	now := s.now().UnixMilli()
	token := strings.TrimSpace(s.cfg.NewLeaseToken())
	if token == "" || len(token) > 128 {
		return runtime.Claim{}, false, errors.New("cloud orchestrator lease token is invalid")
	}
	row := s.db.QueryRowContext(ctx, `
		WITH selected AS (
			SELECT outbox.outbox_id
			FROM p2p_cloud_outbox AS outbox
			JOIN p2p_cloud_goals AS goal ON goal.goal_id = outbox.aggregate_id
			JOIN p2p_cloud_plans AS plan ON plan.plan_id = goal.plan_id
			WHERE outbox.kind = $1
				AND outbox.aggregate_type = 'goal'
				AND outbox.completed_at = 0
				AND outbox.available_at <= $2
				AND outbox.lease_until <= $2
				AND goal.status = 'researching'
				AND plan.status = 'researching'
				AND plan.cloud_connection_id <> ''
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
			goal.goal_id, goal.plan_id, plan.cloud_connection_id, plan.revision,
			claimed.payload_json, claimed.lease_token, claimed.attempts
		FROM claimed
		JOIN p2p_cloud_goals AS goal ON goal.goal_id = claimed.aggregate_id
		JOIN p2p_cloud_plans AS plan ON plan.plan_id = goal.plan_id
	`, runtime.ResearchGoalRequested, now, workerID, token, now+lease.Milliseconds())
	var claim runtime.Claim
	if err := row.Scan(
		&claim.OutboxID, &claim.Kind, &claim.AggregateType, &claim.AggregateID,
		&claim.GoalID, &claim.PlanID, &claim.ConnectionID, &claim.PlanRevision,
		&claim.PayloadJSON, &claim.LeaseToken, &claim.Attempt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.Claim{}, false, nil
		}
		return runtime.Claim{}, false, fmt.Errorf("claim cloud research outbox: %w", err)
	}
	return claim, true, nil
}

func (s *Store) DeferResearch(ctx context.Context, claim runtime.Claim, code string, availableAt time.Time) error {
	code = durableErrorCode(code, "research_retryable")
	return s.withClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if _, err := transitionResearchJob(ctx, tx, claim, now, researchJobTransition{
			execution: "queued", outcome: "pending", checkpoint: "research_retry_scheduled", errorCode: code,
			stepStatus: "queued", stepSummary: "Cloud research is waiting to retry.",
		}); err != nil {
			return err
		}
		available := availableAt.UTC().UnixMilli()
		if available < now {
			available = now
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_outbox
			SET lease_owner = '', lease_token = '', lease_until = 0,
				available_at = $1, last_error_code = $2
			WHERE outbox_id = $3 AND lease_token = $4 AND completed_at = 0
		`, available, code, claim.OutboxID, claim.LeaseToken)
		if err != nil {
			return err
		}
		return requireOneAffected(result)
	})
}

func (s *Store) MarkResearchStarted(ctx context.Context, claim runtime.Claim) error {
	return s.withClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		_, err := transitionResearchJob(ctx, tx, claim, now, researchJobTransition{
			execution: "queued", outcome: "pending", checkpoint: "research_leased", errorCode: "",
			stepStatus: "running", stepSummary: "Cloud research is evaluating official sources and a quote.",
		})
		return err
	})
}

func (s *Store) FailResearch(ctx context.Context, claim runtime.Claim, code string) error {
	code = durableErrorCode(code, "research_planner_failed")
	return s.withClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if _, err := transitionResearchJob(ctx, tx, claim, now, researchJobTransition{
			execution: "finished", outcome: "failed", checkpoint: "research_failed", errorCode: code,
			stepStatus: "failed", stepSummary: "Cloud research did not produce a deployable plan.",
		}); err != nil {
			return err
		}
		return completeOutbox(ctx, tx, claim, now)
	})
}

func (s *Store) CommitResearch(ctx context.Context, claim runtime.Claim, output runtime.ResearchOutput) error {
	input := runtime.ResearchInput{
		GoalID: claim.GoalID, PlanID: claim.PlanID, ConnectionID: claim.ConnectionID,
		PlanRevision: claim.PlanRevision, Prompt: "validated-by-store",
	}
	if err := output.ValidateFor(input); err != nil {
		return fmt.Errorf("validate research output: %w", err)
	}
	recipeDigest, err := output.Recipe.Digest()
	if err != nil {
		return err
	}
	recipeCBOR, err := output.Recipe.CanonicalRecipeCBOR()
	if err != nil {
		return err
	}
	recipeJSON, err := json.Marshal(output.Recipe)
	if err != nil {
		return err
	}
	draftDigest, err := output.Draft.Digest()
	if err != nil {
		return err
	}
	quoteRequest := cloudcontracts.QuoteRequestV1{
		SchemaVersion:     cloudcontracts.SchemaVersionV1,
		QuoteRequestID:    stableID("cloud_quote_request_", claim.PlanID, fmt.Sprint(claim.PlanRevision+1), recipeDigest, draftDigest),
		PlanID:            claim.PlanID,
		PlanRevision:      uint64(claim.PlanRevision + 1),
		CloudConnectionID: claim.ConnectionID,
		RecipeDigest:      recipeDigest,
		Region:            output.Draft.Region,
		Candidates:        append([]cloudcontracts.QuoteRequestCandidateV1(nil), output.Draft.Candidates...),
	}
	if err := quoteRequest.Validate(); err != nil {
		return fmt.Errorf("validate quote request: %w", err)
	}
	quoteRequestJSON, err := json.Marshal(quoteRequest)
	if err != nil {
		return err
	}
	quoteOutboxID := stableID("cloud_outbox_quote_", quoteRequest.QuoteRequestID)
	quoteJobID := cloudmodule.QuoteJobID(quoteOutboxID)

	return s.withClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if err := ensureRecipe(ctx, tx, output.Recipe, recipeDigest, recipeCBOR, recipeJSON, now); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_plans
			SET status = $1, title = $2, summary = $3, recipe_digest = $4,
				quote_id = $5, plan_hash = $6, revision = $7, updated_at = $8
			WHERE plan_id = $9 AND revision = $10 AND status = 'researching'
		`, string(cloudcontracts.PlanQuoting), output.Title, output.Summary, recipeDigest,
			"", "", int64(quoteRequest.PlanRevision), now, claim.PlanID, claim.PlanRevision)
		if err != nil {
			return err
		}
		if err := requireOneAffected(result); err != nil {
			return err
		}
		var planCreatedAt int64
		if err := tx.QueryRowContext(ctx, `SELECT created_at FROM p2p_cloud_plans WHERE plan_id = $1`, claim.PlanID).Scan(&planCreatedAt); err != nil {
			return err
		}
		planSummary := map[string]any{
			"plan_id": claim.PlanID, "goal_id": claim.GoalID, "cloud_connection_id": claim.ConnectionID,
			"status": string(cloudcontracts.PlanQuoting), "title": output.Title,
			"summary": output.Summary, "recipe_digest": recipeDigest, "quote_id": "",
			"plan_hash": "", "revision": int64(quoteRequest.PlanRevision), "created_at": planCreatedAt, "updated_at": now,
		}
		if err := writeEventAndProjection(ctx, tx, stableID("cloud_event_", claim.PlanID, fmt.Sprint(quoteRequest.PlanRevision), "quote_requested"), "cloud.plan.changed", "plan", claim.PlanID, int64(quoteRequest.PlanRevision), planSummary, now); err != nil {
			return err
		}
		if _, err := transitionResearchJob(ctx, tx, claim, now, researchJobTransition{
			execution: "finished", outcome: "succeeded", checkpoint: "research_ready", errorCode: "",
			stepStatus: "finished", stepSummary: "Official-source research draft is ready for a verified price quote.",
		}); err != nil {
			return err
		}
		quoteJob := cloudmodule.Job{
			JobID: quoteJobID, PlanID: claim.PlanID, Kind: "quote", Execution: "queued", Outcome: "pending",
			Checkpoint: "quote_queued", Revision: 1, CreatedAt: now, UpdatedAt: now,
		}
		if err := insertQuoteJob(ctx, tx, quoteJob, now); err != nil {
			return err
		}
		if err := writeEventAndProjection(ctx, tx,
			stableID("cloud_event_", quoteJob.JobID, "1", quoteJob.Checkpoint),
			"cloud.job.changed", "job", quoteJob.JobID, quoteJob.Revision, jobSummary(quoteJob), now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_outbox (outbox_id, kind, aggregate_type, aggregate_id, payload_json, created_at)
			VALUES ($1, $2, 'plan', $3, $4, $5)
		`, quoteOutboxID, runtime.QuotePlanRequested, claim.PlanID, string(quoteRequestJSON), now); err != nil {
			return err
		}
		return completeOutbox(ctx, tx, claim, now)
	})
}

type researchJobTransition struct {
	execution   string
	outcome     string
	checkpoint  string
	errorCode   string
	stepStatus  string
	stepSummary string
}

func insertQuoteJob(ctx context.Context, tx *sql.Tx, job cloudmodule.Job, now int64) error {
	if job.JobID == "" || job.PlanID == "" || job.Kind != "quote" || job.Revision != 1 || job.Execution != "queued" || job.Outcome != "pending" || job.Checkpoint != "quote_queued" {
		return errors.New("quote job is invalid")
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO p2p_cloud_jobs (
			job_id, plan_id, deployment_id, kind, execution_status, outcome_status,
			checkpoint, error_code, revision, created_at, updated_at
		) VALUES ($1, $2, '', 'quote', 'queued', 'pending', 'quote_queued', '', 1, $3, $3)
	`, job.JobID, job.PlanID, now); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO p2p_cloud_job_steps (
			job_id, step_id, status, summary, checkpoint, error_code, revision, created_at, updated_at
		) VALUES ($1, 'quote', 'queued', 'A verified AWS price quote is queued for the selected connection.', 'quote_queued', '', 1, $2, $2)
	`, job.JobID, now)
	return err
}

// transitionResearchJob is always called from a claim-fenced transaction. It
// makes the research lifecycle durable independently of the Plan: an expired
// lease, retry, or terminal source failure cannot be mistaken for a service
// deployment, but it remains visible after reconnecting the client.
func transitionResearchJob(ctx context.Context, tx *sql.Tx, claim runtime.Claim, now int64, transition researchJobTransition) (cloudmodule.Job, error) {
	jobID := cloudmodule.ResearchJobID(claim.OutboxID)
	return transitionCloudJob(ctx, tx, jobID, claim.PlanID, "", "research", "research", now, transition)
}

func transitionQuoteJob(ctx context.Context, tx *sql.Tx, claim runtime.QuoteClaim, now int64, transition researchJobTransition) (cloudmodule.Job, error) {
	jobID := cloudmodule.QuoteJobID(claim.OutboxID)
	return transitionCloudJob(ctx, tx, jobID, claim.PlanID, "", "quote", "quote", now, transition)
}

func transitionCloudJob(ctx context.Context, tx *sql.Tx, jobID, planID, deploymentID, kind, stepID string, now int64, transition researchJobTransition) (cloudmodule.Job, error) {
	if jobID == "" || kind == "" || stepID == "" ||
		(kind == "connection_registration" && (planID != "" || deploymentID != "")) ||
		(kind == "provision" && (planID == "" || deploymentID == "")) ||
		(kind != "connection_registration" && kind != "provision" && (planID == "" || deploymentID != "")) {
		return cloudmodule.Job{}, errors.New("cloud job identity is invalid")
	}
	transition.errorCode = durableErrorCode(transition.errorCode, "")
	var job cloudmodule.Job
	err := tx.QueryRowContext(ctx, `
		SELECT job_id, plan_id, deployment_id, kind, execution_status, outcome_status,
			checkpoint, error_code, revision, created_at, updated_at
		FROM p2p_cloud_jobs WHERE job_id = $1 FOR UPDATE`, jobID).Scan(
		&job.JobID, &job.PlanID, &job.DeploymentID, &job.Kind, &job.Execution, &job.Outcome,
		&job.Checkpoint, &job.ErrorCode, &job.Revision, &job.CreatedAt, &job.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		job = cloudmodule.Job{
			JobID: jobID, PlanID: planID, DeploymentID: deploymentID, Kind: kind, Execution: transition.execution,
			Outcome: transition.outcome, Checkpoint: transition.checkpoint, ErrorCode: transition.errorCode,
			Revision: 1, CreatedAt: now, UpdatedAt: now,
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_jobs (
				job_id, plan_id, deployment_id, kind, execution_status, outcome_status,
				checkpoint, error_code, revision, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, $9, $9)
		`, job.JobID, job.PlanID, job.DeploymentID, kind, job.Execution, job.Outcome, job.Checkpoint, job.ErrorCode, now); err != nil {
			return cloudmodule.Job{}, err
		}
	} else if err != nil {
		return cloudmodule.Job{}, err
	} else {
		if job.PlanID != planID || job.DeploymentID != deploymentID || job.Kind != kind || job.Revision <= 0 {
			return cloudmodule.Job{}, errors.New("cloud job does not match the claimed outbox")
		}
		previousRevision := job.Revision
		job.Execution = transition.execution
		job.Outcome = transition.outcome
		job.Checkpoint = transition.checkpoint
		job.ErrorCode = transition.errorCode
		job.Revision++
		job.UpdatedAt = now
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_jobs
			SET execution_status = $1, outcome_status = $2, checkpoint = $3, error_code = $4,
				revision = $5, updated_at = $6
			WHERE job_id = $7 AND revision = $8
		`, job.Execution, job.Outcome, job.Checkpoint, job.ErrorCode, job.Revision, now, job.JobID, previousRevision)
		if err != nil {
			return cloudmodule.Job{}, err
		}
		if err := requireOneAffected(result); err != nil {
			return cloudmodule.Job{}, err
		}
	}

	stepResult, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_job_steps
		SET status = $1, summary = $2, checkpoint = $3, error_code = $4,
			revision = revision + 1, updated_at = $5
		WHERE job_id = $6 AND step_id = $7
	`, transition.stepStatus, transition.stepSummary, transition.checkpoint, transition.errorCode, now, job.JobID, stepID)
	if err != nil {
		return cloudmodule.Job{}, err
	}
	updatedSteps, err := stepResult.RowsAffected()
	if err != nil {
		return cloudmodule.Job{}, err
	}
	if updatedSteps == 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_job_steps (
				job_id, step_id, status, summary, checkpoint, error_code, revision, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $7)
		`, job.JobID, stepID, transition.stepStatus, transition.stepSummary, transition.checkpoint, transition.errorCode, now); err != nil {
			return cloudmodule.Job{}, err
		}
	}
	if err := writeEventAndProjection(ctx, tx,
		stableID("cloud_event_", job.JobID, fmt.Sprint(job.Revision), job.Checkpoint),
		"cloud.job.changed", "job", job.JobID, job.Revision, jobSummary(job), now); err != nil {
		return cloudmodule.Job{}, err
	}
	return job, nil
}

func jobSummary(job cloudmodule.Job) map[string]any {
	return map[string]any{
		"job_id": job.JobID, "plan_id": job.PlanID, "deployment_id": job.DeploymentID,
		"kind": job.Kind, "execution_status": job.Execution, "outcome_status": job.Outcome,
		"checkpoint": job.Checkpoint, "error_code": job.ErrorCode, "revision": job.Revision,
		"created_at": job.CreatedAt, "updated_at": job.UpdatedAt,
	}
}

func (s *Store) withClaimTransaction(ctx context.Context, claim runtime.Claim, run func(*sql.Tx, int64) error) (err error) {
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
	if err = verifyClaimFence(ctx, tx, claim, now); err != nil {
		return err
	}
	if err = run(tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

func verifyClaimFence(ctx context.Context, tx *sql.Tx, claim runtime.Claim, now int64) error {
	var leaseToken, aggregateType, aggregateID, goalID, planID, connectionID, planStatus string
	var leaseUntil, completedAt, revision int64
	err := tx.QueryRowContext(ctx, `
		SELECT outbox.lease_token, outbox.lease_until, outbox.completed_at, outbox.aggregate_type, outbox.aggregate_id,
			goal.goal_id, goal.plan_id, plan.cloud_connection_id, plan.status, plan.revision
		FROM p2p_cloud_outbox AS outbox
		JOIN p2p_cloud_goals AS goal ON goal.goal_id = outbox.aggregate_id
		JOIN p2p_cloud_plans AS plan ON plan.plan_id = goal.plan_id
		WHERE outbox.outbox_id = $1
		FOR UPDATE OF outbox
	`, claim.OutboxID).Scan(&leaseToken, &leaseUntil, &completedAt, &aggregateType, &aggregateID, &goalID, &planID, &connectionID, &planStatus, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if claim.Kind != runtime.ResearchGoalRequested || claim.LeaseToken == "" || leaseToken != claim.LeaseToken || leaseUntil <= now || completedAt != 0 ||
		aggregateType != "goal" || aggregateID != claim.AggregateID || goalID != claim.GoalID || planID != claim.PlanID ||
		connectionID != claim.ConnectionID || revision != claim.PlanRevision || planStatus != "researching" {
		return ErrLeaseLost
	}
	return nil
}

func ensureRecipe(ctx context.Context, tx *sql.Tx, recipe cloudcontracts.RecipeV1, digest string, canonicalCBOR, displayJSON []byte, now int64) error {
	var existingDigest string
	err := tx.QueryRowContext(ctx, `SELECT digest FROM p2p_cloud_recipes WHERE recipe_id = $1 FOR UPDATE`, recipe.RecipeID).Scan(&existingDigest)
	if err == nil {
		if existingDigest != digest {
			return errors.New("recipe id is already bound to another digest")
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	} else {
		var existingRecipeID string
		err = tx.QueryRowContext(ctx, `SELECT recipe_id FROM p2p_cloud_recipes WHERE digest = $1 FOR UPDATE`, digest).Scan(&existingRecipeID)
		if err == nil {
			return errors.New("recipe digest is already bound to another recipe id")
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err = tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_recipes (recipe_id, name, version, digest, maturity, revision, created_at, updated_at)
			VALUES ($1, $2, 'v1', $3, $4, 1, $5, $5)
		`, recipe.RecipeID, recipe.Name, digest, string(recipe.Maturity), now); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO p2p_cloud_recipe_versions (recipe_id, revision, canonical_cbor, display_json, digest, maturity, created_at)
		VALUES ($1, 1, $2, $3, $4, $5, $6)
		ON CONFLICT (recipe_id, revision) DO NOTHING
	`, recipe.RecipeID, canonicalCBOR, string(displayJSON), digest, string(recipe.Maturity), now); err != nil {
		return err
	}
	var versionDigest string
	if err := tx.QueryRowContext(ctx, `
		SELECT digest FROM p2p_cloud_recipe_versions WHERE recipe_id = $1 AND revision = 1
	`, recipe.RecipeID).Scan(&versionDigest); err != nil {
		return err
	}
	if versionDigest != digest {
		return errors.New("recipe version is already bound to another digest")
	}
	return nil
}

func ensureQuote(ctx context.Context, tx *sql.Tx, quote cloudcontracts.QuoteV1, digest string, canonicalCBOR, displayJSON []byte, now int64) error {
	var existingDigest string
	err := tx.QueryRowContext(ctx, `SELECT digest FROM p2p_cloud_quotes WHERE quote_id = $1 FOR UPDATE`, quote.QuoteID).Scan(&existingDigest)
	if err == nil {
		if existingDigest != digest {
			return errors.New("quote id is already bound to another digest")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var existingQuoteID string
	err = tx.QueryRowContext(ctx, `SELECT quote_id FROM p2p_cloud_quotes WHERE digest = $1 FOR UPDATE`, digest).Scan(&existingQuoteID)
	if err == nil {
		return errors.New("quote digest is already bound to another quote id")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO p2p_cloud_quotes (
			quote_id, cloud_connection_id, region, currency, digest, canonical_cbor, display_json,
			quoted_at, valid_until, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, quote.QuoteID, quote.CloudConnectionID, quote.Region, quote.Currency, digest, canonicalCBOR, string(displayJSON),
		quote.QuotedAt.UTC().UnixMilli(), quote.ValidUntil.UTC().UnixMilli(), now)
	return err
}

func writeEventAndProjection(ctx context.Context, tx *sql.Tx, eventID, eventType, aggregateType, aggregateID string, revision int64, summary map[string]any, now int64) error {
	summaryJSON, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO p2p_cloud_events (event_id, type, aggregate_type, aggregate_id, revision, summary_json, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, eventID, eventType, aggregateType, aggregateID, revision, string(summaryJSON), now); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO p2p_cloud_projection_outbox (
			projection_id, cloud_event_id, type, payload_json, available_at, created_at
		) VALUES ($1, $2, $3, $4, $5, $5)
	`, stableID("cloud_projection_", eventID), eventID, eventType, string(summaryJSON), now)
	return err
}

func completeOutbox(ctx context.Context, tx *sql.Tx, claim runtime.Claim, now int64) error {
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

func requireOneAffected(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) now() time.Time {
	if s != nil && s.cfg.Now != nil {
		return s.cfg.Now().UTC()
	}
	return time.Now().UTC()
}

func durableErrorCode(value, fallback string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || len(value) > 96 {
		return fallback
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' && r != '-' {
			return fallback
		}
	}
	return value
}

func stableID(prefix string, values ...string) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(prefix))
	for _, value := range values {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(value))
	}
	return prefix + hex.EncodeToString(hash.Sum(nil))[:32]
}
