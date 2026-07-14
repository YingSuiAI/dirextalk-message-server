package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	invalidResearchPayloadCode = "invalid_research_payload"
	invalidResearchOutputCode  = "invalid_research_output"
	plannerFailedCode          = "research_planner_failed"
)

type Config struct {
	WorkerID       string
	Lease          time.Duration
	AttemptTimeout time.Duration
	RetryDelay     time.Duration
	Now            func() time.Time
}

type Runner struct {
	store   Store
	planner Planner
	cfg     Config
}

func New(store Store, planner Planner, cfg Config) *Runner {
	if cfg.Lease <= 0 {
		cfg.Lease = 2 * time.Minute
	}
	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = cfg.Lease / 2
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = time.Minute
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Runner{store: store, planner: planner, cfg: cfg}
}

// RunOnce claims at most one private research request. It never returns a
// planner error to its caller after a durable defer/fail state has been
// written, because those text errors may contain upstream details. Store
// errors are returned so the process supervisor can retry safely.
func (r *Runner) RunOnce(ctx context.Context) (bool, error) {
	if r == nil || r.store == nil {
		return false, errors.New("cloud orchestrator store is unavailable")
	}
	if r.planner == nil {
		return false, errors.New("cloud orchestrator planner is unavailable")
	}
	workerID := strings.TrimSpace(r.cfg.WorkerID)
	if workerID == "" {
		return false, errors.New("cloud orchestrator worker id is required")
	}
	if r.cfg.Lease > 5*time.Minute {
		return false, errors.New("cloud orchestrator lease must not exceed five minutes")
	}
	if r.cfg.AttemptTimeout <= 0 || r.cfg.AttemptTimeout >= r.cfg.Lease {
		return false, errors.New("cloud orchestrator attempt timeout must be shorter than its lease")
	}
	claim, found, err := r.store.ClaimResearchGoal(ctx, workerID, r.cfg.Lease)
	if err != nil || !found {
		return found, err
	}
	input, err := researchInputFromClaim(claim)
	if err != nil {
		return true, r.store.FailResearch(ctx, claim, invalidResearchPayloadCode)
	}
	if err := r.store.MarkResearchStarted(ctx, claim); err != nil {
		// Keep the leased source outbox unsettled. The lease fence makes a
		// later worker retry safely, while avoiding an unobservable model call.
		return true, fmt.Errorf("mark cloud research started: %w", err)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
	output, err := r.planner.Research(attemptCtx, input)
	attemptErr := attemptCtx.Err()
	cancel()
	if ctx.Err() != nil {
		// During shutdown leave the durable claim unsettled. A later process
		// will reclaim it only after the lease expires, rather than racing a
		// cancelled worker into a terminal state.
		return true, ctx.Err()
	}
	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return true, r.store.DeferResearch(ctx, claim, "research_attempt_timed_out", r.now().Add(r.cfg.RetryDelay))
	}
	if err != nil {
		if code, retry := retryCode(err); retry {
			return true, r.store.DeferResearch(ctx, claim, code, r.now().Add(r.cfg.RetryDelay))
		}
		return true, r.store.FailResearch(ctx, claim, plannerFailedCode)
	}
	if err := output.ValidateFor(input); err != nil {
		return true, r.store.FailResearch(ctx, claim, invalidResearchOutputCode)
	}
	if err := r.store.CommitResearch(ctx, claim, output); err != nil {
		return true, fmt.Errorf("commit cloud research: %w", err)
	}
	return true, nil
}

func (r *Runner) now() time.Time {
	if r != nil && r.cfg.Now != nil {
		return r.cfg.Now().UTC()
	}
	return time.Now().UTC()
}

type researchPayload struct {
	GoalID       string `json:"goal_id"`
	PlanID       string `json:"plan_id"`
	ConnectionID string `json:"cloud_connection_id"`
	Goal         string `json:"goal"`
}

func researchInputFromClaim(claim Claim) (ResearchInput, error) {
	if claim.Kind != ResearchGoalRequested || claim.AggregateType != "goal" || claim.OutboxID == "" || claim.LeaseToken == "" || claim.PlanRevision <= 0 {
		return ResearchInput{}, errors.New("claim envelope is invalid")
	}
	decoder := json.NewDecoder(strings.NewReader(claim.PayloadJSON))
	decoder.DisallowUnknownFields()
	var payload researchPayload
	if err := decoder.Decode(&payload); err != nil {
		return ResearchInput{}, errors.New("payload is not a valid research request")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ResearchInput{}, errors.New("payload contains trailing JSON")
	}
	if payload.GoalID != claim.GoalID || payload.GoalID != claim.AggregateID || payload.PlanID != claim.PlanID || payload.ConnectionID != claim.ConnectionID {
		return ResearchInput{}, errors.New("payload does not match the leased aggregate")
	}
	input := ResearchInput{
		GoalID: payload.GoalID, PlanID: payload.PlanID, ConnectionID: payload.ConnectionID,
		PlanRevision: claim.PlanRevision, Prompt: payload.Goal,
	}
	if err := input.Validate(); err != nil {
		return ResearchInput{}, err
	}
	return input, nil
}
