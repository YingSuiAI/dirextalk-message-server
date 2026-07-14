package cloud

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	"github.com/google/uuid"
)

const (
	actionBootstrap                = "cloud.bootstrap"
	actionConnectionsList          = "cloud.connections.list"
	actionConnectionsGet           = "cloud.connections.get"
	actionPlansList                = "cloud.plans.list"
	actionPlansGet                 = "cloud.plans.get"
	actionDeploymentsList          = "cloud.deployments.list"
	actionDeploymentsGet           = "cloud.deployments.get"
	actionServicesList             = "cloud.services.list"
	actionServicesGet              = "cloud.services.get"
	actionRecipesList              = "cloud.recipes.list"
	actionRecipesGet               = "cloud.recipes.get"
	actionEventsList               = "cloud.events.list"
	actionGoalsCreate              = "cloud.goals.create"
	actionConnectionsRolePlan      = "cloud.connections.role_plan"
	actionPlansApprove             = "cloud.plans.approve"
	actionDeploymentsPairingResume = "cloud.deployments.pairing.resume"
	actionServicesOperationPlan    = "cloud.services.operation.plan"
	actionServicesOperationApprove = "cloud.services.operation.approve"
	actionServicesDestroyPlan      = "cloud.services.destroy.plan"
	actionServicesDestroyApprove   = "cloud.services.destroy.approve"
	cloudUnavailableCode           = "cloud_orchestrator_unavailable"
	cloudIdempotencyInvalidCode    = "cloud_idempotency_key_invalid"
	cloudGoalInvalidCode           = "cloud_goal_invalid"
	cloudConnectionIDInvalidCode   = "cloud_connection_id_invalid"
	cloudConnectionRequiredCode    = "cloud_connection_required"
	cloudInvalidParamsCode         = "cloud_invalid_params"
	cloudIdempotencyConflictCode   = "cloud_idempotency_conflict"
)

type Config struct {
	OwnerMXID func() string
	Now       func() time.Time
	NewID     func(kind string) string
	Publish   func(context.Context, string, string, map[string]any) error
}

type Module struct {
	store Store
	cfg   Config
}

func New(store Store, cfg Config) *Module {
	return &Module{store: store, cfg: cfg}
}

func (m *Module) Handlers() map[string]actionbase.Handler {
	return map[string]actionbase.Handler{
		actionBootstrap:                m.bootstrap,
		actionConnectionsList:          m.connectionsList,
		actionConnectionsGet:           m.connectionsGet,
		actionPlansList:                m.plansList,
		actionPlansGet:                 m.plansGet,
		actionDeploymentsList:          m.deploymentsList,
		actionDeploymentsGet:           m.deploymentsGet,
		actionServicesList:             m.servicesList,
		actionServicesGet:              m.servicesGet,
		actionRecipesList:              m.recipesList,
		actionRecipesGet:               m.recipesGet,
		actionEventsList:               m.eventsList,
		actionGoalsCreate:              m.createGoal,
		actionConnectionsRolePlan:      m.unavailableWrite,
		actionPlansApprove:             m.unavailableWrite,
		actionDeploymentsPairingResume: m.unavailableWrite,
		actionServicesOperationPlan:    m.unavailableWrite,
		actionServicesOperationApprove: m.unavailableWrite,
		actionServicesDestroyPlan:      m.unavailableWrite,
		actionServicesDestroyApprove:   m.unavailableWrite,
	}
}

// CreateResearchGoal is the only Native Agent-facing entrypoint. It keeps the
// Eino runtime on the same validated, idempotent ProductCore path as an owner
// request, while deliberately exposing no approval or cloud mutation method.
func (m *Module) CreateResearchGoal(ctx context.Context, goal, connectionID, idempotencyKey string) (map[string]any, error) {
	result, actionErr := m.createGoal(ctx, map[string]any{
		"goal":                goal,
		"cloud_connection_id": connectionID,
		"idempotency_key":     idempotencyKey,
	})
	if actionErr != nil {
		return nil, fmt.Errorf("%s", actionErr.Error)
	}
	response, ok := result.(map[string]any)
	if !ok {
		return nil, errors.New("cloud research goal returned an invalid response")
	}
	return response, nil
}

// ReadCloudStatus is the narrow read-only Agent port. The returned snapshot is
// the same de-secretsed projection an owner receives from cloud.bootstrap; it
// never includes private goal prompts, outbox payloads, secret values, or
// pairing material.
func (m *Module) ReadCloudStatus(ctx context.Context) (map[string]any, error) {
	result, actionErr := m.bootstrap(ctx, map[string]any{})
	if actionErr != nil {
		return nil, fmt.Errorf("%s", actionErr.Error)
	}
	response, ok := result.(map[string]any)
	if !ok {
		return nil, errors.New("cloud status returned an invalid response")
	}
	return response, nil
}

func (m *Module) createGoal(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "goal", "cloud_connection_id", "idempotency_key"); err != nil {
		return nil, err
	}
	values := actionbase.Params(params)
	goalText := values.String("goal")
	if count := utf8.RuneCountInString(goalText); count == 0 || count > 12000 || strings.IndexByte(goalText, 0) >= 0 {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudGoalInvalidCode, "goal must contain 1 to 12000 characters")
	}
	if ContainsSensitiveGoalMaterial(goalText) {
		return nil, actionbase.CodedError(http.StatusBadRequest, "cloud_goal_secret_not_allowed", "cloud goal must use a secret_ref instead of secret material")
	}
	idempotencyKey := values.String("idempotency_key")
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	connectionID := values.String("cloud_connection_id")
	if len(connectionID) > 128 || strings.ContainsAny(connectionID, "\r\n\t") || strings.IndexByte(connectionID, 0) >= 0 {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionIDInvalidCode, "cloud_connection_id is invalid")
	}
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	if connectionID == "" {
		// A reviewable QuoteV1 and PlanV1 bind one immutable Cloud Connection.
		// Do not create a private outbox row that no compliant Orchestrator can
		// ever claim. A future waiting_connection + attach flow must be a
		// separate, revisioned state transition rather than an implicit retry.
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionRequiredCode, "cloud_connection_id is required before research can start")
	}
	_, found, err := m.store.GetCloudConnection(ctx, connectionID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !found {
		return nil, actionbase.CodedError(http.StatusNotFound, "cloud_connection_not_found", "cloud connection was not found")
	}
	ownerMXID := ""
	if m.cfg.OwnerMXID != nil {
		ownerMXID = strings.TrimSpace(m.cfg.OwnerMXID())
	}
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UnixMilli()
	goalID := m.newID("goal")
	planID := m.newID("plan")
	outboxID := m.newID("outbox")
	jobID := ResearchJobID(outboxID)
	goal := Goal{
		GoalID: goalID, OwnerMXID: ownerMXID, Prompt: goalText, ConnectionID: connectionID,
		PlanID: planID, Status: GoalStatusResearching,
		IdempotencyHash: digest(idempotencyKey), RequestDigest: digestFields(goalText, connectionID),
		Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	plan := Plan{
		PlanID: planID, GoalID: goalID, ConnectionID: connectionID, Status: PlanStatusResearching,
		Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	job := Job{
		JobID: jobID, PlanID: planID, Kind: "research", Execution: "queued", Outcome: "pending",
		Checkpoint: "research_queued", Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	payload, err := json.Marshal(map[string]string{
		"goal_id": goalID, "plan_id": planID, "cloud_connection_id": connectionID, "goal": goalText,
	})
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	request := CreateGoalRequest{
		Goal: goal,
		Plan: plan,
		Job:  job,
		Events: []Event{
			{EventID: m.newID("event"), Type: "cloud.goal.changed", AggregateType: "goal", AggregateID: goalID, Revision: 1, SummaryJSON: mustJSON(goal.Summary()), CreatedAt: now},
			{EventID: m.newID("event"), Type: "cloud.plan.changed", AggregateType: "plan", AggregateID: planID, Revision: 1, SummaryJSON: mustJSON(plan), CreatedAt: now},
			{EventID: m.newID("event"), Type: "cloud.job.changed", AggregateType: "job", AggregateID: jobID, Revision: 1, SummaryJSON: mustJSON(job), CreatedAt: now},
		},
		Outbox: OutboxEntry{
			OutboxID: outboxID, Kind: OutboxKindResearchGoalRequested,
			AggregateType: "goal", AggregateID: goalID, PayloadJSON: string(payload), CreatedAt: now,
		},
	}
	created, err := m.store.CreateCloudGoal(ctx, request)
	if err != nil {
		if err == ErrIdempotencyConflict {
			return nil, actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud goal")
		}
		return nil, actionbase.InternalError(err)
	}
	if created.Created {
		for _, event := range request.Events {
			switch event.Type {
			case "cloud.goal.changed":
				m.publish(ctx, event.Type, event.EventID, goalPayload(created.Goal.Summary()))
			case "cloud.plan.changed":
				m.publish(ctx, event.Type, event.EventID, planPayload(created.Plan))
			case "cloud.job.changed":
				m.publish(ctx, event.Type, event.EventID, jobPayload(job))
			}
		}
	}
	return map[string]any{"goal": created.Goal.Summary(), "plan": created.Plan}, nil
}

func (m *Module) bootstrap(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params); err != nil {
		return nil, err
	}
	goals, err := m.store.ListCloudGoals(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	plans, err := m.store.ListCloudPlans(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	plans = planSummaries(plans)
	jobs, err := m.store.ListCloudJobs(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	connections, err := m.store.ListCloudConnections(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	deployments, err := m.store.ListCloudDeployments(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	services, err := m.store.ListCloudServices(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	recipes, err := m.store.ListCloudRecipes(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	alerts, err := m.store.ListCloudAlerts(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	summaries := make([]GoalSummary, 0, len(goals))
	for _, goal := range goals {
		summaries = append(summaries, goal.Summary())
	}
	return map[string]any{
		"synced_at": time.Now().UTC().Format(time.RFC3339Nano), "goals": summaries, "plans": plans, "jobs": jobs,
		"connections": connections, "deployments": deployments, "services": services, "recipes": recipes, "alerts": alerts,
	}, nil
}

func (m *Module) connectionsList(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params); err != nil {
		return nil, err
	}
	items, err := m.store.ListCloudConnections(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"connections": items}, nil
}

func (m *Module) connectionsGet(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "cloud_connection_id"); err != nil {
		return nil, err
	}
	id := actionbase.Params(params).String("cloud_connection_id")
	if id == "" {
		return nil, actionbase.BadRequest("cloud_connection_id is required")
	}
	item, ok, err := m.store.GetCloudConnection(ctx, id)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.CodedError(http.StatusNotFound, "cloud_connection_not_found", "cloud connection was not found")
	}
	return item, nil
}

func (m *Module) plansList(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params); err != nil {
		return nil, err
	}
	items, err := m.store.ListCloudPlans(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"plans": planSummaries(items)}, nil
}

func (m *Module) plansGet(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "plan_id"); err != nil {
		return nil, err
	}
	id := actionbase.Params(params).String("plan_id")
	if id == "" {
		return nil, actionbase.BadRequest("plan_id is required")
	}
	item, ok, err := m.store.GetCloudPlan(ctx, id)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.CodedError(http.StatusNotFound, "cloud_plan_not_found", "cloud plan was not found")
	}
	// A store only persists the quote ID on plans. Clear any implementation
	// supplied detail first so this is the sole projection path that can attach
	// a quote, and only when the immutable binding exists.
	item.Quote = nil
	if item.QuoteID != "" {
		quote, found, err := m.store.GetCloudQuote(ctx, item.QuoteID)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		if found {
			if quote.QuoteID != item.QuoteID || quote.ConnectionID != item.ConnectionID {
				return nil, actionbase.InternalError(errors.New("cloud quote does not match plan"))
			}
			item.Quote = &quote
		}
	}
	return item, nil
}

func (m *Module) deploymentsList(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params); err != nil {
		return nil, err
	}
	items, err := m.store.ListCloudDeployments(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"deployments": items}, nil
}

func (m *Module) deploymentsGet(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "deployment_id"); err != nil {
		return nil, err
	}
	id := actionbase.Params(params).String("deployment_id")
	if id == "" {
		return nil, actionbase.BadRequest("deployment_id is required")
	}
	item, ok, err := m.store.GetCloudDeployment(ctx, id)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.CodedError(http.StatusNotFound, "cloud_deployment_not_found", "cloud deployment was not found")
	}
	return item, nil
}

func (m *Module) servicesList(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params); err != nil {
		return nil, err
	}
	items, err := m.store.ListCloudServices(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"services": items}, nil
}

func (m *Module) servicesGet(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id"); err != nil {
		return nil, err
	}
	id := actionbase.Params(params).String("service_id")
	if id == "" {
		return nil, actionbase.BadRequest("service_id is required")
	}
	item, ok, err := m.store.GetCloudService(ctx, id)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.CodedError(http.StatusNotFound, "cloud_service_not_found", "cloud service was not found")
	}
	return item, nil
}

func (m *Module) recipesList(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params); err != nil {
		return nil, err
	}
	items, err := m.store.ListCloudRecipes(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"recipes": items}, nil
}

func (m *Module) recipesGet(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "recipe_id"); err != nil {
		return nil, err
	}
	id := actionbase.Params(params).String("recipe_id")
	if id == "" {
		return nil, actionbase.BadRequest("recipe_id is required")
	}
	item, ok, err := m.store.GetCloudRecipe(ctx, id)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.CodedError(http.StatusNotFound, "cloud_recipe_not_found", "cloud recipe was not found")
	}
	return item, nil
}

func (m *Module) eventsList(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "limit"); err != nil {
		return nil, err
	}
	limit := actionbase.Params(params).Int64("limit")
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	items, err := m.store.ListCloudEvents(ctx, int(limit))
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"events": items}, nil
}

func (m *Module) unavailableWrite(_ context.Context, _ map[string]any) (any, *actionbase.Error) {
	return nil, unavailableError()
}

func (m *Module) publish(ctx context.Context, eventType, cloudEventID string, payload map[string]any) {
	if m != nil && m.cfg.Publish != nil {
		_ = m.cfg.Publish(ctx, eventType, cloudEventID, payload)
	}
}

func (m *Module) now() time.Time {
	if m != nil && m.cfg.Now != nil {
		return m.cfg.Now().UTC()
	}
	return time.Now().UTC()
}

func (m *Module) newID(kind string) string {
	if m != nil && m.cfg.NewID != nil {
		return m.cfg.NewID(kind)
	}
	return "cloud_" + kind + "_" + uuid.NewString()
}

func only(params map[string]any, allowed ...string) *actionbase.Error {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key := range params {
		if _, ok := allowedSet[key]; !ok {
			return actionbase.CodedError(http.StatusBadRequest, cloudInvalidParamsCode, "cloud action received an unsupported parameter")
		}
	}
	return nil
}

func unavailableError() *actionbase.Error {
	return actionbase.CodedError(http.StatusServiceUnavailable, cloudUnavailableCode, "cloud orchestrator is not configured")
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// digestFields keeps the idempotency comparison structurally unambiguous.
// It is deliberately length-prefixed instead of concatenating user-controlled
// fields with a delimiter.
func digestFields(values ...string) string {
	hash := sha256.New()
	var length [8]byte
	for _, value := range values {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func mustJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

func goalPayload(summary GoalSummary) map[string]any {
	return map[string]any{
		"goal_id": summary.GoalID, "plan_id": summary.PlanID, "cloud_connection_id": summary.ConnectionID,
		"status": summary.Status, "revision": summary.Revision, "created_at": summary.CreatedAt, "updated_at": summary.UpdatedAt,
	}
}

func planPayload(plan Plan) map[string]any {
	return map[string]any{
		"plan_id": plan.PlanID, "goal_id": plan.GoalID, "cloud_connection_id": plan.ConnectionID,
		"status": plan.Status, "title": plan.Title, "summary": plan.Summary, "recipe_digest": plan.RecipeDigest,
		"quote_id": plan.QuoteID, "plan_hash": plan.PlanHash, "revision": plan.Revision, "updated_at": plan.UpdatedAt,
		"created_at": plan.CreatedAt,
	}
}

func planSummaries(plans []Plan) []Plan {
	for index := range plans {
		plans[index].Quote = nil
	}
	return plans
}

func jobPayload(job Job) map[string]any {
	return map[string]any{
		"job_id": job.JobID, "plan_id": job.PlanID, "deployment_id": job.DeploymentID,
		"kind": job.Kind, "execution_status": job.Execution, "outcome_status": job.Outcome,
		"checkpoint": job.Checkpoint, "error_code": job.ErrorCode, "revision": job.Revision,
		"created_at": job.CreatedAt, "updated_at": job.UpdatedAt,
	}
}
