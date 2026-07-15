package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/serviceapi"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
	"github.com/google/uuid"
)

const cloudTestConnectionID = "connection-test-1"

type cloudConnectedMemoryStore struct {
	*p2pstorage.MemoryStore
	connection cloudmodule.Connection
}

type cloudQuoteMemoryStore struct {
	*p2pstorage.MemoryStore
	plan       cloudmodule.Plan
	quote      cloudmodule.QuoteView
	quoteFound bool
	quoteCalls int
}

func (s *cloudQuoteMemoryStore) GetCloudPlan(_ context.Context, id string) (cloudmodule.Plan, bool, error) {
	if s != nil && id == s.plan.PlanID {
		return s.plan, true, nil
	}
	return cloudmodule.Plan{}, false, nil
}

func (s *cloudQuoteMemoryStore) ListCloudPlans(_ context.Context) ([]cloudmodule.Plan, error) {
	if s == nil || s.plan.PlanID == "" {
		return []cloudmodule.Plan{}, nil
	}
	return []cloudmodule.Plan{s.plan}, nil
}

func (s *cloudQuoteMemoryStore) GetCloudQuote(_ context.Context, id string) (cloudmodule.QuoteView, bool, error) {
	if s != nil {
		s.quoteCalls++
		if s.quoteFound && id == s.quote.QuoteID {
			return s.quote, true, nil
		}
	}
	return cloudmodule.QuoteView{}, false, nil
}

func (s *cloudConnectedMemoryStore) GetCloudConnection(_ context.Context, id string) (cloudmodule.Connection, bool, error) {
	if s != nil && id == s.connection.ConnectionID {
		return s.connection, true, nil
	}
	return cloudmodule.Connection{}, false, nil
}

func (s *cloudConnectedMemoryStore) ListCloudConnections(_ context.Context) ([]cloudmodule.Connection, error) {
	if s == nil || s.connection.ConnectionID == "" {
		return []cloudmodule.Connection{}, nil
	}
	return []cloudmodule.Connection{s.connection}, nil
}

func newCloudConnectedService(t *testing.T) *Service {
	t.Helper()
	store := &cloudConnectedMemoryStore{
		MemoryStore: p2pstorage.NewMemoryStore(),
		connection: cloudmodule.Connection{
			ConnectionID: cloudTestConnectionID,
			Provider:     "aws",
			Mode:         "role",
			Status:       "active",
			Revision:     1,
			CreatedAt:    1,
			UpdatedAt:    1,
		},
	}
	service, err := NewServiceWithStore(context.Background(), Config{ServerName: "example.com"}, store)
	if err != nil {
		t.Fatalf("new cloud-connected service: %v", err)
	}
	return service
}

func TestNativeCloudPlannerPortCreatesOnlyOneResearchGoal(t *testing.T) {
	service := newCloudConnectedService(t)
	planner := serviceNativeCloudPlannerPort{service: service}
	requestKey := uuid.NewString()

	first, err := planner.CreateResearchGoal(context.Background(), "Deploy a private knowledge node after a reviewed plan.", cloudTestConnectionID, requestKey)
	if err != nil {
		t.Fatalf("create native cloud research goal: %v", err)
	}
	second, err := planner.CreateResearchGoal(context.Background(), "Deploy a private knowledge node after a reviewed plan.", cloudTestConnectionID, requestKey)
	if err != nil {
		t.Fatalf("replay native cloud research goal: %v", err)
	}
	third, err := planner.CreateResearchGoal(context.Background(), "Deploy a private knowledge node after a reviewed plan.", cloudTestConnectionID, uuid.NewString())
	if err != nil {
		t.Fatalf("new native cloud research goal: %v", err)
	}
	firstGoal := first["goal"].(cloudmodule.GoalSummary)
	firstPlan := first["plan"].(cloudmodule.Plan)
	secondGoal := second["goal"].(cloudmodule.GoalSummary)
	if firstGoal.GoalID != secondGoal.GoalID || firstPlan.Status != "researching" {
		t.Fatalf("native planner must be idempotent and research-only: first=%#v second=%#v", first, second)
	}
	thirdGoal := third["goal"].(cloudmodule.GoalSummary)
	if thirdGoal.GoalID == firstGoal.GoalID {
		t.Fatalf("a distinct native agent request must create a new research goal: first=%#v third=%#v", first, third)
	}
	events := mustListP2PEvents(t, service)
	if len(events) != 6 || events[0].Type != "cloud.goal.changed" || events[1].Type != "cloud.plan.changed" || events[2].Type != "cloud.job.changed" || events[3].Type != "cloud.goal.changed" || events[4].Type != "cloud.plan.changed" || events[5].Type != "cloud.job.changed" {
		t.Fatalf("native planner must project the queued research job with each goal and plan: %#v", events)
	}
}

func TestServiceCloudProjectionRelayPublishesAndStopsWithLifecycleContext(t *testing.T) {
	store := &serviceProjectionStore{
		MemoryStore: p2pstorage.NewMemoryStore(),
		claim: cloudmodule.ProjectionClaim{
			ProjectionID: "projection-1", CloudEventID: "cloud-event-1", Type: "cloud.goal.changed",
			PayloadJSON: `{"goal_id":"goal-1","plan_id":"plan-1","cloud_connection_id":"connection-1","status":"researching","revision":1,"created_at":1,"updated_at":1}`,
		},
		completed: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service, err := NewServiceWithStore(ctx, Config{ServerName: "example.com"}, store)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- service.RunCloudProjectionRelay(ctx) }()
	select {
	case <-store.completed:
	case <-time.After(time.Second):
		t.Fatal("cloud projection relay did not settle the available projection")
	}
	events := mustListP2PEvents(t, service)
	if len(events) != 1 || events[0].Type != "cloud.goal.changed" || events[0].DedupeKey != "cloud-event:cloud-event-1" || events[0].Payload["goal"] != nil {
		t.Fatalf("cloud relay projection = %#v", events)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cloud projection relay stopped with %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cloud projection relay did not stop with its lifecycle context")
	}
}

type serviceProjectionStore struct {
	*p2pstorage.MemoryStore
	mu        sync.Mutex
	claim     cloudmodule.ProjectionClaim
	claimed   bool
	completed chan struct{}
}

func (s *serviceProjectionStore) ClaimCloudProjection(_ context.Context, _ string, _ time.Duration, token string) (cloudmodule.ProjectionClaim, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed {
		return cloudmodule.ProjectionClaim{}, false, nil
	}
	s.claimed = true
	claim := s.claim
	claim.LeaseToken = token
	return claim, true, nil
}

func (s *serviceProjectionStore) CompleteCloudProjection(_ context.Context, _ cloudmodule.ProjectionClaim) error {
	select {
	case <-s.completed:
	default:
		close(s.completed)
	}
	return nil
}

func (s *serviceProjectionStore) DeferCloudProjection(_ context.Context, _ cloudmodule.ProjectionClaim, _ string, _ time.Time) error {
	return nil
}

func (s *serviceProjectionStore) RejectCloudProjection(_ context.Context, _ cloudmodule.ProjectionClaim, _ string) error {
	return nil
}

func TestCloudGoalCreateRejectsSecretMaterialBeforePersistence(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	request := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "cloud.goals.create",
		"params": map[string]any{
			"goal":            "Deploy the service; AWS_SECRET_ACCESS_KEY=not-a-real-secret-value",
			"idempotency_key": uuid.NewString(),
		},
	})
	request.Header.Set("Authorization", "Bearer "+service.AccessToken())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("secret-bearing cloud goal = %d body=%s", recorder.Code, recorder.Body.String())
	}
	response := decodeJSONMap(t, recorder.Body.String())
	if response["code"] != "cloud_goal_secret_not_allowed" {
		t.Fatalf("secret-bearing cloud goal error = %#v", response)
	}
	if events := mustListP2PEvents(t, service); len(events) != 0 {
		t.Fatalf("secret-bearing goal must not emit events: %#v", events)
	}
}

func TestCloudGoalCreateRejectsNullBytesBeforePersistence(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	request := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "cloud.goals.create",
		"params": map[string]any{
			"goal":            "Deploy a private service\x00with an invalid delimiter.",
			"idempotency_key": uuid.NewString(),
		},
	})
	request.Header.Set("Authorization", "Bearer "+service.AccessToken())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("null-bearing cloud goal = %d body=%s", recorder.Code, recorder.Body.String())
	}
	response := decodeJSONMap(t, recorder.Body.String())
	if response["code"] != "cloud_goal_invalid" {
		t.Fatalf("null-bearing cloud goal error = %#v", response)
	}
	if events := mustListP2PEvents(t, service); len(events) != 0 {
		t.Fatalf("null-bearing goal must not emit events: %#v", events)
	}
}

func TestCloudGoalCreateRequiresAnExistingConnectionBeforePersistingResearch(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	request := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "cloud.goals.create",
		"params": map[string]any{
			"goal":            "Deploy a private knowledge service with a reviewable recipe.",
			"idempotency_key": uuid.NewString(),
		},
	})
	request.Header.Set("Authorization", "Bearer "+service.AccessToken())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("unbound cloud goal = %d body=%s", recorder.Code, recorder.Body.String())
	}
	response := decodeJSONMap(t, recorder.Body.String())
	if response["code"] != "cloud_connection_required" {
		t.Fatalf("unbound cloud goal error = %#v", response)
	}
	if events := mustListP2PEvents(t, service); len(events) != 0 {
		t.Fatalf("unbound goal must not emit events: %#v", events)
	}
}

func TestCloudGoalCreateIsOwnerOnlyIdempotentAndProjectsResearchPlan(t *testing.T) {
	service := newCloudConnectedService(t)
	router := newP2PTestRouter(service)
	idempotencyKey := uuid.NewString()
	body := map[string]any{
		"action": "cloud.goals.create",
		"params": map[string]any{
			"goal":                "Deploy a private knowledge service with a reviewable recipe.",
			"cloud_connection_id": cloudTestConnectionID,
			"idempotency_key":     idempotencyKey,
		},
	}

	first := jsonRequest(t, "/_p2p/command", body)
	first.Header.Set("Authorization", "Bearer "+service.AccessToken())
	firstRec := httptest.NewRecorder()
	router.ServeHTTP(firstRec, first)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("goal create = %d body=%s", firstRec.Code, firstRec.Body.String())
	}
	firstResult := decodeJSONMap(t, firstRec.Body.String())
	goal, ok := firstResult["goal"].(map[string]any)
	if !ok || goal["status"] != "researching" || goal["goal_id"] == "" {
		t.Fatalf("goal result = %#v", firstResult)
	}
	plan, ok := firstResult["plan"].(map[string]any)
	if !ok || plan["status"] != "researching" || plan["plan_id"] == "" || plan["goal_id"] != goal["goal_id"] {
		t.Fatalf("plan result = %#v", firstResult)
	}

	replay := jsonRequest(t, "/_p2p/command", body)
	replay.Header.Set("Authorization", "Bearer "+service.AccessToken())
	replayRec := httptest.NewRecorder()
	router.ServeHTTP(replayRec, replay)
	if replayRec.Code != http.StatusOK {
		t.Fatalf("goal replay = %d body=%s", replayRec.Code, replayRec.Body.String())
	}
	replayResult := decodeJSONMap(t, replayRec.Body.String())
	replayGoal := replayResult["goal"].(map[string]any)
	replayPlan := replayResult["plan"].(map[string]any)
	if replayGoal["goal_id"] != goal["goal_id"] || replayPlan["plan_id"] != plan["plan_id"] {
		t.Fatalf("idempotent replay created a different cloud request: first=%#v replay=%#v", firstResult, replayResult)
	}

	bootstrap := jsonRequest(t, "/_p2p/query", map[string]any{"action": "cloud.bootstrap", "params": map[string]any{}})
	bootstrap.Header.Set("Authorization", "Bearer "+service.AccessToken())
	bootstrapRec := httptest.NewRecorder()
	router.ServeHTTP(bootstrapRec, bootstrap)
	if bootstrapRec.Code != http.StatusOK {
		t.Fatalf("cloud bootstrap = %d body=%s", bootstrapRec.Code, bootstrapRec.Body.String())
	}
	var snapshot map[string]any
	if err := json.Unmarshal(bootstrapRec.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	plans, ok := snapshot["plans"].([]any)
	if !ok || len(plans) != 1 || plans[0].(map[string]any)["plan_id"] != plan["plan_id"] {
		t.Fatalf("cloud bootstrap plans = %#v", snapshot["plans"])
	}
	jobs, ok := snapshot["jobs"].([]any)
	if !ok || len(jobs) != 1 || jobs[0].(map[string]any)["plan_id"] != plan["plan_id"] || jobs[0].(map[string]any)["execution_status"] != "queued" || jobs[0].(map[string]any)["outcome_status"] != "pending" {
		t.Fatalf("cloud bootstrap queued research job = %#v", snapshot["jobs"])
	}

	events := mustListP2PEvents(t, service)
	if len(events) != 3 || events[0].Type != "cloud.goal.changed" || events[1].Type != "cloud.plan.changed" || events[2].Type != "cloud.job.changed" {
		t.Fatalf("cloud create must emit non-secret goal, plan, and queued-job projections, got %#v", events)
	}
	if _, leaked := events[0].Payload["goal"]; leaked {
		t.Fatalf("goal prompt must not be copied into realtime event payload: %#v", events[0])
	}
}

func TestCloudPlansGetHydratesOnlySafeQuoteDetail(t *testing.T) {
	quotedAt := time.Date(2026, time.July, 14, 8, 0, 0, 0, time.UTC)
	store := &cloudQuoteMemoryStore{
		MemoryStore: p2pstorage.NewMemoryStore(),
		plan: cloudmodule.Plan{
			PlanID: "plan-quote-1", GoalID: "goal-quote-1", ConnectionID: cloudTestConnectionID,
			Status: cloudmodule.PlanStatusReadyForConfirmation, QuoteID: "quote-quote-1", Revision: 2,
			CreatedAt: 1, UpdatedAt: 2,
			// Detail supplied by a store must never leak through list/bootstrap;
			// only plans.get is allowed to attach the fetched QuoteView below.
			Quote: &cloudmodule.QuoteView{QuoteID: "must-not-appear"},
		},
		quoteFound: true,
		quote: cloudmodule.QuoteView{
			QuoteID: "quote-quote-1", ConnectionID: cloudTestConnectionID, Region: "ap-south-1", Currency: "USD",
			QuotedAt: quotedAt, ValidUntil: quotedAt.Add(15 * time.Minute),
			Candidates: []cloudmodule.QuoteCandidateView{{
				Tier: "recommended", InstanceType: "m7i.xlarge", PurchaseOption: "on_demand",
				HourlyMinor: 2000, ThirtyDayMinor: 1440000, StartupUpperMinor: 500, EstimatedDiskGiB: 80,
				AvailabilityZones: []string{"ap-south-1a"},
			}},
			IncludedItems: []string{"ec2_linux_ondemand"}, UnincludedItems: []string{"ebs_gp3", "taxes"},
		},
	}
	service, err := NewServiceWithStore(context.Background(), Config{ServerName: "example.com"}, store)
	if err != nil {
		t.Fatal(err)
	}
	router := newP2PTestRouter(service)

	get := jsonRequest(t, "/_p2p/query", map[string]any{"action": "cloud.plans.get", "params": map[string]any{"plan_id": store.plan.PlanID}})
	get.Header.Set("Authorization", "Bearer "+service.AccessToken())
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, get)
	if getRec.Code != http.StatusOK {
		t.Fatalf("cloud plans.get = %d body=%s", getRec.Code, getRec.Body.String())
	}
	getResult := decodeJSONMap(t, getRec.Body.String())
	quote, ok := getResult["quote"].(map[string]any)
	if !ok || quote["quote_id"] != store.quote.QuoteID || quote["cloud_connection_id"] != cloudTestConnectionID || quote["region"] != "ap-south-1" || quote["currency"] != "USD" {
		t.Fatalf("cloud plans.get quote = %#v", getResult)
	}
	candidates, ok := quote["candidates"].([]any)
	if !ok || len(candidates) != 1 {
		t.Fatalf("cloud plans.get candidates = %#v", quote)
	}
	candidate, ok := candidates[0].(map[string]any)
	if !ok || candidate["tier"] != "recommended" || candidate["instance_type"] != "m7i.xlarge" || candidate["hourly_minor"] != float64(2000) {
		t.Fatalf("cloud plans.get candidate = %#v", candidates[0])
	}
	for _, forbidden := range []string{"candidate_id", "schema_version", "command_id", "request_sha256", "receipt", "envelope", "endpoint", "key", "secret"} {
		if _, exists := quote[forbidden]; exists {
			t.Fatalf("cloud plans.get leaked %q in quote: %#v", forbidden, quote)
		}
		if strings.Contains(getRec.Body.String(), forbidden) {
			t.Fatalf("cloud plans.get body leaked %q: %s", forbidden, getRec.Body.String())
		}
	}
	if store.quoteCalls != 1 {
		t.Fatalf("cloud plans.get quote reads = %d, want 1", store.quoteCalls)
	}

	for _, action := range []string{"cloud.plans.list", "cloud.bootstrap"} {
		request := jsonRequest(t, "/_p2p/query", map[string]any{"action": action, "params": map[string]any{}})
		request.Header.Set("Authorization", "Bearer "+service.AccessToken())
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("%s = %d body=%s", action, recorder.Code, recorder.Body.String())
		}
		response := decodeJSONMap(t, recorder.Body.String())
		plans, ok := response["plans"].([]any)
		if !ok || len(plans) != 1 {
			t.Fatalf("%s plans = %#v", action, response["plans"])
		}
		if _, leaked := plans[0].(map[string]any)["quote"]; leaked {
			t.Fatalf("%s must not include plan quote detail: %#v", action, plans[0])
		}
	}
	if store.quoteCalls != 1 {
		t.Fatalf("list/bootstrap must not hydrate quote detail, reads=%d", store.quoteCalls)
	}
}

func TestCloudActionsAreOwnerScopedAndWritesAreHTTPOnly(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	for _, action := range []string{
		"cloud.bootstrap", "cloud.connections.list", "cloud.connections.get", "cloud.plans.list", "cloud.plans.get",
		"cloud.deployments.list", "cloud.deployments.get", "cloud.services.list", "cloud.services.get",
		"cloud.recipes.list", "cloud.recipes.get", "cloud.events.list",
	} {
		spec, ok := serviceapi.ActionSpecFor(action)
		if !ok || spec.Auth != serviceapi.ActionAuthOwner || spec.Transport != serviceapi.ActionTransportHTTPAndWS {
			t.Fatalf("read action %s metadata = %#v, present=%v", action, spec, ok)
		}
	}
	for _, action := range []string{
		"cloud.goals.create", "cloud.connections.role_plan", "cloud.connections.registration.complete", "cloud.plans.confirmation.prepare", "cloud.plans.approve", "cloud.deployments.recipe_execution.confirmation.prepare", "cloud.deployments.recipe_execution.approve", "cloud.secrets.bootstrap.plan", "cloud.deployments.pairing.resume",
		"cloud.services.operation.plan", "cloud.services.operation.approve", "cloud.services.destroy.plan", "cloud.services.destroy.approve",
	} {
		spec, ok := serviceapi.ActionSpecFor(action)
		if !ok || spec.Auth != serviceapi.ActionAuthOwner || spec.Transport != serviceapi.ActionTransportHTTPOnly {
			t.Fatalf("write action %s metadata = %#v, present=%v", action, spec, ok)
		}
		if serviceapi.RealtimeWSClientRequestAction(action) {
			t.Fatalf("high-risk cloud action %s must not be callable through client.request", action)
		}
	}

	agentRequest := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "cloud.goals.create",
		"params": map[string]any{
			"goal":            "must not be accepted from agent token",
			"idempotency_key": uuid.NewString(),
		},
	})
	agentRequest.Header.Set("Authorization", "Bearer "+service.AgentToken())
	agentRec := httptest.NewRecorder()
	router.ServeHTTP(agentRec, agentRequest)
	if agentRec.Code != http.StatusUnauthorized {
		t.Fatalf("agent token must not create cloud goals, got %d body=%s", agentRec.Code, agentRec.Body.String())
	}
}
