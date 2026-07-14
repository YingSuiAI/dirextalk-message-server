package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/serviceapi"
	"github.com/google/uuid"
)

func TestNativeCloudPlannerPortCreatesOnlyOneResearchGoal(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	planner := serviceNativeCloudPlannerPort{service: service}
	requestKey := uuid.NewString()

	first, err := planner.CreateResearchGoal(context.Background(), "Deploy a private knowledge node after a reviewed plan.", "", requestKey)
	if err != nil {
		t.Fatalf("create native cloud research goal: %v", err)
	}
	second, err := planner.CreateResearchGoal(context.Background(), "Deploy a private knowledge node after a reviewed plan.", "", requestKey)
	if err != nil {
		t.Fatalf("replay native cloud research goal: %v", err)
	}
	third, err := planner.CreateResearchGoal(context.Background(), "Deploy a private knowledge node after a reviewed plan.", "", uuid.NewString())
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
	if len(events) != 4 || events[0].Type != "cloud.goal.changed" || events[1].Type != "cloud.plan.changed" || events[2].Type != "cloud.goal.changed" || events[3].Type != "cloud.plan.changed" {
		t.Fatalf("native planner must only project goal and plan research events: %#v", events)
	}
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

func TestCloudGoalCreateIsOwnerOnlyIdempotentAndProjectsResearchPlan(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	idempotencyKey := uuid.NewString()
	body := map[string]any{
		"action": "cloud.goals.create",
		"params": map[string]any{
			"goal":            "Deploy a private knowledge service with a reviewable recipe.",
			"idempotency_key": idempotencyKey,
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

	events := mustListP2PEvents(t, service)
	if len(events) != 2 || events[0].Type != "cloud.goal.changed" || events[1].Type != "cloud.plan.changed" {
		t.Fatalf("cloud create must emit non-secret entity projections, got %#v", events)
	}
	if _, leaked := events[0].Payload["goal"]; leaked {
		t.Fatalf("goal prompt must not be copied into realtime event payload: %#v", events[0])
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
		"cloud.goals.create", "cloud.connections.role_plan", "cloud.plans.approve", "cloud.deployments.pairing.resume",
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
