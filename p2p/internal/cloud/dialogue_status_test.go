package cloud

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestReadCloudStatusExposesOnlyDialogueProjection(t *testing.T) {
	module := New(dialogueStatusStore{
		goals: []Goal{{
			GoalID: "goal-1", PlanID: "plan-1", ConnectionID: "connection-1",
			Prompt: "PRIVATE_GOAL_PROMPT_MUST_NOT_REACH_MODEL", Status: GoalStatusResearching,
			Revision: 2, CreatedAt: 100, UpdatedAt: 200,
		}},
		plans: []Plan{{
			PlanID: "plan-1", GoalID: "goal-1", ConnectionID: "connection-1",
			Status: PlanStatusReadyForConfirmation, Title: "Knowledge node", Summary: "Safe reviewed summary",
			RecipeDigest: "sha256:PRIVATE_RECIPE_DIGEST", QuoteID: "quote-1", PlanHash: "PRIVATE_PLAN_HASH",
			Revision: 3, CreatedAt: 100, UpdatedAt: 300,
		}},
		jobs: []Job{{
			JobID: "job-1", PlanID: "plan-1", DeploymentID: "deployment-1", Kind: "verify",
			Execution: "verifying", Outcome: "pending", Checkpoint: "execution_probe_issued",
			ErrorCode: "", Revision: 4, CreatedAt: 100, UpdatedAt: 400,
		}},
		connections: []Connection{{
			ConnectionID: "connection-1", Provider: "aws", AccountID: "123456789012",
			Region: "cn-north-1", Mode: "connection_stack_v2", Status: "active",
			Revision: 5, CreatedAt: 100, UpdatedAt: 500,
		}},
		deployments: []Deployment{{
			DeploymentID: "deployment-1", PlanID: "plan-1", ConnectionID: "connection-1",
			Execution: "verifying", Outcome: "pending", Resource: "retained_tracked",
			Revision: 6, CreatedAt: 100, UpdatedAt: 600,
		}},
		services: []Service{{
			ServiceID: "service-1", DeploymentID: "deployment-1", RecipeID: "recipe-1",
			Name: "Knowledge node", Status: "experimental", Integration: "pending",
			Revision: 7, CreatedAt: 100, UpdatedAt: 700,
		}},
		recipes: []Recipe{{
			RecipeID: "recipe-1", Name: "Knowledge node", Version: "1", Digest: "sha256:PRIVATE_RECIPE_DIGEST",
			Maturity: "experimental", Revision: 8, CreatedAt: 100, UpdatedAt: 800,
		}},
		alerts: []Alert{{
			AlertID: "alert-1", DeploymentID: "deployment-1", ServiceID: "service-1",
			Severity: "warning", Code: "worker_waiting", Message: "MODEL_API_KEY=PRIVATE_ALERT_VALUE",
			Revision: 9, CreatedAt: 100, UpdatedAt: 900,
		}},
	}, Config{})

	status, err := module.ReadCloudStatus(context.Background())
	if err != nil {
		t.Fatalf("ReadCloudStatus: %v", err)
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal dialogue status: %v", err)
	}
	for _, forbidden := range []string{
		"PRIVATE_GOAL_PROMPT_MUST_NOT_REACH_MODEL",
		"connection-1",
		"123456789012",
		"cn-north-1",
		"connection_stack_v2",
		"recipe-1",
		"PRIVATE_RECIPE_DIGEST",
		"PRIVATE_PLAN_HASH",
		"PRIVATE_ALERT_VALUE",
	} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("dialogue status leaked %q: %s", forbidden, encoded)
		}
	}

	plans, ok := status["plans"].([]cloudDialoguePlanStatus)
	if !ok || len(plans) != 1 || plans[0].PlanID != "plan-1" || plans[0].Status != PlanStatusReadyForConfirmation {
		t.Fatalf("dialogue plans = %#v", status["plans"])
	}
	jobs, ok := status["jobs"].([]cloudDialogueJobStatus)
	if !ok || len(jobs) != 1 || jobs[0].Checkpoint != "execution_probe_issued" {
		t.Fatalf("dialogue jobs = %#v", status["jobs"])
	}
	connections, ok := status["connections"].([]cloudDialogueConnectionStatus)
	if !ok || len(connections) != 1 || connections[0].Status != "active" {
		t.Fatalf("dialogue connections = %#v", status["connections"])
	}
	alerts, ok := status["alerts"].([]cloudDialogueAlertStatus)
	if !ok || len(alerts) != 1 || alerts[0].Code != "worker_waiting" {
		t.Fatalf("dialogue alerts = %#v", status["alerts"])
	}
}

func TestBootstrapRetainsOwnerCloudProjectionAfterDialogueStatusIsolation(t *testing.T) {
	module := New(dialogueStatusStore{
		connections: []Connection{{
			ConnectionID: "connection-1", Provider: "aws", AccountID: "123456789012",
			Region: "cn-north-1", Mode: "connection_stack_v2", Status: "active",
			Revision: 1, CreatedAt: 100, UpdatedAt: 100,
		}},
	}, Config{})

	result, actionErr := module.bootstrap(context.Background(), map[string]any{})
	if actionErr != nil {
		t.Fatalf("cloud.bootstrap: %#v", actionErr)
	}
	bootstrap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("cloud.bootstrap result = %#v", result)
	}
	connections, ok := bootstrap["connections"].([]Connection)
	if !ok || len(connections) != 1 || connections[0].AccountID != "123456789012" || connections[0].Region != "cn-north-1" {
		t.Fatalf("owner cloud.bootstrap connections = %#v", bootstrap["connections"])
	}
}

type dialogueStatusStore struct {
	Store
	goals       []Goal
	plans       []Plan
	jobs        []Job
	connections []Connection
	deployments []Deployment
	services    []Service
	recipes     []Recipe
	alerts      []Alert
}

func (s dialogueStatusStore) ListCloudGoals(context.Context) ([]Goal, error) {
	return s.goals, nil
}

func (s dialogueStatusStore) ListCloudPlans(context.Context) ([]Plan, error) {
	return s.plans, nil
}

func (s dialogueStatusStore) ListCloudJobs(context.Context) ([]Job, error) {
	return s.jobs, nil
}

func (s dialogueStatusStore) ListCloudConnections(context.Context) ([]Connection, error) {
	return s.connections, nil
}

func (s dialogueStatusStore) ListCloudDeployments(context.Context) ([]Deployment, error) {
	return s.deployments, nil
}

func (s dialogueStatusStore) ListCloudServices(context.Context) ([]Service, error) {
	return s.services, nil
}

func (s dialogueStatusStore) ListCloudRecipes(context.Context) ([]Recipe, error) {
	return s.recipes, nil
}

func (s dialogueStatusStore) ListCloudAlerts(context.Context) ([]Alert, error) {
	return s.alerts, nil
}
