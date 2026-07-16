package cloud

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAgentCloudGoalAdapterReturnsDurableTaskWithoutInventingPlan(t *testing.T) {
	connectionID, key := uuid.NewString(), uuid.NewString()
	goal := "Deploy a private knowledge service with a reviewable recipe."
	client := &agentControlModuleClient{goalResult: validAgentGoalModuleResult(connectionID, goal)}
	module := New(nil, Config{OwnerMXID: func() string { return "@owner:example.com" }, AgentCloudControlClient: client})
	first, apiErr := module.createAgentCloudGoal(t.Context(), goal, connectionID, key)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	result := first.(map[string]any)
	task, taskOK := result["task"].(map[string]any)
	planning, planningOK := result["planning"].(map[string]any)
	if !taskOK || !planningOK || task["task_id"] != planning["task_id"] || task["owner_id"] != planning["owner_id"] ||
		planning["owner_id"] != client.goalResult.Planning.OwnerID || planning["state"] != AgentCloudGoalPlanningResearchQueued ||
		planning["cloud_connection_id"] != connectionID || client.goalRequest.IdempotencyKey != key || client.goalRequest.Goal != goal ||
		client.goalRequest.RecipeID != "" || client.goalRequest.RetentionPolicy != AgentCloudGoalRetentionEphemeralAutoDestroy {
		t.Fatalf("Agent Goal result=%#v request=%#v", result, client.goalRequest)
	}
	if _, exists := result["plan"]; exists {
		t.Fatal("initial Agent Goal response invented a Plan")
	}
	if _, exists := planning["related_plan_id"]; exists {
		t.Fatal("empty related_plan_id was exposed as a Plan correlation")
	}
	if _, apiErr = module.createAgentCloudGoal(t.Context(), goal, connectionID, key); apiErr != nil || client.goalCalls != 2 {
		t.Fatalf("idempotent replay err=%v calls=%d", apiErr, client.goalCalls)
	}
}

func TestAgentCloudGoalAdapterFailsClosedForInvalidAgentResults(t *testing.T) {
	connectionID, key := uuid.NewString(), uuid.NewString()
	goal := "Compile and test the project on temporary compute."
	client := &agentControlModuleClient{goalResult: validAgentGoalModuleResult(connectionID, goal)}
	module := New(nil, Config{OwnerMXID: func() string { return "@owner:example.com" }, AgentCloudControlClient: client})

	client.goalResult.Task.Goal = "AWS_SECRET_ACCESS_KEY=" + strings.Repeat("x", 32)
	if _, apiErr := module.createAgentCloudGoal(t.Context(), goal, connectionID, key); apiErr == nil || apiErr.Status != http.StatusBadGateway || strings.Contains(apiErr.Error, "AWS_SECRET") {
		t.Fatalf("invalid Agent response leaked detail: %#v", apiErr)
	}

	unavailable := New(nil, Config{OwnerMXID: func() string { return "@owner:example.com" }})
	if _, apiErr := unavailable.createAgentCloudGoal(t.Context(), goal, connectionID, key); apiErr == nil || apiErr.Status != http.StatusServiceUnavailable || apiErr.Code != cloudUnavailableCode {
		t.Fatalf("unavailable Agent err=%#v", apiErr)
	}
}

func validAgentGoalModuleResult(connectionID, goal string) AgentCloudGoalResult {
	now := time.Date(2026, time.July, 17, 8, 0, 0, 0, time.UTC)
	taskID := uuid.NewString()
	return AgentCloudGoalResult{
		Task: AgentCloudGoalTask{
			TaskID: taskID, OwnerID: "dirextalk-project:example.com", Goal: goal,
			ExecutionStatus: AgentCloudGoalExecutionQueued, OutcomeStatus: AgentCloudGoalOutcomePending,
			RetentionPolicy: AgentCloudGoalRetentionEphemeralAutoDestroy, Revision: 1, CreatedAt: now, UpdatedAt: now,
		},
		Planning: AgentCloudGoalPlanning{
			TaskID: taskID, OwnerID: "dirextalk-project:example.com", ConnectionID: connectionID,
			RecipeID: "recipe-cloud-goal-derived", State: AgentCloudGoalPlanningResearchQueued,
		},
	}
}
