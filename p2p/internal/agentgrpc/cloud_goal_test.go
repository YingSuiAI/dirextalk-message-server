package agentgrpc

import (
	"errors"
	"strings"
	"testing"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const cloudGoalCredentialCanary = "AKIA0123456789ABCDEF"

func TestCreateAgentCloudGoalBindsOwnerAndReturnsOnlyInitialPlanningFacts(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	request := cloudmodule.AgentCloudGoalCreateRequest{
		IdempotencyKey: uuid.NewString(), ConnectionID: uuid.NewString(),
		Goal:            "Deploy a private knowledge service with a reviewable recipe.",
		RetentionPolicy: cloudmodule.AgentCloudGoalRetentionEphemeralAutoDestroy,
	}
	remote := validCloudGoalResponse(request, "recipe-cloud-goal-derived")
	var received *agentv1.CreateCloudGoalRequest
	server.cloud.createGoal = func(value *agentv1.CreateCloudGoalRequest) (*agentv1.CreateCloudGoalResponse, error) {
		received = proto.Clone(value).(*agentv1.CreateCloudGoalRequest)
		return proto.Clone(remote).(*agentv1.CreateCloudGoalResponse), nil
	}

	result, err := runner.CreateAgentCloudGoal(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if received.GetOwnerId() != "owner-from-config" || received.GetIdempotencyKey() != request.IdempotencyKey ||
		received.GetCloudConnectionId() != request.ConnectionID || received.GetGoal() != request.Goal || received.GetRecipeId() != "" ||
		received.GetRetentionPolicy() != agentv1.RetentionPolicy_RETENTION_POLICY_EPHEMERAL_AUTO_DESTROY {
		t.Fatalf("owner-bound CreateCloudGoal request=%#v", received)
	}
	if result.Task.OwnerID != "owner-from-config" || result.Task.TaskID != result.Planning.TaskID ||
		result.Planning.ConnectionID != request.ConnectionID || result.Planning.RecipeID != "recipe-cloud-goal-derived" ||
		result.Planning.State != cloudmodule.AgentCloudGoalPlanningResearchQueued || result.Planning.RelatedPlanID != "" {
		t.Fatalf("mapped Goal result=%#v", result)
	}
}

func TestCreateAgentCloudGoalRejectsScopeDriftAndSanitizesFailures(t *testing.T) {
	request := cloudmodule.AgentCloudGoalCreateRequest{
		IdempotencyKey: uuid.NewString(), ConnectionID: uuid.NewString(),
		Goal:            "Compile and test the project on temporary compute.",
		RetentionPolicy: cloudmodule.AgentCloudGoalRetentionEphemeralAutoDestroy,
	}
	mutations := map[string]func(*agentv1.CreateCloudGoalResponse){
		"owner":         func(value *agentv1.CreateCloudGoalResponse) { value.Task.OwnerId = "attacker" },
		"task relation": func(value *agentv1.CreateCloudGoalResponse) { value.Planning.TaskId = uuid.NewString() },
		"connection":    func(value *agentv1.CreateCloudGoalResponse) { value.Planning.CloudConnectionId = uuid.NewString() },
		"recipe canary": func(value *agentv1.CreateCloudGoalResponse) { value.Planning.RecipeId = cloudGoalCredentialCanary },
		"retention": func(value *agentv1.CreateCloudGoalResponse) {
			value.Task.RetentionPolicy = agentv1.RetentionPolicy_RETENTION_POLICY_MANAGED_RETAINED
		},
		"planning state": func(value *agentv1.CreateCloudGoalResponse) {
			value.Planning.State = agentv1.CloudGoalPlanningState_CLOUD_GOAL_PLANNING_STATE_PLAN_READY
		},
		"invented plan":     func(value *agentv1.CreateCloudGoalResponse) { value.Planning.RelatedPlanId = uuid.NewString() },
		"goal substitution": func(value *agentv1.CreateCloudGoalResponse) { value.Task.Goal = cloudGoalCredentialCanary },
		"approved plan":     func(value *agentv1.CreateCloudGoalResponse) { value.Task.ApprovedPlanId = uuid.NewString() },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := startRuntimeServer(t)
			runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
			remote := validCloudGoalResponse(request, "recipe-cloud-goal-derived")
			mutate(remote)
			server.cloud.createGoal = func(*agentv1.CreateCloudGoalRequest) (*agentv1.CreateCloudGoalResponse, error) { return remote, nil }
			_, err := runner.CreateAgentCloudGoal(t.Context(), request)
			if !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) || strings.Contains(err.Error(), cloudGoalCredentialCanary) {
				t.Fatalf("scope drift error=%v", err)
			}
		})
	}

	t.Run("rpc error", func(t *testing.T) {
		t.Parallel()
		server := startRuntimeServer(t)
		runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
		server.cloud.createGoal = func(*agentv1.CreateCloudGoalRequest) (*agentv1.CreateCloudGoalResponse, error) {
			return nil, status.Error(codes.Internal, cloudGoalCredentialCanary)
		}
		_, err := runner.CreateAgentCloudGoal(t.Context(), request)
		if !errors.Is(err, cloudmodule.ErrAgentCloudControlUnavailable) || strings.Contains(err.Error(), cloudGoalCredentialCanary) {
			t.Fatalf("unsanitized RPC error=%v", err)
		}
	})
}

func validCloudGoalResponse(request cloudmodule.AgentCloudGoalCreateRequest, recipeID string) *agentv1.CreateCloudGoalResponse {
	now := time.Date(2026, time.July, 17, 8, 0, 0, 0, time.UTC)
	taskID := uuid.NewString()
	return &agentv1.CreateCloudGoalResponse{
		Task: &agentv1.Task{
			TaskId: taskID, OwnerId: "owner-from-config", Goal: request.Goal,
			ExecutionStatus: agentv1.ExecutionStatus_EXECUTION_STATUS_QUEUED,
			OutcomeStatus:   agentv1.OutcomeStatus_OUTCOME_STATUS_PENDING,
			RetentionPolicy: agentv1.RetentionPolicy_RETENTION_POLICY_EPHEMERAL_AUTO_DESTROY,
			Revision:        1, CreatedAt: timestamppb.New(now), UpdatedAt: timestamppb.New(now),
		},
		Planning: &agentv1.CloudGoalPlanning{
			TaskId: taskID, OwnerId: "owner-from-config", CloudConnectionId: request.ConnectionID,
			RecipeId: recipeID, State: agentv1.CloudGoalPlanningState_CLOUD_GOAL_PLANNING_STATE_RESEARCH_QUEUED,
		},
	}
}
