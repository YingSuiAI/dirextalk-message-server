package agentgrpc

import (
	"context"
	"strings"
	"unicode/utf8"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateAgentCloudGoal exposes only the Agent's durable research/planning
// command. Owner identity comes exclusively from Runner configuration.
func (runner *Runner) CreateAgentCloudGoal(ctx context.Context, request cloudmodule.AgentCloudGoalCreateRequest) (cloudmodule.AgentCloudGoalResult, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudGoalResult{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.ConnectionID) ||
		utf8.RuneCountInString(request.Goal) == 0 || utf8.RuneCountInString(request.Goal) > 12000 || strings.IndexByte(request.Goal, 0) >= 0 ||
		cloudmodule.ContainsSensitiveGoalMaterial(request.Goal) || request.RetentionPolicy != cloudmodule.AgentCloudGoalRetentionEphemeralAutoDestroy ||
		(request.RecipeID != "" && (!agentCloudIdentifierPattern.MatchString(request.RecipeID) || cloudmodule.ContainsSensitiveGoalMaterial(request.RecipeID))) {
		return cloudmodule.AgentCloudGoalResult{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.CreateCloudGoal(callContext, &agentv1.CreateCloudGoalRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, CloudConnectionId: request.ConnectionID,
		Goal: request.Goal, RetentionPolicy: agentv1.RetentionPolicy_RETENTION_POLICY_EPHEMERAL_AUTO_DESTROY, RecipeId: request.RecipeID,
	})
	if err != nil {
		return cloudmodule.AgentCloudGoalResult{}, mapAgentCloudGoalRPCError(callContext, err)
	}
	return runner.mapAgentCloudGoal(response, request)
}

func (runner *Runner) mapAgentCloudGoal(response *agentv1.CreateCloudGoalResponse, request cloudmodule.AgentCloudGoalCreateRequest) (cloudmodule.AgentCloudGoalResult, error) {
	if response == nil || response.GetTask() == nil || response.GetPlanning() == nil {
		return cloudmodule.AgentCloudGoalResult{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	task, planning := response.GetTask(), response.GetPlanning()
	createdAt, err := exactAgentCloudTimestamp(task.GetCreatedAt())
	if err != nil {
		return cloudmodule.AgentCloudGoalResult{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	updatedAt, err := exactAgentCloudTimestamp(task.GetUpdatedAt())
	if err != nil || !updatedAt.Equal(createdAt) || !validUUID(task.GetTaskId()) || task.GetOwnerId() != runner.ownerID || task.GetGoal() != request.Goal ||
		task.GetExecutionStatus() != agentv1.ExecutionStatus_EXECUTION_STATUS_QUEUED || task.GetOutcomeStatus() != agentv1.OutcomeStatus_OUTCOME_STATUS_PENDING ||
		task.GetRetentionPolicy() != agentv1.RetentionPolicy_RETENTION_POLICY_EPHEMERAL_AUTO_DESTROY || task.GetCurrentStepId() != "" || task.GetApprovedPlanId() != "" || task.GetRevision() != 1 ||
		planning.GetTaskId() != task.GetTaskId() || planning.GetOwnerId() != runner.ownerID || planning.GetCloudConnectionId() != request.ConnectionID ||
		!agentCloudIdentifierPattern.MatchString(planning.GetRecipeId()) || cloudmodule.ContainsSensitiveGoalMaterial(planning.GetRecipeId()) ||
		(request.RecipeID != "" && planning.GetRecipeId() != request.RecipeID) ||
		planning.GetState() != agentv1.CloudGoalPlanningState_CLOUD_GOAL_PLANNING_STATE_RESEARCH_QUEUED || planning.GetRelatedPlanId() != "" {
		return cloudmodule.AgentCloudGoalResult{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.AgentCloudGoalResult{
		Task: cloudmodule.AgentCloudGoalTask{
			TaskID: task.GetTaskId(), OwnerID: task.GetOwnerId(), Goal: task.GetGoal(),
			ExecutionStatus: cloudmodule.AgentCloudGoalExecutionQueued, OutcomeStatus: cloudmodule.AgentCloudGoalOutcomePending,
			RetentionPolicy: cloudmodule.AgentCloudGoalRetentionEphemeralAutoDestroy, Revision: task.GetRevision(), CreatedAt: createdAt, UpdatedAt: updatedAt,
		},
		Planning: cloudmodule.AgentCloudGoalPlanning{
			TaskID: planning.GetTaskId(), OwnerID: planning.GetOwnerId(), ConnectionID: planning.GetCloudConnectionId(),
			RecipeID: planning.GetRecipeId(), State: cloudmodule.AgentCloudGoalPlanningResearchQueued,
		},
	}, nil
}

func mapAgentCloudGoalRPCError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return cloudmodule.ErrAgentCloudControlUnavailable
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		return cloudmodule.ErrAgentCloudControlInvalid
	case codes.NotFound:
		return cloudmodule.ErrAgentCloudConnectionNotFound
	case codes.AlreadyExists, codes.Aborted, codes.FailedPrecondition:
		return cloudmodule.ErrAgentCloudControlConflict
	case codes.PermissionDenied:
		return cloudmodule.ErrAgentCloudControlRejected
	default:
		return cloudmodule.ErrAgentCloudControlUnavailable
	}
}
