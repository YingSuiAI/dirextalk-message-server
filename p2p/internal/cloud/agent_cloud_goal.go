package cloud

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

func (m *Module) createAgentCloudGoal(ctx context.Context, goal, connectionID, idempotencyKey string) (any, *actionbase.Error) {
	if m == nil || m.cfg.AgentCloudControlClient == nil {
		return nil, unavailableError()
	}
	request := AgentCloudGoalCreateRequest{
		IdempotencyKey: idempotencyKey, ConnectionID: connectionID, Goal: goal,
		RetentionPolicy: AgentCloudGoalRetentionEphemeralAutoDestroy,
	}
	result, err := m.cfg.AgentCloudControlClient.CreateAgentCloudGoal(ctx, request)
	if err != nil {
		return nil, agentCloudGoalError(err)
	}
	if !validAgentCloudGoalResult(result, request) {
		return nil, agentCloudGoalError(ErrAgentCloudControlInvalidResponse)
	}
	return map[string]any{
		"task": map[string]any{
			"task_id": result.Task.TaskID, "owner_id": result.Task.OwnerID, "execution_status": result.Task.ExecutionStatus,
			"outcome_status": result.Task.OutcomeStatus, "retention_policy": result.Task.RetentionPolicy,
			"revision": result.Task.Revision, "created_at": result.Task.CreatedAt.UnixMilli(), "updated_at": result.Task.UpdatedAt.UnixMilli(),
		},
		"planning": map[string]any{
			"task_id": result.Planning.TaskID, "owner_id": result.Planning.OwnerID, "cloud_connection_id": result.Planning.ConnectionID,
			"recipe_id": result.Planning.RecipeID, "state": result.Planning.State,
		},
	}, nil
}

func validAgentCloudGoalResult(result AgentCloudGoalResult, request AgentCloudGoalCreateRequest) bool {
	task, planning := result.Task, result.Planning
	return canonicalUUID(task.TaskID) && task.TaskID == planning.TaskID && task.OwnerID != "" && task.OwnerID == planning.OwnerID &&
		len(task.OwnerID) <= 128 && !strings.ContainsAny(task.OwnerID, "\x00\r\n\t") && !ContainsSensitiveGoalMaterial(task.OwnerID) &&
		task.Goal == request.Goal && utf8.RuneCountInString(task.Goal) > 0 && utf8.RuneCountInString(task.Goal) <= 12000 && !ContainsSensitiveGoalMaterial(task.Goal) &&
		task.ExecutionStatus == AgentCloudGoalExecutionQueued && task.OutcomeStatus == AgentCloudGoalOutcomePending &&
		task.RetentionPolicy == request.RetentionPolicy && task.RetentionPolicy == AgentCloudGoalRetentionEphemeralAutoDestroy &&
		task.CurrentStepID == "" && task.ApprovedPlanID == "" && task.Revision == 1 && validInitialAgentCloudGoalTimes(task.CreatedAt, task.UpdatedAt) &&
		planning.ConnectionID == request.ConnectionID && canonicalUUID(planning.ConnectionID) &&
		cloudIdentifierPattern.MatchString(planning.RecipeID) && !ContainsSensitiveGoalMaterial(planning.RecipeID) &&
		(request.RecipeID == "" || planning.RecipeID == request.RecipeID) && planning.State == AgentCloudGoalPlanningResearchQueued && planning.RelatedPlanID == ""
}

func validInitialAgentCloudGoalTimes(createdAt, updatedAt time.Time) bool {
	return createdAt.Location() == time.UTC && updatedAt.Location() == time.UTC && createdAt.Unix() > 0 && updatedAt.Equal(createdAt)
}

func agentCloudGoalError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrAgentCloudConnectionNotFound):
		return actionbase.CodedError(http.StatusNotFound, "cloud_connection_not_found", "cloud connection was not found")
	case errors.Is(err, ErrAgentCloudControlConflict):
		return actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud goal")
	case errors.Is(err, ErrAgentCloudControlInvalid):
		return actionbase.CodedError(http.StatusBadRequest, cloudGoalInvalidCode, "cloud goal is invalid")
	case errors.Is(err, ErrAgentCloudControlRejected):
		return actionbase.CodedError(http.StatusForbidden, cloudGoalInvalidCode, "cloud goal was rejected")
	case errors.Is(err, ErrAgentCloudControlInvalidResponse):
		return actionbase.CodedError(http.StatusBadGateway, cloudGoalInvalidCode, "cloud Agent returned an invalid planning task")
	default:
		return actionbase.CodedError(http.StatusServiceUnavailable, cloudUnavailableCode, "cloud Agent planning is unavailable")
	}
}
