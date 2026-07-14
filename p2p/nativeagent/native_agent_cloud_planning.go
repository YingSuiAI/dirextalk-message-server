package nativeagent

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/google/uuid"
)

const nativeAgentCloudDeploymentPlanTool = "native_agent_cloud_deployment_plan"

type cloudPlanningRequestScopeContextKey struct{}

// withCloudPlanningRequestScope creates the idempotency scope for one Agent
// chat invocation. A model may retry the same tool call in that invocation,
// but an identical goal submitted in a later user request remains a new task.
func withCloudPlanningRequestScope(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if existing, _ := ctx.Value(cloudPlanningRequestScopeContextKey{}).(string); existing != "" {
		return ctx
	}
	return context.WithValue(ctx, cloudPlanningRequestScopeContextKey{}, uuid.NewString())
}

func cloudPlanningIdempotencyKey(ctx context.Context, goal, connectionID string) string {
	if ctx == nil {
		ctx = context.Background()
	}
	scope, _ := ctx.Value(cloudPlanningRequestScopeContextKey{}).(string)
	if scope == "" {
		// Direct internal invocation has no request envelope to replay. Give it
		// a fresh scope rather than accidentally collapsing future user tasks
		// merely because their text is identical.
		scope = uuid.NewString()
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("dirextalk.native-agent.cloud-research.v1\x00"+scope+"\x00"+connectionID+"\x00"+goal)).String()
}

func (r *Runtime) cloudPlanningTools() []Tool {
	if r == nil || r.cloudPlanner == nil {
		return nil
	}
	return []Tool{{
		Name: nativeAgentCloudDeploymentPlanTool,
		Description: "Create a research-only Cloud deployment goal from an explicit user request. " +
			"It never accepts credentials, approves spend, creates infrastructure, opens a network endpoint, or destroys resources. " +
			"Use it to hand an intent to the independent Cloud Orchestrator so it can return a reviewed plan and quote.",
		Write: true,
		Parameters: objectSchema(map[string]any{
			"goal": map[string]any{
				"type":        "string",
				"description": "The user-approved deployment intent, service requirement, resource needs, and constraints. Do not include credentials or secret values.",
			},
			"cloud_connection_id": map[string]any{
				"type":        "string",
				"description": "Optional existing Dirextalk Cloud Connection identifier. Omit when the user has not connected AWS yet.",
			},
		}),
		Handler: r.createCloudResearchGoal,
	}}
}

func (r *Runtime) createCloudResearchGoal(ctx context.Context, args map[string]any) (any, error) {
	if r == nil || r.cloudPlanner == nil {
		return nil, fmt.Errorf("cloud orchestrator is not configured")
	}
	for key := range args {
		if key != "goal" && key != "cloud_connection_id" {
			return nil, fmt.Errorf("cloud deployment planning does not accept %q", key)
		}
	}
	goal := trimString(args["goal"])
	if count := utf8.RuneCountInString(goal); count == 0 || count > 12000 {
		return nil, fmt.Errorf("goal must contain 1 to 12000 characters")
	}
	if cloudmodule.ContainsSensitiveGoalMaterial(goal) {
		return nil, fmt.Errorf("cloud deployment planning accepts secret_ref values, not raw secret material")
	}
	connectionID := trimString(args["cloud_connection_id"])
	if len(connectionID) > 128 || strings.ContainsAny(connectionID, "\r\n\t") {
		return nil, fmt.Errorf("cloud_connection_id is invalid")
	}
	return r.cloudPlanner.CreateResearchGoal(ctx, goal, connectionID, cloudPlanningIdempotencyKey(ctx, goal, connectionID))
}
