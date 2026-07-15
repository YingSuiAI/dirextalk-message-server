package nativeagent

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/google/uuid"
)

const (
	nativeAgentCloudDeploymentPlanTool = "native_agent_cloud_deployment_plan"
	nativeAgentCloudStatusTool         = "native_agent_cloud_status"
	nativeAgentCloudDialogueModeParam  = "cloud_dialogue_mode"
	nativeAgentCloudConnectionIDParam  = "cloud_connection_id"
)

type cloudPlanningRequestScopeContextKey struct{}
type cloudPlanningConnectionScopeContextKey struct{}

type cloudPlanningConnectionScope struct {
	connectionID string
}

// cloudDialogueMode is an opt-in, request-scoped capability reduction for a
// cloud planning conversation. It never grants an additional capability: it
// removes every tool except the credential-free research-goal tool.
func cloudDialogueMode(params map[string]any) bool {
	return boolParam(params[nativeAgentCloudDialogueModeParam])
}

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

// prepareCloudDialogueRequest applies the Cloud dialogue's input and
// Connection-selection boundaries before any model-provider request. A model
// may describe a workload but never chooses the AWS Connection on which its
// research goal is recorded.
func prepareCloudDialogueRequest(ctx context.Context, params map[string]any) (context.Context, error) {
	ctx = withCloudPlanningRequestScope(ctx)
	if !cloudDialogueMode(params) {
		return ctx, nil
	}
	if cloudDialogueContainsSensitiveInput(params) {
		return nil, fmt.Errorf("cloud dialogue does not accept raw credential material; use the dedicated encrypted secret upload flow")
	}
	connectionID, err := selectedCloudDialogueConnectionID(params)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, cloudPlanningConnectionScopeContextKey{}, cloudPlanningConnectionScope{connectionID: connectionID})
	return withCloudWorkloadCollector(ctx), nil
}

func selectedCloudDialogueConnectionID(params map[string]any) (string, error) {
	connectionID := trimString(params[nativeAgentCloudConnectionIDParam])
	if connectionID == "" {
		// Status-only conversations remain useful before a Connection exists. The
		// planning tool rejects the missing selection if it is later invoked.
		return "", nil
	}
	if !validCloudPlanningConnectionID(connectionID) {
		return "", fmt.Errorf("cloud_connection_id must be a valid client-selected Cloud Connection")
	}
	return connectionID, nil
}

func cloudDialogueContainsSensitiveInput(params map[string]any) bool {
	for _, key := range []string{"prompt", "message"} {
		if cloudmodule.ContainsSensitiveGoalMaterial(trimString(params[key])) {
			return true
		}
	}
	for _, raw := range cloudDialogueRequestMessages(params["messages"]) {
		for _, key := range []string{"content", "text"} {
			if cloudmodule.ContainsSensitiveGoalMaterial(trimString(raw[key])) {
				return true
			}
		}
	}
	return false
}

func cloudDialogueRequestMessages(value any) []map[string]any {
	switch messages := value.(type) {
	case []any:
		result := make([]map[string]any, 0, len(messages))
		for _, raw := range messages {
			if message, ok := raw.(map[string]any); ok {
				result = append(result, message)
			}
		}
		return result
	case []map[string]any:
		return messages
	default:
		return nil
	}
}

func cloudPlanningConnectionScopeFromContext(ctx context.Context) (cloudPlanningConnectionScope, bool) {
	if ctx == nil {
		return cloudPlanningConnectionScope{}, false
	}
	scope, ok := ctx.Value(cloudPlanningConnectionScopeContextKey{}).(cloudPlanningConnectionScope)
	return scope, ok
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
	return r.cloudPlanningToolsForRequest(false)
}

func (r *Runtime) cloudDialoguePlanningTools() []Tool {
	return r.cloudPlanningToolsForRequest(true)
}

func (r *Runtime) cloudPlanningToolsForRequest(connectionSelectedByClient bool) []Tool {
	if r == nil {
		return nil
	}
	tools := make([]Tool, 0, 2)
	if r.cloudPlanner != nil {
		parameters := map[string]any{
			"type": "object",
			"properties": map[string]any{
				"goal": map[string]any{
					"type":        "string",
					"description": "The user-approved deployment intent, service requirement, resource needs, and constraints. Do not include credentials or secret values.",
				},
			},
			"required":             []string{"goal"},
			"additionalProperties": false,
		}
		if !connectionSelectedByClient {
			parameters["properties"].(map[string]any)[nativeAgentCloudConnectionIDParam] = map[string]any{
				"type":        "string",
				"description": "Required existing Dirextalk Cloud Connection identifier. Ask the user to create a Connection through the dedicated client flow before creating research when none is available.",
			}
			parameters["required"] = []string{"goal", nativeAgentCloudConnectionIDParam}
		}
		tools = append(tools, Tool{
			Name: nativeAgentCloudDeploymentPlanTool,
			Description: "Create a research-only Cloud deployment goal from an explicit user request. " +
				"It never accepts credentials, approves spend, creates infrastructure, opens a network endpoint, or destroys resources. " +
				"Use it to hand an intent to the independent Cloud Orchestrator so it can return a reviewed plan and quote.",
			Write:      true,
			Parameters: parameters,
			Handler:    r.createCloudResearchGoal,
		})
	}
	if r.cloudStatusReader != nil {
		tools = append(tools, Tool{
			Name: nativeAgentCloudStatusTool,
			Description: "Read the de-secretsed Cloud goals, plans, jobs, deployments, services, and alerts. " +
				"It cannot create, approve, expose, stop, resume, destroy, or upload a secret.",
			Write:      false,
			Parameters: objectSchema(map[string]any{}),
			Handler:    r.readCloudStatus,
		})
	}
	return tools
}

func (r *Runtime) createCloudResearchGoal(ctx context.Context, args map[string]any) (any, error) {
	if r == nil || r.cloudPlanner == nil {
		return nil, fmt.Errorf("cloud orchestrator is not configured")
	}
	scope, cloudDialogue := cloudPlanningConnectionScopeFromContext(ctx)
	for key := range args {
		switch key {
		case "goal":
		case nativeAgentCloudConnectionIDParam:
			if cloudDialogue {
				return nil, fmt.Errorf("cloud dialogue planning does not accept cloud_connection_id from the model")
			}
		default:
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
	connectionID := trimString(args[nativeAgentCloudConnectionIDParam])
	if cloudDialogue {
		connectionID = scope.connectionID
	}
	if !validCloudPlanningConnectionID(connectionID) {
		if cloudDialogue && connectionID == "" {
			return nil, fmt.Errorf("cloud dialogue planning requires a Cloud Connection selected by the client")
		}
		return nil, fmt.Errorf("cloud_connection_id is required and must be valid")
	}
	idempotencyKey := cloudPlanningIdempotencyKey(ctx, goal, connectionID)
	collector, collectingWorkload := cloudWorkloadCollectorFromContext(ctx)
	if cloudDialogue && (!collectingWorkload || !collector.reserve(idempotencyKey)) {
		return nil, fmt.Errorf("cloud dialogue creates at most one research plan per request")
	}
	result, err := r.cloudPlanner.CreateResearchGoal(ctx, goal, connectionID, idempotencyKey)
	if err != nil {
		return nil, err
	}
	if cloudDialogue {
		collector.record(result)
	}
	return result, nil
}

func validCloudPlanningConnectionID(connectionID string) bool {
	return connectionID != "" && len(connectionID) <= 128 && !strings.ContainsAny(connectionID, "\r\n\t")
}

func (r *Runtime) readCloudStatus(ctx context.Context, args map[string]any) (any, error) {
	if r == nil || r.cloudStatusReader == nil {
		return nil, fmt.Errorf("cloud status is not configured")
	}
	if len(args) != 0 {
		return nil, fmt.Errorf("cloud status does not accept parameters")
	}
	return r.cloudStatusReader.ReadCloudStatus(ctx)
}
