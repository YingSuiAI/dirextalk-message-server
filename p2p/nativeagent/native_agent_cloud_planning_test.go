package nativeagent

import (
	"context"
	"strings"
	"testing"

	einotool "github.com/cloudwego/eino/components/tool"
	"github.com/google/uuid"
)

type recordingCloudPlanner struct {
	goal            string
	connectionID    string
	idempotencyKeys []string
	calls           int
}

type recordingCloudStatusReader struct {
	calls int
}

type recordingCloudRecipeReader struct{ calls int }

func (r *recordingCloudRecipeReader) ReadCloudRecipes(context.Context) ([]CloudRecipeRecommendation, error) {
	r.calls++
	return []CloudRecipeRecommendation{{RecipeID: "recipe-private-1", Name: "Private knowledge node", Version: "v1", Maturity: "managed", Revision: 3, Resources: CloudRecipeResourceSummary{MinVCPU: 4, MinMemoryMiB: 8192, MinDiskGiB: 80, Architecture: "amd64"}}}, nil
}

func (r *recordingCloudStatusReader) ReadCloudStatus(context.Context) (map[string]any, error) {
	r.calls++
	return map[string]any{
		"plans": []map[string]any{{"plan_id": "cloud_plan_1", "status": "researching"}},
	}, nil
}

func (p *recordingCloudPlanner) CreateResearchGoal(_ context.Context, goal, connectionID, idempotencyKey string) (map[string]any, error) {
	p.goal = goal
	p.connectionID = connectionID
	p.idempotencyKeys = append(p.idempotencyKeys, idempotencyKey)
	p.calls++
	return map[string]any{
		"goal": map[string]any{
			"goal_id":  "cloud_goal_1",
			"plan_id":  "cloud_plan_1",
			"status":   "researching",
			"revision": int64(1),
		},
		"plan": map[string]any{
			"plan_id":  "cloud_plan_1",
			"goal_id":  "cloud_goal_1",
			"status":   "researching",
			"revision": int64(1),
		},
	}, nil
}

func TestCloudDeploymentPlanningIdempotencyIsScopedToOneAgentRequest(t *testing.T) {
	planner := &recordingCloudPlanner{}
	runtime := New(Config{CloudPlanner: planner})
	tool, ok := nativeToolByName(runtime.availableTools(), nativeAgentCloudDeploymentPlanTool)
	if !ok {
		t.Fatal("cloud deployment planning tool must be available")
	}
	args := map[string]any{"goal": "Deploy a private knowledge node after a reviewed plan.", "cloud_connection_id": "connection-1"}
	firstRequest := withCloudPlanningRequestScope(context.Background())
	if _, err := tool.Handler(firstRequest, args); err != nil {
		t.Fatalf("first planner call: %v", err)
	}
	if _, err := tool.Handler(firstRequest, args); err != nil {
		t.Fatalf("same-request planner retry: %v", err)
	}
	if _, err := tool.Handler(withCloudPlanningRequestScope(context.Background()), args); err != nil {
		t.Fatalf("new-request planner call: %v", err)
	}
	if len(planner.idempotencyKeys) != 3 || planner.idempotencyKeys[0] != planner.idempotencyKeys[1] || planner.idempotencyKeys[0] == planner.idempotencyKeys[2] {
		t.Fatalf("cloud planning idempotency scopes = %#v", planner.idempotencyKeys)
	}
	for _, key := range planner.idempotencyKeys {
		if _, err := uuid.Parse(key); err != nil {
			t.Fatalf("cloud planning idempotency key %q must be a UUID: %v", key, err)
		}
	}
}

func TestCloudDeploymentPlanningToolIsEinoNativeAndCredentialFree(t *testing.T) {
	planner := &recordingCloudPlanner{}
	runtime := New(Config{CloudPlanner: planner})
	tool, ok := nativeToolByName(runtime.enabledTools(context.Background(), nil, nil), "native_agent_cloud_deployment_plan")
	if !ok {
		t.Fatal("cloud deployment planning tool must be enabled for the Eino Native Agent")
	}
	if !tool.Write {
		t.Fatal("cloud deployment planning creates a durable research goal and must be marked write")
	}
	result, err := tool.Handler(context.Background(), map[string]any{
		"goal":                "Deploy a private knowledge node after a reviewed plan.",
		"cloud_connection_id": "connection-1",
	})
	if err != nil {
		t.Fatalf("cloud deployment planning = %v", err)
	}
	if planner.calls != 1 || planner.goal == "" || planner.connectionID != "connection-1" {
		t.Fatalf("planner received %#v, calls=%d", planner, planner.calls)
	}
	if result.(map[string]any)["plan"].(map[string]any)["status"] != "researching" {
		t.Fatalf("planner result = %#v", result)
	}
	if _, err := tool.Handler(context.Background(), map[string]any{
		"goal":              "must reject AWS credential fields",
		"aws_access_key_id": "AKIA-not-accepted",
	}); err == nil {
		t.Fatal("cloud deployment planning must reject credential-shaped arguments")
	}
	if planner.calls != 1 {
		t.Fatalf("rejected credential-shaped request reached planner, calls=%d", planner.calls)
	}
	if _, err := tool.Handler(context.Background(), map[string]any{
		"goal": "Deploy it; AWS_SECRET_ACCESS_KEY=not-a-real-secret-value",
	}); err == nil {
		t.Fatal("cloud deployment planning must reject raw secret material in a goal")
	}
	if planner.calls != 1 {
		t.Fatalf("secret-bearing goal reached planner, calls=%d", planner.calls)
	}
	if _, err := tool.Handler(context.Background(), map[string]any{
		"goal": "must require a selected connection before research is queued",
	}); err == nil {
		t.Fatal("cloud deployment planning must reject an unbound research goal")
	}
	if planner.calls != 1 {
		t.Fatalf("unbound request reached planner, calls=%d", planner.calls)
	}

	einoTools, cleanup, err := runtime.enabledEinoTools(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("build Eino tools: %v", err)
	}
	defer cleanup()
	if !einoToolNamed(t, einoTools, "native_agent_cloud_deployment_plan") {
		t.Fatal("cloud deployment planning tool was not exposed to the Eino ReAct graph")
	}
}

func TestCloudDeploymentSkillIsEmbeddedInTheServerSideEinoAgent(t *testing.T) {
	runtime := New(Config{CloudPlanner: &recordingCloudPlanner{}})
	prompt := runtime.agentSystemPrompt(context.Background(), map[string]any{}, map[string]any{}, "")
	if strings.TrimSpace(cloudDeploymentPlannerSkillPrompt) == "" || !strings.Contains(prompt, "## Built-in Skill: Cloud Deployment Planner") ||
		!strings.Contains(prompt, nativeAgentCloudDeploymentPlanTool) || !strings.Contains(prompt, nativeAgentCloudStatusTool) || !strings.Contains(prompt, nativeAgentCloudRecipesTool) || !strings.Contains(prompt, "secret slots") {
		t.Fatalf("server-side Eino cloud skill prompt = %q", prompt)
	}
	withoutPlanner := New(Config{}).agentSystemPrompt(context.Background(), map[string]any{}, map[string]any{}, "")
	if strings.Contains(withoutPlanner, "## Built-in Skill: Cloud Deployment Planner") {
		t.Fatalf("cloud planner skill must not be advertised without its narrow control-plane port: %q", withoutPlanner)
	}
	inspect, err := runtime.runtimeInspect(context.Background())
	if err != nil {
		t.Fatalf("inspect runtime: %v", err)
	}
	if !containsStringValue(inspect["built_in_skills"], "cloud_deployment_planner") {
		t.Fatalf("runtime inspect must surface the server-side Cloud skill, got %#v", inspect)
	}
}

func TestCloudDialogueModeExposesOnlyPlanningAndReadOnlyStatus(t *testing.T) {
	planner := &recordingCloudPlanner{}
	reader := &recordingCloudStatusReader{}
	recipeReader := &recordingCloudRecipeReader{}
	runtime := New(Config{
		CloudPlanner:      planner,
		CloudStatusReader: reader,
		CloudRecipeReader: recipeReader,
		Tools: []Tool{{
			Name: "runtime_shell",
			Handler: func(context.Context, map[string]any) (any, error) {
				return nil, nil
			},
		}},
	})
	tools := runtime.enabledTools(context.Background(), map[string]any{"enabled_tools": []any{"all"}}, map[string]any{"cloud_dialogue_mode": true})
	if len(tools) != 3 || tools[0].Name != nativeAgentCloudDeploymentPlanTool || tools[1].Name != nativeAgentCloudStatusTool || tools[2].Name != nativeAgentCloudRecipesTool || tools[1].Write || tools[2].Write {
		t.Fatalf("cloud dialogue tools = %#v", tools)
	}
	properties, _ := tools[0].Parameters["properties"].(map[string]any)
	if _, exists := properties["cloud_connection_id"]; exists {
		t.Fatal("cloud dialogue planning must use the client-bound Cloud Connection, not a model-supplied argument")
	}
	status, err := tools[1].Handler(context.Background(), map[string]any{})
	if err != nil || reader.calls != 1 || status.(map[string]any)["plans"] == nil {
		t.Fatalf("cloud status result=%#v calls=%d err=%v", status, reader.calls, err)
	}
	if _, err := tools[1].Handler(context.Background(), map[string]any{"destroy": true}); err == nil {
		t.Fatal("cloud status tool must reject mutation-shaped arguments")
	}
	recipes, err := tools[2].Handler(context.Background(), map[string]any{})
	if err != nil || recipeReader.calls != 1 || len(recipes.(map[string]any)["recipes"].([]CloudRecipeRecommendation)) != 1 {
		t.Fatalf("cloud recipes result=%#v calls=%d err=%v", recipes, recipeReader.calls, err)
	}
	if _, err := tools[2].Handler(context.Background(), map[string]any{"recipe_id": "forged"}); err == nil {
		t.Fatal("cloud recipe recommendation tool accepted a model-selected recipe_id")
	}
	if _, found := nativeToolByName(runtime.availableTools(), nativeAgentCloudRecipesTool); found {
		t.Fatal("cloud recipe recommendation tool escaped the cloud dialogue fixed allowlist")
	}
}

func TestCloudDialogueCannotTurnForgedRecipeIDIntoAWrite(t *testing.T) {
	planner := &recordingCloudPlanner{}
	runtime := New(Config{CloudPlanner: planner, CloudRecipeReader: &recordingCloudRecipeReader{}})
	tools := runtime.cloudDialoguePlanningTools()
	planning, _ := nativeToolByName(tools, nativeAgentCloudDeploymentPlanTool)
	recipes, _ := nativeToolByName(tools, nativeAgentCloudRecipesTool)
	if _, err := recipes.Handler(context.Background(), map[string]any{"recipe_id": "recipe-attacker"}); err == nil {
		t.Fatal("recipe reader accepted a selection argument")
	}
	if _, err := planning.Handler(context.Background(), map[string]any{"goal": "deploy it", "recipe_id": "recipe-attacker"}); err == nil {
		t.Fatal("planning tool accepted a model-selected recipe_id")
	}
	if _, err := planning.Handler(context.Background(), map[string]any{"goal": "deploy it", "secret_scope": []any{"secret_ref:attacker"}}); err == nil {
		t.Fatal("planning tool accepted a model-supplied secret_scope")
	}
	if planner.calls != 0 {
		t.Fatalf("forged recipe selection reached the only cloud write port, calls=%d", planner.calls)
	}
}

func TestCloudDialoguePlanningBindsTheClientSelectedConnection(t *testing.T) {
	planner := &recordingCloudPlanner{}
	runtime := New(Config{CloudPlanner: planner})
	tools := runtime.enabledTools(context.Background(), nil, map[string]any{"cloud_dialogue_mode": true})
	planningTool, ok := nativeToolByName(tools, nativeAgentCloudDeploymentPlanTool)
	if !ok {
		t.Fatal("cloud dialogue planning tool must be available")
	}

	ctx, err := prepareCloudDialogueRequest(context.Background(), map[string]any{
		"cloud_dialogue_mode": true,
		"cloud_connection_id": "connection-selected-by-client",
	})
	if err != nil {
		t.Fatalf("prepare selected Cloud Connection: %v", err)
	}
	if _, err := planningTool.Handler(ctx, map[string]any{
		"goal": "Deploy a private knowledge node after a reviewed plan.",
	}); err != nil {
		t.Fatalf("create with selected Cloud Connection: %v", err)
	}
	if planner.calls != 1 || planner.connectionID != "connection-selected-by-client" {
		t.Fatalf("planner must receive only the client-selected Cloud Connection, got %#v", planner)
	}
	if _, err := planningTool.Handler(ctx, map[string]any{
		"goal":                "A model must not choose another connection.",
		"cloud_connection_id": "connection-not-selected-by-client",
	}); err == nil {
		t.Fatal("cloud dialogue must reject a model-supplied Cloud Connection")
	}
	if planner.calls != 1 {
		t.Fatalf("rejected model connection reached planner, calls=%d", planner.calls)
	}

	withoutSelection, err := prepareCloudDialogueRequest(context.Background(), map[string]any{"cloud_dialogue_mode": true})
	if err != nil {
		t.Fatalf("prepare status-only cloud dialogue: %v", err)
	}
	if _, err := planningTool.Handler(withoutSelection, map[string]any{
		"goal": "A Cloud Connection must be selected before research is queued.",
	}); err == nil {
		t.Fatal("cloud dialogue must not let the model create an unbound research goal")
	}
	if planner.calls != 1 {
		t.Fatalf("unselected cloud dialogue reached planner, calls=%d", planner.calls)
	}
}

func TestCloudDialogueModeHardRestrictsToolsPromptAndMemory(t *testing.T) {
	runtime := New(Config{
		DataDir:      t.TempDir(),
		CloudPlanner: &recordingCloudPlanner{},
		Tools: []Tool{{
			Name:        "dirextalk_messages_send",
			Description: "Send a message.",
			Write:       true,
			Handler: func(context.Context, map[string]any) (any, error) {
				return map[string]any{"sent": true}, nil
			},
		}},
	})
	config := map[string]any{
		"runtime_shell_enabled": true,
		"runtime_tools": []any{
			map[string]any{"id": "unsafe", "command": "unsafe-command"},
		},
		"system_prompt": "CONFIG_PROMPT_MUST_NOT_REACH_CLOUD_DIALOGUE",
	}
	params := map[string]any{
		"cloud_dialogue_mode": true,
		"enabled_tools":       []any{"all"},
		"system_prompt":       "REQUEST_PROMPT_MUST_NOT_REACH_CLOUD_DIALOGUE",
		"conversation_id":     "cloud-dialogue-test",
	}

	tools, cleanup, err := runtime.enabledEinoTools(context.Background(), config, params)
	if err != nil {
		t.Fatalf("enabled cloud dialogue Eino tools: %v", err)
	}
	defer cleanup()
	if len(tools) != 1 {
		t.Fatalf("cloud dialogue must expose exactly one tool, got %d", len(tools))
	}
	info, err := tools[0].Info(context.Background())
	if err != nil {
		t.Fatalf("read cloud dialogue tool info: %v", err)
	}
	if info.Name != nativeAgentCloudDeploymentPlanTool {
		t.Fatalf("cloud dialogue tool = %q, want %q", info.Name, nativeAgentCloudDeploymentPlanTool)
	}

	profile := nativeModelProfile{ContextWindow: 1024}
	run, err := runtime.prepareEinoRun(context.Background(), config, params, profile)
	if err != nil {
		t.Fatalf("prepare cloud dialogue run: %v", err)
	}
	if !run.memoryDisabled {
		t.Fatal("cloud dialogue must force memory off")
	}
	for _, forbidden := range []string{
		"CONFIG_PROMPT_MUST_NOT_REACH_CLOUD_DIALOGUE",
		"REQUEST_PROMPT_MUST_NOT_REACH_CLOUD_DIALOGUE",
		"runtime__shell",
		"native_agent_skills_",
	} {
		if strings.Contains(run.session.systemPrompt, forbidden) {
			t.Fatalf("cloud dialogue prompt leaked forbidden capability or prompt %q: %q", forbidden, run.session.systemPrompt)
		}
	}
	if !strings.Contains(run.session.systemPrompt, nativeAgentCloudDeploymentPlanTool) {
		t.Fatalf("cloud dialogue prompt must name the deployment-plan tool: %q", run.session.systemPrompt)
	}
	if !strings.Contains(run.session.systemPrompt, nativeAgentCloudStatusTool) {
		t.Fatalf("cloud dialogue prompt must explain the read-only status tool: %q", run.session.systemPrompt)
	}
	if !strings.Contains(run.session.systemPrompt, nativeAgentCloudRecipesTool) {
		t.Fatalf("cloud dialogue prompt must explain the read-only Recipe recommendation tool: %q", run.session.systemPrompt)
	}
}

func containsStringValue(value any, want string) bool {
	values, _ := value.([]string)
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func nativeToolByName(tools []Tool, want string) (Tool, bool) {
	for _, tool := range tools {
		if tool.Name == want {
			return tool, true
		}
	}
	return Tool{}, false
}

func einoToolNamed(t *testing.T, tools []einotool.BaseTool, want string) bool {
	t.Helper()
	for _, tool := range tools {
		info, err := tool.Info(context.Background())
		if err != nil {
			t.Fatalf("read Eino tool info: %v", err)
		}
		if info != nil && info.Name == want {
			return true
		}
	}
	return false
}
