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

func (p *recordingCloudPlanner) CreateResearchGoal(_ context.Context, goal, connectionID, idempotencyKey string) (map[string]any, error) {
	p.goal = goal
	p.connectionID = connectionID
	p.idempotencyKeys = append(p.idempotencyKeys, idempotencyKey)
	p.calls++
	return map[string]any{
		"goal": map[string]any{"goal_id": "cloud_goal_1", "status": "researching"},
		"plan": map[string]any{"plan_id": "cloud_plan_1", "status": "researching"},
	}, nil
}

func TestCloudDeploymentPlanningIdempotencyIsScopedToOneAgentRequest(t *testing.T) {
	planner := &recordingCloudPlanner{}
	runtime := New(Config{CloudPlanner: planner})
	tool, ok := nativeToolByName(runtime.availableTools(), nativeAgentCloudDeploymentPlanTool)
	if !ok {
		t.Fatal("cloud deployment planning tool must be available")
	}
	args := map[string]any{"goal": "Deploy a private knowledge node after a reviewed plan."}
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
	if !strings.Contains(prompt, "## Built-in Skill: Cloud Deployment Planner") || !strings.Contains(prompt, nativeAgentCloudDeploymentPlanTool) {
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
		t.Fatalf("cloud dialogue prompt must name the only available tool: %q", run.session.systemPrompt)
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
