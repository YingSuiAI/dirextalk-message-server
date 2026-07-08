package nativeagent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	einotool "github.com/cloudwego/eino/components/tool"
)

func TestModelLoopCallsToolThenFinalizesAndStoresMemory(t *testing.T) {
	var requestCount int
	var sawToolResult bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode model request: %v", err)
		}
		messages, _ := payload["messages"].([]any)
		for _, raw := range messages {
			message, _ := raw.(map[string]any)
			if message["role"] == "tool" && strings.Contains(trimString(message["content"]), "Ada") {
				sawToolResult = true
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if requestCount == 1 {
			_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"call_1","type":"function","function":{"name":"dirextalk_contacts_list","arguments":"{\"query\":\"ada\"}"}}]}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"总结：联系人 Ada 已找到。"}}]}`))
	}))
	defer server.Close()

	var toolCalled bool
	runtime := New(Config{
		DataDir: filepath.Join(t.TempDir(), "agent"),
		Store:   &testConfigStore{config: map[string]any{"enabled_tools": []any{"dirextalk_contacts_list"}}},
		Tools: []Tool{{
			Name:        "dirextalk_contacts_list",
			Description: "List contacts.",
			Parameters:  objectSchema(map[string]any{"query": stringSchema()}),
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				toolCalled = true
				return map[string]any{"contacts": []map[string]any{{"display_name": "Ada"}}}, nil
			},
		}},
	})

	result, err := runtime.Invoke(context.Background(), "agent.chat", map[string]any{
		"conversation_id": "loop-test",
		"prompt":          "找 Ada 并总结",
		"model_profile": map[string]any{
			"provider": "openai_compatible",
			"model":    "mock-model",
			"base_url": server.URL,
			"api_key":  "test-key",
		},
	})
	if err != nil {
		t.Fatalf("agent chat failed: %v", err)
	}
	if !toolCalled || !sawToolResult || requestCount != 2 {
		t.Fatalf("expected tool loop with second model call, toolCalled=%v sawToolResult=%v requestCount=%d", toolCalled, sawToolResult, requestCount)
	}
	if result["text"] != "总结：联系人 Ada 已找到。" {
		t.Fatalf("expected final model answer, got %#v", result)
	}
	if result["framework"] != "eino" {
		t.Fatalf("expected eino framework marker, got %#v", result)
	}
	steps, ok := result["steps"].([]map[string]any)
	if !ok || len(steps) < 4 {
		t.Fatalf("expected trace steps for context/tool/final display, got %#v", result["steps"])
	}
	if !traceHasStep(steps, "tool_call", "dirextalk_contacts_list") || !traceHasStep(steps, "tool_result", "dirextalk_contacts_list") {
		t.Fatalf("expected tool call and tool result trace steps, got %#v", steps)
	}
	trace, ok := result["trace"].(map[string]any)
	if !ok || trace["framework"] != "eino" || trace["disclaimer"] == "" {
		t.Fatalf("expected observable Eino trace with disclaimer, got %#v", result["trace"])
	}
	memory, err := runtime.loadMemory(context.Background(), "loop-test")
	if err != nil {
		t.Fatalf("load memory: %v", err)
	}
	if len(memory.Messages) != 4 || memory.Messages[0].Role != "user" || memory.Messages[1].Role != "assistant" || memory.Messages[2].Role != "tool" || memory.Messages[3].Role != "assistant" {
		t.Fatalf("expected Eino user/tool-loop/assistant memory messages, got %#v", memory.Messages)
	}
	if len(memory.Messages[1].ToolCalls) != 1 || memory.Messages[2].ToolName != "dirextalk_contacts_list" {
		t.Fatalf("expected tool call and tool result in Eino memory, got %#v", memory.Messages)
	}
}

func TestModelLoopCanCallInstalledRuntimeCLITool(t *testing.T) {
	var requestCount int
	var sawRuntimeOutput bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode model request: %v", err)
		}
		messages, _ := payload["messages"].([]any)
		for _, raw := range messages {
			message, _ := raw.(map[string]any)
			if message["role"] == "tool" && strings.Contains(trimString(message["content"]), "runtime-eino from-model") {
				sawRuntimeOutput = true
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if requestCount == 1 {
			_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"call_runtime","type":"function","function":{"name":"runtime__hello_agent","arguments":"{\"args\":[\"from-model\"]}"}}]}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"CLI 已执行并返回结果。"}}]}`))
	}))
	defer server.Close()

	store := &testConfigStore{config: map[string]any{}}
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent"), Store: store})
	if _, err := runtime.Invoke(context.Background(), "agent.runtime.install", map[string]any{
		"id":       "hello-agent",
		"filename": runtimeTestToolFilename("hello-agent"),
		"content":  runtimeTestToolContent("runtime-eino", true),
	}); err != nil {
		t.Fatalf("install runtime tool: %v", err)
	}
	result, err := runtime.Invoke(context.Background(), "agent.chat", map[string]any{
		"prompt": "调用 hello-agent 工具并总结",
		"model_profile": map[string]any{
			"provider": "openai_compatible",
			"model":    "mock-model",
			"base_url": server.URL,
			"api_key":  "test-key",
		},
	})
	if err != nil {
		t.Fatalf("agent chat failed: %v", err)
	}
	if requestCount != 2 || !sawRuntimeOutput || result["text"] != "CLI 已执行并返回结果。" {
		t.Fatalf("expected runtime CLI tool loop, requestCount=%d sawRuntimeOutput=%v result=%#v", requestCount, sawRuntimeOutput, result)
	}
}

func TestModelLoopCanCallBuiltInRuntimeShellTool(t *testing.T) {
	var requestCount int
	var sawShellOutput bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode model request: %v", err)
		}
		messages, _ := payload["messages"].([]any)
		for _, raw := range messages {
			message, _ := raw.(map[string]any)
			if message["role"] == "tool" && strings.Contains(trimString(message["content"]), "shell-eino") {
				sawShellOutput = true
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if requestCount == 1 {
			_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"call_shell","type":"function","function":{"name":"runtime__shell","arguments":"{\"command\":\"printf shell-eino\"}"}}]}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"Shell 已执行并返回结果。"}}]}`))
	}))
	defer server.Close()

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent"), Store: &testConfigStore{config: map[string]any{}}})
	result, err := runtime.Invoke(context.Background(), "agent.chat", map[string]any{
		"prompt": "执行 shell 命令并总结",
		"model_profile": map[string]any{
			"provider": "openai_compatible",
			"model":    "mock-model",
			"base_url": server.URL,
			"api_key":  "test-key",
		},
	})
	if err != nil {
		t.Fatalf("agent chat failed: %v", err)
	}
	if requestCount != 2 || !sawShellOutput || result["text"] != "Shell 已执行并返回结果。" {
		t.Fatalf("expected runtime shell tool loop, requestCount=%d sawShellOutput=%v result=%#v", requestCount, sawShellOutput, result)
	}
}

func TestRuntimeShellToolCanBeDisabled(t *testing.T) {
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	tools, cleanup, err := runtime.enabledEinoTools(context.Background(), map[string]any{"runtime_shell_enabled": false}, map[string]any{})
	if err != nil {
		t.Fatalf("enabled Eino tools: %v", err)
	}
	defer cleanup()
	for _, tool := range tools {
		info, err := tool.Info(context.Background())
		if err != nil {
			t.Fatalf("tool info: %v", err)
		}
		if info.Name == "runtime__shell" {
			t.Fatalf("runtime__shell should be disabled")
		}
	}
}

func TestRuntimeShellEinoToolRunsCommand(t *testing.T) {
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	tools, cleanup, err := runtime.enabledEinoTools(context.Background(), map[string]any{}, map[string]any{})
	if err != nil {
		t.Fatalf("enabled Eino tools: %v", err)
	}
	defer cleanup()
	for _, tool := range tools {
		info, err := tool.Info(context.Background())
		if err != nil {
			t.Fatalf("tool info: %v", err)
		}
		if info.Name != "runtime__shell" {
			continue
		}
		invokable, ok := tool.(interface {
			InvokableRun(context.Context, string, ...einotool.Option) (string, error)
		})
		if !ok {
			t.Fatalf("runtime__shell is not invokable")
		}
		result, err := invokable.InvokableRun(context.Background(), `{"command":"printf shell-direct"}`)
		if err != nil {
			t.Fatalf("run runtime shell tool: %v", err)
		}
		if !strings.Contains(result, "shell-direct") {
			t.Fatalf("expected shell output, got %s", result)
		}
		return
	}
	t.Fatalf("expected runtime__shell tool")
}

func TestModelLoopHonorsConfiguredMaxToolCalls(t *testing.T) {
	const shellCalls = 8
	var requestCount int
	var observedShellResults int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode model request: %v", err)
		}
		messages, _ := payload["messages"].([]any)
		for _, raw := range messages {
			message, _ := raw.(map[string]any)
			if message["role"] == "tool" && strings.Contains(trimString(message["content"]), "shell-loop-") {
				observedShellResults++
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if requestCount <= shellCalls {
			callID := "call_shell_" + string(rune('0'+requestCount))
			body := `{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"` + callID + `","type":"function","function":{"name":"runtime__shell","arguments":"{\"command\":\"printf shell-loop-` + callID + `\"}"}}]}}]}`
			_, _ = w.Write([]byte(body))
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"所有 shell 步骤已完成。"}}]}`))
	}))
	defer server.Close()

	runtime := New(Config{
		DataDir: filepath.Join(t.TempDir(), "agent"),
		Store: &testConfigStore{config: map[string]any{
			"max_tool_calls": shellCalls,
		}},
	})
	result, err := runtime.Invoke(context.Background(), "agent.chat", map[string]any{
		"prompt": "连续执行多个 shell 步骤",
		"model_profile": map[string]any{
			"provider": "openai_compatible",
			"model":    "mock-model",
			"base_url": server.URL,
			"api_key":  "test-key",
		},
	})
	if err != nil {
		t.Fatalf("agent chat failed: %v", err)
	}
	if requestCount != shellCalls+1 || observedShellResults < shellCalls || result["text"] != "所有 shell 步骤已完成。" {
		t.Fatalf("expected configured shell loop to finish, requestCount=%d observedShellResults=%d result=%#v", requestCount, observedShellResults, result)
	}
}

func TestModelLoopCanInstallSkillFromDialogue(t *testing.T) {
	var requestCount int
	var sawInstallResult bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode model request: %v", err)
		}
		messages, _ := payload["messages"].([]any)
		for _, raw := range messages {
			message, _ := raw.(map[string]any)
			if message["role"] == "tool" && strings.Contains(trimString(message["content"]), "dialogue-skill") {
				sawInstallResult = true
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if requestCount == 1 {
			_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"call_skill_install","type":"function","function":{"name":"native_agent_skills_install","arguments":"{\"id\":\"dialogue-skill\",\"content\":\"# Skill\\n\\nWhen installed, say DIALOGUE_SKILL_READY.\"}"}}]}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"Skill 已安装，下一轮对话会启用。"}}]}`))
	}))
	defer server.Close()

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent"), Store: &testConfigStore{config: map[string]any{}}})
	result, err := runtime.Invoke(context.Background(), "agent.chat", map[string]any{
		"prompt": "安装一个 dialogue skill",
		"model_profile": map[string]any{
			"provider": "openai_compatible",
			"model":    "mock-model",
			"base_url": server.URL,
			"api_key":  "test-key",
		},
	})
	if err != nil {
		t.Fatalf("agent chat failed: %v", err)
	}
	if requestCount != 2 || !sawInstallResult || result["text"] != "Skill 已安装，下一轮对话会启用。" {
		t.Fatalf("expected dialogue skill install loop, requestCount=%d sawInstallResult=%v result=%#v", requestCount, sawInstallResult, result)
	}
	steps, ok := result["steps"].([]map[string]any)
	if !ok || !traceHasStep(steps, "tool_call", "native_agent_skills_install") {
		t.Fatalf("expected skill install trace step, got %#v", result["steps"])
	}
	list, err := runtime.skillsList(context.Background())
	if err != nil {
		t.Fatalf("list skills: %v", err)
	}
	skills := list["skills"].([]map[string]any)
	if len(skills) != 1 || skills[0]["id"] != "dialogue-skill" {
		t.Fatalf("expected dialogue skill installed, got %#v", list)
	}
	config, _, err := runtime.agentConfig(context.Background())
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if prompt := runtime.enabledSkillsPrompt(context.Background(), config); !strings.Contains(prompt, "DIALOGUE_SKILL_READY") {
		t.Fatalf("expected installed skill in next prompt, got %q", prompt)
	}
}

func TestConfigEnabledToolsStillExposeDialogueManagementTools(t *testing.T) {
	runtime := New(Config{
		DataDir: filepath.Join(t.TempDir(), "agent"),
		Store:   &testConfigStore{config: map[string]any{"enabled_tools": []any{"search_contacts", "search_rooms", "list_messages", "send_message", "summarize_conversation"}}},
	})
	tools, cleanup, err := runtime.enabledEinoTools(context.Background(), map[string]any{
		"enabled_tools": []any{"search_contacts", "search_rooms", "list_messages", "send_message", "summarize_conversation"},
	}, map[string]any{})
	if err != nil {
		t.Fatalf("enabled Eino tools: %v", err)
	}
	defer cleanup()
	names := map[string]bool{}
	for _, tool := range tools {
		info, err := tool.Info(context.Background())
		if err != nil {
			t.Fatalf("tool info: %v", err)
		}
		names[info.Name] = true
	}
	if !names["native_agent_skills_install"] || !names["native_agent_mcp_servers_install"] {
		t.Fatalf("expected config-level enabled_tools to keep dialogue management tools, got %#v", names)
	}

	requestTools, requestCleanup, err := runtime.enabledEinoTools(context.Background(), map[string]any{
		"enabled_tools": []any{"search_contacts", "search_rooms", "list_messages", "send_message", "summarize_conversation"},
	}, map[string]any{"enabled_tools": []any{"search_contacts"}})
	if err != nil {
		t.Fatalf("request enabled Eino tools: %v", err)
	}
	defer requestCleanup()
	requestNames := map[string]bool{}
	for _, tool := range requestTools {
		info, err := tool.Info(context.Background())
		if err != nil {
			t.Fatalf("request tool info: %v", err)
		}
		requestNames[info.Name] = true
	}
	if !requestNames["native_agent_skills_install"] || !requestNames["native_agent_mcp_servers_install"] {
		t.Fatalf("request-level enabled_tools must keep dialogue management tools, got %#v", requestNames)
	}
}

func TestOpenAIProviderUsesChatCompletionsEndpoint(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"openai ok"}}]}`))
	}))
	defer server.Close()
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	result, err := runtime.Invoke(context.Background(), "agent.chat", map[string]any{
		"prompt": "hello",
		"model_profile": map[string]any{
			"provider": "openai",
			"model":    "mock-openai",
			"base_url": server.URL + "/v1",
			"api_key":  "test-key",
		},
	})
	if err != nil {
		t.Fatalf("openai provider: %v", err)
	}
	if gotPath != "/v1/chat/completions" || result["text"] != "openai ok" {
		t.Fatalf("expected openai chat completions, path=%q result=%#v", gotPath, result)
	}
}

func TestStreamCompactsMessagesByContextWindow(t *testing.T) {
	var gotMessages []any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode stream payload: %v", err)
		}
		gotMessages, _ = payload["messages"].([]any)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	var events []Event
	err := runtime.Stream(context.Background(), "agent.chat.stream", map[string]any{
		"messages": []any{
			map[string]any{"role": "user", "content": "old"},
			map[string]any{"role": "user", "content": "new"},
		},
		"model_profile": map[string]any{
			"provider":       "openai_compatible",
			"model":          "mock-stream",
			"base_url":       server.URL,
			"api_key":        "test-key",
			"context_window": 1,
		},
	}, func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("stream chat: %v", err)
	}
	if len(gotMessages) != 1 {
		t.Fatalf("expected compacted stream messages, got %#v", gotMessages)
	}
	last, _ := gotMessages[0].(map[string]any)
	if last["content"] != "new" {
		t.Fatalf("expected newest message after compaction, got %#v", gotMessages)
	}
	if len(events) != 3 || events[0].Event != "delta" || events[1].Event != "trace" || events[2].Event != "done" {
		t.Fatalf("expected delta, trace, and done events, got %#v", events)
	}
	if events[1].Data["framework"] != "eino" || events[1].Data["steps"] == nil {
		t.Fatalf("expected eino stream trace marker, got %#v", events[1])
	}
	if events[2].Data["framework"] != "eino" || events[2].Data["trace"] == nil {
		t.Fatalf("expected eino stream done marker with trace, got %#v", events[2])
	}
}

func traceHasStep(steps []map[string]any, stepType, name string) bool {
	for _, step := range steps {
		if step["type"] == stepType && step["name"] == name {
			return true
		}
	}
	return false
}

func TestAnthropicProviderUsesMessagesEndpoint(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if r.Header.Get("x-api-key") != "test-key" || r.Header.Get("anthropic-version") == "" {
			t.Fatalf("missing anthropic headers: %#v", r.Header)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"anthropic ok"}]}`))
	}))
	defer server.Close()
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	result, err := runtime.Invoke(context.Background(), "agent.chat", map[string]any{
		"prompt": "hello",
		"model_profile": map[string]any{
			"provider": "anthropic",
			"model":    "mock-claude",
			"base_url": server.URL,
			"api_key":  "test-key",
		},
	})
	if err != nil {
		t.Fatalf("anthropic provider: %v", err)
	}
	if gotPath != "/v1/messages" || result["text"] != "anthropic ok" {
		t.Fatalf("expected anthropic messages endpoint, path=%q result=%#v", gotPath, result)
	}
}
