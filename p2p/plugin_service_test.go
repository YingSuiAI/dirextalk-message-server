package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

type recordingPluginRunner struct {
	operations []PluginRunnerOperation
	invokes    []PluginInvokeRequest
	streams    []PluginInvokeRequest
}

func (r *recordingPluginRunner) ApplyPlugin(ctx context.Context, op PluginRunnerOperation) error {
	r.operations = append(r.operations, op)
	return nil
}

func (r *recordingPluginRunner) InvokePlugin(ctx context.Context, req PluginInvokeRequest) (map[string]any, error) {
	r.invokes = append(r.invokes, req)
	return map[string]any{
		"ok":    true,
		"text":  "hello from plugin",
		"model": req.Params["model"],
	}, nil
}

func (r *recordingPluginRunner) StreamPlugin(ctx context.Context, req PluginInvokeRequest, emit func(PluginStreamEvent) error) error {
	r.streams = append(r.streams, req)
	if err := emit(PluginStreamEvent{Event: "delta", Data: map[string]any{"text": "hel"}}); err != nil {
		return err
	}
	return emit(PluginStreamEvent{Event: "done", Data: map[string]any{"text": "hello"}})
}

func TestPluginActionsAreOwnerOnly(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	agentToken := service.AgentToken()

	if service.Authorize(agentToken, "plugins.catalog.list") {
		t.Fatal("agent token must not authorize plugin management actions")
	}

	agentReq := jsonRequest(t, "/_p2p/query", map[string]any{
		"action": "plugins.catalog.list",
		"params": map[string]any{},
	})
	agentReq.Header.Set("Authorization", "Bearer "+agentToken)
	agentRec := httptest.NewRecorder()
	router.ServeHTTP(agentRec, agentReq)
	if agentRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected agent token plugin catalog request to be unauthorized, got %d body=%s", agentRec.Code, agentRec.Body.String())
	}

	ownerReq := jsonRequest(t, "/_p2p/query", map[string]any{
		"action": "plugins.catalog.list",
		"params": map[string]any{},
	})
	ownerReq.Header.Set("Authorization", "Bearer "+service.AccessToken())
	ownerRec := httptest.NewRecorder()
	router.ServeHTTP(ownerRec, ownerReq)
	if ownerRec.Code != http.StatusOK {
		t.Fatalf("expected owner token plugin catalog request to succeed, got %d body=%s", ownerRec.Code, ownerRec.Body.String())
	}
}

func TestPluginInstallAndEnableUseOfficialRunnerAndState(t *testing.T) {
	runner := &recordingPluginRunner{}
	service := NewService(Config{ServerName: "example.com", PluginRunner: runner})

	catalog := mustHandle[map[string]any](t, service, "plugins.catalog.list", nil)
	entries, ok := catalog["plugins"].([]pluginCatalogEntry)
	if !ok || len(entries) != 1 || entries[0].ID != "io.dirextalk.agent" || !officialPluginImage(entries[0].Image) {
		t.Fatalf("expected official agent catalog entry with dirextalk image, got %#v", catalog)
	}

	install := mustHandle[map[string]any](t, service, "plugins.install", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})
	if install["status"] != "installed" || install["job_id"] == "" {
		t.Fatalf("expected installed plugin job result, got %#v", install)
	}
	if len(runner.operations) != 1 || runner.operations[0].Action != "install" || runner.operations[0].PluginID != "io.dirextalk.agent" || !officialPluginImage(runner.operations[0].Image) {
		t.Fatalf("expected install runner operation for official plugin, got %#v", runner.operations)
	}

	enable := mustHandle[map[string]any](t, service, "plugins.enable", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})
	if enable["status"] != "enabled" {
		t.Fatalf("expected enabled plugin result, got %#v", enable)
	}
	if len(runner.operations) != 2 || runner.operations[1].Action != "enable" {
		t.Fatalf("expected enable runner operation, got %#v", runner.operations)
	}

	installed := mustHandle[map[string]any](t, service, "plugins.installed.list", nil)
	plugins, ok := installed["plugins"].([]pluginInstance)
	if !ok || len(plugins) != 1 || plugins[0].ID != "io.dirextalk.agent" || !plugins[0].Enabled || plugins[0].Status != "enabled" {
		t.Fatalf("expected enabled plugin in installed list, got %#v", installed)
	}
}

func TestPluginEnableProvidesAgentRuntimeEnvironment(t *testing.T) {
	t.Setenv("DEEPSEEK_API_KEY", "test-deepseek-key")
	runner := &recordingPluginRunner{}
	service := NewService(Config{
		ServerName:   "example.com",
		Homeserver:   "http://message-server:8008",
		PluginRunner: runner,
	})

	mustHandle[map[string]any](t, service, "plugins.install", map[string]any{
		"plugin_id": "io.dirextalk.agent",
		"config": map[string]any{
			"provider":          "deepseek",
			"model":             "deepseek-chat",
			"api_key_ref":       "env:DEEPSEEK_API_KEY",
			"enabled_tools":     []any{"search_rooms", "list_messages"},
			"system_prompt":     "You are the local agent.",
			"mcp_servers":       []any{map[string]any{"name": "filesystem", "transport": "stdio", "enabled": false}},
			"unexpected_key":    "kept in config only",
			"max_output_tokens": float64(1024),
		},
	})
	mustHandle[map[string]any](t, service, "plugins.enable", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})

	if len(runner.operations) != 2 {
		t.Fatalf("expected install and enable operations, got %#v", runner.operations)
	}
	op := runner.operations[1]
	if op.Env["DIREXTALK_BASE_URL"] != "http://message-server:8008" {
		t.Fatalf("expected backend URL in plugin env, got %#v", op.Env)
	}
	if op.Env["DIREXTALK_AGENT_TOKEN"] != service.AgentToken() || op.Env["DIREXTALK_AGENT_TOKEN"] == "" {
		t.Fatalf("expected agent token in runtime env, got %#v", op.Env)
	}
	if op.Env["AGENT_MODEL_PROVIDER"] != "deepseek" || op.Env["AGENT_MODEL"] != "deepseek-chat" {
		t.Fatalf("expected DeepSeek model env, got %#v", op.Env)
	}
	if op.Env["AGENT_API_KEY_REF"] != "env:DEEPSEEK_API_KEY" || op.Env["DEEPSEEK_API_KEY"] != "test-deepseek-key" {
		t.Fatalf("expected DeepSeek key passthrough by env ref, got %#v", op.Env)
	}
	if op.Env["AGENT_ENABLED_TOOLS"] != "search_rooms,list_messages" {
		t.Fatalf("expected enabled tools env, got %#v", op.Env)
	}
	if op.Env["AGENT_MCP_SERVERS_JSON"] == "" || op.Env["AGENT_MAX_OUTPUT_TOKENS"] != "1024" {
		t.Fatalf("expected JSON and numeric config env, got %#v", op.Env)
	}
	if _, ok := op.Config["DIREXTALK_AGENT_TOKEN"]; ok {
		t.Fatalf("runtime secrets must not be persisted in plugin config: %#v", op.Config)
	}
}

func TestPluginRuntimeEnvironmentUsesConfiguredBackendURLForAutoHomeserver(t *testing.T) {
	t.Setenv("P2P_PLUGIN_BACKEND_BASE_URL", "http://message-server:8008")
	runner := &recordingPluginRunner{}
	service := NewService(Config{
		ServerName:   "example.com",
		Homeserver:   "http://auto",
		PluginRunner: runner,
	})

	mustHandle[map[string]any](t, service, "plugins.install", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})
	mustHandle[map[string]any](t, service, "plugins.enable", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})

	op := runner.operations[len(runner.operations)-1]
	if op.Env["DIREXTALK_BASE_URL"] != "http://message-server:8008" {
		t.Fatalf("expected configured internal backend URL, got %#v", op.Env)
	}
}

func TestPluginDirectSecretIsWriteOnlyAndInjectedAtEnable(t *testing.T) {
	runner := &recordingPluginRunner{}
	service := NewService(Config{
		ServerName:   "example.com",
		Homeserver:   "http://message-server:8008",
		PluginRunner: runner,
	})

	install := mustHandle[map[string]any](t, service, "plugins.install", map[string]any{
		"plugin_id": "io.dirextalk.agent",
		"config": map[string]any{
			"provider": "openai",
			"model":    "gpt-4.1-mini",
		},
		"secrets": map[string]any{
			"api_key": "sk-test-secret",
		},
	})
	plugin := install["plugin"].(pluginInstance)
	if _, ok := plugin.Config["api_key"]; ok {
		t.Fatalf("raw API key must not be persisted in plugin config: %#v", plugin.Config)
	}
	if plugin.Config["api_key_ref"] != "secret:api_key" {
		t.Fatalf("expected direct secret to become secret ref, got %#v", plugin.Config)
	}

	config := mustHandle[map[string]any](t, service, "plugins.config.get", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})
	if strings.Contains(mustJSON(t, config), "sk-test-secret") {
		t.Fatalf("config response must not leak plugin secret: %#v", config)
	}
	status := config["secret_status"].(map[string]any)
	apiKey := status["api_key"].(map[string]any)
	if apiKey["configured"] != true {
		t.Fatalf("expected configured secret status, got %#v", config)
	}

	mustHandle[map[string]any](t, service, "plugins.enable", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})
	op := runner.operations[len(runner.operations)-1]
	if op.Env["AGENT_API_KEY_REF"] != "env:AGENT_API_KEY" || op.Env["AGENT_API_KEY"] != "sk-test-secret" {
		t.Fatalf("expected write-only secret injected through runtime env, got %#v", op.Env)
	}
	if strings.Contains(mustJSON(t, op.Config), "sk-test-secret") {
		t.Fatalf("runner config must not include raw plugin secret: %#v", op.Config)
	}
}

func TestPluginInvokeIsOwnerOnlyAndCallsEnabledOfficialPlugin(t *testing.T) {
	runner := &recordingPluginRunner{}
	service := NewService(Config{ServerName: "example.com", PluginRunner: runner})
	router := newP2PTestRouter(service)

	mustHandle[map[string]any](t, service, "plugins.install", map[string]any{
		"plugin_id": "io.dirextalk.agent",
		"config": map[string]any{
			"provider": "openai",
			"model":    "gpt-4.1-mini",
		},
	})
	mustHandle[map[string]any](t, service, "plugins.enable", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})

	agentReq := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "plugins.invoke",
		"params": map[string]any{
			"plugin_id": "io.dirextalk.agent",
			"action":    "agent.chat",
			"params": map[string]any{
				"prompt": "hello",
			},
		},
	})
	agentReq.Header.Set("Authorization", "Bearer "+service.AgentToken())
	agentRec := httptest.NewRecorder()
	router.ServeHTTP(agentRec, agentReq)
	if agentRec.Code != http.StatusUnauthorized {
		t.Fatalf("agent token must not invoke plugins, got %d body=%s", agentRec.Code, agentRec.Body.String())
	}

	ownerReq := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "plugins.invoke",
		"params": map[string]any{
			"plugin_id": "io.dirextalk.agent",
			"action":    "agent.chat",
			"params": map[string]any{
				"prompt":           "hello",
				"model_profile_id": "work",
			},
		},
	})
	ownerReq.Header.Set("Authorization", "Bearer "+service.AccessToken())
	ownerRec := httptest.NewRecorder()
	router.ServeHTTP(ownerRec, ownerReq)
	if ownerRec.Code != http.StatusOK {
		t.Fatalf("expected owner invoke to succeed, got %d body=%s", ownerRec.Code, ownerRec.Body.String())
	}
	if len(runner.invokes) != 1 || runner.invokes[0].Action != "agent.chat" || runner.invokes[0].PluginID != "io.dirextalk.agent" {
		t.Fatalf("expected plugin invoke runner call, got %#v", runner.invokes)
	}
	result := decodeJSONMap(t, ownerRec.Body.String())
	if result["plugin_id"] != "io.dirextalk.agent" || result["action"] != "agent.chat" {
		t.Fatalf("expected invoke envelope, got %#v", result)
	}
}

func TestPluginInvokeStreamUsesRealtimeWebSocketFrames(t *testing.T) {
	runner := &recordingPluginRunner{}
	service := NewService(Config{ServerName: "example.com", PluginRunner: runner})
	router := newP2PTestRouter(service)

	mustHandle[map[string]any](t, service, "plugins.install", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})
	mustHandle[map[string]any](t, service, "plugins.enable", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})

	httpReq := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "plugins.invoke.stream",
		"params": map[string]any{
			"plugin_id": "io.dirextalk.agent",
			"action":    "agent.chat",
			"params": map[string]any{
				"prompt": "hello",
			},
		},
	})
	httpReq.Header.Set("Authorization", "Bearer "+service.AccessToken())
	httpRec := httptest.NewRecorder()
	router.ServeHTTP(httpRec, httpReq)
	if httpRec.Code != http.StatusBadRequest {
		t.Fatalf("expected HTTP stream invoke to require websocket, got %d body=%s", httpRec.Code, httpRec.Body.String())
	}

	server := httptest.NewServer(router)
	defer server.Close()
	conn := dialRealtimeWS(t, server.URL, mustCreateRealtimeWSTicket(t, router, service.AccessToken()))
	defer conn.Close(websocket.StatusNormalClosure, "")

	writeRealtimeFrame(t, conn, map[string]any{"type": "client.hello"})
	if got := readRealtimeFrame(t, conn); got["type"] != "server.ready" {
		t.Fatalf("expected ready, got %#v", got)
	}
	writeRealtimeFrame(t, conn, map[string]any{
		"type":      "client.plugin_stream",
		"id":        "agent-stream-1",
		"plugin_id": "io.dirextalk.agent",
		"action":    "agent.chat",
		"params": map[string]any{
			"prompt": "hello",
		},
	})
	delta := readRealtimeFrame(t, conn)
	if delta["type"] != "server.plugin_stream.event" || delta["id"] != "agent-stream-1" || delta["event"] != "delta" {
		t.Fatalf("expected plugin stream delta frame, got %#v", delta)
	}
	data := delta["data"].(map[string]any)
	if data["text"] != "hel" {
		t.Fatalf("expected delta text, got %#v", delta)
	}
	done := readRealtimeFrame(t, conn)
	if done["type"] != "server.plugin_stream.event" || done["id"] != "agent-stream-1" || done["event"] != "done" {
		t.Fatalf("expected plugin stream done frame, got %#v", done)
	}
	if len(runner.streams) != 1 || runner.streams[0].Action != "agent.chat.stream" {
		t.Fatalf("expected stream runner to use agent.chat.stream, got %#v", runner.streams)
	}
}

func TestPluginInstallRejectsUnknownPluginBeforeRunner(t *testing.T) {
	runner := &recordingPluginRunner{}
	service := NewService(Config{ServerName: "example.com", PluginRunner: runner})

	_, apiErr := service.Handle(context.Background(), "plugins.install", map[string]any{
		"plugin_id": "io.example.unknown",
	})
	if apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected unknown plugin to be rejected with 400, got %#v", apiErr)
	}
	if len(runner.operations) != 0 {
		t.Fatalf("unknown plugin must not reach runner, got %#v", runner.operations)
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return string(data)
}

func decodeJSONMap(t *testing.T, body string) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("decode json body %q: %v", body, err)
	}
	return decoded
}
