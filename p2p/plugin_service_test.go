package p2p

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

type recordingPluginRunner struct {
	operations []PluginRunnerOperation
}

func (r *recordingPluginRunner) ApplyPlugin(ctx context.Context, op PluginRunnerOperation) error {
	r.operations = append(r.operations, op)
	return nil
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
	if !ok || len(entries) != 1 || entries[0].ID != "io.dirextalk.agent" || entries[0].Digest == "" {
		t.Fatalf("expected official agent catalog entry with digest, got %#v", catalog)
	}

	install := mustHandle[map[string]any](t, service, "plugins.install", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})
	if install["status"] != "installed" || install["job_id"] == "" {
		t.Fatalf("expected installed plugin job result, got %#v", install)
	}
	if len(runner.operations) != 1 || runner.operations[0].Action != "install" || runner.operations[0].PluginID != "io.dirextalk.agent" || runner.operations[0].Digest == "" {
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
