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
	if !ok || !catalogHasPlugin(entries, "io.dirextalk.agent") || !catalogHasPlugin(entries, "io.dirextalk.ops") {
		t.Fatalf("expected official agent and ops catalog entries, got %#v", catalog)
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

func TestPluginEnableProvidesOpsRuntimeEnvironmentAndMounts(t *testing.T) {
	t.Setenv("P2P_OPS_BACKUP_VOLUME", "dirextalk_ops_backups_test")
	t.Setenv("P2P_OPS_MAX_BACKUPS", "9")
	t.Setenv("P2P_OPS_MESSAGE_SERVER_CONTAINER", "message-server-test")
	t.Setenv("P2P_OPS_POSTGRES_CONTAINER", "postgres-test")
	runner := &recordingPluginRunner{}
	service := NewService(Config{
		ServerName:   "example.com",
		Homeserver:   "http://message-server:8008",
		PluginRunner: runner,
	})

	mustHandle[map[string]any](t, service, "plugins.install", map[string]any{
		"plugin_id": "io.dirextalk.ops",
	})
	mustHandle[map[string]any](t, service, "plugins.enable", map[string]any{
		"plugin_id": "io.dirextalk.ops",
	})

	if len(runner.operations) != 2 {
		t.Fatalf("expected install and enable operations, got %#v", runner.operations)
	}
	op := runner.operations[1]
	if op.Env["DIREXTALK_BASE_URL"] != "http://message-server:8008" {
		t.Fatalf("expected backend URL in ops env, got %#v", op.Env)
	}
	if _, ok := op.Env["DIREXTALK_AGENT_TOKEN"]; ok {
		t.Fatalf("ops plugin must not receive agent token, got %#v", op.Env)
	}
	if _, ok := op.Env["DIREXTALK_AGENT_TOKEN_REF"]; ok {
		t.Fatalf("ops plugin must not receive agent token ref, got %#v", op.Env)
	}
	wantEnv := map[string]string{
		"OPS_BACKUP_ROOT":              "/var/lib/dirextalk-ops/backups",
		"OPS_MAX_BACKUPS":              "9",
		"OPS_MESSAGE_SERVER_CONTAINER": "message-server-test",
		"OPS_POSTGRES_CONTAINER":       "postgres-test",
	}
	for key, want := range wantEnv {
		if got := op.Env[key]; got != want {
			t.Fatalf("expected ops env %s=%q, got %q in %#v", key, want, got, op.Env)
		}
	}
	for _, want := range []string{
		"/var/run/docker.sock:/var/run/docker.sock",
		"dirextalk_ops_backups_test:/var/lib/dirextalk-ops",
	} {
		if !stringSliceContains(op.Volumes, want) {
			t.Fatalf("expected ops volume %q, got %#v", want, op.Volumes)
		}
	}
}

func TestPluginEnableProvidesAgentRuntimeEnvironment(t *testing.T) {
	t.Setenv("P2P_AGENT_KNOWLEDGE_DATABASE_URL", "postgres://dirextalk_message_server:dirextalk_message_server@postgres/dirextalk_message_server?sslmode=disable")
	t.Setenv("P2P_AGENT_KNOWLEDGE_VOLUME", "agent-data-test")
	runner := &recordingPluginRunner{}
	service := NewService(Config{
		ServerName:   "example.com",
		Homeserver:   "http://message-server:8008",
		PluginRunner: runner,
	})

	mustHandle[map[string]any](t, service, "plugins.install", map[string]any{
		"plugin_id": "io.dirextalk.agent",
		"config": map[string]any{
			"enabled_tools":     []any{"search_rooms", "list_messages"},
			"system_prompt":     "You are the local agent.",
			"mcp_servers":       []any{map[string]any{"name": "filesystem", "transport": "stdio", "enabled": false}},
			"unexpected_key":    "kept in config only",
			"max_output_tokens": float64(1024),
			"model_profiles": []any{
				map[string]any{
					"id":          "deepseek:deepseek-chat",
					"provider":    "deepseek",
					"model":       "deepseek-chat",
					"api_key":     "sk-test-secret",
					"api_key_ref": "secret:legacy-profile-key",
				},
			},
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
	if _, ok := op.Env["AGENT_API_KEY"]; ok {
		t.Fatalf("model API keys must not be injected into plugin env, got %#v", op.Env)
	}
	if _, ok := op.Env["AGENT_API_KEY_REF"]; ok {
		t.Fatalf("model API key refs must not be injected into plugin env, got %#v", op.Env)
	}
	for _, key := range []string{
		"AGENT_MODEL_PROVIDER",
		"AGENT_MODEL",
		"AGENT_BASE_URL",
		"AGENT_DEFAULT_MODEL_PROFILE_ID",
		"AGENT_TEMPERATURE",
		"AGENT_MAX_OUTPUT_TOKENS",
		"AGENT_CONTEXT_WINDOW",
		"AGENT_TOP_P",
		"AGENT_TOP_K",
		"AGENT_REASONING_MODE",
		"AGENT_MODEL_PROFILES_JSON",
	} {
		if _, ok := op.Env[key]; ok {
			t.Fatalf("model call setting %s must be request-local, got env %#v", key, op.Env)
		}
	}
	if op.Env["AGENT_ENABLED_TOOLS"] != "search_rooms,list_messages" {
		t.Fatalf("expected enabled tools env, got %#v", op.Env)
	}
	if op.Env["AGENT_MCP_SERVERS_JSON"] == "" {
		t.Fatalf("expected MCP JSON config env, got %#v", op.Env)
	}
	if op.Env["AGENT_KNOWLEDGE_DIR"] != "/var/lib/dirextalk-agent/knowledge" {
		t.Fatalf("expected knowledge dir env, got %#v", op.Env)
	}
	if op.Env["AGENT_KNOWLEDGE_DATABASE_URL"] == "" {
		t.Fatalf("expected knowledge database URL env, got %#v", op.Env)
	}
	if !stringSliceContains(op.Volumes, "agent-data-test:/var/lib/dirextalk-agent") {
		t.Fatalf("expected agent data volume, got %#v", op.Volumes)
	}
	if _, ok := op.Env["AGENT_PROFILE_API_KEY_DEEPSEEK_DEEPSEEK_CHAT"]; ok {
		t.Fatalf("model profile API keys must not be injected into plugin env, got %#v", op.Env)
	}
	if _, ok := op.Config["DIREXTALK_AGENT_TOKEN"]; ok {
		t.Fatalf("runtime secrets must not be persisted in plugin config: %#v", op.Config)
	}
}

func TestPluginActionAllowlistIncludesOpsActions(t *testing.T) {
	entry, ok := findOfficialPlugin("io.dirextalk.ops")
	if !ok {
		t.Fatalf("expected ops plugin in official catalog")
	}
	for _, action := range []string{
		"ops.status.get",
		"ops.backup.create",
		"ops.backup.download_chunk",
		"ops.cleanup.plan",
		"ops.cleanup.run",
		"ops.rooms.cleanup.plan",
		"ops.rooms.cleanup.run",
		"ops.media.orphans.plan",
		"ops.migration.export",
		"ops.restore.plan",
	} {
		if !pluginActionAllowed(entry, action) {
			t.Fatalf("expected ops action %q to be allowed by catalog %#v", action, entry.Actions)
		}
	}
}

func TestPluginActionAllowlistIncludesAgentKnowledgeActions(t *testing.T) {
	entry, ok := findOfficialPlugin("io.dirextalk.agent")
	if !ok {
		t.Fatalf("expected agent plugin in official catalog")
	}
	for _, action := range []string{
		"agent.knowledge.config.get",
		"agent.knowledge.config.update",
		"agent.knowledge.sources.list",
		"agent.knowledge.sources.delete",
		"agent.knowledge.upload.start",
		"agent.knowledge.upload.chunk",
		"agent.knowledge.upload.finish",
		"agent.knowledge.memory.create",
		"agent.knowledge.search",
		"agent.knowledge.status",
	} {
		if !pluginActionAllowed(entry, action) {
			t.Fatalf("expected agent knowledge action %q to be allowed by catalog %#v", action, entry.Actions)
		}
	}
}

func TestPluginActionAllowlistIncludesAgentRuntimeActions(t *testing.T) {
	entry, ok := findOfficialPlugin("io.dirextalk.agent")
	if !ok {
		t.Fatalf("expected agent plugin in official catalog")
	}
	for _, action := range []string{
		"agent.runtime.install",
		"agent.runtime.uninstall",
		"agent.runtime.run",
		"agent.runtime.which",
		"agent.runtime.tools.list",
	} {
		if !pluginActionAllowed(entry, action) {
			t.Fatalf("expected agent runtime action %q to be allowed by catalog %#v", action, entry.Actions)
		}
	}
}

func catalogHasPlugin(entries []pluginCatalogEntry, pluginID string) bool {
	for _, entry := range entries {
		if entry.ID == pluginID && officialPluginImage(entry.Image) {
			return true
		}
	}
	return false
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
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

func TestPluginModelProfileAPIKeyIsInvokeOnly(t *testing.T) {
	runner := &recordingPluginRunner{}
	service := NewService(Config{
		ServerName:   "example.com",
		Homeserver:   "http://message-server:8008",
		PluginRunner: runner,
	})

	install := mustHandle[map[string]any](t, service, "plugins.install", map[string]any{
		"plugin_id": "io.dirextalk.agent",
		"config": map[string]any{
			"provider":    "openai",
			"model":       "gpt-4.1-mini",
			"api_key":     "sk-test-secret",
			"api_key_ref": "secret:legacy-api-key",
			"model_profiles": []any{
				map[string]any{
					"id":          "deepseek:deepseek-chat",
					"provider":    "deepseek",
					"model":       "deepseek-chat",
					"api_key":     "sk-test-secret",
					"api_key_ref": "secret:legacy-profile-key",
				},
			},
		},
	})
	plugin := install["plugin"].(pluginInstance)
	if _, ok := plugin.Config["api_key"]; ok {
		t.Fatalf("raw API key must not be persisted in plugin config: %#v", plugin.Config)
	}
	if _, ok := plugin.Config["api_key_ref"]; ok {
		t.Fatalf("API key refs must not be persisted for client-local model keys: %#v", plugin.Config)
	}
	if strings.Contains(mustJSON(t, plugin.Config), "api_key") ||
		strings.Contains(mustJSON(t, plugin.Config), "secret:") ||
		strings.Contains(mustJSON(t, plugin.Config), "sk-test-secret") {
		t.Fatalf("agent config must not persist model API key fields: %#v", plugin.Config)
	}

	config := mustHandle[map[string]any](t, service, "plugins.config.get", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})
	if strings.Contains(mustJSON(t, config), "api_key") ||
		strings.Contains(mustJSON(t, config), "secret:") ||
		strings.Contains(mustJSON(t, config), "sk-test-secret") {
		t.Fatalf("config response must not leak plugin secret fields: %#v", config)
	}

	mustHandle[map[string]any](t, service, "plugins.enable", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})
	op := runner.operations[len(runner.operations)-1]
	if _, ok := op.Env["AGENT_API_KEY"]; ok {
		t.Fatalf("model API keys must not be injected through runtime env, got %#v", op.Env)
	}
	mustHandle[map[string]any](t, service, "plugins.invoke", map[string]any{
		"plugin_id": "io.dirextalk.agent",
		"action":    "agent.chat",
		"params": map[string]any{
			"prompt": "hello",
			"model_profile": map[string]any{
				"id":       "deepseek:deepseek-chat",
				"provider": "deepseek",
				"model":    "deepseek-chat",
				"api_key":  "sk-test-secret",
			},
		},
	})
	if len(runner.invokes) != 1 {
		t.Fatalf("expected one plugin invoke, got %#v", runner.invokes)
	}
	profile, ok := runner.invokes[0].Params["model_profile"].(map[string]any)
	if !ok || profile["api_key"] != "sk-test-secret" {
		t.Fatalf("expected invoke-only model profile API key, got %#v", runner.invokes[0].Params)
	}
}

func TestPluginInvokeDoesNotResolveSavedAgentModelKeys(t *testing.T) {
	runner := &recordingPluginRunner{}
	service := NewService(Config{ServerName: "example.com", PluginRunner: runner})

	mustHandle[map[string]any](t, service, "plugins.install", map[string]any{
		"plugin_id": "io.dirextalk.agent",
		"config": map[string]any{
			"provider": "deepseek",
			"model":    "deepseek-v4-flash",
			"model_profiles": []any{
				map[string]any{
					"id":          "deepseek:deepseek-v4-flash",
					"name":        "DeepSeek v4 flash",
					"provider":    "deepseek",
					"model":       "deepseek-v4-flash",
					"base_url":    "https://api.deepseek.com",
					"api_key":     "sk-test-secret",
					"api_key_ref": "secret:legacy-profile-key",
				},
			},
		},
	})
	config := mustHandle[map[string]any](t, service, "plugins.config.get", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})
	if strings.Contains(mustJSON(t, config), "api_key") ||
		strings.Contains(mustJSON(t, config), "secret:") ||
		strings.Contains(mustJSON(t, config), "sk-test-secret") {
		t.Fatalf("config response must not leak saved model profile API key fields: %#v", config)
	}
	mustHandle[map[string]any](t, service, "plugins.enable", map[string]any{
		"plugin_id": "io.dirextalk.agent",
	})

	mustHandle[map[string]any](t, service, "plugins.invoke", map[string]any{
		"plugin_id": "io.dirextalk.agent",
		"action":    "agent.chat",
		"params": map[string]any{
			"prompt":           "hello",
			"model_profile_id": "deepseek:deepseek-v4-flash",
		},
	})
	if len(runner.invokes) != 1 {
		t.Fatalf("expected one plugin invoke, got %#v", runner.invokes)
	}
	profile, ok := runner.invokes[0].Params["model_profile"].(map[string]any)
	if !ok {
		t.Fatalf("expected saved model profile to be injected, got %#v", runner.invokes[0].Params)
	}
	if profile["id"] != "deepseek:deepseek-v4-flash" {
		t.Fatalf("expected saved non-secret profile, got %#v", profile)
	}
	if _, exists := profile["api_key"]; exists {
		t.Fatalf("saved profile API key must not be injected, got %#v", profile)
	}
	if _, exists := profile["api_key_ref"]; exists {
		t.Fatalf("invoke profile must not expose internal api_key_ref, got %#v", profile)
	}

	mustHandle[map[string]any](t, service, "plugins.invoke", map[string]any{
		"plugin_id": "io.dirextalk.agent",
		"action":    "agent.chat",
		"params": map[string]any{
			"prompt":           "hello again",
			"model_profile_id": "deepseek:deepseek-v4-flash",
			"model_profile": map[string]any{
				"id":       "deepseek:deepseek-v4-flash",
				"provider": "deepseek",
				"model":    "deepseek-v4-flash",
				"base_url": "https://api.deepseek.com",
				"api_key":  "sk-client-local",
			},
		},
	})
	if len(runner.invokes) != 2 {
		t.Fatalf("expected second plugin invoke, got %#v", runner.invokes)
	}
	profile, ok = runner.invokes[1].Params["model_profile"].(map[string]any)
	if !ok || profile["api_key"] != "sk-client-local" {
		t.Fatalf("expected invoke request profile key to pass through, got %#v", runner.invokes[1].Params)
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
