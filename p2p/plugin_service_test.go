package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

func TestPluginCatalogExcludesNativeAgentWhenDockerRunnerDisabled(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})

	catalog := mustHandle[map[string]any](t, service, "plugins.catalog.list", nil)
	entries, ok := catalog["plugins"].([]pluginCatalogEntry)
	if !ok {
		t.Fatalf("expected typed plugin catalog entries, got %#v", catalog["plugins"])
	}
	if catalogHasPluginID(entries, "io.dirextalk.agent") {
		t.Fatalf("native agent must not be exposed through plugin catalog, got %#v", entries)
	}
	if catalogHasPluginID(entries, "io.dirextalk.ops") {
		t.Fatalf("ops plugin requires docker runner and must be hidden when docker runner is disabled, got %#v", entries)
	}
}

func TestOpsInstallAndEnableUseOfficialDockerRunnerAndState(t *testing.T) {
	runner := &recordingPluginRunner{}
	service := NewService(Config{ServerName: "example.com", PluginRunner: runner})

	install := mustHandle[map[string]any](t, service, "plugins.install", map[string]any{
		"plugin_id": "io.dirextalk.ops",
	})
	if install["status"] != "installed" || install["job_id"] == "" {
		t.Fatalf("expected installed plugin job result, got %#v", install)
	}
	if len(runner.operations) != 1 || runner.operations[0].Action != "install" || runner.operations[0].PluginID != "io.dirextalk.ops" || !officialPluginImage(runner.operations[0].Image) {
		t.Fatalf("expected install runner operation for ops plugin, got %#v", runner.operations)
	}

	enable := mustHandle[map[string]any](t, service, "plugins.enable", map[string]any{
		"plugin_id": "io.dirextalk.ops",
	})
	if enable["status"] != "enabled" {
		t.Fatalf("expected enabled plugin result, got %#v", enable)
	}
	if len(runner.operations) != 2 || runner.operations[1].Action != "enable" || runner.operations[1].PluginID != "io.dirextalk.ops" {
		t.Fatalf("expected ops enable runner operation, got %#v", runner.operations)
	}
}

func TestPluginEnableProvidesOpsRuntimeEnvironmentAndMounts(t *testing.T) {
	t.Setenv("P2P_OPS_BACKUP_VOLUME", "dirextalk_ops_backups_test")
	t.Setenv("P2P_OPS_MAX_BACKUPS", "9")
	t.Setenv("P2P_OPS_MESSAGE_SERVER_CONTAINER", "message-server-test")
	t.Setenv("P2P_OPS_POSTGRES_CONTAINER", "postgres-test")
	t.Setenv("P2P_OPS_POSTGRES_USER", "postgres-user-test")
	t.Setenv("P2P_OPS_POSTGRES_PASSWORD", "postgres-password-test")
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
		"OPS_POSTGRES_USER":            "postgres-user-test",
		"OPS_POSTGRES_PASSWORD":        "postgres-password-test",
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

func TestPluginEnableUsesSingleNodeOpsDefaults(t *testing.T) {
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
	wantEnv := map[string]string{
		"OPS_BACKUP_ROOT":              "/var/lib/dirextalk-ops/backups",
		"OPS_MAX_BACKUPS":              "10",
		"OPS_MESSAGE_SERVER_CONTAINER": "dirextalk-p2p-message-server-1",
		"OPS_POSTGRES_CONTAINER":       "dirextalk-p2p-postgres-1",
		"OPS_POSTGRES_USER":            "dirextalk_message_server",
		"OPS_POSTGRES_PASSWORD":        "dirextalk_message_server",
	}
	for key, want := range wantEnv {
		if got := op.Env[key]; got != want {
			t.Fatalf("expected default ops env %s=%q, got %q in %#v", key, want, got, op.Env)
		}
	}
	for _, want := range []string{
		"/var/run/docker.sock:/var/run/docker.sock",
		"p2p_ops_backups:/var/lib/dirextalk-ops",
	} {
		if !stringSliceContains(op.Volumes, want) {
			t.Fatalf("expected default ops volume %q, got %#v", want, op.Volumes)
		}
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
		"ops.backup.status",
		"ops.backup.download_chunk",
		"ops.cleanup.plan",
		"ops.cleanup.run",
		"ops.rooms.cleanup.plan",
		"ops.rooms.cleanup.run",
		"ops.media.orphans.plan",
		"ops.migration.export",
		"ops.restore.plan",
		"ops.restore.run",
	} {
		if !pluginActionAllowed(entry, action) {
			t.Fatalf("expected ops action %q to be allowed by catalog %#v", action, entry.Actions)
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

func catalogHasPluginID(entries []pluginCatalogEntry, pluginID string) bool {
	for _, entry := range entries {
		if entry.ID == pluginID {
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
		"plugin_id": "io.dirextalk.ops",
	})
	mustHandle[map[string]any](t, service, "plugins.enable", map[string]any{
		"plugin_id": "io.dirextalk.ops",
	})

	op := runner.operations[len(runner.operations)-1]
	if op.Env["DIREXTALK_BASE_URL"] != "http://message-server:8008" {
		t.Fatalf("expected configured internal backend URL, got %#v", op.Env)
	}
}

func TestPluginInvokeIsOwnerOnlyAndCallsEnabledOfficialPlugin(t *testing.T) {
	runner := &recordingPluginRunner{}
	service := NewService(Config{ServerName: "example.com", PluginRunner: runner})
	router := newP2PTestRouter(service)

	mustHandle[map[string]any](t, service, "plugins.install", map[string]any{
		"plugin_id": "io.dirextalk.ops",
	})
	mustHandle[map[string]any](t, service, "plugins.enable", map[string]any{
		"plugin_id": "io.dirextalk.ops",
	})

	agentReq := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "plugins.invoke",
		"params": map[string]any{
			"plugin_id": "io.dirextalk.ops",
			"action":    "ops.status.get",
			"params":    map[string]any{},
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
			"plugin_id": "io.dirextalk.ops",
			"action":    "ops.status.get",
			"params":    map[string]any{},
		},
	})
	ownerReq.Header.Set("Authorization", "Bearer "+service.AccessToken())
	ownerRec := httptest.NewRecorder()
	router.ServeHTTP(ownerRec, ownerReq)
	if ownerRec.Code != http.StatusOK {
		t.Fatalf("expected owner invoke to succeed, got %d body=%s", ownerRec.Code, ownerRec.Body.String())
	}
	if len(runner.invokes) != 1 || runner.invokes[0].Action != "ops.status.get" || runner.invokes[0].PluginID != "io.dirextalk.ops" {
		t.Fatalf("expected ops plugin invoke runner call, got %#v", runner.invokes)
	}
	result := decodeJSONMap(t, ownerRec.Body.String())
	if result["plugin_id"] != "io.dirextalk.ops" || result["action"] != "ops.status.get" {
		t.Fatalf("expected invoke envelope, got %#v", result)
	}
}

func TestNativeAgentBuiltInDirextalkToolsUseServiceCapabilities(t *testing.T) {
	transport := &recordingTransport{roomMembers: []memberRecord{
		{RoomID: "!group:example.com", UserID: "@owner:example.com", DisplayName: "Owner", Membership: "join", Role: "owner"},
	}}
	service := NewServiceWithTransport(Config{ServerName: "example.com", NativeAgentDataDir: t.TempDir()}, transport)
	service.SetMatrixMessageReader(&fakeMCPMessageReader{messages: []mcpMessageSummary{
		{EventID: "$native-agent-message", OriginServerTS: 1710000000000, CreatedAt: "2024-03-09T16:00:00Z", Sender: "Alice", SenderMXID: "@alice:example.com", Msg: "hello from db reader"},
	}})
	if err := service.saveContact(context.Background(), contactRecord{
		PeerMXID:    "@alice:example.com",
		DisplayName: "Alice",
		Domain:      "example.com",
		RoomID:      "!room:example.com",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}
	mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Agent Test Group",
	})
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":       "agent-channel",
		"room_id":          "!channel:example.com",
		"name":             "Agent Channel",
		"channel_type":     "post",
		"comments_enabled": true,
	})
	post := mustHandle[channelPostRecord](t, service, "channels.posts.create", map[string]any{
		"channel_id": ch.ChannelID,
		"body":       "channel post body",
	})

	contacts := nativeAgentToolResult(t, service, "agent.contacts.list", map[string]any{})["contacts"].([]mcpContactSummary)
	if len(contacts) != 1 || contacts[0].DisplayName != "Alice" {
		t.Fatalf("expected contacts through native agent, got %#v", contacts)
	}
	rooms := nativeAgentToolResult(t, service, "agent.rooms.search", map[string]any{"query": "Agent Test"})["rooms"].([]mcpRoomSummary)
	if len(rooms) != 1 || rooms[0].RoomID != "!group:example.com" {
		t.Fatalf("expected rooms through native agent, got %#v", rooms)
	}
	messages := nativeAgentToolResult(t, service, "agent.messages.list", map[string]any{"room_id": "!room:example.com", "from_time": "2024-03-09T15:59:00Z"})["messages"].([]mcpMessageSummary)
	if len(messages) != 1 || messages[0].Msg != "hello from db reader" {
		t.Fatalf("expected messages through native agent reader, got %#v", messages)
	}
	members := nativeAgentToolResult(t, service, "agent.room_members.list", map[string]any{"room_id": "!group:example.com"})["members"].([]mcpMemberSummary)
	if len(members) != 1 || members[0].DisplayName != "Owner" {
		t.Fatalf("expected members through native agent, got %#v", members)
	}
	posts := nativeAgentToolResult(t, service, "agent.channel_posts.list", map[string]any{"room_id": ch.RoomID})["posts"].([]mcpPostSummary)
	if len(posts) != 1 || posts[0].Msg != "channel post body" {
		t.Fatalf("expected channel posts through native agent, got %#v", posts)
	}
	nativeAgentToolResult(t, service, "agent.channel_comments.create", map[string]any{"post_id": post.PostID, "msg": "comment body"})
	comments := nativeAgentToolResult(t, service, "agent.channel_comments.list", map[string]any{"post_id": post.PostID})["comments"].([]mcpCommentSummary)
	if len(comments) != 1 || comments[0].Msg != "comment body" {
		t.Fatalf("expected channel comments through native agent, got %#v", comments)
	}
	send := nativeAgentToolResult(t, service, "agent.messages.send", map[string]any{"room_id": "!room:example.com", "msg": "native send", "agent_gateway": true})
	if send["ok"] != true || len(transport.messages) == 0 || transport.messages[len(transport.messages)-1].Content["body"] != "native send" {
		t.Fatalf("expected send through transport, result=%#v messages=%#v", send, transport.messages)
	}
	summary := nativeAgentToolResult(t, service, "agent.summarize", map[string]any{"text": "one two three"})["summary"]
	if !strings.Contains(summary.(string), "one two three") {
		t.Fatalf("expected summarize through native agent, got %#v", summary)
	}
}

func nativeAgentToolResult(t *testing.T, service *Service, action string, params map[string]any) map[string]any {
	t.Helper()
	result := mustHandle[map[string]any](t, service, action, params)
	return result
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
