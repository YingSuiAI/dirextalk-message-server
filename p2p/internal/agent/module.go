// Package agent owns Native Agent runtime actions and their MCP tool bridge.
package agent

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/nativeagent"
)

// Runner is the stable Native Agent runtime boundary exposed by p2p.Config.
type Runner interface {
	Apply(context.Context, string) error
	Invoke(context.Context, string, map[string]any) (map[string]any, error)
	Stream(context.Context, string, map[string]any, func(nativeagent.Event) error) error
}

// Config contains the runtime dependencies owned outside the Agent module.
type Config struct {
	Runner  Runner
	DataDir string
	Store   nativeagent.ConfigStore
	MCP     *dirextalkmcp.Service
	Account AccountPort
}

// Module owns runtime-backed ProductCore actions and streaming invocation.
type Module struct {
	runner  Runner
	account AccountPort
	voice   *voiceCoordinator
}

func New(cfg Config) *Module {
	runner := cfg.Runner
	if runner == nil {
		runner = runtimeRunner{runtime: nativeagent.New(nativeagent.Config{
			DataDir: cfg.DataDir,
			Store:   cfg.Store,
			Tools:   Tools(cfg.MCP),
		})}
	}
	return &Module{runner: runner, account: cfg.Account, voice: newVoiceCoordinator(voiceConfigFromEnv())}
}

// Handlers returns the complete Agent ProductCore action surface.
func (m *Module) Handlers() map[string]actionbase.Handler {
	handlers := make(map[string]actionbase.Handler, len(runtimeActions)+9)
	for _, action := range runtimeActions {
		handlers[action] = m.invoke(action)
	}
	handlers[actionPassword] = m.accountPassword
	handlers[actionMatrixSessionCreate] = m.createMatrixSession
	handlers[actionConfigGet] = m.getConfig
	handlers[actionConfigUpdate] = m.updateConfig
	handlers["agent.chat.stream"] = streamOnly
	handlers["agent.voice.session.create"] = m.createVoiceSession
	handlers["agent.voice.session.interrupt"] = m.interruptVoiceSession
	handlers["agent.voice.session.end"] = m.endVoiceSession
	handlers["agent.voice.session.stream"] = streamOnly
	return handlers
}

// Stream invokes a runtime streaming action after the websocket adapter has
// established its connection-scoped cancellation and frame writer.
func (m *Module) Stream(ctx context.Context, action string, params map[string]any, emit func(nativeagent.Event) error) error {
	if m == nil || m.runner == nil {
		return fmt.Errorf("native agent runtime is not configured")
	}
	action = strings.TrimSpace(action)
	if action == "agent.voice.session.stream" {
		if m.voice == nil {
			return fmt.Errorf("native agent voice service is not configured")
		}
		return m.voice.stream(ctx, params, emit)
	}
	return m.runner.Stream(ctx, action, cloneMap(params), emit)
}

func (m *Module) invoke(action string) actionbase.Handler {
	return func(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
		if m == nil || m.runner == nil {
			return nil, actionbase.StatusError(http.StatusBadGateway, "native agent runtime is not configured")
		}
		result, err := m.runner.Invoke(ctx, strings.TrimSpace(action), cloneMap(params))
		if err != nil {
			return nil, actionbase.StatusError(http.StatusBadGateway, err.Error())
		}
		return result, nil
	}
}

func streamOnly(context.Context, map[string]any) (any, *actionbase.Error) {
	return nil, actionbase.BadRequest("action requires websocket")
}

type runtimeRunner struct {
	runtime *nativeagent.Runtime
}

func (r runtimeRunner) Apply(ctx context.Context, action string) error {
	if r.runtime == nil {
		return fmt.Errorf("native agent runtime is not configured")
	}
	return r.runtime.Apply(ctx, strings.TrimSpace(action))
}

func (r runtimeRunner) Invoke(ctx context.Context, action string, params map[string]any) (map[string]any, error) {
	if r.runtime == nil {
		return nil, fmt.Errorf("native agent runtime is not configured")
	}
	return r.runtime.Invoke(ctx, strings.TrimSpace(action), cloneMap(params))
}

func (r runtimeRunner) Stream(ctx context.Context, action string, params map[string]any, emit func(nativeagent.Event) error) error {
	if r.runtime == nil {
		return fmt.Errorf("native agent runtime is not configured")
	}
	return r.runtime.Stream(ctx, strings.TrimSpace(action), cloneMap(params), emit)
}

var runtimeActions = []string{
	"agent.config.propose_patch",
	"agent.chat",
	"agent.context.compress",
	"agent.models.list",
	"agent.runtime.inspect",
	"agent.runtime.install",
	"agent.runtime.which",
	"agent.runtime.run",
	"agent.skills.list",
	"agent.skills.install",
	"agent.skills.enable",
	"agent.skills.disable",
	"agent.skills.uninstall",
	"agent.skills.registry.search",
	"agent.mcp.servers.list",
	"agent.mcp.servers.install",
	"agent.mcp.servers.enable",
	"agent.mcp.servers.disable",
	"agent.mcp.servers.uninstall",
	"agent.mcp.registry.search",
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
	"agent.contacts.list",
	"agent.contacts.search",
	"agent.rooms.search",
	"agent.messages.list",
	"agent.messages.send",
	"agent.room_members.list",
	"agent.channel_posts.list",
	"agent.channel_comments.list",
	"agent.channel_comments.create",
	"agent.summarize",
}
