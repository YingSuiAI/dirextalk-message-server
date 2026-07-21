package realtimews

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/plugins"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/nativeagent"
)

type pluginStreamPortStub struct {
	started chan struct{}
	once    sync.Once
}

func (p *pluginStreamPortStub) PrepareStream(_ context.Context, params map[string]any) (plugins.PreparedStream, *actionbase.Error) {
	return plugins.PreparedStream{
		PluginID: actionbase.String(params["plugin_id"]),
		Action:   actionbase.String(params["action"]),
	}, nil
}

func (p *pluginStreamPortStub) RunStream(
	ctx context.Context,
	prepared plugins.PreparedStream,
	emit func(plugins.StreamEvent) error,
) error {
	if prepared.Action == "hold" {
		p.once.Do(func() { close(p.started) })
		<-ctx.Done()
		return ctx.Err()
	}
	return emit(plugins.StreamEvent{Event: "delta", Data: map[string]any{"text": "plugin"}})
}

type agentStreamPortStub struct {
	action string
}

func (p *agentStreamPortStub) Stream(
	_ context.Context,
	action string,
	_ map[string]any,
	emit func(nativeagent.Event) error,
) error {
	p.action = action
	return emit(nativeagent.Event{Event: "delta", Data: map[string]any{"text": "agent"}})
}

func TestPluginAndAgentStreamsPreserveFramesAndSharedIDNamespace(t *testing.T) {
	pluginPort := &pluginStreamPortStub{started: make(chan struct{})}
	agentPort := &agentStreamPortStub{}
	module := New(Dependencies{Plugins: pluginPort, Agent: agentPort}, Config{})
	connection := newConnection("session", Ticket{Role: "owner"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	module.startPluginStream(ctx, connection, map[string]any{
		"id": "plugin-happy", "plugin_id": "io.dirextalk.ops", "action": "status",
	})
	pluginDelta := nextOutbound(t, connection)
	pluginDone := nextOutbound(t, connection)
	if pluginDelta["type"] != "server.plugin_stream.event" || pluginDelta["event"] != "delta" || pluginDone["event"] != "done" {
		t.Fatalf("plugin frames = %#v / %#v", pluginDelta, pluginDone)
	}

	module.startNativeAgentStream(ctx, connection, map[string]any{
		"id": "agent-happy", "action": "agent.chat", "params": map[string]any{"prompt": "hello"},
	})
	agentDelta := nextOutbound(t, connection)
	agentDone := nextOutbound(t, connection)
	if agentDelta["type"] != "server.native_agent_stream.event" || agentDelta["event"] != "delta" || agentDone["event"] != "done" {
		t.Fatalf("agent frames = %#v / %#v", agentDelta, agentDone)
	}
	if agentPort.action != "agent.chat.stream" {
		t.Fatalf("agent stream action = %q", agentPort.action)
	}

	module.startPluginStream(ctx, connection, map[string]any{
		"id": "shared", "plugin_id": "io.dirextalk.ops", "action": "hold",
	})
	select {
	case <-pluginPort.started:
	case <-time.After(time.Second):
		t.Fatal("blocking plugin stream did not start")
	}
	module.startNativeAgentStream(ctx, connection, map[string]any{"id": "shared", "action": "agent.chat"})
	conflict := nextOutbound(t, connection)
	if conflict["type"] != "server.native_agent_stream.error" || conflict["status"] != http.StatusConflict {
		t.Fatalf("shared ID conflict = %#v", conflict)
	}
	module.cancelPluginStream(connection, map[string]any{"id": "shared"})
	cancelled := nextOutbound(t, connection)
	if cancelled["type"] != "server.plugin_stream.cancelled" || cancelled["ok"] != true {
		t.Fatalf("cancelled frame = %#v", cancelled)
	}
}

func TestNativeAgentVoiceStreamActionIsAllowed(t *testing.T) {
	agentPort := &agentStreamPortStub{}
	module := New(Dependencies{Agent: agentPort}, Config{})
	connection := newConnection("session", Ticket{Role: "owner"})
	module.startNativeAgentStream(context.Background(), connection, map[string]any{
		"id": "voice", "action": "agent.voice.session.stream", "params": map[string]any{"session_id": "voice_1"},
	})
	delta := nextOutbound(t, connection)
	done := nextOutbound(t, connection)
	if delta["type"] != "server.native_agent_stream.event" || done["event"] != "done" {
		t.Fatalf("voice frames = %#v / %#v", delta, done)
	}
	if agentPort.action != "agent.voice.session.stream" {
		t.Fatalf("voice stream action = %q", agentPort.action)
	}
}

func nextOutbound(t *testing.T, connection *connection) map[string]any {
	t.Helper()
	select {
	case frame := <-connection.outbound:
		return frame
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for outbound frame")
		return nil
	}
}
