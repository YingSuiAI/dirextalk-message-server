package p2p

import (
	"context"
	"fmt"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/nativeagent"
)

const (
	agentPluginID = "io.dirextalk.agent"
	opsPluginID   = "io.dirextalk.ops"
)

type NativeAgentRunner interface {
	Apply(ctx context.Context, action string) error
	Invoke(ctx context.Context, action string, params map[string]any) (map[string]any, error)
	Stream(ctx context.Context, action string, params map[string]any, emit func(nativeagent.Event) error) error
}

func isNativeAgentPlugin(pluginID string) bool {
	return strings.TrimSpace(pluginID) == agentPluginID
}

func dockerPluginRunnerEnabled(r PluginRunner) bool {
	return pluginRunnerEnabled(r)
}

type nativeAgentRuntimeRunner struct {
	runtime *nativeagent.Runtime
}

func newNativeAgentRuntime(service *Service, dataDir string) NativeAgentRunner {
	return nativeAgentRuntimeRunner{
		runtime: nativeagent.New(nativeagent.Config{
			DataDir: dataDir,
			Store:   nativeAgentConfigStore{service: service},
			Tools:   nativeAgentTools(service),
		}),
	}
}

func (s *Service) nativeAgentInvokeAction(action string) actionHandler {
	return func(ctx context.Context, params map[string]any) (any, *apiError) {
		if s.nativeAgentRunner == nil {
			return nil, statusError(502, "native agent runtime is not configured")
		}
		result, err := s.nativeAgentRunner.Invoke(ctx, strings.TrimSpace(action), cloneAnyMap(params))
		if err != nil {
			return nil, statusError(502, err.Error())
		}
		return result, nil
	}
}

func (s *Service) nativeAgentInvokeStreamAction(context.Context, map[string]any) (any, *apiError) {
	return nil, badRequest("action requires websocket")
}

func (r nativeAgentRuntimeRunner) Apply(ctx context.Context, action string) error {
	if r.runtime == nil {
		return fmt.Errorf("native agent runtime is not configured")
	}
	return r.runtime.Apply(ctx, strings.TrimSpace(action))
}

func (r nativeAgentRuntimeRunner) Invoke(ctx context.Context, action string, params map[string]any) (map[string]any, error) {
	if r.runtime == nil {
		return nil, fmt.Errorf("native agent runtime is not configured")
	}
	return r.runtime.Invoke(ctx, strings.TrimSpace(action), cloneAnyMap(params))
}

func (r nativeAgentRuntimeRunner) Stream(ctx context.Context, action string, params map[string]any, emit func(nativeagent.Event) error) error {
	if r.runtime == nil {
		return fmt.Errorf("native agent runtime is not configured")
	}
	return r.runtime.Stream(ctx, strings.TrimSpace(action), cloneAnyMap(params), emit)
}

type nativeAgentConfigStore struct {
	service *Service
}

func (s nativeAgentConfigStore) Load(ctx context.Context) (map[string]any, bool, error) {
	if s.service == nil {
		return map[string]any{}, false, nil
	}
	s.service.mu.Lock()
	defer s.service.mu.Unlock()
	return agentConfigToNativeMap(s.service.agentConfig), true, nil
}

func (s nativeAgentConfigStore) Save(ctx context.Context, config map[string]any) error {
	if s.service == nil {
		return fmt.Errorf("native agent config store is unavailable")
	}
	s.service.mu.Lock()
	s.service.agentConfig = agentConfigFromNativeMap(s.service.agentConfig, config)
	state := s.service.portalStateLocked()
	s.service.mu.Unlock()
	if store := s.service.portalStore(); store != nil {
		return store.SavePortal(ctx, state)
	}
	return nil
}

func nativeAgentTools(service *Service) []nativeagent.Tool {
	tools := make([]nativeagent.Tool, 0, len(service.dirextalkMCPService().Tools())+1)
	for _, tool := range service.dirextalkMCPService().Tools() {
		tools = append(tools, nativeAgentServiceTool(tool.Name, tool.Description, tool.InputSchema, tool.Write, service.invokeDirextalkMCPAction(tool.Action)))
	}
	tools = append(tools,
		nativeagent.Tool{
			Name:        "dirextalk_summarize",
			Description: "Summarize provided text or room messages.",
			Parameters:  nativeAgentObjectSchema(map[string]any{"room_id": nativeAgentStringSchema(), "text": nativeAgentStringSchema(), "limit": nativeAgentNumberSchema()}),
			Handler: func(ctx context.Context, params map[string]any) (any, error) {
				return nativeAgentSummarize(ctx, service, params)
			},
		},
	)
	return tools
}

func nativeAgentServiceTool(name, description string, schema map[string]any, write bool, handler func(context.Context, map[string]any) (any, *apiError)) nativeagent.Tool {
	return nativeagent.Tool{
		Name:        name,
		Description: description,
		Parameters:  schema,
		Write:       write,
		Handler: func(ctx context.Context, params map[string]any) (any, error) {
			value, apiErr := handler(ctx, params)
			if apiErr != nil {
				return nil, fmt.Errorf("%s", apiErr.Error)
			}
			return value, nil
		},
	}
}

func nativeAgentSummarize(ctx context.Context, service *Service, params map[string]any) (map[string]any, error) {
	text := trimString(params["text"])
	if text == "" && trimString(params["room_id"]) != "" {
		value, apiErr := service.invokeDirextalkMCP(ctx, dirextalkmcp.ActionMessagesList, params)
		if apiErr != nil {
			return nil, fmt.Errorf("%s", apiErr.Error)
		}
		text = jsonValue(value)
	}
	if text == "" {
		return map[string]any{"summary": "", "message": "no content"}, nil
	}
	runes := []rune(strings.Join(strings.Fields(text), " "))
	limit := 500
	if len(runes) < limit {
		limit = len(runes)
	}
	summary := string(runes[:limit])
	if len(runes) > limit {
		summary += "..."
	}
	return map[string]any{"summary": summary, "source_chars": len([]rune(text))}, nil
}

func nativeAgentObjectSchema(properties map[string]any) map[string]any {
	return map[string]any{"type": "object", "properties": properties}
}

func nativeAgentStringSchema() map[string]any { return map[string]any{"type": "string"} }
func nativeAgentNumberSchema() map[string]any { return map[string]any{"type": "number"} }
func nativeAgentBoolSchema() map[string]any   { return map[string]any{"type": "boolean"} }
