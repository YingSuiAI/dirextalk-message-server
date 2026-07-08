package p2p

import (
	"context"
	"fmt"
	"strings"

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
	if s.service.store != nil {
		return s.service.store.SavePortal(ctx, state)
	}
	return nil
}

func nativeAgentTools(service *Service) []nativeagent.Tool {
	return []nativeagent.Tool{
		nativeAgentServiceTool("dirextalk_contacts_list", "List Dirextalk contacts.", nativeAgentObjectSchema(map[string]any{"query": nativeAgentStringSchema(), "limit": nativeAgentNumberSchema()}), service.mcpContactsList),
		nativeAgentServiceTool("dirextalk_contacts_search", "Search Dirextalk contacts.", nativeAgentObjectSchema(map[string]any{"query": nativeAgentStringSchema(), "limit": nativeAgentNumberSchema()}), service.mcpContactsSearch),
		nativeAgentServiceTool("dirextalk_rooms_search", "Search Dirextalk rooms, groups, channels, or contacts.", nativeAgentObjectSchema(map[string]any{"query": nativeAgentStringSchema(), "type": nativeAgentStringSchema(), "limit": nativeAgentNumberSchema()}), service.mcpRoomsSearch),
		nativeAgentServiceTool("dirextalk_messages_list", "List ordinary messages in an allowed room with optional RFC3339 UTC time range and cursor pagination.", nativeAgentObjectSchema(map[string]any{"room_id": nativeAgentStringSchema(), "from_time": nativeAgentStringSchema(), "to_time": nativeAgentStringSchema(), "cursor": nativeAgentStringSchema(), "limit": nativeAgentNumberSchema()}), service.mcpMessagesList),
		nativeAgentServiceTool("dirextalk_messages_send", "Send a Matrix message through Dirextalk transport.", nativeAgentObjectSchema(map[string]any{"room_id": nativeAgentStringSchema(), "msg": nativeAgentStringSchema(), "agent_gateway": nativeAgentBoolSchema()}), service.mcpMessagesSend),
		nativeAgentServiceTool("dirextalk_room_members_list", "List room members.", nativeAgentObjectSchema(map[string]any{"room_id": nativeAgentStringSchema(), "channel_id": nativeAgentStringSchema(), "status": nativeAgentStringSchema(), "role": nativeAgentStringSchema(), "limit": nativeAgentNumberSchema()}), service.mcpRoomMembersList),
		nativeAgentServiceTool("dirextalk_channel_posts_list", "List channel posts with optional RFC3339 UTC time range and cursor pagination.", nativeAgentObjectSchema(map[string]any{"room_id": nativeAgentStringSchema(), "from_time": nativeAgentStringSchema(), "to_time": nativeAgentStringSchema(), "cursor": nativeAgentStringSchema(), "limit": nativeAgentNumberSchema()}), service.mcpChannelPostsList),
		nativeAgentServiceTool("dirextalk_channel_comments_list", "List channel comments for a post with optional RFC3339 UTC time range and cursor pagination.", nativeAgentObjectSchema(map[string]any{"post_id": nativeAgentStringSchema(), "from_time": nativeAgentStringSchema(), "to_time": nativeAgentStringSchema(), "cursor": nativeAgentStringSchema(), "limit": nativeAgentNumberSchema()}), service.mcpChannelCommentsList),
		nativeAgentServiceTool("dirextalk_channel_comments_create", "Create a channel comment through Dirextalk transport.", nativeAgentObjectSchema(map[string]any{"post_id": nativeAgentStringSchema(), "msg": nativeAgentStringSchema()}), service.mcpChannelCommentCreate),
		{
			Name:        "dirextalk_summarize",
			Description: "Summarize provided text or room messages.",
			Parameters:  nativeAgentObjectSchema(map[string]any{"room_id": nativeAgentStringSchema(), "text": nativeAgentStringSchema(), "limit": nativeAgentNumberSchema()}),
			Handler: func(ctx context.Context, params map[string]any) (any, error) {
				return nativeAgentSummarize(ctx, service, params)
			},
		},
	}
}

func nativeAgentServiceTool(name, description string, schema map[string]any, handler func(context.Context, map[string]any) (any, *apiError)) nativeagent.Tool {
	return nativeagent.Tool{
		Name:        name,
		Description: description,
		Parameters:  schema,
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
		value, apiErr := service.mcpMessagesList(ctx, params)
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
