package nativeagent

import (
	"context"
	"fmt"
	"strings"
)

type Tool struct {
	Name        string
	Description string
	Parameters  map[string]any
	Handler     func(context.Context, map[string]any) (any, error)
}

func (r *Runtime) enabledTools(ctx context.Context, config map[string]any, params map[string]any) []Tool {
	selected := stringSliceParam(params["enabled_tools"])
	if len(selected) == 0 {
		selected = stringSliceParam(config["enabled_tools"])
	}
	availableTools := r.availableTools()
	enabled := map[string]bool{}
	if len(selected) == 0 {
		for _, tool := range availableTools {
			if nativeToolWriteAction(tool.Name) {
				continue
			}
			enabled[tool.Name] = true
		}
	} else {
		for _, value := range selected {
			if strings.EqualFold(value, "all") {
				for _, tool := range availableTools {
					enabled[tool.Name] = true
				}
				break
			}
			if name := nativeToolAlias(value); name != "" {
				enabled[name] = true
			}
		}
	}
	tools := make([]Tool, 0, len(availableTools))
	for _, tool := range availableTools {
		if enabled[tool.Name] {
			tools = append(tools, tool)
		}
	}
	return tools
}

func nativeToolAlias(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.ReplaceAll(value, ".", "_")
	value = strings.ReplaceAll(value, "-", "_")
	aliases := map[string]string{
		"contacts_list":                 "dirextalk_contacts_list",
		"search_contacts":               "dirextalk_contacts_search",
		"contacts_search":               "dirextalk_contacts_search",
		"rooms_search":                  "dirextalk_rooms_search",
		"search_rooms":                  "dirextalk_rooms_search",
		"messages_list":                 "dirextalk_messages_list",
		"list_messages":                 "dirextalk_messages_list",
		"messages_send":                 "dirextalk_messages_send",
		"send_message":                  "dirextalk_messages_send",
		"room_members_list":             "dirextalk_room_members_list",
		"channel_posts_list":            "dirextalk_channel_posts_list",
		"channel_comments_list":         "dirextalk_channel_comments_list",
		"channel_comments_create":       "dirextalk_channel_comments_create",
		"summarize":                     "dirextalk_summarize",
		"summarize_conversation":        "dirextalk_summarize",
		"agent_contacts_list":           "dirextalk_contacts_list",
		"agent_contacts_search":         "dirextalk_contacts_search",
		"agent_rooms_search":            "dirextalk_rooms_search",
		"agent_messages_list":           "dirextalk_messages_list",
		"agent_messages_send":           "dirextalk_messages_send",
		"agent_room_members_list":       "dirextalk_room_members_list",
		"agent_channel_posts_list":      "dirextalk_channel_posts_list",
		"agent_channel_comments_list":   "dirextalk_channel_comments_list",
		"agent_channel_comments_create": "dirextalk_channel_comments_create",
		"agent_summarize":               "dirextalk_summarize",
	}
	if strings.HasPrefix(value, "dirextalk_") {
		return value
	}
	return aliases[value]
}

func (r *Runtime) availableTools() []Tool {
	return append([]Tool{}, r.tools...)
}

func nativeToolWriteAction(name string) bool {
	switch strings.TrimSpace(name) {
	case "dirextalk_messages_send", "dirextalk_channel_comments_create":
		return true
	default:
		return false
	}
}

func (r *Runtime) invokeDirectTool(ctx context.Context, action string, params map[string]any) (map[string]any, error) {
	toolName := nativeToolAlias(action)
	if toolName == "" {
		return nil, fmt.Errorf("unknown native agent tool action %q", action)
	}
	result, err := r.callTool(ctx, r.availableTools(), toolName, params)
	if err != nil {
		return nil, err
	}
	return anyToMap(result)
}

func (r *Runtime) summarize(ctx context.Context, params map[string]any) (map[string]any, error) {
	text := trimString(params["text"])
	roomID := trimString(params["room_id"])
	if text == "" && roomID != "" {
		result, err := r.invokeDirectTool(ctx, "agent.messages.list", params)
		if err != nil {
			return nil, err
		}
		text = jsonValue(result["messages"])
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

func (r *Runtime) callTool(ctx context.Context, tools []Tool, name string, args map[string]any) (any, error) {
	for _, tool := range tools {
		if tool.Name == name {
			return tool.Handler(ctx, args)
		}
	}
	return nil, fmt.Errorf("tool %q is not available", name)
}

func objectSchema(properties map[string]any) map[string]any {
	return map[string]any{"type": "object", "properties": properties}
}

func stringSchema() map[string]any { return map[string]any{"type": "string"} }
func numberSchema() map[string]any { return map[string]any{"type": "number"} }
func boolSchema() map[string]any   { return map[string]any{"type": "boolean"} }
