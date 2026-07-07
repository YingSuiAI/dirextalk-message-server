package nativeagent

import (
	"context"
	"strings"
)

func (r *Runtime) chat(ctx context.Context, params map[string]any) (map[string]any, error) {
	config, _, err := r.agentConfig(ctx)
	if err != nil {
		return nil, err
	}
	profile := r.resolveModelProfile(config, params)
	if profile.APIKey == "" {
		return map[string]any{
			"ok":          false,
			"native":      true,
			"provider":    profile.Provider,
			"model":       profile.Model,
			"error":       "model_profile.api_key is required",
			"model_ready": false,
		}, nil
	}
	messages, systemPrompt := r.agentMessages(ctx, config, params)
	messages = compactAgentMessages(messages, profile.ContextWindow)
	tools := r.enabledTools(ctx, config, params)
	var text string
	var toolCalls []map[string]any
	switch profile.Provider {
	case "anthropic":
		text, toolCalls, err = r.completeAnthropic(ctx, profile, messages, systemPrompt, tools)
	default:
		text, toolCalls, err = r.completeOpenAICompatible(ctx, profile, messages, systemPrompt, tools)
	}
	if err != nil {
		return nil, err
	}
	r.rememberTurn(ctx, config, params, text)
	return map[string]any{
		"ok":         true,
		"native":     true,
		"provider":   profile.Provider,
		"model":      profile.Model,
		"text":       text,
		"tool_calls": toolCalls,
	}, nil
}

func (r *Runtime) agentMessages(ctx context.Context, config map[string]any, params map[string]any) ([]map[string]any, string) {
	systemPrompt := strings.TrimSpace(pluginConfigString(config, "system_prompt"))
	if requestPrompt := trimString(params["system_prompt"]); requestPrompt != "" {
		if systemPrompt != "" {
			systemPrompt += "\n\n"
		}
		systemPrompt += requestPrompt
	}
	if skillsPrompt := r.enabledSkillsPrompt(ctx, config); skillsPrompt != "" {
		if systemPrompt != "" {
			systemPrompt += "\n\n"
		}
		systemPrompt += skillsPrompt
	}
	if memorySummary, memoryMessages := r.memoryContext(ctx, config, params); memorySummary != "" || len(memoryMessages) > 0 {
		if memorySummary != "" {
			if systemPrompt != "" {
				systemPrompt += "\n\n"
			}
			systemPrompt += memorySummary
		}
		messages := make([]map[string]any, 0, len(memoryMessages)+8)
		messages = append(messages, memoryMessages...)
		return r.appendRequestMessages(messages, params), systemPrompt
	}
	messages := make([]map[string]any, 0, 8)
	return r.appendRequestMessages(messages, params), systemPrompt
}

func (r *Runtime) appendRequestMessages(messages []map[string]any, params map[string]any) []map[string]any {
	if history, ok := params["messages"].([]any); ok {
		for _, raw := range history {
			message, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			role := fallbackString(trimString(message["role"]), "user")
			content := trimString(message["content"])
			if content == "" {
				content = trimString(message["text"])
			}
			if content != "" {
				messages = append(messages, map[string]any{"role": role, "content": content})
			}
		}
	}
	prompt := fallbackString(trimString(params["prompt"]), trimString(params["message"]))
	if prompt != "" {
		messages = append(messages, map[string]any{"role": "user", "content": prompt})
	}
	if len(messages) == 0 {
		messages = append(messages, map[string]any{"role": "user", "content": "你好"})
	}
	return messages
}

func compactAgentMessages(messages []map[string]any, contextWindow int) []map[string]any {
	if contextWindow <= 0 || len(messages) <= contextWindow {
		return messages
	}
	return append([]map[string]any{}, messages[len(messages)-contextWindow:]...)
}
