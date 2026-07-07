package nativeagent

import (
	"context"
	"strings"

	"github.com/cloudwego/eino/schema"
)

type einoAgentSession struct {
	systemPrompt  string
	contextWindow int
}

func (s einoAgentSession) rewrite(_ context.Context, messages []*schema.Message) []*schema.Message {
	return compactEinoMessages(messages, s.contextWindow)
}

func (s einoAgentSession) modify(_ context.Context, messages []*schema.Message) []*schema.Message {
	systemPrompt := strings.TrimSpace(s.systemPrompt)
	if systemPrompt == "" {
		return messages
	}
	result := make([]*schema.Message, 0, len(messages)+1)
	result = append(result, schema.SystemMessage(systemPrompt))
	result = append(result, messages...)
	return result
}

func requestEinoMessages(params map[string]any) []*schema.Message {
	result := make([]*schema.Message, 0, 8)
	if history, ok := params["messages"].([]any); ok {
		for _, raw := range history {
			message, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if msg := mapToEinoMessage(message); msg != nil {
				result = append(result, msg)
			}
		}
	}
	prompt := fallbackString(trimString(params["prompt"]), trimString(params["message"]))
	if prompt != "" {
		result = append(result, schema.UserMessage(prompt))
	}
	if len(result) == 0 {
		result = append(result, schema.UserMessage("你好"))
	}
	return result
}

func mapToEinoMessage(message map[string]any) *schema.Message {
	content := trimString(message["content"])
	if content == "" {
		content = trimString(message["text"])
	}
	if content == "" {
		return nil
	}
	switch strings.ToLower(trimString(message["role"])) {
	case "system":
		return schema.SystemMessage(content)
	case "assistant":
		return schema.AssistantMessage(content, nil)
	case "tool":
		return schema.ToolMessage(content, trimString(message["tool_call_id"]), schema.WithToolName(trimString(message["name"])))
	default:
		return schema.UserMessage(content)
	}
}

func compactEinoMessages(messages []*schema.Message, contextWindow int) []*schema.Message {
	if contextWindow <= 0 {
		contextWindow = 48
	}
	if len(messages) <= contextWindow {
		return messages
	}
	result := make([]*schema.Message, 0, contextWindow+1)
	if messages[0] != nil && messages[0].Role == schema.System {
		result = append(result, messages[0])
	}
	result = append(result, messages[len(messages)-contextWindow:]...)
	return result
}

func cloneEinoMessages(messages []*schema.Message) []*schema.Message {
	if len(messages) == 0 {
		return nil
	}
	result := make([]*schema.Message, 0, len(messages))
	for _, message := range messages {
		if message == nil {
			continue
		}
		clone := *message
		if len(message.ToolCalls) > 0 {
			clone.ToolCalls = append([]schema.ToolCall{}, message.ToolCalls...)
		}
		if len(message.Extra) > 0 {
			clone.Extra = cloneAnyMap(message.Extra)
		}
		result = append(result, &clone)
	}
	return result
}

func trimEinoMessageForMemory(message *schema.Message) *schema.Message {
	if message == nil {
		return nil
	}
	switch message.Role {
	case schema.System:
		return nil
	case schema.User, schema.Assistant, schema.Tool:
	default:
		return nil
	}
	clone := *message
	clone.ResponseMeta = nil
	clone.Extra = nil
	if clone.Role == schema.Assistant && len(clone.ToolCalls) == 0 && strings.TrimSpace(clone.Content) == "" {
		return nil
	}
	if clone.Role != schema.Assistant && strings.TrimSpace(clone.Content) == "" && len(clone.UserInputMultiContent) == 0 {
		return nil
	}
	if len(clone.ToolCalls) > 0 {
		clone.ToolCalls = append([]schema.ToolCall{}, clone.ToolCalls...)
	}
	return &clone
}

func compactEinoMessagesForMemory(messages []*schema.Message) []*schema.Message {
	result := make([]*schema.Message, 0, len(messages))
	for _, message := range messages {
		if msg := trimEinoMessageForMemory(message); msg != nil {
			result = append(result, msg)
		}
	}
	return result
}

func einoMessagesToSummary(messages []*schema.Message) string {
	parts := make([]string, 0, len(messages))
	for _, message := range messages {
		if message == nil {
			continue
		}
		content := strings.TrimSpace(message.Content)
		switch message.Role {
		case schema.Assistant:
			if len(message.ToolCalls) > 0 {
				names := make([]string, 0, len(message.ToolCalls))
				for _, call := range message.ToolCalls {
					names = append(names, call.Function.Name)
				}
				parts = append(parts, "assistant tool_call: "+strings.Join(names, ", "))
			}
			if content != "" {
				parts = append(parts, "assistant: "+content)
			}
		case schema.Tool:
			toolName := fallbackString(message.ToolName, "tool")
			if content != "" {
				parts = append(parts, toolName+": "+content)
			}
		case schema.User:
			if content != "" {
				parts = append(parts, "user: "+content)
			}
		}
	}
	return strings.Join(parts, "\n")
}

func hasExplicitRequestMessages(params map[string]any) bool {
	values, ok := params["messages"].([]any)
	return ok && len(values) > 0
}
