package nativeagent

import (
	"encoding/json"
	"strings"

	"github.com/cloudwego/eino/schema"
)

const nativeAgentTracePreviewRunes = 1200

func buildAgentTrace(run nativeAgentRunContext, produced []*schema.Message, toolCalls []map[string]any, finalText string) map[string]any {
	steps := make([]map[string]any, 0, len(produced)+2)
	if run.conversationID != "" {
		contextStep := map[string]any{
			"type":            "context",
			"conversation_id": run.conversationID,
			"memory_enabled":  !run.memoryDisabled,
			"memory_messages": len(run.memory.Messages),
			"input_messages":  len(run.inputMessages),
			"summary_used":    strings.TrimSpace(run.memory.Summary) != "",
		}
		if strings.TrimSpace(run.memory.Summary) != "" {
			contextStep["summary_preview"] = previewText(run.memory.Summary, nativeAgentTracePreviewRunes)
		}
		steps = append(steps, contextStep)
	}
	for _, message := range produced {
		if message == nil {
			continue
		}
		switch message.Role {
		case schema.Assistant:
			for _, call := range message.ToolCalls {
				var args map[string]any
				_ = json.Unmarshal([]byte(call.Function.Arguments), &args)
				if args == nil {
					args = map[string]any{}
				}
				steps = append(steps, map[string]any{
					"type":        "tool_call",
					"id":          call.ID,
					"name":        call.Function.Name,
					"arguments":   args,
					"raw_args":    call.Function.Arguments,
					"description": "Model requested a tool call.",
				})
			}
			if strings.TrimSpace(message.Content) != "" {
				steps = append(steps, map[string]any{
					"type": "assistant_message",
					"text": previewText(message.Content, nativeAgentTracePreviewRunes),
				})
			}
		case schema.Tool:
			content := strings.TrimSpace(message.Content)
			steps = append(steps, map[string]any{
				"type":         "tool_result",
				"name":         fallbackString(message.ToolName, "tool"),
				"tool_call_id": message.ToolCallID,
				"ok":           !strings.Contains(strings.ToLower(content), `"error"`),
				"output":       previewText(content, nativeAgentTracePreviewRunes),
			})
		}
	}
	finalText = strings.TrimSpace(finalText)
	if finalText != "" && !traceAlreadyHasFinal(steps, finalText) {
		steps = append(steps, map[string]any{
			"type": "final",
			"text": previewText(finalText, nativeAgentTracePreviewRunes),
		})
	}
	return map[string]any{
		"version":    1,
		"framework":  "eino",
		"disclaimer": "This trace shows observable planning, tool calls, tool results, and final output. It does not expose hidden model chain-of-thought.",
		"context": map[string]any{
			"conversation_id": run.conversationID,
			"memory_enabled":  !run.memoryDisabled,
			"memory_messages": len(run.memory.Messages),
			"input_messages":  len(run.inputMessages),
			"summary_used":    strings.TrimSpace(run.memory.Summary) != "",
		},
		"tool_calls": toolCalls,
		"steps":      steps,
		"final": map[string]any{
			"text": finalText,
		},
	}
}

func traceAlreadyHasFinal(steps []map[string]any, finalText string) bool {
	finalText = strings.TrimSpace(finalText)
	if finalText == "" || len(steps) == 0 {
		return false
	}
	last := steps[len(steps)-1]
	if last["type"] != "assistant_message" {
		return false
	}
	return strings.TrimSpace(trimString(last["text"])) == previewText(finalText, nativeAgentTracePreviewRunes)
}

func previewText(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit <= 0 {
		return value
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit]) + "...(truncated)"
}
