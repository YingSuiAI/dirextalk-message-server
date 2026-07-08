package nativeagent

import (
	"testing"

	"github.com/cloudwego/eino/schema"
)

func TestSanitizeEinoMessagesDropsOrphanToolMessages(t *testing.T) {
	messages := requestEinoMessages(map[string]any{
		"messages": []any{
			map[string]any{"role": "user", "content": "deploy a node"},
			map[string]any{"role": "tool", "tool_call_id": "call_missing", "name": "runtime__bash", "content": "shell output"},
		},
	})
	if len(messages) != 1 || messages[0].Role != schema.User {
		t.Fatalf("expected orphan tool message to be dropped, got %#v", messages)
	}
}

func TestSanitizeEinoMessagesKeepsMatchedToolMessages(t *testing.T) {
	messages := sanitizeEinoMessagesForModel([]*schema.Message{
		schema.UserMessage("deploy a node"),
		schema.AssistantMessage("", []schema.ToolCall{{
			ID:       "call_runtime",
			Type:     "function",
			Function: schema.FunctionCall{Name: "runtime__bash", Arguments: `{"cmd":"pwd"}`},
		}}),
		schema.ToolMessage("/tmp", "call_runtime", schema.WithToolName("runtime__bash")),
		schema.AssistantMessage("done", nil),
	})
	if len(messages) != 4 || messages[2].Role != schema.Tool || messages[2].ToolCallID != "call_runtime" {
		t.Fatalf("expected matched tool message to be preserved, got %#v", messages)
	}
}

func TestCompactEinoMessagesDropsToolResultWhenCallerWasTrimmed(t *testing.T) {
	messages := compactEinoMessages([]*schema.Message{
		schema.AssistantMessage("", []schema.ToolCall{{
			ID:       "call_old",
			Type:     "function",
			Function: schema.FunctionCall{Name: "runtime__bash", Arguments: `{"cmd":"pwd"}`},
		}}),
		schema.ToolMessage("/tmp", "call_old", schema.WithToolName("runtime__bash")),
		schema.UserMessage("continue"),
	}, 2)
	if len(messages) != 1 || messages[0].Role != schema.User {
		t.Fatalf("expected trimmed orphan tool message to be dropped, got %#v", messages)
	}
}
