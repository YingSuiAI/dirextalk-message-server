package nativeagent

import (
	"testing"

	"github.com/cloudwego/eino/schema"
)

func TestGeminiNativeContentsPreserveToolCallAndResponse(t *testing.T) {
	assistant := schema.AssistantMessage("", []schema.ToolCall{{
		ID:   "call-1",
		Type: "function",
		Function: schema.FunctionCall{
			Name:      "lookup_contact",
			Arguments: `{"name":"Alice"}`,
		},
	}})
	toolResult := &schema.Message{
		Role:       schema.Tool,
		ToolCallID: "call-1",
		Content:    `{"mxid":"@alice:example.com"}`,
	}

	system, contents := geminiDirectContents([]*schema.Message{
		schema.SystemMessage("system prompt"),
		schema.UserMessage("find Alice"),
		assistant,
		toolResult,
	})
	if system != "system prompt" || len(contents) != 3 {
		t.Fatalf("unexpected Gemini contents system=%q contents=%#v", system, contents)
	}
	assistantParts := contents[1]["parts"].([]map[string]any)
	functionCall := assistantParts[0]["functionCall"].(map[string]any)
	if functionCall["name"] != "lookup_contact" {
		t.Fatalf("unexpected function call: %#v", functionCall)
	}
	toolParts := contents[2]["parts"].([]map[string]any)
	functionResponse := toolParts[0]["functionResponse"].(map[string]any)
	if functionResponse["name"] != "lookup_contact" {
		t.Fatalf("tool name was not recovered from call id: %#v", functionResponse)
	}
	response := functionResponse["response"].(map[string]any)
	if response["mxid"] != "@alice:example.com" {
		t.Fatalf("unexpected function response: %#v", functionResponse)
	}
}
