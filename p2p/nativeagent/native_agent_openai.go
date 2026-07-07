package nativeagent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func (r *Runtime) completeOpenAICompatible(ctx context.Context, profile nativeModelProfile, messages []map[string]any, systemPrompt string, tools []Tool) (string, []map[string]any, error) {
	openAIMessages := make([]map[string]any, 0, len(messages)+1)
	if systemPrompt != "" {
		openAIMessages = append(openAIMessages, map[string]any{"role": "system", "content": systemPrompt})
	}
	openAIMessages = append(openAIMessages, messages...)
	var calls []map[string]any
	for i := 0; i <= nativeAgentToolCallLimit; i++ {
		body := map[string]any{
			"model":    profile.Model,
			"messages": openAIMessages,
			"stream":   false,
		}
		if profile.Temperature != nil {
			body["temperature"] = *profile.Temperature
		}
		if profile.TopP != nil {
			body["top_p"] = *profile.TopP
		}
		if profile.MaxOutputTokens > 0 {
			body["max_tokens"] = profile.MaxOutputTokens
		}
		if len(tools) > 0 {
			body["tools"] = openAIToolDefinitions(tools)
			body["tool_choice"] = "auto"
		}
		decoded, err := r.postJSON(ctx, openAIChatCompletionsURL(profile), profile.APIKey, body)
		if err != nil {
			return "", calls, err
		}
		message := firstOpenAIMessage(decoded)
		toolCalls := openAIToolCalls(message)
		if len(toolCalls) == 0 {
			return trimString(message["content"]), calls, nil
		}
		calls = append(calls, toolCalls...)
		openAIMessages = append(openAIMessages, message)
		for _, call := range toolCalls {
			name, args, callID := openAIToolCallParts(call)
			result, err := r.callTool(ctx, tools, name, args)
			content := jsonValue(map[string]any{"result": result})
			if err != nil {
				content = jsonValue(map[string]any{"error": err.Error()})
			}
			openAIMessages = append(openAIMessages, map[string]any{
				"role":         "tool",
				"tool_call_id": callID,
				"content":      content,
			})
		}
	}
	return "", calls, fmt.Errorf("native agent exceeded tool call limit")
}

func (r *Runtime) streamOpenAICompatible(ctx context.Context, profile nativeModelProfile, messages []map[string]any, systemPrompt string, emit func(Event) error) (string, error) {
	openAIMessages := make([]map[string]any, 0, len(messages)+1)
	if systemPrompt != "" {
		openAIMessages = append(openAIMessages, map[string]any{"role": "system", "content": systemPrompt})
	}
	openAIMessages = append(openAIMessages, messages...)
	body := map[string]any{
		"model":    profile.Model,
		"messages": openAIMessages,
		"stream":   true,
	}
	if profile.Temperature != nil {
		body["temperature"] = *profile.Temperature
	}
	if profile.TopP != nil {
		body["top_p"] = *profile.TopP
	}
	if profile.MaxOutputTokens > 0 {
		body["max_tokens"] = profile.MaxOutputTokens
	}
	data, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, openAIChatCompletionsURL(profile), bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer "+profile.APIKey)
	resp, err := r.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return "", fmt.Errorf("model provider returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var full strings.Builder
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" {
			break
		}
		var decoded map[string]any
		if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
			continue
		}
		delta := openAIDeltaText(decoded)
		if delta == "" {
			continue
		}
		full.WriteString(delta)
		if err := emit(Event{Event: "delta", Data: map[string]any{"text": delta}}); err != nil {
			return "", err
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	text := full.String()
	return text, emit(Event{Event: "done", Data: map[string]any{
		"ok":       true,
		"native":   true,
		"provider": profile.Provider,
		"model":    profile.Model,
		"text":     text,
	}})
}

func openAIChatCompletionsURL(profile nativeModelProfile) string {
	base := strings.TrimRight(profile.BaseURL, "/")
	if base == "" {
		base = defaultBaseURLForProvider(profile.Provider)
	}
	if strings.HasSuffix(base, "/chat/completions") {
		return base
	}
	switch profile.Provider {
	case "deepseek":
		return base + "/chat/completions"
	case "openai":
		return base + "/chat/completions"
	default:
		if strings.HasSuffix(base, "/v1") {
			return base + "/chat/completions"
		}
		return base + "/v1/chat/completions"
	}
}

func firstOpenAIMessage(decoded map[string]any) map[string]any {
	choices, _ := decoded["choices"].([]any)
	if len(choices) == 0 {
		return map[string]any{}
	}
	choice, _ := choices[0].(map[string]any)
	message, _ := choice["message"].(map[string]any)
	if message == nil {
		return map[string]any{}
	}
	return message
}

func openAIToolCalls(message map[string]any) []map[string]any {
	rawCalls, _ := message["tool_calls"].([]any)
	calls := make([]map[string]any, 0, len(rawCalls))
	for _, raw := range rawCalls {
		call, ok := raw.(map[string]any)
		if ok {
			calls = append(calls, call)
		}
	}
	return calls
}

func openAIToolCallParts(call map[string]any) (string, map[string]any, string) {
	callID := trimString(call["id"])
	function, _ := call["function"].(map[string]any)
	name := trimString(function["name"])
	var args map[string]any
	_ = json.Unmarshal([]byte(trimString(function["arguments"])), &args)
	if args == nil {
		args = map[string]any{}
	}
	return name, args, callID
}

func openAIDeltaText(decoded map[string]any) string {
	choices, _ := decoded["choices"].([]any)
	if len(choices) == 0 {
		return ""
	}
	choice, _ := choices[0].(map[string]any)
	delta, _ := choice["delta"].(map[string]any)
	return trimString(delta["content"])
}

func openAIToolDefinitions(tools []Tool) []map[string]any {
	defs := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		defs = append(defs, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        tool.Name,
				"description": tool.Description,
				"parameters":  tool.Parameters,
			},
		})
	}
	return defs
}
