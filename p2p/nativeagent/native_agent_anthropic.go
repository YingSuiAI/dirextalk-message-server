package nativeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func (r *Runtime) completeAnthropic(ctx context.Context, profile nativeModelProfile, messages []map[string]any, systemPrompt string, tools []Tool) (string, []map[string]any, error) {
	anthropicMessages := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		role := trimString(message["role"])
		if role != "assistant" {
			role = "user"
		}
		anthropicMessages = append(anthropicMessages, map[string]any{"role": role, "content": trimString(message["content"])})
	}
	body := map[string]any{
		"model":      profile.Model,
		"messages":   anthropicMessages,
		"max_tokens": fallbackInt(profile.MaxOutputTokens, 1024),
	}
	if systemPrompt != "" {
		body["system"] = systemPrompt
	}
	if profile.Temperature != nil {
		body["temperature"] = *profile.Temperature
	}
	if profile.TopP != nil {
		body["top_p"] = *profile.TopP
	}
	if len(tools) > 0 {
		body["tools"] = anthropicToolDefinitions(tools)
	}
	decoded, err := r.postAnthropic(ctx, profile, body)
	if err != nil {
		return "", nil, err
	}
	content, _ := decoded["content"].([]any)
	parts := make([]string, 0, len(content))
	for _, raw := range content {
		part, ok := raw.(map[string]any)
		if !ok || trimString(part["type"]) != "text" {
			continue
		}
		if text := trimString(part["text"]); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, ""), nil, nil
}

func (r *Runtime) postAnthropic(ctx context.Context, profile nativeModelProfile, payload map[string]any) (map[string]any, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	url := strings.TrimRight(profile.BaseURL, "/") + "/v1/messages"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", profile.APIKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("model provider returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func anthropicToolDefinitions(tools []Tool) []map[string]any {
	defs := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		defs = append(defs, map[string]any{
			"name":         tool.Name,
			"description":  tool.Description,
			"input_schema": tool.Parameters,
		})
	}
	return defs
}
