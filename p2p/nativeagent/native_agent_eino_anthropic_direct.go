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

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

const anthropicVersion = "2023-06-01"

type anthropicDirectChatModel struct {
	r       *Runtime
	profile nativeModelProfile
	tools   []*schema.ToolInfo
}

func newAnthropicDirectChatModel(r *Runtime, profile nativeModelProfile) model.ToolCallingChatModel {
	return &anthropicDirectChatModel{r: r, profile: profile}
}

func (m *anthropicDirectChatModel) Generate(ctx context.Context, input []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	payload, err := m.requestPayload(input, false)
	if err != nil {
		return nil, err
	}
	decoded, err := m.post(ctx, payload)
	if err != nil {
		return nil, err
	}
	return anthropicDirectMessageFromResponse(decoded), nil
}

func (m *anthropicDirectChatModel) Stream(ctx context.Context, input []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	payload, err := m.requestPayload(input, true)
	if err != nil {
		return nil, err
	}
	req, err := m.newRequest(ctx, payload)
	if err != nil {
		return nil, err
	}
	resp, err := m.r.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		return nil, fmt.Errorf("model provider returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	reader, writer := schema.Pipe[*schema.Message](8)
	go func() {
		defer writer.Close()
		defer resp.Body.Close()
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 1024), 4<<20)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data == "" || data == "[DONE]" {
				continue
			}
			if msg := anthropicDirectMessageFromStreamEvent([]byte(data)); msg != nil {
				writer.Send(msg, nil)
			}
		}
		if err := scanner.Err(); err != nil {
			writer.Send(nil, err)
		}
	}()
	return reader, nil
}

func (m *anthropicDirectChatModel) WithTools(tools []*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	clone := *m
	clone.tools = append([]*schema.ToolInfo{}, tools...)
	return &clone, nil
}

func (m *anthropicDirectChatModel) requestPayload(input []*schema.Message, stream bool) (map[string]any, error) {
	system, messages := anthropicDirectMessages(input)
	payload := map[string]any{
		"model":      m.profile.Model,
		"messages":   messages,
		"max_tokens": fallbackInt(m.profile.MaxOutputTokens, 2048),
	}
	if stream {
		payload["stream"] = true
	}
	if system != "" {
		payload["system"] = system
	}
	if m.profile.Temperature != nil {
		payload["temperature"] = *m.profile.Temperature
	}
	if m.profile.TopP != nil {
		payload["top_p"] = *m.profile.TopP
	}
	if len(m.tools) > 0 {
		payload["tools"] = anthropicDirectTools(m.tools)
	}
	return payload, nil
}

func (m *anthropicDirectChatModel) post(ctx context.Context, payload map[string]any) (map[string]any, error) {
	req, err := m.newRequest(ctx, payload)
	if err != nil {
		return nil, err
	}
	resp, err := m.r.client.Do(req)
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

func (m *anthropicDirectChatModel) newRequest(ctx context.Context, payload map[string]any) (*http.Request, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	url := strings.TrimRight(m.profile.BaseURL, "/") + "/v1/messages"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", m.profile.APIKey)
	req.Header.Set("anthropic-version", anthropicVersion)
	return req, nil
}
