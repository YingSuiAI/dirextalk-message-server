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

func (r *Runtime) mcpHTTPListTools(ctx context.Context, server map[string]any) ([]any, error) {
	result, err := r.mcpHTTPRequest(ctx, server, "tools/list", map[string]any{})
	if err != nil {
		return nil, err
	}
	return mcpToolsFromResult(result), nil
}

func (r *Runtime) mcpHTTPCallTool(ctx context.Context, server map[string]any, toolName string, args map[string]any) (any, error) {
	return r.mcpHTTPRequest(ctx, server, "tools/call", map[string]any{"name": toolName, "arguments": args})
}

func (r *Runtime) mcpHTTPRequest(ctx context.Context, server map[string]any, method string, params map[string]any) (any, error) {
	url := trimString(server["url"])
	if url == "" {
		return nil, fmt.Errorf("mcp url is required")
	}
	payload := map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
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
		return nil, fmt.Errorf("mcp http returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}
	if rawErr, ok := decoded["error"]; ok {
		return nil, fmt.Errorf("mcp error: %v", rawErr)
	}
	return decoded["result"], nil
}
