package agentclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type P2PRoute string

const (
	P2PQuery   P2PRoute = "query"
	P2PCommand P2PRoute = "command"
)

type Client struct {
	cfg  Config
	http *http.Client
}

func New(cfg Config, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{cfg: cfg, http: httpClient}
}

func (c *Client) CallP2PAction(ctx context.Context, action string, params map[string]any, route P2PRoute) (map[string]any, error) {
	action = strings.TrimSpace(action)
	if action == "" {
		return nil, fmt.Errorf("action is required")
	}
	if params == nil {
		params = map[string]any{}
	}
	path := string(route)
	if path == "" {
		path = string(P2PCommand)
	}
	body, err := json.Marshal(map[string]any{"action": action, "params": params})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.P2PBaseURL()+"/"+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.cfg.AgentToken)
	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = res.Body.Close()
	}()
	return decodeObjectResponse(res, "p2p "+action)
}

func decodeObjectResponse(res *http.Response, label string) (map[string]any, error) {
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	var payload map[string]any
	if len(bytes.TrimSpace(data)) != 0 {
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, fmt.Errorf("%s returned invalid json with status %d", label, res.StatusCode)
		}
	} else {
		payload = map[string]any{}
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		msg, _ := payload["error"].(string)
		if msg == "" {
			msg = strings.TrimSpace(string(data))
		}
		return nil, fmt.Errorf("%s failed with %d: %s", label, res.StatusCode, msg)
	}
	return payload, nil
}
