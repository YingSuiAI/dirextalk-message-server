// Package productagent owns the optional HTTP bridge to Direxio Product Agent.
package productagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultMaxResponseBytes int64 = 1 << 20

type MessageRequest struct {
	NodeID           string         `json:"node_id"`
	RoomID           string         `json:"room_id"`
	ConversationID   string         `json:"conversation_id"`
	ConversationType string         `json:"conversation_type"`
	SenderID         string         `json:"sender_id"`
	SenderKind       string         `json:"sender_kind"`
	MessageID        string         `json:"message_id,omitempty"`
	Content          string         `json:"content"`
	AgentConfig      map[string]any `json:"agent_config,omitempty"`
	AgentAction      map[string]any `json:"agent_action,omitempty"`
}

type MessageResponse struct {
	Accepted        bool             `json:"accepted,omitempty"`
	TaskID          string           `json:"task_id,omitempty"`
	Status          string           `json:"status,omitempty"`
	Ignored         bool             `json:"ignored,omitempty"`
	Reason          string           `json:"reason,omitempty"`
	Reply           string           `json:"reply,omitempty"`
	OutboundMessage *OutboundMessage `json:"outbound_message,omitempty"`
	Error           *ResponseError   `json:"error,omitempty"`
}

type OutboundMessage struct {
	ConversationID string `json:"conversation_id,omitempty"`
	Content        string `json:"content,omitempty"`
}

type ResponseError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type Client interface {
	HandleMessage(context.Context, MessageRequest) (MessageResponse, error)
}

type HTTPClient struct {
	baseURL          string
	client           *http.Client
	maxResponseBytes int64
}

func NewHTTPClient(baseURL string, client *http.Client, maxResponseBytes int64) (*HTTPClient, error) {
	baseURL = strings.TrimSpace(baseURL)
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
		return nil, errors.New("product-agent URL must be an absolute HTTP(S) URL without credentials")
	}
	if client == nil {
		client = &http.Client{Timeout: 45 * time.Second}
	}
	if maxResponseBytes <= 0 {
		maxResponseBytes = defaultMaxResponseBytes
	}
	return &HTTPClient{
		baseURL:          strings.TrimRight(baseURL, "/"),
		client:           client,
		maxResponseBytes: maxResponseBytes,
	}, nil
}

func (c *HTTPClient) HandleMessage(ctx context.Context, request MessageRequest) (MessageResponse, error) {
	var result MessageResponse
	payload, err := json.Marshal(request)
	if err != nil {
		return result, fmt.Errorf("encode product-agent request: %w", err)
	}
	httpRequest, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.baseURL+"/v1/message-server/new-message",
		bytes.NewReader(payload),
	)
	if err != nil {
		return result, fmt.Errorf("build product-agent request: %w", err)
	}
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("Content-Type", "application/json; charset=utf-8")

	response, err := c.client.Do(httpRequest)
	if err != nil {
		return result, fmt.Errorf("product-agent request failed: %w", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(io.LimitReader(response.Body, c.maxResponseBytes+1))
	if err != nil {
		return result, errors.New("read product-agent response")
	}
	if int64(len(body)) > c.maxResponseBytes {
		return result, errors.New("product-agent response exceeded size limit")
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return result, errors.New("product-agent returned invalid JSON")
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return result, responseFailure(result.Error, response.StatusCode)
	}
	if result.Error != nil {
		return result, responseFailure(result.Error, response.StatusCode)
	}
	return result, nil
}

func responseFailure(productError *ResponseError, status int) error {
	code := "product_agent_failed"
	if productError != nil && strings.TrimSpace(productError.Code) != "" {
		code = strings.TrimSpace(productError.Code)
	}
	return fmt.Errorf("product-agent request failed (%s, HTTP %d)", code, status)
}
