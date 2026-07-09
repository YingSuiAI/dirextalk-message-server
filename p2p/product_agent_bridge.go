package p2p

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	productAgentGatewaySource     = "product-agent"
	agentActionResultSchema       = "direxio.agent_action_result.v1"
	agentActionResultContentKey   = "io.direxio.agent_action_result"
	agentActionResultHideBodyKey  = "io.direxio.agent_hide_body"
	agentActionResultFallbackBody = "Agent card"
)

type ProductAgentClient interface {
	HandleMessage(ctx context.Context, req ProductAgentMessageRequest) (ProductAgentMessageResponse, error)
	ListMemory(ctx context.Context, conversationID string) (ProductAgentMemoryListResponse, error)
	SaveMemory(ctx context.Context, req ProductAgentMemorySaveRequest) (ProductAgentMemoryItemResponse, error)
	DeleteMemory(ctx context.Context, conversationID, id string) (ProductAgentMemoryDeleteResponse, error)
}

type ProductAgentMessageRequest struct {
	NodeID           string         `json:"node_id"`
	RoomID           string         `json:"room_id"`
	ConversationType string         `json:"conversation_type"`
	SenderID         string         `json:"sender_id,omitempty"`
	SenderKind       string         `json:"sender_kind,omitempty"`
	Content          string         `json:"content"`
	AgentConfig      map[string]any `json:"agent_config,omitempty"`
}

type ProductAgentMessageResponse struct {
	Ignored         bool                         `json:"ignored,omitempty"`
	Reason          string                       `json:"reason,omitempty"`
	Reply           string                       `json:"reply,omitempty"`
	OutboundMessage *ProductAgentOutboundMessage `json:"outbound_message,omitempty"`
	Error           *ProductAgentError           `json:"error,omitempty"`
}

type ProductAgentOutboundMessage struct {
	ConversationID string `json:"conversation_id,omitempty"`
	Content        string `json:"content,omitempty"`
}

type ProductAgentError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type ProductAgentMemoryItem struct {
	ID             string   `json:"id,omitempty"`
	OwnerID        string   `json:"ownerId,omitempty"`
	ConversationID string   `json:"conversationId,omitempty"`
	Type           string   `json:"type,omitempty"`
	Text           string   `json:"text,omitempty"`
	Tags           []string `json:"tags,omitempty"`
	Source         string   `json:"source,omitempty"`
	CreatedAt      string   `json:"createdAt,omitempty"`
	UpdatedAt      string   `json:"updatedAt,omitempty"`
	DeletedAt      string   `json:"deletedAt,omitempty"`
}

type ProductAgentMemoryListResponse struct {
	Schema string                   `json:"schema,omitempty"`
	Items  []ProductAgentMemoryItem `json:"items,omitempty"`
	Error  *ProductAgentError       `json:"error,omitempty"`
}

type ProductAgentMemorySaveRequest struct {
	ConversationID string   `json:"conversation_id"`
	Text           string   `json:"text"`
	Type           string   `json:"type,omitempty"`
	Tags           []string `json:"tags,omitempty"`
	Source         string   `json:"source,omitempty"`
}

type ProductAgentMemoryItemResponse struct {
	Schema string                  `json:"schema,omitempty"`
	Item   *ProductAgentMemoryItem `json:"item,omitempty"`
	Error  *ProductAgentError      `json:"error,omitempty"`
}

type ProductAgentMemoryDeleteResponse struct {
	Schema  string             `json:"schema,omitempty"`
	ID      string             `json:"id,omitempty"`
	Deleted bool               `json:"deleted,omitempty"`
	Error   *ProductAgentError `json:"error,omitempty"`
}

type httpProductAgentClient struct {
	baseURL string
	client  *http.Client
}

/**
 * Function: Builds the optional product-agent bridge client from service config.
 * Inputs:
 * - cfg: Service configuration, including test-injected ProductAgent and URL.
 * Output:
 * - A ProductAgentClient, or nil when the bridge is not configured.
 * Side effects:
 * - Reads DIREXIO_PRODUCT_AGENT_URL when cfg.ProductAgentURL is empty.
 * Errors:
 * - None; malformed URLs are handled later as request errors.
 */
func productAgentClientFromConfig(cfg Config) ProductAgentClient {
	if cfg.ProductAgent != nil {
		return cfg.ProductAgent
	}
	baseURL := strings.TrimSpace(cfg.ProductAgentURL)
	if baseURL == "" {
		baseURL = strings.TrimSpace(os.Getenv("DIREXIO_PRODUCT_AGENT_URL"))
	}
	if baseURL == "" {
		return nil
	}
	return httpProductAgentClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 45 * time.Second},
	}
}

/**
 * Function: Sends one agent-room message event to product-agent.
 * Inputs:
 * - ctx: Request context controlling cancellation.
 * - req: Normalized agent-room message plus optional official Agent plugin config.
 * Output:
 * - Product-agent response containing a reply, ignored marker, or error shape.
 * Side effects:
 * - Performs one HTTP POST to product-agent `/v1/message-server/new-message`.
 * Errors:
 * - Returns network, HTTP status, JSON, or product-agent error responses.
 */
func (c httpProductAgentClient) HandleMessage(ctx context.Context, req ProductAgentMessageRequest) (ProductAgentMessageResponse, error) {
	var result ProductAgentMessageResponse
	if strings.TrimSpace(c.baseURL) == "" {
		return result, errors.New("product-agent URL is empty")
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return result, err
	}
	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.baseURL+"/v1/message-server/new-message",
		bytes.NewReader(payload),
	)
	if err != nil {
		return result, err
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := c.client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return result, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if result.Error != nil && strings.TrimSpace(result.Error.Message) != "" {
			return result, fmt.Errorf("product-agent failed: %s", result.Error.Message)
		}
		return result, fmt.Errorf("product-agent failed: %s", resp.Status)
	}
	if result.Error != nil {
		return result, fmt.Errorf("product-agent failed: %s", strings.TrimSpace(result.Error.Message))
	}
	return result, nil
}

/**
 * Function: Lists saved product-agent memories for one conversation.
 * Inputs:
 * - ctx: Request context controlling cancellation.
 * - conversationID: Product-agent conversation id whose memories should be read.
 * Output:
 * - Product-agent memory list response.
 * Side effects:
 * - Performs one HTTP GET to product-agent `/v1/agent/memory`.
 * Errors:
 * - Returns network, HTTP status, JSON, or product-agent error responses.
 */
func (c httpProductAgentClient) ListMemory(ctx context.Context, conversationID string) (ProductAgentMemoryListResponse, error) {
	var result ProductAgentMemoryListResponse
	if strings.TrimSpace(c.baseURL) == "" {
		return result, errors.New("product-agent URL is empty")
	}
	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		c.baseURL+"/v1/agent/memory?conversation_id="+url.QueryEscape(strings.TrimSpace(conversationID)),
		nil,
	)
	if err != nil {
		return result, err
	}
	if err := c.doJSON(httpReq, &result); err != nil {
		return result, err
	}
	return result, productAgentResponseError(result.Error, "")
}

/**
 * Function: Saves one explicit memory through product-agent.
 * Inputs:
 * - ctx: Request context controlling cancellation.
 * - req: Conversation id plus memory text, type, source, and tags.
 * Output:
 * - Product-agent memory item response.
 * Side effects:
 * - Performs one HTTP POST to product-agent `/v1/agent/memory`.
 * Errors:
 * - Returns network, HTTP status, JSON, or product-agent error responses.
 */
func (c httpProductAgentClient) SaveMemory(ctx context.Context, req ProductAgentMemorySaveRequest) (ProductAgentMemoryItemResponse, error) {
	var result ProductAgentMemoryItemResponse
	if strings.TrimSpace(c.baseURL) == "" {
		return result, errors.New("product-agent URL is empty")
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return result, err
	}
	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.baseURL+"/v1/agent/memory",
		bytes.NewReader(payload),
	)
	if err != nil {
		return result, err
	}
	httpReq.Header.Set("Content-Type", "application/json; charset=utf-8")
	if err := c.doJSON(httpReq, &result); err != nil {
		return result, err
	}
	return result, productAgentResponseError(result.Error, "")
}

/**
 * Function: Deletes one explicit memory through product-agent.
 * Inputs:
 * - ctx: Request context controlling cancellation.
 * - conversationID: Conversation that owns the memory.
 * - id: Memory item id to delete.
 * Output:
 * - Product-agent delete response including whether an item was removed.
 * Side effects:
 * - Performs one HTTP DELETE to product-agent `/v1/agent/memory/:id`.
 * Errors:
 * - Returns network, HTTP status, JSON, or product-agent error responses.
 */
func (c httpProductAgentClient) DeleteMemory(ctx context.Context, conversationID, id string) (ProductAgentMemoryDeleteResponse, error) {
	var result ProductAgentMemoryDeleteResponse
	if strings.TrimSpace(c.baseURL) == "" {
		return result, errors.New("product-agent URL is empty")
	}
	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		c.baseURL+"/v1/agent/memory/"+url.PathEscape(strings.TrimSpace(id))+"?conversation_id="+url.QueryEscape(strings.TrimSpace(conversationID)),
		nil,
	)
	if err != nil {
		return result, err
	}
	if err := c.doJSON(httpReq, &result); err != nil {
		return result, err
	}
	return result, productAgentResponseError(result.Error, "")
}

/**
 * Function: Executes a product-agent HTTP request and decodes a JSON response.
 * Inputs:
 * - httpReq: Prepared request including method, URL, headers, and body.
 * - result: Pointer to the typed response destination.
 * Output:
 * - Populates result when product-agent returns valid JSON.
 * Side effects:
 * - Performs one HTTP request through the configured client.
 * Errors:
 * - Returns network, HTTP status, or JSON decoding failures.
 */
func (c httpProductAgentClient) doJSON(httpReq *http.Request, result any) error {
	httpReq.Header.Set("Accept", "application/json")
	client := c.client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if err := productAgentResponseError(productAgentErrorFromResult(result), resp.Status); err != nil {
			return err
		}
		return productAgentResponseError(nil, resp.Status)
	}
	return nil
}

/**
 * Function: Converts product-agent error envelopes into Go errors.
 * Inputs:
 * - productError: Optional structured product-agent error.
 * - fallback: HTTP status or generic message used when no structured message exists.
 * Output:
 * - nil for successful envelopes, otherwise an error suitable for a 502 bridge response.
 * Side effects:
 * - None.
 * Errors:
 * - None; this function creates error values instead of throwing.
 */
func productAgentResponseError(productError *ProductAgentError, fallback string) error {
	if productError == nil {
		if strings.TrimSpace(fallback) == "" {
			return nil
		}
		return fmt.Errorf("product-agent failed: %s", strings.TrimSpace(fallback))
	}
	message := strings.TrimSpace(productError.Message)
	if message == "" {
		message = strings.TrimSpace(productError.Code)
	}
	if message == "" {
		message = strings.TrimSpace(fallback)
	}
	if message == "" {
		message = "unknown product-agent error"
	}
	return fmt.Errorf("product-agent failed: %s", message)
}

/**
 * Function: Reads the shared product-agent error field from typed response pointers.
 * Inputs:
 * - result: Pointer passed to doJSON for a known product-agent response type.
 * Output:
 * - ProductAgentError when the decoded response contains one, otherwise nil.
 * Side effects:
 * - None.
 * Errors:
 * - Unknown response shapes return nil and fall back to HTTP status handling.
 */
func productAgentErrorFromResult(result any) *ProductAgentError {
	switch typed := result.(type) {
	case *ProductAgentMemoryListResponse:
		return typed.Error
	case *ProductAgentMemoryItemResponse:
		return typed.Error
	case *ProductAgentMemoryDeleteResponse:
		return typed.Error
	default:
		return nil
	}
}

/**
 * Function: Builds Matrix message body and optional structured card content for a product-agent reply.
 * Inputs:
 * - response: Product-agent response from one handled user message.
 * Output:
 * - Plain Matrix body plus content fields that Flutter can render as a visual Agent card.
 * Side effects:
 * - None.
 * Errors:
 * - None.
 */
func productAgentReplyMatrixPayload(response ProductAgentMessageResponse) (string, map[string]any) {
	if response.OutboundMessage != nil {
		if content := strings.TrimSpace(response.OutboundMessage.Content); content != "" {
			if card, ok := productAgentActionResultCard(content); ok {
				body := firstNonEmptyString(response.Reply, productAgentActionResultSummary(card), agentActionResultFallbackBody)
				return body, map[string]any{
					agentActionResultContentKey:  card,
					agentActionResultHideBodyKey: true,
				}
			}
			return content, nil
		}
	}
	return strings.TrimSpace(response.Reply), nil
}

func productAgentActionResultCard(raw string) (map[string]any, bool) {
	var parsed map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &parsed); err != nil {
		return nil, false
	}
	if strings.TrimSpace(fmt.Sprint(parsed["schema"])) != agentActionResultSchema {
		return nil, false
	}
	return parsed, true
}

func productAgentActionResultSummary(card map[string]any) string {
	return firstNonEmptyString(
		stringFromAny(card["summary"]),
		stringFromAny(card["title"]),
	)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func stringFromAny(value any) string {
	if stringValue, ok := value.(string); ok {
		return stringValue
	}
	return ""
}
