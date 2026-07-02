package p2p

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	productAgentConversationType      = "agent"
	productAgentGatewaySource         = "product-agent"
	defaultProductAgentRequestTimeout = 45 * time.Second
	productAgentUnavailableMessage    = "Direxio AI is temporarily unavailable. Please try again later."
)

type productAgentDispatch struct {
	RoomID    string
	EventID   string
	SenderID  string
	Body      string
	CreatedAt int64
}

type productAgentNewMessageRequest struct {
	NodeID           string `json:"node_id"`
	ConversationID   string `json:"conversation_id"`
	RoomID           string `json:"room_id"`
	ConversationType string `json:"conversation_type"`
	SenderID         string `json:"sender_id,omitempty"`
	SenderKind       string `json:"sender_kind"`
	Content          string `json:"content"`
	Task             string `json:"task,omitempty"`
	Model            string `json:"model,omitempty"`
	MessageID        string `json:"message_id,omitempty"`
	OriginServerTS   int64  `json:"origin_server_ts,omitempty"`
}

type productAgentMessageResponse struct {
	Ignored         bool   `json:"ignored"`
	Reason          string `json:"reason"`
	Reply           string `json:"reply"`
	OutboundMessage struct {
		ConversationID string `json:"conversation_id"`
		Content        string `json:"content"`
	} `json:"outbound_message"`
	Error *productAgentResponseError `json:"error"`
}

type productAgentResponseError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func normalizedProductAgentURL(value string) string {
	return strings.TrimRight(strings.TrimSpace(value), "/")
}

func productAgentHTTPClient(cfg Config) *http.Client {
	if cfg.ProductAgentHTTPClient != nil {
		return cfg.ProductAgentHTTPClient
	}
	timeout := cfg.ProductAgentRequestTimeout
	if timeout <= 0 {
		timeout = defaultProductAgentRequestTimeout
	}
	return &http.Client{Timeout: timeout}
}

func (s *Service) productAgentBridgeOnlineLocked() bool {
	return s.agentConfig.Enabled && strings.TrimSpace(s.productAgentURL) != ""
}

func (s *Service) dispatchProductAgentMessage(ctx context.Context, dispatch productAgentDispatch) error {
	dispatch.Body = strings.TrimSpace(dispatch.Body)
	dispatch.RoomID = strings.TrimSpace(dispatch.RoomID)
	if dispatch.RoomID == "" || dispatch.Body == "" {
		return nil
	}

	s.mu.Lock()
	if !s.productAgentBridgeOnlineLocked() {
		s.mu.Unlock()
		return nil
	}
	baseURL := s.productAgentURL
	client := s.productAgentHTTPClient
	transport := s.transport
	agentMXID := s.agentMXIDLocked()
	nodeID := s.serverName
	model := strings.TrimSpace(s.agentConfig.Model)
	s.mu.Unlock()

	if transport == nil {
		return errors.New("matrix transport is unavailable")
	}

	request := productAgentNewMessageRequest{
		NodeID:           nodeID,
		ConversationID:   dispatch.RoomID,
		RoomID:           dispatch.RoomID,
		ConversationType: productAgentConversationType,
		SenderID:         dispatch.SenderID,
		SenderKind:       "user",
		Content:          dispatch.Body,
		Task:             "chat",
		Model:            model,
		MessageID:        dispatch.EventID,
		OriginServerTS:   dispatch.CreatedAt,
	}
	response, err := callProductAgent(ctx, client, baseURL, request)
	replyText := strings.TrimSpace(response.replyText())
	if err != nil {
		replyText = productAgentUnavailableMessage
	}
	if replyText == "" {
		return err
	}
	if _, sendErr := transport.SendMessage(ctx, SendMessageRequest{
		SenderMXID:  agentMXID,
		RoomID:      dispatch.RoomID,
		MessageType: "text",
		Timestamp:   time.Now().UTC(),
		Content: map[string]any{
			"msgtype":                    "m.text",
			"body":                       replyText,
			AgentGatewayContentKey:       true,
			AgentGatewaySourceContentKey: productAgentGatewaySource,
		},
	}); sendErr != nil {
		return sendErr
	}
	return err
}

func callProductAgent(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	payload productAgentNewMessageRequest,
) (productAgentMessageResponse, error) {
	if client == nil {
		client = productAgentHTTPClient(Config{})
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return productAgentMessageResponse{}, err
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		normalizedProductAgentURL(baseURL)+"/v1/message-server/new-message",
		bytes.NewReader(body),
	)
	if err != nil {
		return productAgentMessageResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return productAgentMessageResponse{}, err
	}
	defer func() {
		_ = res.Body.Close()
	}()

	var response productAgentMessageResponse
	if res.Body != nil {
		decodeErr := json.NewDecoder(io.LimitReader(res.Body, 1024*1024)).Decode(&response)
		if decodeErr != nil && !errors.Is(decodeErr, io.EOF) {
			return productAgentMessageResponse{}, decodeErr
		}
	}
	if res.StatusCode >= 200 && res.StatusCode < 300 {
		return response, nil
	}
	if strings.TrimSpace(response.replyText()) != "" {
		return response, nil
	}
	return response, fmt.Errorf("product agent returned status %d", res.StatusCode)
}

func (r productAgentMessageResponse) replyText() string {
	if r.Ignored {
		return ""
	}
	if text := strings.TrimSpace(r.OutboundMessage.Content); text != "" {
		return text
	}
	if text := strings.TrimSpace(r.Reply); text != "" {
		return text
	}
	if r.Error != nil {
		return strings.TrimSpace(r.Error.Message)
	}
	return ""
}
