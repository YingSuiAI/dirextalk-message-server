package productagent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	actionResultSchema       = "direxio.agent_action_result.v1"
	actionResultContentKey   = "io.direxio.agent_action_result"
	actionResultHideBodyKey  = "io.direxio.agent_hide_body"
	actionResultFallbackBody = "Agent card"
	maxClaimedTurns          = 2048
)

type Message struct {
	RoomID      string
	EventID     string
	SenderMXID  string
	Body        string
	AgentConfig map[string]any
}

type Reply struct {
	RoomID string
	Body   string
	Fields map[string]any
}

type Sender interface {
	SendProductAgentReply(context.Context, Reply) error
}

type Config struct {
	NodeID          string
	Client          Client
	Sender          Sender
	RequestTimeout  time.Duration
	DeliveryTimeout time.Duration
	Dispatch        func(func())
}

type Module struct {
	config Config
	mu     sync.Mutex
	turns  map[string]time.Time
}

func New(config Config) *Module {
	if config.Client == nil || config.Sender == nil {
		return nil
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 50 * time.Second
	}
	if config.DeliveryTimeout <= 0 {
		config.DeliveryTimeout = 10 * time.Second
	}
	if config.Dispatch == nil {
		config.Dispatch = func(run func()) { go run() }
	}
	return &Module{config: config, turns: make(map[string]time.Time)}
}

// Handle accepts a normalized owner message and returns without blocking the projector.
func (m *Module) Handle(_ context.Context, message Message) {
	if m == nil || strings.TrimSpace(message.RoomID) == "" || strings.TrimSpace(message.SenderMXID) == "" || strings.TrimSpace(message.Body) == "" {
		return
	}
	if !m.claim(message.EventID) {
		return
	}
	m.config.Dispatch(func() { m.dispatch(message) })
}

func (m *Module) dispatch(message Message) {
	requestContext, cancelRequest := context.WithTimeout(context.Background(), m.config.RequestTimeout)
	response, err := m.config.Client.HandleMessage(requestContext, MessageRequest{
		NodeID:           strings.TrimSpace(m.config.NodeID),
		RoomID:           strings.TrimSpace(message.RoomID),
		ConversationID:   strings.TrimSpace(message.RoomID),
		ConversationType: "agent",
		SenderID:         strings.TrimSpace(message.SenderMXID),
		SenderKind:       "user",
		MessageID:        strings.TrimSpace(message.EventID),
		Content:          strings.TrimSpace(message.Body),
		AgentConfig:      cloneMap(message.AgentConfig),
	})
	cancelRequest()
	if err != nil {
		logrus.WithError(err).Warn("Product Agent bridge request failed")
		return
	}
	if response.Ignored {
		return
	}
	body, fields := ReplyPayload(response)
	if body == "" && len(fields) == 0 {
		return
	}
	deliveryContext, cancelDelivery := context.WithTimeout(context.Background(), m.config.DeliveryTimeout)
	err = m.config.Sender.SendProductAgentReply(deliveryContext, Reply{
		RoomID: strings.TrimSpace(message.RoomID),
		Body:   body,
		Fields: fields,
	})
	cancelDelivery()
	if err != nil {
		logrus.WithError(err).Warn("Product Agent bridge reply delivery failed")
	}
}

func (m *Module) claim(eventID string) bool {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return true
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.turns[eventID]; exists {
		return false
	}
	m.turns[eventID] = time.Now().UTC()
	for len(m.turns) > maxClaimedTurns {
		var oldestID string
		var oldest time.Time
		for id, seenAt := range m.turns {
			if oldestID == "" || seenAt.Before(oldest) {
				oldestID, oldest = id, seenAt
			}
		}
		delete(m.turns, oldestID)
	}
	return true
}

func ReplyPayload(response MessageResponse) (string, map[string]any) {
	if response.OutboundMessage != nil {
		content := strings.TrimSpace(response.OutboundMessage.Content)
		if content != "" {
			if card, ok := actionResultCard(content); ok {
				body := firstNonEmpty(response.Reply, text(card["summary"]), text(card["title"]), actionResultFallbackBody)
				return body, map[string]any{
					actionResultContentKey:  card,
					actionResultHideBodyKey: true,
				}
			}
			return content, nil
		}
	}
	return strings.TrimSpace(response.Reply), nil
}

func actionResultCard(raw string) (map[string]any, bool) {
	var card map[string]any
	if json.Unmarshal([]byte(raw), &card) != nil || strings.TrimSpace(fmt.Sprint(card["schema"])) != actionResultSchema {
		return nil, false
	}
	return card, true
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func text(value any) string {
	if result, ok := value.(string); ok {
		return result
	}
	return ""
}

func cloneMap(values map[string]any) map[string]any {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
