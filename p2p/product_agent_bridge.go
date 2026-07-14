package p2p

import (
	"context"
	"strings"
	"time"

	productagentmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/productagent"
	"github.com/sirupsen/logrus"
)

const productAgentGatewaySource = "product-agent"

func newProductAgentModule(cfg Config, service *Service) *productagentmodule.Module {
	baseURL := strings.TrimSpace(cfg.ProductAgentURL)
	if baseURL == "" || service == nil {
		return nil
	}
	client, err := productagentmodule.NewHTTPClient(baseURL, cfg.ProductAgentHTTPClient, 0)
	if err != nil {
		logrus.WithError(err).Warn("Product Agent bridge is disabled")
		return nil
	}
	return productagentmodule.New(productagentmodule.Config{
		NodeID: service.serverName,
		Client: client,
		Sender: serviceProductAgentSender{service: service},
	})
}

type serviceProductAgentSender struct {
	service *Service
}

func (s serviceProductAgentSender) SendProductAgentReply(ctx context.Context, reply productagentmodule.Reply) error {
	if s.service == nil {
		return nil
	}
	s.service.mu.Lock()
	transport := s.service.transport
	agentMXID := s.service.agentMXIDLocked()
	s.service.mu.Unlock()
	if transport == nil {
		return nil
	}
	content := map[string]any{
		"msgtype":                    "m.text",
		"body":                       strings.TrimSpace(reply.Body),
		AgentGatewayContentKey:       true,
		AgentGatewaySourceContentKey: productAgentGatewaySource,
	}
	for key, value := range reply.Fields {
		content[key] = value
	}
	_, err := transport.SendMessage(ctx, SendMessageRequest{
		SenderMXID:  agentMXID,
		RoomID:      strings.TrimSpace(reply.RoomID),
		EventType:   "m.room.message",
		MessageType: "text",
		Content:     content,
		Timestamp:   time.Now().UTC(),
	})
	return err
}
