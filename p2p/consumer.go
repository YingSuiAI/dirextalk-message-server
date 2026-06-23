package p2p

import (
	"context"
	"encoding/json"

	roomserverAPI "github.com/YingSuiAI/direxio-message-server/roomserver/api"
	"github.com/YingSuiAI/direxio-message-server/setup/config"
	"github.com/YingSuiAI/direxio-message-server/setup/jetstream"
	"github.com/YingSuiAI/direxio-message-server/setup/process"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

type OutputRoomEventConsumer struct {
	ctx       context.Context
	jetstream nats.JetStreamContext
	topic     string
	durable   string
	service   *Service
}

func NewOutputRoomEventConsumer(process *process.ProcessContext, cfg *config.JetStream, js nats.JetStreamContext, service *Service) *OutputRoomEventConsumer {
	return &OutputRoomEventConsumer{
		ctx:       process.Context(),
		jetstream: js,
		topic:     cfg.Prefixed(jetstream.OutputRoomEvent),
		durable:   cfg.Durable("P2POutputRoomEventConsumer"),
		service:   service,
	}
}

func (c *OutputRoomEventConsumer) Start() error {
	return jetstream.JetStreamConsumer(
		c.ctx, c.jetstream, c.topic, c.durable, 1,
		c.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

func (c *OutputRoomEventConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0]
	var output roomserverAPI.OutputEvent
	if err := json.Unmarshal(msg.Data, &output); err != nil {
		logrus.WithError(err).Warn("P2P projector ignored invalid roomserver output event")
		return true
	}
	if err := c.service.ProjectOutputEvent(ctx, output); err != nil {
		logrus.WithError(err).Warn("P2P projector failed to process roomserver output event")
		return false
	}
	return true
}
