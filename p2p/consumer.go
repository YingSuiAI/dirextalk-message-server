package p2p

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"

	roomserverAPI "github.com/YingSuiAI/direxio-message-server/roomserver/api"
	"github.com/YingSuiAI/direxio-message-server/setup/config"
	"github.com/YingSuiAI/direxio-message-server/setup/jetstream"
	"github.com/YingSuiAI/direxio-message-server/setup/process"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

type OutputRoomEventConsumer struct {
	ctx                context.Context
	jetstream          nats.JetStreamContext
	topic              string
	durable            string
	service            *Service
	projectOutputEvent func(context.Context, roomserverAPI.OutputEvent) error
	metrics            *p2pProjectorConsumerMetrics
	batchSize          int
}

func NewOutputRoomEventConsumer(process *process.ProcessContext, cfg *config.JetStream, js nats.JetStreamContext, service *Service) *OutputRoomEventConsumer {
	return &OutputRoomEventConsumer{
		ctx:                process.Context(),
		jetstream:          js,
		topic:              cfg.Prefixed(jetstream.OutputRoomEvent),
		durable:            cfg.Durable("P2POutputRoomEventConsumer"),
		service:            service,
		projectOutputEvent: service.ProjectOutputEvent,
		metrics:            defaultP2PProjectorConsumerMetrics,
		batchSize:          p2pProjectorBatchSizeFromEnv(),
	}
}

func (c *OutputRoomEventConsumer) Start() error {
	batchSize := c.batchSize
	if batchSize <= 0 {
		batchSize = 1
	}
	return jetstream.JetStreamConsumer(
		c.ctx, c.jetstream, c.topic, c.durable, batchSize,
		c.onMessage, nats.DeliverAll(), nats.ManualAck(),
	)
}

func (c *OutputRoomEventConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	for _, msg := range msgs {
		if !c.processMessage(ctx, msg) {
			return false
		}
	}
	return true
}

func (c *OutputRoomEventConsumer) processMessage(ctx context.Context, msg *nats.Msg) bool {
	c.metrics.recordReceived(msg)
	var output roomserverAPI.OutputEvent
	if err := json.Unmarshal(msg.Data, &output); err != nil {
		c.metrics.recordDiscarded()
		logrus.WithError(err).Warn("P2P projector ignored invalid roomserver output event")
		return true
	}
	projectOutputEvent := c.projectOutputEvent
	if projectOutputEvent == nil && c.service != nil {
		projectOutputEvent = c.service.ProjectOutputEvent
	}
	if projectOutputEvent == nil {
		c.metrics.recordFailed()
		logrus.Warn("P2P projector has no output event handler")
		return false
	}
	if err := projectOutputEvent(ctx, output); err != nil {
		c.metrics.recordFailed()
		logrus.WithError(err).Warn("P2P projector failed to process roomserver output event")
		return false
	}
	c.metrics.recordProcessed()
	return true
}

func p2pProjectorBatchSizeFromEnv() int {
	value := strings.TrimSpace(os.Getenv("P2P_PROJECTOR_BATCH_SIZE"))
	if value == "" {
		return 1
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		logrus.WithField("value", value).Warn("Ignoring invalid P2P_PROJECTOR_BATCH_SIZE value")
		return 1
	}
	if parsed > 100 {
		return 100
	}
	return parsed
}
