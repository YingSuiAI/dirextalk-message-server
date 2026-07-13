// Package legacygateway owns the bounded Matrix-to-Agent-Gateway compatibility
// boundary. It deliberately uses a separate JetStream durable so gateway
// retries cannot stall the existing product projector.
package legacygateway

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"time"

	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/setup/jetstream"
	"github.com/YingSuiAI/dirextalk-message-server/setup/process"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

const consumerDurable = "P2PLegacyAgentGatewayConsumer"

type OutputRoomEventHandler func(context.Context, roomserverAPI.OutputEvent) error

type OutputRoomEventConsumer struct {
	ctx       context.Context
	jetstream nats.JetStreamContext
	topic     string
	durable   string
	handler   OutputRoomEventHandler
}

func NewOutputRoomEventConsumer(
	processContext *process.ProcessContext,
	cfg *config.JetStream,
	js nats.JetStreamContext,
	handler OutputRoomEventHandler,
) *OutputRoomEventConsumer {
	return &OutputRoomEventConsumer{
		ctx:       processContext.Context(),
		jetstream: js,
		topic:     cfg.Prefixed(jetstream.OutputRoomEvent),
		durable:   cfg.Durable(consumerDurable),
		handler:   handler,
	}
}

func (c *OutputRoomEventConsumer) Start() error {
	return jetstream.JetStreamConsumerWithNakDelay(
		c.ctx,
		c.jetstream,
		c.topic,
		c.durable,
		1,
		c.onMessage,
		legacyGatewayNakDelay,
		nats.DeliverAll(),
		nats.ManualAck(),
	)
}

const (
	legacyGatewayRetryBase = 2 * time.Second
	legacyGatewayRetryMax  = time.Minute
)

func legacyGatewayNakDelay(message *nats.Msg) time.Duration {
	delivery := uint64(1)
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(message.Subject))
	seed := hash.Sum64()
	if metadata, err := message.Metadata(); err == nil {
		delivery = metadata.NumDelivered
		seed ^= metadata.Sequence.Stream
	}
	return legacyGatewayRetryDelay(delivery, seed)
}

func legacyGatewayRetryDelay(delivery, seed uint64) time.Duration {
	if delivery == 0 {
		delivery = 1
	}
	window := legacyGatewayRetryBase
	for attempt := uint64(1); attempt < delivery && window < legacyGatewayRetryMax; attempt++ {
		if window >= legacyGatewayRetryMax/2 {
			window = legacyGatewayRetryMax
			break
		}
		window *= 2
	}
	if window > legacyGatewayRetryMax {
		window = legacyGatewayRetryMax
	}

	// Equal jitter keeps every retry away from zero while spreading messages
	// deterministically across the latter half of the exponential window.
	mixed := seed + delivery*0x9e3779b97f4a7c15
	mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9
	mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111eb
	mixed ^= mixed >> 31
	minimum := window / 2
	span := uint64(window-minimum) + 1
	return minimum + time.Duration(mixed%span)
}

func (c *OutputRoomEventConsumer) onMessage(ctx context.Context, messages []*nats.Msg) bool {
	for _, message := range messages {
		var output roomserverAPI.OutputEvent
		if err := json.Unmarshal(message.Data, &output); err != nil {
			logrus.WithError(err).Warn("Legacy Agent Gateway ignored invalid roomserver output event")
			continue
		}
		if c.handler == nil {
			logrus.Warn("Legacy Agent Gateway consumer has no output event handler")
			return false
		}
		if err := c.handler(ctx, output); err != nil {
			logrus.WithError(err).Warn("Legacy Agent Gateway failed to process roomserver output event")
			return false
		}
	}
	return true
}
