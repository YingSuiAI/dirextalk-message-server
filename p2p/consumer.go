package p2p

import (
	"context"

	projectormodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/projector"
	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/setup/process"
	"github.com/nats-io/nats.go"
)

type OutputRoomEventConsumer = projectormodule.OutputRoomEventConsumer

func NewOutputRoomEventConsumer(process *process.ProcessContext, cfg *config.JetStream, js nats.JetStreamContext, service *Service) *OutputRoomEventConsumer {
	var handler func(context.Context, roomserverAPI.OutputEvent) error
	if service != nil {
		handler = service.ProjectOutputEvent
	}
	return projectormodule.NewOutputRoomEventConsumer(process, cfg, js, handler)
}
