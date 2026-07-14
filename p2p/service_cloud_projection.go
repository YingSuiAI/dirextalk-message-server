package p2p

import (
	"context"
	"errors"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

// RunCloudProjectionRelay is the only bridge from durable Cloud Orchestrator
// events into the ProductCore event stream. It is intentionally blocking so
// the production process owns its lifecycle; constructors never start a test
// goroutine implicitly.
func (s *Service) RunCloudProjectionRelay(ctx context.Context) error {
	if s == nil || s.store == nil || s.eventsModule == nil {
		return errors.New("cloud projection relay is unavailable")
	}
	projectionStore, ok := s.store.(cloudmodule.ProjectionStore)
	if !ok {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	relay := cloudmodule.NewProjectionRelay(projectionStore, func(ctx context.Context, eventID, eventType string, payload map[string]any) error {
		return s.appendP2PEvent(ctx, p2pEvent{
			Type: eventType, EventID: eventID, DedupeKey: "cloud-event:" + eventID, Payload: payload,
		})
	}, cloudmodule.ProjectionRelayConfig{
		WorkerID: "message-server-" + randomToken("cloud_projection"),
	})
	return relay.Run(ctx)
}
