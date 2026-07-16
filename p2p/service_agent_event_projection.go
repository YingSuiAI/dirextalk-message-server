package p2p

import (
	"context"
	"errors"

	agentevents "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/agentevents"
)

// RunAgentEventRelay is the only bridge from the independent Agent's durable
// WatchEvents sequence into ProductCore cloud projections. Agent seq remains
// the fact cursor; p2p_events seq is only the Message Server projection order.
func (s *Service) RunAgentEventRelay(ctx context.Context) error {
	if s == nil || s.agentEventClient == nil {
		return nil
	}
	if s.store == nil || s.eventsModule == nil {
		return errors.New("Agent event projection relay is unavailable")
	}
	store, ok := s.store.(agentevents.Store)
	if !ok {
		return errors.New("Agent event projection store is unavailable")
	}
	relay := agentevents.New(s.agentEventClient, accountScopedAgentEventStore{service: s, store: store}, s.agentEventClient.AgentEventSource(), agentevents.Config{
		Notify: s.eventsModule.NotifyPersisted,
	})
	return relay.Run(ctx)
}

// accountScopedAgentEventStore keeps the long-lived Agent relay behind the
// same account-deletion barrier as ProductCore and Matrix projectors. A commit
// already in flight drains before reset; a queued or later commit observes the
// terminal account state and cannot repopulate the cleared database.
type accountScopedAgentEventStore struct {
	service *Service
	store   agentevents.Store
}

func (guard accountScopedAgentEventStore) Cursor(ctx context.Context, source agentevents.Source) (int64, error) {
	ctx, finish := guard.service.beginAccountOperation(ctx)
	defer finish()
	if guard.service.accountIsDeprovisioned() {
		return 0, agentevents.ErrProjectionStopped
	}
	return guard.store.Cursor(ctx, source)
}

func (guard accountScopedAgentEventStore) Commit(ctx context.Context, request agentevents.CommitRequest) (agentevents.CommitResult, error) {
	ctx, finish := guard.service.beginAccountOperation(ctx)
	defer finish()
	if guard.service.accountIsDeprovisioned() {
		return agentevents.CommitResult{}, agentevents.ErrProjectionStopped
	}
	return guard.store.Commit(ctx, request)
}
