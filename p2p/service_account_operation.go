package p2p

import "context"

type accountOperationContextKey struct{}

// beginAccountOperation prevents account deletion from resetting product state
// while an already-authorized ProductCore, MCP, or projector operation is still
// reading or writing it.
// The context marker makes calls that cross adapters re-entrant without taking a
// second RWMutex read lock, which could deadlock behind a waiting deletion.
func (s *Service) beginAccountOperation(ctx context.Context) (context.Context, func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	if owner, _ := ctx.Value(accountOperationContextKey{}).(*Service); owner == s {
		return ctx, func() {}
	}
	s.accountOperationMu.RLock()
	return context.WithValue(ctx, accountOperationContextKey{}, s), s.accountOperationMu.RUnlock
}

func (s *Service) accountIsDeprovisioned() bool {
	s.mu.Lock()
	deprovisioned := s.accountDeprovisioned
	s.mu.Unlock()
	return deprovisioned
}
