package p2p

import (
	"context"
	"net/http"
)

// authorizeChannelContentRecall preserves the transportless compatibility
// rule: the local author or a projected channel owner may recall content.
func (s *Service) authorizeChannelContentRecall(ctx context.Context, roomID, authorMXID string) *apiError {
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if ownerMXID != "" && ownerMXID == authorMXID {
		return nil
	}
	if apiErr := s.requireOwnerMember(ctx, roomID); apiErr != nil {
		if apiErr.Status != http.StatusForbidden {
			return apiErr
		}
		return statusError(http.StatusForbidden, "content author or channel owner role is required")
	}
	return nil
}
