package p2p

import "context"

func (s *Service) blockExists(ctx context.Context, targetType string, identifiers ...string) (bool, error) {
	return s.blocksModule.Exists(ctx, targetType, identifiers...)
}

func (s *Service) blockExistsInRoom(ctx context.Context, roomID, peerMXID string) (bool, error) {
	return s.blocksModule.ExistsInRoom(ctx, "contact", peerMXID, roomID)
}

// BlockedDirectMessage checks the owner's exact contact-and-room block pair.
func (s *Service) BlockedDirectMessage(ctx context.Context, roomID, peerMXID string) (bool, error) {
	return s.blockExistsInRoom(ctx, roomID, peerMXID)
}

func (s *Service) rejectIfBlocked(ctx context.Context, targetType string, identifiers ...string) *apiError {
	blocked, err := s.blockExists(ctx, targetType, identifiers...)
	if err != nil {
		return internalError(err)
	}
	if blocked {
		return statusError(403, "already blocked")
	}
	return nil
}
