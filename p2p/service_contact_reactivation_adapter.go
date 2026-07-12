package p2p

import (
	"context"

	contactsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/contacts"
)

func (s *Service) localContactProfileSnapshot() contactsmodule.LocalProfileSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return contactsmodule.LocalProfileSnapshot{
		MXID:        s.ownerMXID,
		DisplayName: s.profile.DisplayName,
		AvatarURL:   s.profile.AvatarURL,
	}
}

func (s *Service) reactivateRetainedDirectRoom(
	ctx context.Context,
	profile contactsmodule.LocalProfileSnapshot,
	roomID string,
	requesterMXID string,
) *apiError {
	if s.transport == nil {
		return nil
	}
	directName := fallbackString(profile.DisplayName, profile.MXID)
	if err := s.transport.InviteUser(ctx, InviteUserRequest{
		RoomID:      roomID,
		InviterMXID: profile.MXID,
		InviteeMXID: requesterMXID,
		IsDirect:    true,
		InviteRoomState: []RoomStateEvent{
			roomProfileForDirect(directName, profile.MXID, requesterMXID, profile.DisplayName, profile.AvatarURL, "", false),
		},
	}); err != nil && !isAlreadyJoinedRoomError(err) {
		return transportWriteError(err)
	}
	return nil
}
