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

func (s *Service) createContactDirectRoom(ctx context.Context, request contactsmodule.DirectRoomCreateRequest) (string, *apiError) {
	if s.transport == nil {
		return request.FallbackRoomID, nil
	}
	return s.createContactDirectRoomWithProfile(ctx, request, s.localContactProfileSnapshot())
}

func (s *Service) createContactDirectRoomWithProfile(
	ctx context.Context,
	request contactsmodule.DirectRoomCreateRequest,
	profile contactsmodule.LocalProfileSnapshot,
) (string, *apiError) {
	if s.transport == nil {
		return request.FallbackRoomID, nil
	}
	directName := fallbackString(request.DisplayName, request.PeerMXID)
	result, err := s.transport.CreateRoom(ctx, CreateRoomRequest{
		CreatorMXID:        profile.MXID,
		CreatorDisplayName: profile.DisplayName,
		CreatorAvatarURL:   profile.AvatarURL,
		Name:               directName,
		Visibility:         "private",
		RoomType:           DirextalkRoomTypeDirect,
		IsDirect:           true,
		InviteMXIDs:        []string{request.PeerMXID},
		InitialState: []RoomStateEvent{
			roomProfileForDirect(directName, profile.MXID, request.PeerMXID, profile.DisplayName, profile.AvatarURL, request.Remark, false),
		},
	})
	if err != nil {
		return "", transportWriteError(err)
	}
	return result.RoomID, nil
}

func (s *Service) inviteContactDirectRoom(ctx context.Context, request contactsmodule.DirectRoomInviteRequest) *apiError {
	if s.transport == nil || request.Contact.RoomID == "" {
		return nil
	}
	profile := s.localContactProfileSnapshot()
	directName := fallbackString(request.Contact.DisplayName, request.Contact.PeerMXID)
	if err := s.transport.InviteUser(ctx, InviteUserRequest{
		RoomID:      request.Contact.RoomID,
		InviterMXID: profile.MXID,
		InviteeMXID: request.Contact.PeerMXID,
		IsDirect:    true,
		InviteRoomState: []RoomStateEvent{
			roomProfileForDirect(directName, profile.MXID, request.Contact.PeerMXID, profile.DisplayName, profile.AvatarURL, request.Contact.Remark, false),
		},
	}); err != nil && !isSenderNotJoinedDirextalkRoom(err) {
		return transportWriteError(err)
	}
	return nil
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
