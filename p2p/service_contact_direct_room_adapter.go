package p2p

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
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

func (s *Service) acceptDirectContactRoom(ctx context.Context, contact contactStorageRecord, serverNames []string) (string, *apiError) {
	if s.transport == nil || strings.TrimSpace(contact.RoomID) == "" {
		return contact.RoomID, nil
	}
	profile := s.localContactProfileSnapshot()
	join, err := s.joinRoomWithRetry(ctx, JoinRoomRequest{
		RoomIDOrAlias:             contact.RoomID,
		UserMXID:                  profile.MXID,
		DisplayName:               profile.DisplayName,
		AvatarURL:                 profile.AvatarURL,
		ServerNames:               serverNames,
		DirectContactReactivation: contactPendingInbound(contact.Status),
	}, 6, isFederatedJoinInProgress)
	if err != nil {
		if contactPendingInbound(contact.Status) && isDirectContactReactivationJoinFailed(err) {
			return s.createAcceptedReplacementDirectRoom(ctx, contact, profile)
		}
		return "", transportWriteError(err)
	}
	if strings.TrimSpace(join.RoomID) != "" {
		return join.RoomID, nil
	}
	return contact.RoomID, nil
}

func (s *Service) createAcceptedReplacementDirectRoom(
	ctx context.Context,
	contact contactStorageRecord,
	profile contactsmodule.LocalProfileSnapshot,
) (string, *apiError) {
	return s.createContactDirectRoomWithProfile(ctx, contactsmodule.DirectRoomCreateRequest{
		PeerMXID:       contact.PeerMXID,
		DisplayName:    contact.DisplayName,
		FallbackRoomID: contact.RoomID,
	}, profile)
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

func (s *Service) joinContactDirectRoomTransport(ctx context.Context, request contactsmodule.DirectRoomJoinRequest) contactsmodule.DirectRoomJoinOutcome {
	if s.transport == nil || request.RoomID == "" {
		return contactsmodule.DirectRoomJoinOutcome{Kind: contactsmodule.DirectRoomJoinSucceeded, RoomID: request.RoomID}
	}
	serverNames := request.ServerNames
	if request.UseRoomServerFallback && len(serverNames) == 0 {
		serverNames = retainedRoomServerNames(nil, request.RoomID)
	}
	joinRequest := JoinRoomRequest{
		RoomIDOrAlias:             request.RoomID,
		UserMXID:                  request.Profile.MXID,
		DisplayName:               request.Profile.DisplayName,
		AvatarURL:                 request.Profile.AvatarURL,
		ServerNames:               serverNames,
		DirectContactReactivation: request.Mode == contactsmodule.DirectRoomJoinReactivation,
	}
	retryable := isFederatedJoinInProgress
	if request.Mode == contactsmodule.DirectRoomJoinReactivation {
		retryable = func(err error) bool {
			return isDirectRoomJoinRequiresInvite(err) || isFederatedJoinInProgress(err)
		}
	}
	result, err := s.joinRoomWithRetry(ctx, joinRequest, 6, retryable)
	if err != nil {
		if request.Mode == contactsmodule.DirectRoomJoinNormal && isDirectRoomJoinRequiresInvite(err) {
			return contactsmodule.DirectRoomJoinOutcome{Kind: contactsmodule.DirectRoomJoinInviteRequired}
		}
		if request.Mode == contactsmodule.DirectRoomJoinReactivation && isDirectContactReactivationJoinFailed(err) {
			return contactsmodule.DirectRoomJoinOutcome{
				Kind: contactsmodule.DirectRoomJoinRetainedUnavailable, Failure: transportWriteError(err),
			}
		}
		return contactsmodule.DirectRoomJoinOutcome{Kind: contactsmodule.DirectRoomJoinFailed, Failure: transportWriteError(err)}
	}
	roomID := request.RoomID
	if strings.TrimSpace(result.RoomID) != "" {
		roomID = result.RoomID
	}
	return contactsmodule.DirectRoomJoinOutcome{Kind: contactsmodule.DirectRoomJoinSucceeded, RoomID: roomID}
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

func isDirectRoomJoinRequiresInvite(err error) bool {
	var policyErr *productpolicy.PolicyError
	return errors.As(err, &policyErr) &&
		policyErr.Code == http.StatusForbidden &&
		policyErr.Message == "direct room join requires invite"
}

func isSenderNotJoinedDirextalkRoom(err error) bool {
	var policyErr *productpolicy.PolicyError
	return errors.As(err, &policyErr) &&
		policyErr.Code == http.StatusForbidden &&
		policyErr.Message == "sender is not joined to the dirextalk room"
}

func isDirectContactReactivationJoinFailed(err error) bool {
	if isDirectRoomJoinRequiresInvite(err) {
		return true
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "inputwasrejected") ||
		strings.Contains(message, "local server not currently joined to room") ||
		strings.Contains(message, "unsupported room version") ||
		strings.Contains(message, "join rule \"invite\" forbids it")
}

func isAlreadyLeftRoomError(err error) bool {
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "not joined to the room") &&
		strings.Contains(message, "membership is \"leave\"")
}

func isAlreadyJoinedRoomError(err error) bool {
	return strings.Contains(strings.ToLower(strings.TrimSpace(err.Error())), "already joined")
}
