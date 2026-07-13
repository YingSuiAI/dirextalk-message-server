package p2p

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	contactsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/contacts"
)

func (s *Service) contactRequest(ctx context.Context, params map[string]any) (any, *apiError) {
	mxid := trimString(params["mxid"])
	if mxid == "" {
		return nil, badRequest("mxid is required")
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if mxid == ownerMXID {
		return nil, badRequest("mxid must be a remote peer")
	}
	var result any
	var apiErr *apiError
	s.contactsModule.SerializePeer(mxid, func() {
		result, apiErr = s.contactRequestForPeer(ctx, mxid, params)
	})
	return result, apiErr
}

func (s *Service) contactRequestForPeer(ctx context.Context, mxid string, params map[string]any) (any, *apiError) {
	if apiErr := s.rejectIfBlocked(ctx, "contact", mxid); apiErr != nil {
		return nil, apiErr
	}
	domain := trimString(params["domain"])
	if domain == "" && strings.Contains(mxid, ":") {
		domain = mxid[strings.Index(mxid, ":")+1:]
	}
	if existing, ok, err := s.lookupContactByPeer(ctx, mxid); err != nil {
		return nil, internalError(err)
	} else if ok && contactDeleted(existing.Status) {
		return s.contactsModule.RestoreDeleted(ctx, contactStorageRecordFromContact(existing), params, domain)
	} else if ok && contactPendingInbound(existing.Status) {
		return s.contactsModule.AcceptPendingInbound(ctx, contactStorageRecordFromContact(existing), params)
	} else if ok && strings.EqualFold(strings.TrimSpace(existing.Status), "pending_outbound") {
		return s.contactsModule.ResendPendingOutbound(ctx, contactStorageRecordFromContact(existing), params, domain)
	} else if ok {
		return s.contactsModule.ResolveExistingRequest(ctx, contactStorageRecordFromContact(existing), params, fallbackString(domain, existing.Domain))
	}
	if contact, restored, apiErr := s.contactsModule.RestoreRetainedPeer(ctx, mxid, params, domain); apiErr != nil {
		return nil, apiErr
	} else if restored {
		return contact, nil
	}
	return s.contactsModule.CreateRequest(ctx, mxid, params, domain)
}

func (s *Service) acceptDirectContactRoom(ctx context.Context, contact contactStorageRecord, serverNames []string) (string, *apiError) {
	if s.transport == nil || strings.TrimSpace(contact.RoomID) == "" {
		return contact.RoomID, nil
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	ownerDisplayName := s.profile.DisplayName
	ownerAvatarURL := s.profile.AvatarURL
	s.mu.Unlock()
	join, err := s.joinRoomWithRetry(ctx, JoinRoomRequest{
		RoomIDOrAlias:             contact.RoomID,
		UserMXID:                  ownerMXID,
		DisplayName:               ownerDisplayName,
		AvatarURL:                 ownerAvatarURL,
		ServerNames:               serverNames,
		DirectContactReactivation: contactPendingInbound(contact.Status),
	}, 6, isFederatedJoinInProgress)
	if err != nil {
		if contactPendingInbound(contact.Status) && isDirectContactReactivationJoinFailed(err) {
			return s.createAcceptedReplacementDirectRoom(ctx, contact, ownerMXID, ownerDisplayName, ownerAvatarURL)
		}
		return "", transportWriteError(err)
	}
	if strings.TrimSpace(join.RoomID) != "" {
		return join.RoomID, nil
	}
	return contact.RoomID, nil
}

func (s *Service) createAcceptedReplacementDirectRoom(ctx context.Context, contact contactStorageRecord, ownerMXID, ownerDisplayName, ownerAvatarURL string) (string, *apiError) {
	profile := contactsmodule.LocalProfileSnapshot{
		MXID:        ownerMXID,
		DisplayName: ownerDisplayName,
		AvatarURL:   ownerAvatarURL,
	}
	return s.createContactDirectRoomWithProfile(ctx, contactsmodule.DirectRoomCreateRequest{
		PeerMXID:       contact.PeerMXID,
		DisplayName:    contact.DisplayName,
		FallbackRoomID: contact.RoomID,
	}, profile)
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

func (s *Service) saveContact(ctx context.Context, contact contactRecord) error {
	return s.contactsModule.Save(ctx, contactStorageRecordFromContact(contact))
}
