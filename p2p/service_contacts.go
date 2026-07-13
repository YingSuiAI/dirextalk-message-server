package p2p

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	contactsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/contacts"
)

type peerContactReactivation = contactsmodule.PeerReactivationResult

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
		return s.restoreDeletedContact(ctx, existing, params, domain)
	} else if ok && contactPendingInbound(existing.Status) {
		return s.acceptPendingInboundContact(ctx, existing, params)
	} else if ok && strings.EqualFold(strings.TrimSpace(existing.Status), "pending_outbound") {
		return s.contactsModule.ResendPendingOutbound(ctx, contactStorageRecordFromContact(existing), params, domain)
	} else if ok {
		return s.contactsModule.ResolveExistingRequest(ctx, contactStorageRecordFromContact(existing), params, fallbackString(domain, existing.Domain))
	}
	if contact, restored, apiErr := s.restoreRetainedPeerContact(ctx, mxid, params, domain); apiErr != nil {
		return nil, apiErr
	} else if restored {
		return contact, nil
	}
	return s.contactsModule.CreateRequest(ctx, mxid, params, domain)
}

func (s *Service) restoreRetainedPeerContact(ctx context.Context, mxid string, params map[string]any, domain string) (contactRecord, bool, *apiError) {
	if remoteNodeBaseURLParam(params) == "" || domain == "" || domain == s.serverName {
		return contactRecord{}, false, nil
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	ownerDisplayName := s.profile.DisplayName
	ownerAvatarURL := s.profile.AvatarURL
	s.mu.Unlock()
	reactivation, apiErr := s.requestPeerContactReactivation(ctx, contactRecord{PeerMXID: mxid, Domain: domain}, params, ownerMXID)
	if apiErr != nil {
		if contactReactivationNotRetained(apiErr) {
			return contactRecord{}, false, nil
		}
		return contactRecord{}, false, apiErr
	}
	if reactivation.PendingInbound || strings.TrimSpace(reactivation.RoomID) == "" {
		return contactRecord{}, false, nil
	}
	roomID := reactivation.RoomID
	if s.transport != nil {
		join, err := s.joinReactivatedDirectRoom(ctx, roomID, ownerMXID, ownerDisplayName, ownerAvatarURL, retainedRoomServerNames(params, roomID))
		if err != nil {
			if isDirectContactReactivationJoinFailed(err) {
				replacement, apiErr := s.contactsModule.CreateReplacementRequest(ctx, contactStorageRecordFromContact(contactRecord{
					PeerMXID:    mxid,
					DisplayName: trimString(params["display_name"]),
					AvatarURL:   trimString(params["avatar_url"]),
					Domain:      domain,
					RoomID:      roomID,
					Status:      "accepted",
				}), params, domain)
				if apiErr != nil {
					return contactRecord{}, false, apiErr
				}
				return replacement, true, nil
			}
			return contactRecord{}, false, transportWriteError(err)
		}
		if strings.TrimSpace(join.RoomID) != "" {
			roomID = join.RoomID
		}
	}
	contact := contactRecord{
		PeerMXID:    mxid,
		DisplayName: trimString(params["display_name"]),
		AvatarURL:   trimString(params["avatar_url"]),
		Domain:      domain,
		RoomID:      roomID,
		Status:      "accepted",
	}
	if err := s.saveContact(ctx, contact); err != nil {
		return contactRecord{}, false, internalError(err)
	}
	if err := s.attachContactConversationOperation(ctx, &contact, "contacts.request", contact.Status); err != nil {
		return contactRecord{}, false, internalError(err)
	}
	return contact, true, nil
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

func (s *Service) acceptPendingInboundContact(ctx context.Context, contact contactRecord, params map[string]any) (any, *apiError) {
	if contact.RoomID != "" && s.transport != nil {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		ownerDisplayName := s.profile.DisplayName
		ownerAvatarURL := s.profile.AvatarURL
		s.mu.Unlock()
		roomID, apiErr := s.joinContactDirectRoom(ctx, contact, params, ownerMXID, ownerDisplayName, ownerAvatarURL)
		if apiErr != nil {
			if contactReactivationRecordedPending(apiErr) {
				return s.contactsModule.RequestPeerApproval(ctx, contactStorageRecordFromContact(contact), params, fallbackString(trimString(params["domain"]), contact.Domain), false)
			}
			if contactReactivationNotRetained(apiErr) {
				return s.contactsModule.RequestPeerApproval(ctx, contactStorageRecordFromContact(contact), params, fallbackString(trimString(params["domain"]), contact.Domain), true)
			}
			return nil, apiErr
		}
		contact.RoomID = roomID
	}
	if displayName := trimString(params["display_name"]); contact.DisplayName == "" && displayName != "" {
		contact.DisplayName = displayName
	}
	if avatarURL := trimString(params["avatar_url"]); contact.AvatarURL == "" && avatarURL != "" {
		contact.AvatarURL = avatarURL
	}
	if domain := trimString(params["domain"]); contact.Domain == "" && domain != "" {
		contact.Domain = domain
	}
	contact.Status = "accepted"
	contact.Remark = ""
	if err := s.saveContact(ctx, contact); err != nil {
		return nil, internalError(err)
	}
	if err := s.attachContactConversationOperation(ctx, &contact, "contacts.request", contact.Status); err != nil {
		return nil, internalError(err)
	}
	return contact, nil
}

func (s *Service) restoreDeletedContact(ctx context.Context, contact contactRecord, params map[string]any, fallbackDomain string) (any, *apiError) {
	if contact.RoomID != "" && s.transport != nil {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		ownerDisplayName := s.profile.DisplayName
		ownerAvatarURL := s.profile.AvatarURL
		s.mu.Unlock()
		if remoteNodeBaseURLParam(params) != "" && domainFromMXID(contact.PeerMXID) != s.serverName {
			reactivation, apiErr := s.requestPeerContactReactivation(ctx, contact, params, ownerMXID)
			if apiErr != nil {
				if contactReactivationNotRetained(apiErr) {
					return s.contactsModule.CreateReplacementRequest(ctx, contactStorageRecordFromContact(contact), params, fallbackString(fallbackString(trimString(params["domain"]), contact.Domain), fallbackDomain))
				}
				return nil, apiErr
			}
			if reactivation.PendingInbound {
				return s.contactsModule.CreateReplacementRequest(ctx, contactStorageRecordFromContact(contact), params, fallbackString(fallbackString(trimString(params["domain"]), contact.Domain), fallbackDomain))
			}
			join, err := s.joinReactivatedDirectRoom(ctx, contact.RoomID, ownerMXID, ownerDisplayName, ownerAvatarURL, retainedRoomServerNames(params, contact.RoomID))
			if err != nil {
				return nil, transportWriteError(err)
			}
			if strings.TrimSpace(join.RoomID) != "" {
				contact.RoomID = join.RoomID
			}
		} else {
			roomID, apiErr := s.joinContactDirectRoom(ctx, contact, params, ownerMXID, ownerDisplayName, ownerAvatarURL)
			if apiErr != nil {
				if contactReactivationRecordedPending(apiErr) {
					return s.contactsModule.RequestPeerApproval(ctx, contactStorageRecordFromContact(contact), params, fallbackString(fallbackString(trimString(params["domain"]), contact.Domain), fallbackDomain), false)
				}
				if contactReactivationNotRetained(apiErr) {
					return s.contactsModule.CreateReplacementRequest(ctx, contactStorageRecordFromContact(contact), params, fallbackString(fallbackString(trimString(params["domain"]), contact.Domain), fallbackDomain))
				}
				return nil, apiErr
			}
			contact.RoomID = roomID
		}
	}
	if displayName := trimString(params["display_name"]); displayName != "" {
		contact.DisplayName = displayName
	}
	if avatarURL := trimString(params["avatar_url"]); avatarURL != "" {
		contact.AvatarURL = avatarURL
	}
	if domain := trimString(params["domain"]); domain != "" {
		contact.Domain = domain
	} else if contact.Domain == "" {
		contact.Domain = fallbackDomain
	}
	contact.Status = "accepted"
	contact.Remark = ""
	if err := s.saveContact(ctx, contact); err != nil {
		return nil, internalError(err)
	}
	if err := s.attachContactConversationOperation(ctx, &contact, "contacts.request", contact.Status); err != nil {
		return nil, internalError(err)
	}
	return contact, nil
}

func (s *Service) joinContactDirectRoom(ctx context.Context, contact contactRecord, params map[string]any, ownerMXID, ownerDisplayName, ownerAvatarURL string) (string, *apiError) {
	serverNames := stringSliceParam(params["server_names"])
	join, err := s.joinRoomWithRetry(ctx, JoinRoomRequest{
		RoomIDOrAlias: contact.RoomID,
		UserMXID:      ownerMXID,
		DisplayName:   ownerDisplayName,
		AvatarURL:     ownerAvatarURL,
		ServerNames:   serverNames,
	}, 6, isFederatedJoinInProgress)
	if err != nil {
		if !isDirectRoomJoinRequiresInvite(err) {
			return "", transportWriteError(err)
		}
		reactivation, apiErr := s.requestPeerContactReactivation(ctx, contact, params, ownerMXID)
		if apiErr != nil {
			return "", apiErr
		}
		if reactivation.PendingInbound {
			return "", statusError(http.StatusConflict, "peer recorded pending contact request")
		}
		join, err = s.joinReactivatedDirectRoom(ctx, contact.RoomID, ownerMXID, ownerDisplayName, ownerAvatarURL, serverNames)
		if err != nil {
			return "", transportWriteError(err)
		}
	}
	if strings.TrimSpace(join.RoomID) != "" {
		return join.RoomID, nil
	}
	return contact.RoomID, nil
}

func (s *Service) joinReactivatedDirectRoom(ctx context.Context, roomID, userMXID, displayName, avatarURL string, serverNames []string) (JoinRoomResult, error) {
	return s.joinRoomWithRetry(ctx, JoinRoomRequest{
		RoomIDOrAlias:             roomID,
		UserMXID:                  userMXID,
		DisplayName:               displayName,
		AvatarURL:                 avatarURL,
		ServerNames:               serverNames,
		DirectContactReactivation: true,
	}, 6, func(err error) bool {
		return isDirectRoomJoinRequiresInvite(err) || isFederatedJoinInProgress(err)
	})
}

func (s *Service) requestPeerContactReactivation(ctx context.Context, contact contactRecord, params map[string]any, requesterMXID string) (peerContactReactivation, *apiError) {
	result, apiErr := s.reactivatePeerContact(ctx, contactsmodule.PeerReactivationRequest{
		Contact:           contactStorageRecordFromContact(contact),
		RequesterMXID:     requesterMXID,
		RemoteNodeBaseURL: remoteNodeBaseURLParam(params),
		DisplayName:       trimString(params["display_name"]),
		AvatarURL:         trimString(params["avatar_url"]),
		Domain:            trimString(params["domain"]),
		Remark:            contactRequestRemark(params),
	})
	if apiErr == nil && result.NotRetained {
		return peerContactReactivation{}, statusError(http.StatusNotFound, "retained contact not found")
	}
	return result, apiErr
}

func contactReactivationNotRetained(apiErr *apiError) bool {
	return apiErr != nil &&
		apiErr.Status == http.StatusNotFound &&
		strings.Contains(strings.ToLower(strings.TrimSpace(apiErr.Error)), "retained contact")
}

func contactReactivationRecordedPending(apiErr *apiError) bool {
	return apiErr != nil &&
		apiErr.Status == http.StatusConflict &&
		strings.Contains(strings.ToLower(strings.TrimSpace(apiErr.Error)), "pending contact request")
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
