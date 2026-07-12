package p2p

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
)

type channelInviteGrantStore interface {
	UpsertChannelInviteGrant(ctx context.Context, grant channelInviteGrant) error
	ListChannelInviteGrants(ctx context.Context) ([]channelInviteGrant, error)
}

func (s *Service) channelInviteGrantStore() channelInviteGrantStore {
	if s.store == nil {
		return nil
	}
	return s.store
}

type peerContactReactivation struct {
	PendingInbound bool
	RoomID         string
}

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
		return s.resendPendingOutboundContactRequest(ctx, existing, params, domain)
	} else if ok && contactAccepted(existing.Status) && remoteNodeBaseURLParam(params) != "" && domainFromMXID(existing.PeerMXID) != s.serverName {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		s.mu.Unlock()
		reactivation, apiErr := s.requestPeerContactReactivation(ctx, existing, params, ownerMXID)
		if apiErr != nil {
			if contactReactivationNotRetained(apiErr) {
				return s.createReplacementDirectContactRequest(ctx, existing, params, fallbackString(domain, existing.Domain))
			}
			return nil, apiErr
		}
		if reactivation.PendingInbound {
			return s.createReplacementDirectContactRequest(ctx, existing, params, fallbackString(domain, existing.Domain))
		}
		if err := s.attachContactConversationOperation(ctx, &existing, "contacts.request", existing.Status); err != nil {
			return nil, internalError(err)
		}
		return existing, nil
	} else if ok {
		if err := s.attachContactConversationOperation(ctx, &existing, "contacts.request", existing.Status); err != nil {
			return nil, internalError(err)
		}
		return existing, nil
	}
	if contact, restored, apiErr := s.restoreRetainedPeerContact(ctx, mxid, params, domain); apiErr != nil {
		return nil, apiErr
	} else if restored {
		return contact, nil
	}
	return s.createDirectContactRequest(ctx, mxid, params, domain)
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
				replacement, apiErr := s.createReplacementDirectContactRequest(ctx, contactRecord{
					PeerMXID:    mxid,
					DisplayName: trimString(params["display_name"]),
					AvatarURL:   trimString(params["avatar_url"]),
					Domain:      domain,
					RoomID:      roomID,
					Status:      "accepted",
				}, params, domain)
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

func (s *Service) createDirectContactRequest(ctx context.Context, mxid string, params map[string]any, domain string) (contactRecord, *apiError) {
	roomID := "!dm-" + randomToken("room") + ":" + s.serverName
	remark := contactRequestRemark(params)
	if s.transport != nil {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		ownerDisplayName := s.profile.DisplayName
		ownerAvatarURL := s.profile.AvatarURL
		s.mu.Unlock()
		directName := fallbackString(trimString(params["display_name"]), mxid)
		res, err := s.transport.CreateRoom(ctx, CreateRoomRequest{
			CreatorMXID:        ownerMXID,
			CreatorDisplayName: ownerDisplayName,
			CreatorAvatarURL:   ownerAvatarURL,
			Name:               directName,
			Visibility:         "private",
			RoomType:           DirextalkRoomTypeDirect,
			IsDirect:           true,
			InviteMXIDs:        []string{mxid},
			InitialState: []RoomStateEvent{
				roomProfileForDirect(directName, ownerMXID, mxid, ownerDisplayName, ownerAvatarURL, remark, false),
			},
		})
		if err != nil {
			return contactRecord{}, transportWriteError(err)
		}
		roomID = res.RoomID
	}
	contact := contactRecord{
		PeerMXID:    mxid,
		DisplayName: trimString(params["display_name"]),
		AvatarURL:   trimString(params["avatar_url"]),
		Domain:      domain,
		RoomID:      roomID,
		Status:      "pending_outbound",
		Remark:      remark,
	}
	if err := s.saveContact(ctx, contact); err != nil {
		return contactRecord{}, internalError(err)
	}
	if err := s.attachContactConversationOperation(ctx, &contact, "contacts.request", contact.Status); err != nil {
		return contactRecord{}, internalError(err)
	}
	return contact, nil
}

func (s *Service) createReplacementDirectContactRequest(ctx context.Context, contact contactRecord, params map[string]any, fallbackDomain string) (contactRecord, *apiError) {
	nextParams := make(map[string]any, len(params)+3)
	for key, value := range params {
		nextParams[key] = value
	}
	if trimString(nextParams["display_name"]) == "" && strings.TrimSpace(contact.DisplayName) != "" {
		nextParams["display_name"] = contact.DisplayName
	}
	if trimString(nextParams["avatar_url"]) == "" && strings.TrimSpace(contact.AvatarURL) != "" {
		nextParams["avatar_url"] = contact.AvatarURL
	}
	if trimString(nextParams["domain"]) == "" {
		nextParams["domain"] = fallbackString(contact.Domain, fallbackDomain)
	}
	return s.createDirectContactRequest(ctx, contact.PeerMXID, nextParams, fallbackString(trimString(nextParams["domain"]), fallbackDomain))
}

func (s *Service) resendPendingOutboundContactRequest(ctx context.Context, contact contactRecord, params map[string]any, fallbackDomain string) (contactRecord, *apiError) {
	if remark := contactRequestRemark(params); remark != "" {
		contact.Remark = remark
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
	if contact.RoomID != "" && s.transport != nil {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		ownerDisplayName := s.profile.DisplayName
		ownerAvatarURL := s.profile.AvatarURL
		s.mu.Unlock()
		directName := fallbackString(contact.DisplayName, contact.PeerMXID)
		if err := s.transport.InviteUser(ctx, InviteUserRequest{
			RoomID:      contact.RoomID,
			InviterMXID: ownerMXID,
			InviteeMXID: contact.PeerMXID,
			IsDirect:    true,
			InviteRoomState: []RoomStateEvent{
				roomProfileForDirect(directName, ownerMXID, contact.PeerMXID, ownerDisplayName, ownerAvatarURL, contact.Remark, false),
			},
		}); err != nil {
			if !isSenderNotJoinedDirextalkRoom(err) {
				return contactRecord{}, transportWriteError(err)
			}
		}
	}
	if err := s.saveContact(ctx, contact); err != nil {
		return contactRecord{}, internalError(err)
	}
	if err := s.attachContactConversationOperation(ctx, &contact, "contacts.request", contact.Status); err != nil {
		return contactRecord{}, internalError(err)
	}
	return contact, nil
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
	if s.transport == nil {
		return contact.RoomID, nil
	}
	directName := fallbackString(contact.DisplayName, contact.PeerMXID)
	res, err := s.transport.CreateRoom(ctx, CreateRoomRequest{
		CreatorMXID:        ownerMXID,
		CreatorDisplayName: ownerDisplayName,
		CreatorAvatarURL:   ownerAvatarURL,
		Name:               directName,
		Visibility:         "private",
		RoomType:           DirextalkRoomTypeDirect,
		IsDirect:           true,
		InviteMXIDs:        []string{contact.PeerMXID},
		InitialState: []RoomStateEvent{
			roomProfileForDirect(directName, ownerMXID, contact.PeerMXID, ownerDisplayName, ownerAvatarURL, "", false),
		},
	})
	if err != nil {
		return "", transportWriteError(err)
	}
	return res.RoomID, nil
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
				return s.requestPeerApprovalInExistingDirectRoom(ctx, contact, params, fallbackString(trimString(params["domain"]), contact.Domain), false)
			}
			if contactReactivationNotRetained(apiErr) {
				return s.requestPeerApprovalInExistingDirectRoom(ctx, contact, params, fallbackString(trimString(params["domain"]), contact.Domain), true)
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
					return s.createReplacementDirectContactRequest(ctx, contact, params, fallbackString(fallbackString(trimString(params["domain"]), contact.Domain), fallbackDomain))
				}
				return nil, apiErr
			}
			if reactivation.PendingInbound {
				return s.createReplacementDirectContactRequest(ctx, contact, params, fallbackString(fallbackString(trimString(params["domain"]), contact.Domain), fallbackDomain))
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
					return s.requestPeerApprovalInExistingDirectRoom(ctx, contact, params, fallbackString(fallbackString(trimString(params["domain"]), contact.Domain), fallbackDomain), false)
				}
				if contactReactivationNotRetained(apiErr) {
					return s.createReplacementDirectContactRequest(ctx, contact, params, fallbackString(fallbackString(trimString(params["domain"]), contact.Domain), fallbackDomain))
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

func (s *Service) requestPeerApprovalInExistingDirectRoom(ctx context.Context, contact contactRecord, params map[string]any, fallbackDomain string, sendMatrixInvite bool) (contactRecord, *apiError) {
	if strings.TrimSpace(contact.RoomID) == "" {
		return s.createDirectContactRequest(ctx, contact.PeerMXID, params, fallbackDomain)
	}
	if remark := contactRequestRemark(params); remark != "" {
		contact.Remark = remark
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
	contact.Status = "pending_outbound"
	if sendMatrixInvite && s.transport != nil {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		ownerDisplayName := s.profile.DisplayName
		ownerAvatarURL := s.profile.AvatarURL
		s.mu.Unlock()
		directName := fallbackString(contact.DisplayName, contact.PeerMXID)
		if err := s.transport.InviteUser(ctx, InviteUserRequest{
			RoomID:      contact.RoomID,
			InviterMXID: ownerMXID,
			InviteeMXID: contact.PeerMXID,
			IsDirect:    true,
			InviteRoomState: []RoomStateEvent{
				roomProfileForDirect(directName, ownerMXID, contact.PeerMXID, ownerDisplayName, ownerAvatarURL, contact.Remark, false),
			},
		}); err != nil {
			if !isSenderNotJoinedDirextalkRoom(err) {
				return contactRecord{}, transportWriteError(err)
			}
		}
	}
	if err := s.saveContact(ctx, contact); err != nil {
		return contactRecord{}, internalError(err)
	}
	if err := s.attachContactConversationOperation(ctx, &contact, "contacts.request", contact.Status); err != nil {
		return contactRecord{}, internalError(err)
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
	peerServer := domainFromMXID(contact.PeerMXID)
	if peerServer == "" || peerServer == s.serverName {
		return peerContactReactivation{}, statusError(http.StatusForbidden, "peer node is required to reactivate direct room")
	}
	remoteBase := remoteNodeBaseURLParam(params)
	if remoteBase == "" {
		remoteBase = "https://" + peerServer + "/_p2p"
	}
	var result map[string]any
	status, err := s.remotePublicAction(ctx, peerServer, "contacts.reactivate", map[string]any{
		"room_id":              contact.RoomID,
		"requester_mxid":       requesterMXID,
		"remote_node_base_url": remoteBase,
		"display_name":         trimString(params["display_name"]),
		"avatar_url":           trimString(params["avatar_url"]),
		"domain":               trimString(params["domain"]),
		"remark":               contactRequestRemark(params),
	}, &result)
	if err != nil {
		if status != 0 && status != http.StatusBadGateway {
			return peerContactReactivation{}, statusError(status, err.Error())
		}
		return peerContactReactivation{}, statusError(http.StatusBadGateway, err.Error())
	}
	if status != http.StatusOK {
		if status == http.StatusNotFound {
			return peerContactReactivation{}, statusError(status, "retained contact not found")
		}
		return peerContactReactivation{}, statusError(status, "target node contact reactivation failed")
	}
	if strings.EqualFold(trimString(result["status"]), "pending_inbound") {
		return peerContactReactivation{PendingInbound: true, RoomID: trimString(result["room_id"])}, nil
	}
	return peerContactReactivation{RoomID: trimString(result["room_id"])}, nil
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

func (s *Service) saveChannelInviteGrant(ctx context.Context, grant channelInviteGrant) error {
	s.mu.Lock()
	s.inviteGrants[grant.GrantID] = grant
	s.mu.Unlock()
	if store := s.channelInviteGrantStore(); store != nil {
		return store.UpsertChannelInviteGrant(ctx, grant)
	}
	return nil
}

func (s *Service) listChannelInviteGrants(ctx context.Context) ([]channelInviteGrant, error) {
	if store := s.channelInviteGrantStore(); store != nil {
		return store.ListChannelInviteGrants(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	grants := make([]channelInviteGrant, 0, len(s.inviteGrants))
	for _, grant := range s.inviteGrants {
		grants = append(grants, grant)
	}
	sort.SliceStable(grants, func(i, j int) bool {
		if grants[i].CreatedAt == grants[j].CreatedAt {
			return grants[i].GrantID < grants[j].GrantID
		}
		return grants[i].CreatedAt > grants[j].CreatedAt
	})
	return grants, nil
}

func (s *Service) lookupChannelInviteGrantForParams(ctx context.Context, params map[string]any) (channelInviteGrant, bool, error) {
	grantID := trimString(params["grant_id"])
	shareRoomID := trimString(params["share_room_id"])
	if shareRoomID == "" {
		shareRoomID = trimString(params["via_room_id"])
	}
	roomID := trimString(params["room_id"])
	channelID := trimString(params["channel_id"])
	grants, err := s.listChannelInviteGrants(ctx)
	if err != nil {
		return channelInviteGrant{}, false, err
	}
	for _, grant := range grants {
		if grantID != "" && grant.GrantID != grantID {
			continue
		}
		if shareRoomID != "" && grant.ShareRoomID != shareRoomID {
			continue
		}
		if roomID != "" && grant.RoomID != roomID {
			continue
		}
		if channelID != "" && grant.ChannelID != channelID {
			continue
		}
		return grant, true, nil
	}
	return channelInviteGrant{}, false, nil
}
