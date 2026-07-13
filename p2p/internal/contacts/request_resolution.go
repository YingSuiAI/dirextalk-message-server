package contacts

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

func (m *Module) handleRequestDelete(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	params := actionbase.Params(raw)
	return m.serializeRoomPeerMutation(ctx, params, func() (any, *actionbase.Error) {
		return m.requestDeleteForPeer(ctx, params)
	})
}

func (m *Module) serializeRoomPeerMutation(ctx context.Context, params actionbase.Params, mutate func() (any, *actionbase.Error)) (any, *actionbase.Error) {
	peerMXID := params.FirstString("peer_mxid", "mxid")
	roomID := params.String("room_id")
	if roomID != "" {
		contact, ok, err := m.LookupByRoom(ctx, roomID)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		if ok && contact.PeerMXID != "" {
			peerMXID = contact.PeerMXID
		}
	}

	mutationKey := dirextalkdomain.FallbackString(peerMXID, roomID)
	var result any
	var actionErr *actionbase.Error
	m.SerializePeer(mutationKey, func() {
		result, actionErr = mutate()
	})
	return result, actionErr
}

func (m *Module) requestDeleteForPeer(ctx context.Context, params actionbase.Params) (any, *actionbase.Error) {
	roomID := params.String("room_id")
	contact, ok, err := m.LookupByRoom(ctx, roomID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		contact = dirextalkdomain.ContactRecord{
			RoomID:      roomID,
			PeerMXID:    params.FirstString("peer_mxid", "mxid"),
			DisplayName: params.String("display_name"),
			AvatarURL:   params.String("avatar_url"),
			Domain:      params.String("domain"),
			Remark:      params.FirstString("remark", "request_message", "message", "reason"),
		}
	}
	if acceptedStatus(contact.Status) {
		return m.statusOperationResult(ctx, actionRequestDelete, contact.Status, contact.RoomID)
	}

	contact.Status = "deleted"
	if contact.DisplayName == "" {
		contact.DisplayName = params.String("display_name")
	}
	if contact.AvatarURL == "" {
		contact.AvatarURL = params.String("avatar_url")
	}
	if contact.Domain == "" {
		contact.Domain = params.String("domain")
	}
	if err := m.Save(ctx, contact); err != nil {
		return nil, actionbase.InternalError(err)
	}
	return m.statusOperationResult(ctx, actionRequestDelete, contact.Status, contact.RoomID)
}

func (m *Module) handleRequestReject(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	params := actionbase.Params(raw)
	return m.serializeRoomPeerMutation(ctx, params, func() (any, *actionbase.Error) {
		return m.requestRejectForPeer(ctx, params)
	})
}

func (m *Module) requestRejectForPeer(ctx context.Context, params actionbase.Params) (any, *actionbase.Error) {
	roomID := params.String("room_id")
	peerMXID := params.FirstString("peer_mxid", "mxid")
	existing, ok, err := m.lookupDecisionContact(ctx, roomID, peerMXID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.CodedError(http.StatusNotFound, actionbase.RequestNotFoundCode, "contact request not found")
	}
	roomID = existing.RoomID
	if dirextalkdomain.ContactDeleted(existing.Status) {
		return nil, actionbase.CodedError(http.StatusGone, actionbase.RequestExpiredCode, "contact request expired")
	}
	if acceptedStatus(existing.Status) {
		staleAcceptedProjection := false
		if m.verifyAccepted {
			joined, verified, actionErr := m.matrixContactJoined(ctx, existing)
			if actionErr != nil {
				return nil, actionErr
			}
			staleAcceptedProjection = verified && !joined
		}
		if !staleAcceptedProjection {
			view, actionErr := m.viewWithOperation(ctx, actionRequestReject, existing)
			if actionErr != nil {
				return nil, actionErr
			}
			return view, nil
		}
	} else {
		if current, joined, actionErr := m.acceptedContactFromMatrix(ctx, existing); actionErr != nil {
			return nil, actionErr
		} else if joined {
			return m.contactDecisionView(ctx, actionRequestReject, current)
		}
	}
	if strings.EqualFold(strings.TrimSpace(existing.Status), "joining") {
		view, actionErr := m.viewWithOperation(ctx, actionRequestReject, existing)
		if actionErr != nil {
			return nil, actionErr
		}
		view.Status = "joining"
		view.ErrorCode = actionbase.MatrixJoinUnconfirmedCode
		return view, nil
	}
	if strings.EqualFold(strings.TrimSpace(existing.Status), "rejected") || strings.EqualFold(strings.TrimSpace(existing.Status), "reject") {
		view, actionErr := m.viewWithOperation(ctx, actionRequestReject, existing)
		if actionErr != nil {
			return nil, actionErr
		}
		return view, nil
	}
	settlementCtx, cancel := actionbase.SettlementContext(ctx)
	defer cancel()

	displayName := params.String("display_name")
	if existing.DisplayName != "" {
		displayName = existing.DisplayName
	}
	contact := dirextalkdomain.ContactRecord{
		PeerMXID:    peerMXID,
		DisplayName: displayName,
		AvatarURL:   params.String("avatar_url"),
		Domain:      params.String("domain"),
		RoomID:      roomID,
		Status:      "rejected",
		Remark:      existing.Remark,
		RequestID:   fallbackRequestString(existing.RequestID, params.String("request_id")),
	}
	if contact.PeerMXID == "" {
		contact.PeerMXID = existing.PeerMXID
	}
	if contact.AvatarURL == "" {
		contact.AvatarURL = existing.AvatarURL
	}
	if contact.Domain == "" {
		contact.Domain = existing.Domain
	}
	current, saved, err := m.saveDecision(settlementCtx, contact, existing, false)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !saved {
		return m.contactDecisionView(settlementCtx, actionRequestReject, current)
	}
	view, actionErr := m.viewWithOperation(settlementCtx, actionRequestReject, contact)
	if actionErr != nil {
		return nil, actionErr
	}
	return view, nil
}

func (m *Module) acceptedContactFromMatrix(
	ctx context.Context,
	existing dirextalkdomain.ContactRecord,
) (dirextalkdomain.ContactRecord, bool, *actionbase.Error) {
	joined, verified, actionErr := m.matrixContactJoined(ctx, existing)
	if actionErr != nil {
		return dirextalkdomain.ContactRecord{}, false, actionErr
	}
	if !verified || !joined {
		return dirextalkdomain.ContactRecord{}, false, nil
	}
	accepted := existing
	accepted.Status = "accepted"
	accepted.Remark = ""
	current, saved, err := m.saveDecision(ctx, accepted, existing, true)
	if err != nil {
		return dirextalkdomain.ContactRecord{}, false, actionbase.InternalError(err)
	}
	if saved {
		return accepted, true, nil
	}
	return current, true, nil
}

func (m *Module) matrixContactJoined(
	ctx context.Context,
	existing dirextalkdomain.ContactRecord,
) (joined, verified bool, actionErr *actionbase.Error) {
	if m.matrixJoined == nil || m.localProfile == nil || existing.RoomID == "" || existing.PeerMXID == "" {
		return false, false, nil
	}
	ownerMXID := strings.TrimSpace(m.localProfile().MXID)
	if ownerMXID == "" {
		return false, false, nil
	}
	joined, err := m.matrixJoined(ctx, existing.RoomID, ownerMXID)
	if err != nil {
		return false, true, actionbase.InternalError(err)
	}
	return joined, true, nil
}

func (m *Module) statusOperationResult(ctx context.Context, action, status, roomID string) (any, *actionbase.Error) {
	operation, conversation, err := m.conversation.Operation(ctx, action, status, roomID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	result := map[string]any{"status": "ok", "operation": operation}
	if conversation != nil {
		result["conversation"] = *conversation
	}
	return result, nil
}

// RestoreDeleted restores a locally deleted contact or starts a replacement
// request when the peer no longer retains the previous direct room. The caller
// owns the surrounding peer workflow boundary.
func (m *Module) RestoreDeleted(
	ctx context.Context,
	contact dirextalkdomain.ContactRecord,
	raw map[string]any,
	fallbackDomain string,
) (any, *actionbase.Error) {
	params := actionbase.Params(raw)
	if contact.RoomID != "" && m.joinRoom != nil {
		var profile LocalProfileSnapshot
		if m.localProfile != nil {
			profile = m.localProfile()
		}
		serverNames := params.Strings("server_names")
		replacementDomain := fallbackRequestString(
			fallbackRequestString(params.String("domain"), contact.Domain),
			fallbackDomain,
		)
		proactive := params.String("remote_node_base_url") != "" &&
			dirextalkdomain.DomainFromMXID(contact.PeerMXID) != m.serverName

		if proactive {
			peer, actionErr := m.reactivateDeletedPeer(ctx, contact, params, profile.MXID)
			if actionErr != nil {
				return nil, actionErr
			}
			if peer.NotRetained || peer.PendingInbound {
				view, actionErr := m.CreateReplacementRequest(ctx, contact, raw, replacementDomain)
				return view, actionErr
			}
			join := m.joinRoom(ctx, DirectRoomJoinRequest{
				RoomID: contact.RoomID, Profile: profile, ServerNames: serverNames,
				Mode: DirectRoomJoinReactivation, UseRoomServerFallback: true,
			})
			if join.Kind != DirectRoomJoinSucceeded {
				return nil, directRoomJoinFailure(join)
			}
			contact.RoomID = join.RoomID
		} else {
			join := m.joinRoom(ctx, DirectRoomJoinRequest{
				RoomID: contact.RoomID, Profile: profile, ServerNames: serverNames, Mode: DirectRoomJoinNormal,
			})
			if join.Kind == DirectRoomJoinInviteRequired {
				peer, actionErr := m.reactivateDeletedPeer(ctx, contact, params, profile.MXID)
				if actionErr != nil {
					return nil, actionErr
				}
				if peer.PendingInbound {
					view, actionErr := m.RequestPeerApproval(ctx, contact, raw, replacementDomain, false)
					return view, actionErr
				}
				if peer.NotRetained {
					view, actionErr := m.CreateReplacementRequest(ctx, contact, raw, replacementDomain)
					return view, actionErr
				}
				join = m.joinRoom(ctx, DirectRoomJoinRequest{
					RoomID: contact.RoomID, Profile: profile, ServerNames: serverNames, Mode: DirectRoomJoinReactivation,
				})
			}
			if join.Kind != DirectRoomJoinSucceeded {
				return nil, directRoomJoinFailure(join)
			}
			contact.RoomID = join.RoomID
		}
	}

	if displayName := params.String("display_name"); displayName != "" {
		contact.DisplayName = displayName
	}
	if avatarURL := params.String("avatar_url"); avatarURL != "" {
		contact.AvatarURL = avatarURL
	}
	if domain := params.String("domain"); domain != "" {
		contact.Domain = domain
	} else if contact.Domain == "" {
		contact.Domain = fallbackDomain
	}
	contact.Status = "accepted"
	contact.Remark = ""
	contact.RequestID = fallbackRequestString(params.String("request_id"), contact.RequestID)
	if err := m.Save(ctx, contact); err != nil {
		return nil, actionbase.InternalError(err)
	}
	view, actionErr := m.viewWithOperation(ctx, actionRequest, contact)
	if actionErr != nil {
		return nil, actionErr
	}
	return view, nil
}

func (m *Module) reactivateDeletedPeer(
	ctx context.Context,
	contact dirextalkdomain.ContactRecord,
	params actionbase.Params,
	requesterMXID string,
) (PeerReactivationResult, *actionbase.Error) {
	if m.reactivatePeer == nil {
		return PeerReactivationResult{}, actionbase.InternalError(fmt.Errorf("peer reactivation is not configured"))
	}
	return m.reactivatePeer(ctx, peerReactivationRequest(contact, params, requesterMXID))
}

// RestoreRetainedPeer probes a remote peer for a direct room when no local
// contact record exists. restored is false when the caller should create a
// normal fresh request instead.
func (m *Module) RestoreRetainedPeer(
	ctx context.Context,
	peerMXID string,
	raw map[string]any,
	domain string,
) (View, bool, *actionbase.Error) {
	params := actionbase.Params(raw)
	if params.String("remote_node_base_url") == "" || domain == "" || domain == m.serverName || m.reactivatePeer == nil {
		return View{}, false, nil
	}
	var profile LocalProfileSnapshot
	if m.localProfile != nil {
		profile = m.localProfile()
	}
	probeContact := dirextalkdomain.ContactRecord{PeerMXID: peerMXID, Domain: domain}
	peer, actionErr := m.reactivatePeer(ctx, peerReactivationRequest(probeContact, params, profile.MXID))
	if actionErr != nil {
		return View{}, false, actionErr
	}
	if peer.NotRetained || peer.PendingInbound || strings.TrimSpace(peer.RoomID) == "" {
		return View{}, false, nil
	}

	roomID := peer.RoomID
	if m.joinRoom != nil {
		join := m.joinRoom(ctx, DirectRoomJoinRequest{
			RoomID: roomID, Profile: profile, ServerNames: params.Strings("server_names"),
			Mode: DirectRoomJoinReactivation, UseRoomServerFallback: true,
		})
		if join.Kind == DirectRoomJoinRetainedUnavailable {
			view, actionErr := m.CreateReplacementRequest(ctx, dirextalkdomain.ContactRecord{
				PeerMXID:    peerMXID,
				DisplayName: params.String("display_name"),
				AvatarURL:   params.String("avatar_url"),
				Domain:      domain,
				RoomID:      roomID,
				Status:      "accepted",
			}, raw, domain)
			if actionErr != nil {
				return View{}, false, actionErr
			}
			return view, true, nil
		}
		if join.Kind != DirectRoomJoinSucceeded {
			return View{}, false, directRoomJoinFailure(join)
		}
		roomID = join.RoomID
	}

	contact := dirextalkdomain.ContactRecord{
		PeerMXID:    peerMXID,
		DisplayName: params.String("display_name"),
		AvatarURL:   params.String("avatar_url"),
		Domain:      domain,
		RoomID:      roomID,
		Status:      "accepted",
	}
	if err := m.Save(ctx, contact); err != nil {
		return View{}, false, actionbase.InternalError(err)
	}
	view, actionErr := m.viewWithOperation(ctx, actionRequest, contact)
	if actionErr != nil {
		return View{}, false, actionErr
	}
	return view, true, nil
}
