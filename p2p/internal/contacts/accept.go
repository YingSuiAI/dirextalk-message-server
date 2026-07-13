package contacts

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

func (m *Module) handleRequestAccept(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	params := actionbase.Params(raw)
	return m.serializeRoomPeerMutation(ctx, params, func() (any, *actionbase.Error) {
		return m.requestAcceptForPeer(ctx, params)
	})
}

func (m *Module) requestAcceptForPeer(ctx context.Context, params actionbase.Params) (any, *actionbase.Error) {
	peerMXID := params.FirstString("peer_mxid", "mxid")
	roomID := params.String("room_id")
	existing, ok, err := m.lookupDecisionContact(ctx, roomID, peerMXID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.CodedError(http.StatusNotFound, actionbase.RequestNotFoundCode, "contact request not found")
	}
	roomID = existing.RoomID
	if peerMXID == "" {
		peerMXID = existing.PeerMXID
	}
	if dirextalkdomain.ContactDeleted(existing.Status) {
		return nil, actionbase.CodedError(http.StatusGone, actionbase.RequestExpiredCode, "contact request expired")
	}
	if acceptedStatus(existing.Status) && (!m.verifyAccepted || m.acceptRoom == nil || existing.RoomID == "") {
		view, actionErr := m.viewWithOperation(ctx, actionRequestAccept, existing)
		if actionErr != nil {
			return nil, actionErr
		}
		return view, nil
	}
	settlementCtx, cancel := actionbase.SettlementContext(ctx)
	defer cancel()

	if m.acceptRoom != nil && roomID != "" {
		resolvedRoomID, actionErr := m.acceptRoom(settlementCtx, existing, params.Strings("server_names"))
		if actionErr != nil {
			if actionErr.Code == actionbase.MatrixJoinUnconfirmedCode {
				responseContact := existing
				responseContact.Status = "joining"
				responseContact.RequestID = fallbackRequestString(responseContact.RequestID, params.String("request_id"))
				current, saved, err := m.saveDecision(settlementCtx, responseContact, existing, false)
				if err != nil {
					return nil, actionbase.InternalError(err)
				}
				if !saved {
					return m.contactDecisionView(settlementCtx, actionRequestAccept, current)
				}
				view, viewErr := m.viewWithOperation(settlementCtx, actionRequestAccept, responseContact)
				if viewErr != nil {
					return nil, viewErr
				}
				view.ErrorCode = actionbase.MatrixJoinUnconfirmedCode
				view.CurrentRoomID = existing.RoomID
				return view, nil
			}
			return nil, actionErr
		}
		roomID = strings.TrimSpace(resolvedRoomID)
		if roomID == "" {
			return nil, actionbase.InternalError(fmt.Errorf("accepted direct room is empty"))
		}
	}

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
		Status:      "accepted",
		RequestID:   fallbackRequestString(existing.RequestID, params.String("request_id")),
	}
	if contact.AvatarURL == "" {
		contact.AvatarURL = existing.AvatarURL
	}
	if contact.Domain == "" {
		contact.Domain = existing.Domain
	}
	current, saved, err := m.saveDecision(settlementCtx, contact, existing, true)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !saved {
		return m.contactDecisionView(settlementCtx, actionRequestAccept, current)
	}
	view, actionErr := m.viewWithOperation(settlementCtx, actionRequestAccept, contact)
	if actionErr != nil {
		return nil, actionErr
	}
	return view, nil
}

func (m *Module) contactDecisionView(
	ctx context.Context,
	action string,
	contact dirextalkdomain.ContactRecord,
) (any, *actionbase.Error) {
	view, actionErr := m.viewWithOperation(ctx, action, contact)
	if actionErr != nil {
		return nil, actionErr
	}
	if strings.EqualFold(strings.TrimSpace(contact.Status), "joining") {
		view.Status = "joining"
		view.ErrorCode = actionbase.MatrixJoinUnconfirmedCode
	}
	return view, nil
}

// AcceptPendingInbound accepts or renegotiates a stored inbound contact
// request. The caller owns the surrounding peer workflow boundary.
func (m *Module) AcceptPendingInbound(
	ctx context.Context,
	contact dirextalkdomain.ContactRecord,
	raw map[string]any,
) (any, *actionbase.Error) {
	params := actionbase.Params(raw)
	if contact.RoomID != "" && m.joinRoom != nil {
		var profile LocalProfileSnapshot
		if m.localProfile != nil {
			profile = m.localProfile()
		}
		serverNames := params.Strings("server_names")
		join := m.joinRoom(ctx, DirectRoomJoinRequest{
			RoomID: contact.RoomID, Profile: profile, ServerNames: serverNames, Mode: DirectRoomJoinNormal,
		})
		if join.Kind == DirectRoomJoinInviteRequired {
			if m.reactivatePeer == nil {
				return nil, actionbase.InternalError(fmt.Errorf("peer reactivation is not configured"))
			}
			peer, actionErr := m.reactivatePeer(ctx, peerReactivationRequest(contact, params, profile.MXID))
			if actionErr != nil {
				return nil, actionErr
			}
			fallbackDomain := fallbackRequestString(params.String("domain"), contact.Domain)
			if peer.PendingInbound {
				view, actionErr := m.RequestPeerApproval(ctx, contact, raw, fallbackDomain, false)
				return view, actionErr
			}
			if peer.NotRetained {
				view, actionErr := m.RequestPeerApproval(ctx, contact, raw, fallbackDomain, true)
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

	if displayName := params.String("display_name"); contact.DisplayName == "" && displayName != "" {
		contact.DisplayName = displayName
	}
	if avatarURL := params.String("avatar_url"); contact.AvatarURL == "" && avatarURL != "" {
		contact.AvatarURL = avatarURL
	}
	if domain := params.String("domain"); contact.Domain == "" && domain != "" {
		contact.Domain = domain
	}
	contact.Status = "accepted"
	contact.Remark = ""
	if err := m.Save(ctx, contact); err != nil {
		return nil, actionbase.InternalError(err)
	}
	view, actionErr := m.viewWithOperation(ctx, actionRequest, contact)
	if actionErr != nil {
		return nil, actionErr
	}
	return view, nil
}

func directRoomJoinFailure(outcome DirectRoomJoinOutcome) *actionbase.Error {
	if outcome.Failure != nil {
		return outcome.Failure
	}
	return actionbase.InternalError(fmt.Errorf("direct room join returned outcome %d without failure", outcome.Kind))
}

// handleReactivate is the synchronous peer-side half of contacts.request. It
// deliberately does not acquire the local peer workflow lock: two nodes may
// call each other while their outbound requests hold that lock, and taking it
// here would create a distributed lock cycle.
func (m *Module) handleReactivate(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	params := actionbase.Params(raw)
	roomID := params.String("room_id")
	requesterMXID := params.String("requester_mxid")
	if requesterMXID == "" {
		return nil, actionbase.BadRequest("requester_mxid is required")
	}

	var profile LocalProfileSnapshot
	if m.localProfile != nil {
		profile = m.localProfile()
	}
	if requesterMXID == profile.MXID {
		return nil, actionbase.BadRequest("requester_mxid must be a remote peer")
	}
	contact, ok, err := m.LookupByPeer(ctx, requesterMXID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok || (roomID != "" && contact.RoomID != roomID) {
		return nil, actionbase.StatusError(http.StatusNotFound, "retained contact not found")
	}
	if roomID == "" {
		if !acceptedStatus(contact.Status) {
			return nil, actionbase.StatusError(http.StatusNotFound, "retained contact not found")
		}
		roomID = contact.RoomID
	}
	if dirextalkdomain.ContactDeleted(contact.Status) {
		return nil, actionbase.StatusError(http.StatusNotFound, "retained contact not found")
	}
	if !acceptedStatus(contact.Status) {
		if contact.DisplayName == "" {
			contact.DisplayName = dirextalkdomain.DisplayNameFromMXID(requesterMXID)
		}
		if contact.Domain == "" {
			contact.Domain = dirextalkdomain.DomainFromMXID(requesterMXID)
		}
		contact.Status = "pending_inbound"
		if requestID := params.String("request_id"); requestID != "" {
			contact.RequestID = requestID
		}
		if err := m.Save(ctx, contact); err != nil {
			return nil, actionbase.InternalError(err)
		}
		return m.reactivationResult(ctx, "pending_inbound", roomID)
	}

	if m.reactivateRoom != nil {
		if actionErr := m.reactivateRoom(ctx, profile, roomID, requesterMXID); actionErr != nil {
			return nil, actionErr
		}
	}
	return m.reactivationResult(ctx, "invited", roomID)
}

func (m *Module) reactivationResult(ctx context.Context, status, roomID string) (any, *actionbase.Error) {
	operation, conversation, err := m.conversation.Operation(ctx, actionReactivate, status, roomID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	result := map[string]any{"status": status, "room_id": roomID, "operation": operation}
	if conversation != nil {
		result["conversation"] = *conversation
	}
	return result, nil
}
