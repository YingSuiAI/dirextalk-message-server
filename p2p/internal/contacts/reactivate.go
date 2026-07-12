package contacts

import (
	"context"
	"net/http"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

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
