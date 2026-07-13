package contacts

import (
	"context"
	"net/http"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

// Delete runs the complete contact deletion workflow for ProductCore and
// cross-domain account-deletion orchestration.
func (m *Module) Delete(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	params := actionbase.Params(raw)
	return m.serializeRoomPeerMutation(ctx, params, func() (any, *actionbase.Error) {
		return m.deleteForPeer(ctx, params)
	})
}

func (m *Module) deleteForPeer(ctx context.Context, params actionbase.Params) (any, *actionbase.Error) {
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

	if !dirextalkdomain.ContactDeleted(contact.Status) && contact.RoomID != "" && m.leaveRoom != nil {
		if actionErr := m.leaveRoom(ctx, contact.RoomID); actionErr != nil {
			return nil, actionErr
		}
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
	return m.statusOperationResult(ctx, actionDelete, contact.Status, contact.RoomID)
}

func (m *Module) handleUpdate(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	params := actionbase.Params(raw)
	roomID := params.String("room_id")
	if roomID == "" {
		return nil, actionbase.BadRequest("room_id is required")
	}
	displayName := params.String("display_name")
	if displayName == "" {
		return nil, actionbase.BadRequest("display_name is required")
	}

	contact, ok, err := m.LookupByRoom(ctx, roomID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.StatusError(http.StatusNotFound, "contact not found")
	}

	var result any
	var actionErr *actionbase.Error
	m.SerializePeer(contact.PeerMXID, func() {
		result, actionErr = m.updateForPeer(ctx, roomID, displayName, params.String("domain"), params.String("avatar_url"))
	})
	return result, actionErr
}

func (m *Module) updateForPeer(ctx context.Context, roomID, displayName, domain, avatarURL string) (any, *actionbase.Error) {
	contact, ok, err := m.LookupByRoom(ctx, roomID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.StatusError(http.StatusNotFound, "contact not found")
	}
	if !acceptedStatus(contact.Status) {
		return nil, actionbase.StatusError(http.StatusForbidden, "contact is not accepted")
	}

	contact.DisplayName = displayName
	contact.DisplayNameOverride = true
	if domain != "" {
		contact.Domain = domain
	}
	if avatarURL != "" {
		contact.AvatarURL = avatarURL
	}
	if err := m.Save(ctx, contact); err != nil {
		return nil, actionbase.InternalError(err)
	}
	view, actionErr := m.viewWithOperation(ctx, actionUpdate, contact)
	if actionErr != nil {
		return nil, actionErr
	}
	return view, nil
}
