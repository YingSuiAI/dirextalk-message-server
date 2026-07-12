package contacts

import (
	"context"
	"net/http"

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
	var existing dirextalkdomain.ContactRecord
	if roomID != "" {
		contact, ok, err := m.LookupByRoom(ctx, roomID)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		if !ok {
			return nil, actionbase.StatusError(http.StatusNotFound, "contact request not found")
		}
		existing = contact
		if peerMXID == "" {
			peerMXID = existing.PeerMXID
		}
	}
	if acceptedStatus(existing.Status) {
		view, actionErr := m.viewWithOperation(ctx, actionRequestAccept, existing)
		if actionErr != nil {
			return nil, actionErr
		}
		return view, nil
	}

	if m.acceptRoom != nil && roomID != "" {
		resolvedRoomID, actionErr := m.acceptRoom(ctx, existing, params.Strings("server_names"))
		if actionErr != nil {
			return nil, actionErr
		}
		roomID = resolvedRoomID
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
	}
	if contact.AvatarURL == "" {
		contact.AvatarURL = existing.AvatarURL
	}
	if contact.Domain == "" {
		contact.Domain = existing.Domain
	}
	if err := m.Save(ctx, contact); err != nil {
		return nil, actionbase.InternalError(err)
	}
	view, actionErr := m.viewWithOperation(ctx, actionRequestAccept, contact)
	if actionErr != nil {
		return nil, actionErr
	}
	return view, nil
}
