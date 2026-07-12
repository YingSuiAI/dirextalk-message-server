package contacts

import (
	"context"
	"net/http"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

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
	operation, conversation, err := m.conversation.Operation(ctx, actionUpdate, contact.Status, contact.RoomID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	view := ViewFromRecord(contact)
	view.Operation = operation
	view.Conversation = conversation
	return view, nil
}
