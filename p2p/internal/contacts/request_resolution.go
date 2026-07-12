package contacts

import (
	"context"
	"net/http"

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
	}
	if acceptedStatus(existing.Status) {
		view, actionErr := m.viewWithOperation(ctx, actionRequestReject, existing)
		if actionErr != nil {
			return nil, actionErr
		}
		return view, nil
	}

	displayName := params.String("display_name")
	if existing.DisplayName != "" {
		displayName = existing.DisplayName
	}
	contact := dirextalkdomain.ContactRecord{
		PeerMXID:    params.FirstString("peer_mxid", "mxid"),
		DisplayName: displayName,
		AvatarURL:   params.String("avatar_url"),
		Domain:      params.String("domain"),
		RoomID:      roomID,
		Status:      "rejected",
		Remark:      existing.Remark,
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
	if err := m.Save(ctx, contact); err != nil {
		return nil, actionbase.InternalError(err)
	}
	view, actionErr := m.viewWithOperation(ctx, actionRequestReject, contact)
	if actionErr != nil {
		return nil, actionErr
	}
	return view, nil
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
