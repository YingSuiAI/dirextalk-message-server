package contacts

import (
	"context"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

// CreateRequest creates and persists one fresh outbound contact request. The
// caller owns the surrounding peer workflow boundary.
func (m *Module) CreateRequest(
	ctx context.Context,
	peerMXID string,
	raw map[string]any,
	domain string,
) (View, *actionbase.Error) {
	params := actionbase.Params(raw)
	fallbackRoomID := ""
	if m.newDirectRoomID != nil {
		fallbackRoomID = m.newDirectRoomID()
	}
	remark := requestRemark(params)
	roomID := fallbackRoomID
	if m.createRoom != nil {
		var actionErr *actionbase.Error
		roomID, actionErr = m.createRoom(ctx, DirectRoomCreateRequest{
			PeerMXID:       peerMXID,
			DisplayName:    params.String("display_name"),
			Remark:         remark,
			FallbackRoomID: fallbackRoomID,
		})
		if actionErr != nil {
			return View{}, actionErr
		}
	}

	contact := dirextalkdomain.ContactRecord{
		PeerMXID:    peerMXID,
		DisplayName: params.String("display_name"),
		AvatarURL:   params.String("avatar_url"),
		Domain:      domain,
		RoomID:      roomID,
		Status:      "pending_outbound",
		Remark:      remark,
	}
	if err := m.Save(ctx, contact); err != nil {
		return View{}, actionbase.InternalError(err)
	}
	return m.viewWithOperation(ctx, actionRequest, contact)
}

// CreateReplacementRequest starts a fresh outbound request while inheriting
// compatible presentation fields from a retained contact snapshot.
func (m *Module) CreateReplacementRequest(
	ctx context.Context,
	contact dirextalkdomain.ContactRecord,
	raw map[string]any,
	fallbackDomain string,
) (View, *actionbase.Error) {
	nextRaw := make(map[string]any, len(raw)+3)
	for key, value := range raw {
		nextRaw[key] = value
	}
	next := actionbase.Params(nextRaw)
	if next.String("display_name") == "" && strings.TrimSpace(contact.DisplayName) != "" {
		nextRaw["display_name"] = contact.DisplayName
	}
	if next.String("avatar_url") == "" && strings.TrimSpace(contact.AvatarURL) != "" {
		nextRaw["avatar_url"] = contact.AvatarURL
	}
	if next.String("domain") == "" {
		nextRaw["domain"] = fallbackRequestString(contact.Domain, fallbackDomain)
	}
	return m.CreateRequest(
		ctx,
		contact.PeerMXID,
		nextRaw,
		fallbackRequestString(actionbase.Params(nextRaw).String("domain"), fallbackDomain),
	)
}

// ResendPendingOutbound refreshes and persists an existing outbound request.
// The caller owns the surrounding peer workflow boundary.
func (m *Module) ResendPendingOutbound(
	ctx context.Context,
	contact dirextalkdomain.ContactRecord,
	raw map[string]any,
	fallbackDomain string,
) (View, *actionbase.Error) {
	params := actionbase.Params(raw)
	if remark := requestRemark(params); remark != "" {
		contact.Remark = remark
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

	if m.inviteRoom != nil && contact.RoomID != "" {
		if actionErr := m.inviteRoom(ctx, DirectRoomInviteRequest{Contact: contact}); actionErr != nil {
			return View{}, actionErr
		}
	}
	if err := m.Save(ctx, contact); err != nil {
		return View{}, actionbase.InternalError(err)
	}
	return m.viewWithOperation(ctx, actionRequest, contact)
}

func requestRemark(params actionbase.Params) string {
	return params.FirstString("remark", "request_message", "message", "reason")
}

func fallbackRequestString(value, fallback string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}
