package contacts

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

// Request validates and serializes the complete outbound contact-request
// lifecycle. All peer, Matrix, and persistence work stays inside one peer
// boundary so a waiting request re-reads the result of the previous one.
func (m *Module) Request(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	params := actionbase.Params(raw)
	peerMXID := params.String("mxid")
	if peerMXID == "" {
		return nil, actionbase.BadRequest("mxid is required")
	}
	if m.localProfile == nil {
		return nil, actionbase.InternalError(errors.New("local contact profile is not configured"))
	}
	if peerMXID == m.localProfile().MXID {
		return nil, actionbase.BadRequest("mxid must be a remote peer")
	}

	var result any
	var actionErr *actionbase.Error
	m.SerializePeer(peerMXID, func() {
		result, actionErr = m.requestForPeer(ctx, peerMXID, raw)
	})
	return result, actionErr
}

func (m *Module) requestForPeer(ctx context.Context, peerMXID string, raw map[string]any) (any, *actionbase.Error) {
	if m.checkPeerBlocked == nil {
		return nil, actionbase.InternalError(errors.New("peer block checker is not configured"))
	}
	blocked, err := m.checkPeerBlocked(ctx, peerMXID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if blocked {
		return nil, actionbase.StatusError(http.StatusForbidden, "already blocked")
	}

	params := actionbase.Params(raw)
	domain := params.String("domain")
	if domain == "" {
		domain = dirextalkdomain.DomainFromMXID(peerMXID)
	}
	existing, ok, err := m.LookupByPeer(ctx, peerMXID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if ok {
		switch strings.ToLower(strings.TrimSpace(existing.Status)) {
		case "deleted":
			return m.RestoreDeleted(ctx, existing, raw, domain)
		case "pending_inbound":
			return m.AcceptPendingInbound(ctx, existing, raw)
		case "pending_outbound":
			return m.ResendPendingOutbound(ctx, existing, raw, domain)
		default:
			return m.ResolveExistingRequest(ctx, existing, raw, fallbackRequestString(domain, existing.Domain))
		}
	}

	contact, restored, actionErr := m.RestoreRetainedPeer(ctx, peerMXID, raw, domain)
	if actionErr != nil {
		return nil, actionErr
	}
	if restored {
		return contact, nil
	}
	return m.CreateRequest(ctx, peerMXID, raw, domain)
}

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
		RequestID:   params.String("request_id"),
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

// ResolveExistingRequest returns an existing contact or replaces an accepted
// remote contact when the peer no longer retains its direct room.
func (m *Module) ResolveExistingRequest(
	ctx context.Context,
	contact dirextalkdomain.ContactRecord,
	raw map[string]any,
	fallbackDomain string,
) (any, *actionbase.Error) {
	if acceptedStatus(contact.Status) {
		var err error
		contact, err = m.EnsureAcceptedProjection(ctx, contact)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
	}
	params := actionbase.Params(raw)
	if !acceptedStatus(contact.Status) ||
		params.String("remote_node_base_url") == "" ||
		dirextalkdomain.DomainFromMXID(contact.PeerMXID) == m.serverName {
		return m.existingRequestView(ctx, contact)
	}

	var profile LocalProfileSnapshot
	if m.localProfile != nil {
		profile = m.localProfile()
	}
	if m.reactivatePeer == nil {
		return m.existingRequestView(ctx, contact)
	}
	result, actionErr := m.reactivatePeer(ctx, peerReactivationRequest(contact, params, profile.MXID))
	if actionErr != nil {
		return nil, actionErr
	}
	if result.PendingInbound || result.NotRetained {
		view, actionErr := m.CreateReplacementRequest(ctx, contact, raw, fallbackDomain)
		return view, actionErr
	}
	return m.existingRequestView(ctx, contact)
}

func (m *Module) existingRequestView(ctx context.Context, contact dirextalkdomain.ContactRecord) (any, *actionbase.Error) {
	view, actionErr := m.viewWithOperation(ctx, actionRequest, contact)
	if actionErr != nil {
		return nil, actionErr
	}
	return view, nil
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
	contact = mergeRequestFields(contact, params, fallbackDomain)

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

// RequestPeerApproval records a pending outbound request in an existing room.
// Blank room IDs start a fresh request instead of inheriting the stale record.
func (m *Module) RequestPeerApproval(
	ctx context.Context,
	contact dirextalkdomain.ContactRecord,
	raw map[string]any,
	fallbackDomain string,
	sendMatrixInvite bool,
) (View, *actionbase.Error) {
	if strings.TrimSpace(contact.RoomID) == "" {
		return m.CreateRequest(ctx, contact.PeerMXID, raw, fallbackDomain)
	}
	contact = mergeRequestFields(contact, actionbase.Params(raw), fallbackDomain)
	contact.Status = "pending_outbound"
	if sendMatrixInvite && m.inviteRoom != nil {
		if actionErr := m.inviteRoom(ctx, DirectRoomInviteRequest{Contact: contact}); actionErr != nil {
			return View{}, actionErr
		}
	}
	if err := m.Save(ctx, contact); err != nil {
		return View{}, actionbase.InternalError(err)
	}
	return m.viewWithOperation(ctx, actionRequest, contact)
}

func mergeRequestFields(
	contact dirextalkdomain.ContactRecord,
	params actionbase.Params,
	fallbackDomain string,
) dirextalkdomain.ContactRecord {
	if requestID := params.String("request_id"); requestID != "" {
		contact.RequestID = requestID
	}
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
	return contact
}

func requestRemark(params actionbase.Params) string {
	return params.FirstString("remark", "request_message", "message", "reason")
}

func peerReactivationRequest(
	contact dirextalkdomain.ContactRecord,
	params actionbase.Params,
	requesterMXID string,
) PeerReactivationRequest {
	contact.RequestID = fallbackRequestString(params.String("request_id"), contact.RequestID)
	return PeerReactivationRequest{
		Contact:           contact,
		RequesterMXID:     requesterMXID,
		RemoteNodeBaseURL: params.String("remote_node_base_url"),
		DisplayName:       params.String("display_name"),
		AvatarURL:         params.String("avatar_url"),
		Domain:            params.String("domain"),
		Remark:            requestRemark(params),
	}
}

func fallbackRequestString(value, fallback string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}
