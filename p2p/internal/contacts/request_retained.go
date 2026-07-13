package contacts

import (
	"context"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

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
