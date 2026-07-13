package contacts

import (
	"context"
	"fmt"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

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
