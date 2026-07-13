package contacts

import (
	"context"
	"fmt"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

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
