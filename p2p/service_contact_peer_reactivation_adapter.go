package p2p

import (
	"context"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	contactsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/contacts"
)

func (s *Service) reactivatePeerContact(ctx context.Context, request contactsmodule.PeerReactivationRequest) (contactsmodule.PeerReactivationResult, *apiError) {
	peerServer := dirextalkdomain.DomainFromMXID(request.Contact.PeerMXID)
	if peerServer == "" || peerServer == s.serverName {
		return contactsmodule.PeerReactivationResult{}, statusError(http.StatusForbidden, "peer node is required to reactivate direct room")
	}

	remoteBase := strings.TrimSpace(request.RemoteNodeBaseURL)
	if remoteBase == "" {
		remoteBase = "https://" + peerServer + "/_p2p"
	}
	var result map[string]any
	status, err := s.remotePublicAction(ctx, peerServer, "contacts.reactivate", map[string]any{
		"room_id":              request.Contact.RoomID,
		"requester_mxid":       request.RequesterMXID,
		"remote_node_base_url": remoteBase,
		"display_name":         request.DisplayName,
		"avatar_url":           request.AvatarURL,
		"domain":               request.Domain,
		"remark":               request.Remark,
	}, &result)
	if err != nil {
		if status != 0 && status != http.StatusBadGateway {
			return contactsmodule.PeerReactivationResult{}, statusError(status, err.Error())
		}
		return contactsmodule.PeerReactivationResult{}, statusError(http.StatusBadGateway, err.Error())
	}
	if status != http.StatusOK {
		if status == http.StatusNotFound {
			return contactsmodule.PeerReactivationResult{NotRetained: true}, nil
		}
		return contactsmodule.PeerReactivationResult{}, statusError(status, "target node contact reactivation failed")
	}
	if strings.EqualFold(trimString(result["status"]), "pending_inbound") {
		return contactsmodule.PeerReactivationResult{PendingInbound: true, RoomID: trimString(result["room_id"])}, nil
	}
	return contactsmodule.PeerReactivationResult{RoomID: trimString(result["room_id"])}, nil
}
