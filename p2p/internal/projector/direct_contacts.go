package projector

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkstate"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
)

func (m *Module) contactRequestFromInvite(event *types.HeaderedEvent, identity IdentitySnapshot) (dirextalkdomain.ContactRecord, bool) {
	unsigned := struct {
		InviteRoomState []struct {
			Type    string         `json:"type"`
			Sender  string         `json:"sender"`
			Content map[string]any `json:"content"`
		} `json:"invite_room_state"`
	}{}
	if len(event.Unsigned()) == 0 {
		return dirextalkdomain.ContactRecord{}, false
	}
	if err := json.Unmarshal(event.Unsigned(), &unsigned); err != nil {
		return dirextalkdomain.ContactRecord{}, false
	}
	for _, state := range unsigned.InviteRoomState {
		if state.Type == dirextalkstate.RoomProfileEventType && textValue(state.Content["room_type"]) == dirextalkstate.RoomTypeDirect {
			if contact, ok := contactRequestFromContent(
				event.RoomID().String(), string(event.SenderID()), state.Content, identity.OwnerMXID,
			); ok {
				return contact, true
			}
		}
	}
	return dirextalkdomain.ContactRecord{}, false
}

func (m *Module) directContactFromInvite(event *types.HeaderedEvent, identity IdentitySnapshot) (dirextalkdomain.ContactRecord, bool) {
	unsigned := struct {
		InviteRoomState []struct {
			Type     string         `json:"type"`
			Sender   string         `json:"sender"`
			StateKey string         `json:"state_key"`
			Content  map[string]any `json:"content"`
		} `json:"invite_room_state"`
	}{}
	if len(event.Unsigned()) == 0 {
		return dirextalkdomain.ContactRecord{}, false
	}
	if err := json.Unmarshal(event.Unsigned(), &unsigned); err != nil {
		return dirextalkdomain.ContactRecord{}, false
	}
	requester := string(event.SenderID())
	if requester == "" || requester == identity.OwnerMXID {
		return dirextalkdomain.ContactRecord{}, false
	}
	displayName := ""
	avatarURL := ""
	for _, state := range unsigned.InviteRoomState {
		if state.Type != "m.room.member" {
			continue
		}
		if state.StateKey != requester && state.Sender != requester {
			continue
		}
		if membership := textValue(state.Content["membership"]); membership != "" && !strings.EqualFold(membership, "join") {
			continue
		}
		displayName = textValue(state.Content["displayname"])
		avatarURL = textValue(state.Content["avatar_url"])
		break
	}
	return dirextalkdomain.ContactRecord{
		PeerMXID:    requester,
		DisplayName: fallbackText(displayName, dirextalkdomain.DisplayNameFromMXID(requester)),
		AvatarURL:   avatarURL,
		Domain:      dirextalkdomain.DomainFromMXID(requester),
		RoomID:      event.RoomID().String(),
		Status:      "pending_inbound",
	}, true
}

func contactRequestFromContent(roomID, sender string, content map[string]any, ownerMXID string) (dirextalkdomain.ContactRecord, bool) {
	requester := strings.TrimSpace(sender)
	if requester == "" || requester == ownerMXID {
		return dirextalkdomain.ContactRecord{}, false
	}
	target := textValue(content["target_mxid"])
	if target != "" && target != ownerMXID {
		return dirextalkdomain.ContactRecord{}, false
	}
	trustedProfile := true
	if claimedRequester := textValue(content["requester_mxid"]); claimedRequester != "" && claimedRequester != requester {
		trustedProfile = false
	}
	displayName := dirextalkdomain.DisplayNameFromMXID(requester)
	avatarURL := ""
	remark := ""
	if trustedProfile {
		displayName = fallbackText(textValue(content["display_name"]), displayName)
		avatarURL = textValue(content["avatar_url"])
		remark = contactRequestRemark(content)
	}
	return dirextalkdomain.ContactRecord{
		PeerMXID: requester, DisplayName: displayName, AvatarURL: avatarURL,
		Domain: dirextalkdomain.DomainFromMXID(requester), RoomID: roomID,
		Status: "pending_inbound", Remark: remark,
	}, true
}

func contactRequestRemark(content map[string]any) string {
	for _, key := range []string{"remark", "request_message", "message", "reason"} {
		if value := textValue(content[key]); value != "" {
			return value
		}
	}
	return ""
}

func (m *Module) savePendingInboundContact(ctx context.Context, contact dirextalkdomain.ContactRecord, identity IdentitySnapshot) error {
	if m.dependencies.Contacts == nil {
		return errors.New("contact projection port is not configured")
	}
	return m.dependencies.Contacts.WithPeer(contact.PeerMXID, func() error {
		return m.savePendingInboundContactForPeer(ctx, contact, identity)
	})
}

func (m *Module) savePendingInboundContactForPeer(ctx context.Context, contact dirextalkdomain.ContactRecord, identity IdentitySnapshot) error {
	if m.dependencies.Blocks == nil {
		return errors.New("block projection port is not configured")
	}
	blocked, err := m.dependencies.Blocks.Exists(ctx, "contact", contact.PeerMXID)
	if err != nil {
		return err
	}
	if blocked {
		return nil
	}
	contact.Status = "pending_inbound"
	contacts, err := m.dependencies.Contacts.ListRaw(ctx)
	if err != nil {
		return err
	}
	for _, existing := range contacts {
		if existing.PeerMXID != contact.PeerMXID {
			continue
		}
		if acceptedContact(existing.Status) {
			retained, err := m.reinviteAcceptedContactToRetainedRoom(ctx, existing, identity)
			if err != nil || retained {
				return err
			}
			return m.acceptReplacementDirectInvite(ctx, existing, contact, identity)
		}
		if strings.EqualFold(strings.TrimSpace(existing.Status), "pending_outbound") {
			return m.acceptReplacementDirectInvite(ctx, existing, contact, identity)
		}
		if !deletedContact(existing.Status) &&
			!strings.EqualFold(strings.TrimSpace(existing.Status), "rejected") &&
			!strings.EqualFold(strings.TrimSpace(existing.Status), "reject") {
			return nil
		}
	}
	if err := m.dependencies.Contacts.Save(ctx, contact); err != nil {
		return err
	}
	return m.appendEvent(ctx, dirextalkdomain.Event{
		Type:   "contact.requested",
		RoomID: contact.RoomID,
		Payload: map[string]any{
			"room_id": contact.RoomID, "peer_mxid": contact.PeerMXID,
			"display_name": contact.DisplayName, "avatar_url": contact.AvatarURL,
			"domain": contact.Domain, "status": contact.Status, "remark": contact.Remark,
		},
	})
}

func (m *Module) reinviteAcceptedContactToRetainedRoom(
	ctx context.Context, contact dirextalkdomain.ContactRecord, identity IdentitySnapshot,
) (bool, error) {
	if m.dependencies.DirectRooms == nil || strings.TrimSpace(contact.RoomID) == "" || strings.TrimSpace(contact.PeerMXID) == "" {
		return true, nil
	}
	disposition, err := m.dependencies.DirectRooms.ReinviteAcceptedContact(ctx, contact, identity)
	if err != nil {
		return false, err
	}
	return disposition != ReinviteReplacementRequired, nil
}

func (m *Module) acceptReplacementDirectInvite(
	ctx context.Context, existing, invite dirextalkdomain.ContactRecord, identity IdentitySnapshot,
) error {
	roomID := strings.TrimSpace(invite.RoomID)
	if roomID == "" {
		return nil
	}
	if m.dependencies.DirectRooms != nil {
		joinedRoomID, err := m.dependencies.DirectRooms.JoinReplacementRoom(ctx, roomID, identity)
		if err != nil {
			return err
		}
		if strings.TrimSpace(joinedRoomID) != "" {
			roomID = joinedRoomID
		}
	}
	replacement := existing
	replacement.RoomID = roomID
	replacement.Status = "accepted"
	replacement.Remark = ""
	if !replacement.DisplayNameOverride && strings.TrimSpace(invite.DisplayName) != "" {
		replacement.DisplayName = invite.DisplayName
	}
	if strings.TrimSpace(invite.AvatarURL) != "" {
		replacement.AvatarURL = invite.AvatarURL
	}
	if strings.TrimSpace(replacement.Domain) == "" {
		replacement.Domain = invite.Domain
	}
	return m.dependencies.Contacts.Save(ctx, replacement)
}
