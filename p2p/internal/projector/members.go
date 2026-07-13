package projector

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkstate"
	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
)

//nolint:gocyclo // Matrix membership fans into direct-contact, group, and channel projections.
func (m *Module) projectMember(ctx context.Context, event *types.HeaderedEvent) error {
	content := map[string]any{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		return err
	}
	identity := m.identity()
	userID := ""
	if event.StateKey() != nil {
		userID = productpolicy.UserIDFromStateKey(*event.StateKey())
	}
	if override := textValue(content["user_id"]); override != "" {
		userID = override
	}
	if userID == "" {
		userID = string(event.SenderID())
	}
	roomID := event.RoomID().String()
	channelID := textValue(content["channel_id"])
	if channelID == "" {
		channelID = m.channelIDForRoom(ctx, roomID)
	}
	if m.dependencies.Members == nil {
		return errors.New("member projection port is not configured")
	}
	existing, hasExisting, err := m.dependencies.Members.Lookup(ctx, roomID, userID)
	if err != nil {
		return err
	}
	if channelID == "" && hasExisting {
		channelID = existing.ChannelID
	}
	displayName := existing.DisplayName
	if _, ok := content["displayname"]; ok {
		displayName = textValue(content["displayname"])
	}
	avatarURL := existing.AvatarURL
	if _, ok := content["avatar_url"]; ok {
		avatarURL = textValue(content["avatar_url"])
	}
	domain := fallbackText(existing.Domain, dirextalkdomain.DomainFromMXID(userID))
	membership := fallbackText(textValue(content["membership"]), fallbackText(existing.Membership, "join"))
	role := existing.Role
	if nextRole := textValue(content["role"]); nextRole != "" {
		role = nextRole
	}
	role = fallbackText(role, "member")
	muted := existing.Muted
	if _, ok := content["muted"]; ok {
		muted = boolValue(content["muted"])
	}
	joinedAt := existing.JoinedAt
	if joinedAt == 0 {
		joinedAt = m.eventTime(event).UnixMilli()
	}
	member := dirextalkdomain.MemberRecord{
		RoomID: roomID, ChannelID: channelID, UserID: userID,
		DisplayName: displayName, AvatarURL: avatarURL, Domain: domain,
		Membership: membership, Role: role, Muted: muted, JoinedAt: joinedAt,
	}
	if member.ChannelID == "" &&
		strings.EqualFold(member.Membership, "invite") &&
		userID == identity.OwnerMXID {
		if contact, ok := m.contactRequestFromInvite(event, identity); ok {
			return m.savePendingInboundContact(ctx, contact, identity)
		}
		if boolValue(content["is_direct"]) {
			if contact, ok := m.directContactFromInvite(event, identity); ok {
				return m.savePendingInboundContact(ctx, contact, identity)
			}
			return m.savePendingInboundContact(ctx, dirextalkdomain.ContactRecord{
				PeerMXID:    string(event.SenderID()),
				DisplayName: dirextalkdomain.DisplayNameFromMXID(string(event.SenderID())),
				Domain:      dirextalkdomain.DomainFromMXID(string(event.SenderID())),
				RoomID:      event.RoomID().String(),
				Status:      "pending_inbound",
			}, identity)
		}
	}
	if err := m.dependencies.Members.Save(ctx, member); err != nil {
		return err
	}
	// Preserve the historic best-effort member delta. Member persistence and
	// downstream invite/contact projection are not rolled back on outbox error.
	_ = m.appendEvent(ctx, dirextalkdomain.Event{
		Type:      "room.member.projected",
		RoomID:    member.RoomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("room.member.projected", event.EventID(), member.UserID),
		Payload: map[string]any{
			"user_id": member.UserID, "membership": member.Membership, "channel_id": member.ChannelID,
		},
	})
	if strings.EqualFold(member.Membership, "invite") &&
		!boolValue(content["is_direct"]) &&
		userID == identity.OwnerMXID {
		if updated, ok, err := m.projectProductInvite(ctx, event, member); err != nil {
			return err
		} else if ok {
			return m.dependencies.Members.Save(ctx, updated)
		}
	}
	if member.ChannelID == "" &&
		strings.EqualFold(member.Membership, "join") &&
		userID != "" &&
		userID != identity.OwnerMXID {
		if err := m.projectDirectContactMember(ctx, member, content); err != nil {
			return err
		}
	}
	return nil
}

func (m *Module) projectDirectContactMember(ctx context.Context, member dirextalkdomain.MemberRecord, content map[string]any) error {
	if m.dependencies.Contacts == nil {
		return errors.New("contact projection port is not configured")
	}
	return m.dependencies.Contacts.WithPeer(member.UserID, func() error {
		return m.projectDirectContactMemberForPeer(ctx, member, content)
	})
}

func (m *Module) projectDirectContactMemberForPeer(ctx context.Context, member dirextalkdomain.MemberRecord, content map[string]any) error {
	contact, ok, err := m.dependencies.Contacts.LookupByRoom(ctx, member.RoomID)
	if err != nil || !ok {
		return err
	}
	if contact.PeerMXID != "" && contact.PeerMXID != member.UserID {
		return nil
	}
	changed := false
	if contact.PeerMXID == "" {
		contact.PeerMXID = member.UserID
		changed = true
	}
	if member.DisplayName != "" && !contact.DisplayNameOverride && contact.DisplayName != member.DisplayName {
		contact.DisplayName = member.DisplayName
		changed = true
	}
	if _, hasAvatar := content["avatar_url"]; hasAvatar && contact.AvatarURL != member.AvatarURL {
		contact.AvatarURL = member.AvatarURL
		changed = true
	}
	if contact.Domain == "" {
		contact.Domain = dirextalkdomain.DomainFromMXID(member.UserID)
		changed = true
	}
	if strings.EqualFold(contact.Status, "pending_outbound") {
		contact.Status = "accepted"
		contact.Remark = ""
		changed = true
	}
	if !changed {
		return nil
	}
	return m.dependencies.Contacts.Save(ctx, contact)
}

func (m *Module) projectProductInvite(ctx context.Context, event *types.HeaderedEvent, member dirextalkdomain.MemberRecord) (dirextalkdomain.MemberRecord, bool, error) {
	content, ok := productInviteFromInvite(event)
	if !ok {
		return member, false, nil
	}
	switch textValue(content["room_type"]) {
	case dirextalkstate.RoomTypeGroup:
		if m.dependencies.Groups == nil {
			return member, false, errors.New("group projection port is not configured")
		}
		group := dirextalkdomain.GroupRecord{
			RoomID:       fallbackText(textValue(content["room_id"]), event.RoomID().String()),
			Name:         fallbackText(textValue(content["name"]), event.RoomID().String()),
			Topic:        textValue(content["topic"]),
			AvatarURL:    textValue(content["avatar_url"]),
			InvitePolicy: fallbackText(textValue(content["invite_policy"]), "member"),
		}
		if err := m.dependencies.Groups.Save(ctx, group); err != nil {
			return member, false, err
		}
		member.RoomID = group.RoomID
		member.ChannelID = ""
		return member, true, nil
	case dirextalkstate.RoomTypeChannel:
		if m.dependencies.Channels == nil {
			return member, false, errors.New("channel projection port is not configured")
		}
		channel := dirextalkdomain.Channel{
			ChannelID:       fallbackText(textValue(content["channel_id"]), event.RoomID().String()),
			RoomID:          fallbackText(textValue(content["room_id"]), event.RoomID().String()),
			Name:            fallbackText(textValue(content["name"]), event.RoomID().String()),
			Description:     textValue(content["description"]),
			AvatarURL:       textValue(content["avatar_url"]),
			Visibility:      fallbackText(textValue(content["visibility"]), "public"),
			JoinPolicy:      fallbackText(textValue(content["join_policy"]), "invite"),
			ChannelType:     fallbackText(textValue(content["channel_type"]), "post"),
			CommentsEnabled: boolValue(content["comments_enabled"]),
			MemberStatus:    "invite",
			Role:            fallbackText(member.Role, "member"),
		}
		if err := m.dependencies.Channels.SaveWithConversation(ctx, channel); err != nil {
			return member, false, err
		}
		member.RoomID = channel.RoomID
		member.ChannelID = channel.ChannelID
		return member, true, nil
	default:
		return member, false, nil
	}
}

func productInviteFromInvite(event *types.HeaderedEvent) (map[string]any, bool) {
	unsigned := struct {
		InviteRoomState []struct {
			Type    string         `json:"type"`
			Content map[string]any `json:"content"`
		} `json:"invite_room_state"`
	}{}
	if len(event.Unsigned()) == 0 {
		return nil, false
	}
	if err := json.Unmarshal(event.Unsigned(), &unsigned); err != nil {
		return nil, false
	}
	for _, state := range unsigned.InviteRoomState {
		if state.Type == dirextalkstate.RoomProfileEventType {
			switch textValue(state.Content["room_type"]) {
			case dirextalkstate.RoomTypeGroup, dirextalkstate.RoomTypeChannel:
				return state.Content, true
			}
		}
	}
	return nil, false
}
