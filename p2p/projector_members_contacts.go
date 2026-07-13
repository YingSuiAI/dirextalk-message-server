package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	channelsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/channels"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
)

func (s *Service) projectReaction(ctx context.Context, event *types.HeaderedEvent) error {
	content := map[string]any{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		return err
	}
	if s.channelContentModule == nil {
		return errors.New("channel content module is not configured")
	}
	return s.channelContentModule.ProjectReaction(ctx, channelsmodule.ProjectionEvent{
		RoomID:         event.RoomID().String(),
		EventID:        event.EventID(),
		SenderMXID:     string(event.SenderID()),
		OriginServerTS: int64(event.OriginServerTS()),
		Content:        content,
	})
}

//nolint:gocyclo // Member projection accepts Matrix, direct-contact, group, and channel membership state.
func (s *Service) projectMember(ctx context.Context, event *types.HeaderedEvent) error {
	content := map[string]any{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		return err
	}
	userID := ""
	if event.StateKey() != nil {
		userID = productpolicy.UserIDFromStateKey(*event.StateKey())
	}
	if override := trimString(content["user_id"]); override != "" {
		userID = override
	}
	if userID == "" {
		userID = string(event.SenderID())
	}
	roomID := event.RoomID().String()
	channelID := trimString(content["channel_id"])
	if channelID == "" {
		channelID = s.channelIDForRoom(ctx, roomID)
	}
	existing, hasExisting, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return err
	}
	if channelID == "" && hasExisting {
		channelID = existing.ChannelID
	}
	displayName := existing.DisplayName
	if _, ok := content["displayname"]; ok {
		displayName = trimString(content["displayname"])
	}
	avatarURL := existing.AvatarURL
	if _, ok := content["avatar_url"]; ok {
		avatarURL = trimString(content["avatar_url"])
	}
	domain := fallbackString(existing.Domain, domainFromMXID(userID))
	membership := fallbackString(trimString(content["membership"]), fallbackString(existing.Membership, "join"))
	role := existing.Role
	if nextRole := trimString(content["role"]); nextRole != "" {
		role = nextRole
	}
	role = fallbackString(role, "member")
	muted := existing.Muted
	if _, ok := content["muted"]; ok {
		muted = boolParam(content["muted"])
	}
	joinedAt := existing.JoinedAt
	if joinedAt == 0 {
		joinedAt = eventTime(event).UnixMilli()
	}
	member := memberRecord{
		RoomID:      roomID,
		ChannelID:   channelID,
		UserID:      userID,
		DisplayName: displayName,
		AvatarURL:   avatarURL,
		Domain:      domain,
		Membership:  membership,
		Role:        role,
		Muted:       muted,
		JoinedAt:    joinedAt,
	}
	if member.ChannelID == "" &&
		strings.EqualFold(member.Membership, "invite") &&
		userID == s.ownerMXID {
		if contact, ok := s.contactRequestFromInvite(event); ok {
			return s.savePendingInboundContact(ctx, contact)
		}
		if boolParam(content["is_direct"]) {
			if contact, ok := s.directContactFromInvite(event); ok {
				return s.savePendingInboundContact(ctx, contact)
			}
			return s.savePendingInboundContact(ctx, contactRecord{
				PeerMXID:    string(event.SenderID()),
				DisplayName: displayNameFromMXID(string(event.SenderID())),
				Domain:      domainFromMXID(string(event.SenderID())),
				RoomID:      event.RoomID().String(),
				Status:      "pending_inbound",
			})
		}
	}
	if err := s.saveMember(ctx, member); err != nil {
		return err
	}
	_ = s.appendP2PEvent(ctx, p2pEvent{
		Type:      "room.member.projected",
		RoomID:    member.RoomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("room.member.projected", event.EventID(), member.UserID),
		Payload:   map[string]any{"user_id": member.UserID, "membership": member.Membership, "channel_id": member.ChannelID},
	})
	if strings.EqualFold(member.Membership, "invite") &&
		!boolParam(content["is_direct"]) &&
		userID == s.ownerMXID {
		if updated, ok, err := s.projectProductInvite(ctx, event, member); err != nil {
			return err
		} else if ok {
			return s.saveMember(ctx, updated)
		}
	}
	if member.ChannelID == "" &&
		strings.EqualFold(member.Membership, "join") &&
		userID != "" &&
		userID != s.ownerMXID {
		if err := s.projectDirectContactMember(ctx, member, content); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) projectDirectContactMember(ctx context.Context, member memberRecord, content map[string]any) error {
	var projectionErr error
	s.contactsModule.SerializePeer(member.UserID, func() {
		projectionErr = s.projectDirectContactMemberForPeer(ctx, member, content)
	})
	return projectionErr
}

func (s *Service) projectDirectContactMemberForPeer(ctx context.Context, member memberRecord, content map[string]any) error {
	contact, ok, err := s.lookupContactByRoom(ctx, member.RoomID)
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
		contact.Domain = domainFromMXID(member.UserID)
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
	return s.saveContact(ctx, contact)
}

func (s *Service) projectProductInvite(ctx context.Context, event *types.HeaderedEvent, member memberRecord) (memberRecord, bool, error) {
	content, ok := s.productInviteFromInvite(event)
	if !ok {
		return member, false, nil
	}
	switch trimString(content["room_type"]) {
	case DirextalkRoomTypeGroup:
		group := groupRecord{
			RoomID:       fallbackString(trimString(content["room_id"]), event.RoomID().String()),
			Name:         fallbackString(trimString(content["name"]), event.RoomID().String()),
			Topic:        trimString(content["topic"]),
			AvatarURL:    trimString(content["avatar_url"]),
			InvitePolicy: fallbackString(trimString(content["invite_policy"]), "member"),
		}
		if err := s.saveGroup(ctx, group); err != nil {
			return member, false, err
		}
		member.RoomID = group.RoomID
		member.ChannelID = ""
		return member, true, nil
	case DirextalkRoomTypeChannel:
		ch := channel{
			ChannelID:       fallbackString(trimString(content["channel_id"]), event.RoomID().String()),
			RoomID:          fallbackString(trimString(content["room_id"]), event.RoomID().String()),
			Name:            fallbackString(trimString(content["name"]), event.RoomID().String()),
			Description:     trimString(content["description"]),
			AvatarURL:       trimString(content["avatar_url"]),
			Visibility:      fallbackString(trimString(content["visibility"]), "public"),
			JoinPolicy:      fallbackString(trimString(content["join_policy"]), "invite"),
			ChannelType:     fallbackString(trimString(content["channel_type"]), "post"),
			CommentsEnabled: boolParam(content["comments_enabled"]),
			MemberStatus:    "invite",
			Role:            fallbackString(member.Role, "member"),
		}
		if err := s.saveChannel(ctx, ch); err != nil {
			return member, false, err
		}
		member.RoomID = ch.RoomID
		member.ChannelID = ch.ChannelID
		return member, true, nil
	default:
		return member, false, nil
	}
}

func (s *Service) productInviteFromInvite(event *types.HeaderedEvent) (map[string]any, bool) {
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
		if state.Type == DirextalkRoomProfileEventType {
			switch trimString(state.Content["room_type"]) {
			case DirextalkRoomTypeGroup, DirextalkRoomTypeChannel:
				return state.Content, true
			}
		}
	}
	return nil, false
}

func (s *Service) contactRequestFromInvite(event *types.HeaderedEvent) (contactRecord, bool) {
	unsigned := struct {
		InviteRoomState []struct {
			Type    string         `json:"type"`
			Sender  string         `json:"sender"`
			Content map[string]any `json:"content"`
		} `json:"invite_room_state"`
	}{}
	if len(event.Unsigned()) == 0 {
		return contactRecord{}, false
	}
	if err := json.Unmarshal(event.Unsigned(), &unsigned); err != nil {
		return contactRecord{}, false
	}
	for _, state := range unsigned.InviteRoomState {
		if state.Type == DirextalkRoomProfileEventType && trimString(state.Content["room_type"]) == DirextalkRoomTypeDirect {
			if contact, ok := s.contactRequestFromContent(event.RoomID().String(), string(event.SenderID()), state.Content); ok {
				return contact, true
			}
		}
	}
	return contactRecord{}, false
}

func (s *Service) directContactFromInvite(event *types.HeaderedEvent) (contactRecord, bool) {
	unsigned := struct {
		InviteRoomState []struct {
			Type     string         `json:"type"`
			Sender   string         `json:"sender"`
			StateKey string         `json:"state_key"`
			Content  map[string]any `json:"content"`
		} `json:"invite_room_state"`
	}{}
	if len(event.Unsigned()) == 0 {
		return contactRecord{}, false
	}
	if err := json.Unmarshal(event.Unsigned(), &unsigned); err != nil {
		return contactRecord{}, false
	}
	requester := string(event.SenderID())
	if requester == "" || requester == s.ownerMXID {
		return contactRecord{}, false
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
		if membership := trimString(state.Content["membership"]); membership != "" && !strings.EqualFold(membership, "join") {
			continue
		}
		displayName = trimString(state.Content["displayname"])
		avatarURL = trimString(state.Content["avatar_url"])
		break
	}
	return contactRecord{
		PeerMXID:    requester,
		DisplayName: fallbackString(displayName, displayNameFromMXID(requester)),
		AvatarURL:   avatarURL,
		Domain:      domainFromMXID(requester),
		RoomID:      event.RoomID().String(),
		Status:      "pending_inbound",
	}, true
}

func (s *Service) contactRequestFromContent(roomID, sender string, content map[string]any) (contactRecord, bool) {
	requester := strings.TrimSpace(sender)
	if requester == "" || requester == s.ownerMXID {
		return contactRecord{}, false
	}
	target := trimString(content["target_mxid"])
	if target != "" && target != s.ownerMXID {
		return contactRecord{}, false
	}
	trustedProfile := true
	if claimedRequester := trimString(content["requester_mxid"]); claimedRequester != "" && claimedRequester != requester {
		trustedProfile = false
	}
	displayName := displayNameFromMXID(requester)
	avatarURL := ""
	remark := ""
	if trustedProfile {
		displayName = fallbackString(trimString(content["display_name"]), displayName)
		avatarURL = trimString(content["avatar_url"])
		remark = contactRequestRemark(content)
	}
	return contactRecord{
		PeerMXID:    requester,
		DisplayName: displayName,
		AvatarURL:   avatarURL,
		Domain:      domainFromMXID(requester),
		RoomID:      roomID,
		Status:      "pending_inbound",
		Remark:      remark,
	}, true
}

func (s *Service) savePendingInboundContact(ctx context.Context, contact contactRecord) error {
	var saveErr error
	s.contactsModule.SerializePeer(contact.PeerMXID, func() {
		saveErr = s.savePendingInboundContactForPeer(ctx, contact)
	})
	return saveErr
}

func (s *Service) savePendingInboundContactForPeer(ctx context.Context, contact contactRecord) error {
	blocked, err := s.blockExists(ctx, "contact", contact.PeerMXID)
	if err != nil {
		return err
	}
	if blocked {
		return nil
	}
	contact.Status = "pending_inbound"
	contacts, err := s.rawContacts(ctx)
	if err != nil {
		return err
	}
	for _, existing := range contacts {
		if existing.PeerMXID != contact.PeerMXID {
			continue
		}
		if contactAccepted(existing.Status) {
			reinvited, err := s.reinviteAcceptedContactToRetainedRoom(ctx, existing)
			if err != nil || reinvited {
				return err
			}
			return s.acceptReplacementDirectInvite(ctx, existing, contact)
		}
		if strings.EqualFold(strings.TrimSpace(existing.Status), "pending_outbound") {
			return s.acceptReplacementDirectInvite(ctx, existing, contact)
		}
		if !contactDeleted(existing.Status) && !strings.EqualFold(strings.TrimSpace(existing.Status), "rejected") && !strings.EqualFold(strings.TrimSpace(existing.Status), "reject") {
			return nil
		}
	}
	if err := s.saveContact(ctx, contact); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:   "contact.requested",
		RoomID: contact.RoomID,
		Payload: map[string]any{
			"room_id":      contact.RoomID,
			"peer_mxid":    contact.PeerMXID,
			"display_name": contact.DisplayName,
			"avatar_url":   contact.AvatarURL,
			"domain":       contact.Domain,
			"status":       contact.Status,
			"remark":       contact.Remark,
		},
	})
}

func (s *Service) reinviteAcceptedContactToRetainedRoom(ctx context.Context, contact contactRecord) (bool, error) {
	if s.transport == nil || strings.TrimSpace(contact.RoomID) == "" || strings.TrimSpace(contact.PeerMXID) == "" {
		return true, nil
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	ownerDisplayName := s.profile.DisplayName
	ownerAvatarURL := s.profile.AvatarURL
	s.mu.Unlock()
	directName := fallbackString(ownerDisplayName, ownerMXID)
	if err := s.transport.InviteUser(ctx, InviteUserRequest{
		RoomID:      contact.RoomID,
		InviterMXID: ownerMXID,
		InviteeMXID: contact.PeerMXID,
		IsDirect:    true,
		InviteRoomState: []RoomStateEvent{
			roomProfileForDirect(directName, ownerMXID, contact.PeerMXID, ownerDisplayName, ownerAvatarURL, "", false),
		},
	}); err != nil {
		if isAlreadyJoinedRoomError(err) || isSenderNotJoinedDirextalkRoom(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Service) acceptReplacementDirectInvite(ctx context.Context, existing, invite contactRecord) error {
	roomID := strings.TrimSpace(invite.RoomID)
	if roomID == "" {
		return nil
	}
	if s.transport != nil {
		s.mu.Lock()
		ownerMXID := s.ownerMXID
		ownerDisplayName := s.profile.DisplayName
		ownerAvatarURL := s.profile.AvatarURL
		s.mu.Unlock()
		join, err := s.transport.JoinRoom(ctx, JoinRoomRequest{
			RoomIDOrAlias: roomID,
			UserMXID:      ownerMXID,
			DisplayName:   ownerDisplayName,
			AvatarURL:     ownerAvatarURL,
			ServerNames:   retainedRoomServerNames(nil, roomID),
		})
		if err != nil && !isAlreadyJoinedRoomError(err) {
			return err
		}
		if strings.TrimSpace(join.RoomID) != "" {
			roomID = join.RoomID
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
	return s.saveContact(ctx, replacement)
}

func (s *Service) channelIDForRoom(ctx context.Context, roomID string) string {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return ""
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, "", roomID)
	if err == nil && ok {
		return ch.ChannelID
	}
	return ""
}
