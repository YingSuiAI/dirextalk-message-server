package p2p

import (
	"context"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkprojection"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkstate"
	groupsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/groups"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
)

func (s *Service) projectRoomProfileState(ctx context.Context, event *types.HeaderedEvent) error {
	profile, err := dirextalkstate.ParseRoomProfileContent(event.Content())
	if err != nil {
		return err
	}
	kind := conversationKindFromStateKind(profile.Kind)
	if kind == "" {
		return nil
	}
	if kind == conversationKindDirect {
		if err := s.deleteGroup(ctx, event.RoomID().String()); err != nil {
			return err
		}
	}
	if err := s.projectConversationProfile(ctx, event, kind, profile.Raw); err != nil {
		return err
	}
	switch kind {
	case conversationKindChannel:
		return s.projectChannelProfileContent(ctx, event, profile)
	case conversationKindGroup:
		return s.projectGroupProfileContent(ctx, event, profile)
	case conversationKindDirect:
		return s.projectDirectProfileContent(ctx, event, profile)
	default:
		return nil
	}
}

func (s *Service) projectConversationProfile(ctx context.Context, event *types.HeaderedEvent, kind conversationKind, content map[string]any) error {
	now := eventTime(event).UnixMilli()
	title := fallbackString(trimString(content["name"]), trimString(content["display_name"]))
	lifecycle := conversationLifecycleActive
	if boolParam(content["dissolved"]) {
		lifecycle = conversationLifecycleDissolved
	}
	return s.saveConversation(ctx, conversationRecord{
		MatrixRoomID:    event.RoomID().String(),
		Kind:            kind,
		Lifecycle:       lifecycle,
		CreatedByMXID:   string(event.SenderID()),
		Title:           title,
		AvatarURL:       trimString(content["avatar_url"]),
		ProjectionState: conversationProjectionReady,
		CreatedAt:       now,
		UpdatedAt:       now,
	})
}

func conversationKindFromStateKind(kind dirextalkstate.RoomKind) conversationKind {
	switch kind {
	case dirextalkstate.RoomKindDirect:
		return conversationKindDirect
	case dirextalkstate.RoomKindGroup:
		return conversationKindGroup
	case dirextalkstate.RoomKindChannel:
		return conversationKindChannel
	default:
		return ""
	}
}

func (s *Service) projectDirectProfileContent(ctx context.Context, event *types.HeaderedEvent, profile dirextalkstate.RoomProfileContent) error {
	roomID := event.RoomID().String()
	if err := s.deleteGroup(ctx, roomID); err != nil {
		return err
	}
	if profile.Dissolved && profile.AccountDeleted {
		deletedMXID := profile.DeletedMXID
		if deletedMXID == "" {
			s.mu.Lock()
			ownerMXID := s.ownerMXID
			s.mu.Unlock()
			switch {
			case profile.RequesterMXID != "" && profile.RequesterMXID != ownerMXID:
				deletedMXID = profile.RequesterMXID
			case profile.TargetMXID != "" && profile.TargetMXID != ownerMXID:
				deletedMXID = profile.TargetMXID
			}
		}
		if err := s.markDirectContactDeleted(ctx, roomID, deletedMXID); err != nil {
			return err
		}
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:      "profile.changed",
		RoomID:    roomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("profile.changed", event.EventID(), roomID),
		Payload:   map[string]any{"room_type": DirextalkRoomTypeDirect, "dissolved": profile.Dissolved},
	})
}

func (s *Service) markDirectContactDeleted(ctx context.Context, roomID, peerMXID string) error {
	mutationKey := strings.TrimSpace(peerMXID)
	if mutationKey == "" {
		contact, ok, err := s.lookupContactByRoom(ctx, roomID)
		if err != nil || !ok {
			return err
		}
		mutationKey = contact.PeerMXID
	}
	var mutationErr error
	s.contactsModule.SerializePeer(mutationKey, func() {
		mutationErr = s.markDirectContactDeletedForPeer(ctx, roomID, peerMXID)
	})
	return mutationErr
}

func (s *Service) markDirectContactDeletedForPeer(ctx context.Context, roomID, peerMXID string) error {
	contact, ok, err := s.lookupContactByRoom(ctx, roomID)
	if err != nil || !ok {
		return err
	}
	if peerMXID != "" && contact.PeerMXID != "" && contact.PeerMXID != peerMXID {
		return nil
	}
	if peerMXID != "" {
		contact.PeerMXID = peerMXID
	}
	contact.Status = "deleted"
	return s.saveContact(ctx, contact)
}

func (s *Service) projectChannelProfileContent(ctx context.Context, event *types.HeaderedEvent, profile dirextalkstate.RoomProfileContent) error {
	channelID := profile.ChannelID
	if channelID == "" {
		if existing, ok, _ := s.channelByIDOrRoom(ctx, "", event.RoomID().String()); ok {
			channelID = existing.ChannelID
		}
	}
	if channelID == "" {
		channelID = event.RoomID().String()
	}
	existing, _, _ := s.channelByIDOrRoom(ctx, channelID, event.RoomID().String())
	if profile.Dissolved {
		return s.deleteChannel(ctx, channelID)
	}
	ch := dirextalkprojection.ChannelProfile(event.RoomID().String(), channelID, existing, profile.Raw)
	if err := s.channelStore().UpsertChannel(ctx, ch); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:      "profile.changed",
		RoomID:    ch.RoomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("profile.changed", event.EventID(), ch.ChannelID),
		Payload:   map[string]any{"room_type": DirextalkRoomTypeChannel, "channel_id": ch.ChannelID, "dissolved": false},
	})
}

func (s *Service) projectGroupProfileContent(ctx context.Context, event *types.HeaderedEvent, profile dirextalkstate.RoomProfileContent) error {
	roomID := event.RoomID().String()
	if profile.Dissolved {
		return s.deleteGroup(ctx, roomID)
	}
	existing, _, _ := s.groupByRoom(ctx, roomID)
	group := groupsmodule.ViewFromRecord(dirextalkprojection.GroupProfile(roomID, groupsmodule.RecordFromView(existing), profile.Raw))
	if err := s.saveGroup(ctx, group); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:      "profile.changed",
		RoomID:    group.RoomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("profile.changed", event.EventID(), group.RoomID),
		Payload:   map[string]any{"room_type": DirextalkRoomTypeGroup, "dissolved": false},
	})
}

func (s *Service) projectMemberPolicyState(ctx context.Context, event *types.HeaderedEvent) error {
	policy, err := dirextalkstate.ParseMemberPolicyContent(event.Content(), event.StateKey())
	if err != nil {
		return err
	}
	if policy.UserID == "" {
		return nil
	}
	member, ok, err := s.lookupMember(ctx, event.RoomID().String(), policy.UserID)
	if err != nil {
		return err
	}
	member = dirextalkprojection.MemberPolicy(event.RoomID().String(), policy.UserID, member, ok, policy.Raw, eventTime(event))
	if err := s.saveMember(ctx, member); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:      "room.member_policy.projected",
		RoomID:    member.RoomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("room.member_policy.projected", event.EventID(), member.UserID),
		Payload:   map[string]any{"user_id": member.UserID, "role": member.Role, "muted": member.Muted},
	})
}

func (s *Service) projectJoinRequestState(ctx context.Context, event *types.HeaderedEvent) error {
	joinRequest, err := dirextalkstate.ParseJoinRequestContent(event.Content(), event.StateKey())
	if err != nil {
		return err
	}
	if joinRequest.UserID == "" {
		return nil
	}
	roomID := event.RoomID().String()
	member, ok, err := s.lookupMember(ctx, roomID, joinRequest.UserID)
	if err != nil {
		return err
	}
	member, valid := dirextalkprojection.JoinRequestMember(roomID, joinRequest.ChannelID, joinRequest.UserID, member, ok, joinRequest.Raw, eventTime(event))
	if !valid {
		return nil
	}
	if err := s.saveMember(ctx, member); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:      "channel.join_request.changed",
		RoomID:    roomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("channel.join_request.changed", event.EventID(), joinRequest.UserID),
		Payload:   map[string]any{"user_id": joinRequest.UserID, "status": joinRequest.Status},
	})
}

func (s *Service) shouldProjectRoomMessage(ctx context.Context, roomID string, content map[string]any) bool {
	if trimString(content["p2p_kind"]) != "" || trimString(content["channel_id"]) != "" {
		return true
	}
	if s.channelIDForRoom(ctx, roomID) != "" {
		return true
	}
	if _, ok, err := s.groupByRoom(ctx, roomID); err == nil && ok {
		return true
	}
	if _, ok, err := s.lookupContactByRoom(ctx, roomID); err == nil && ok {
		return true
	}
	return false
}
