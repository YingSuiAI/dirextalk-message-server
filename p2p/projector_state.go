package p2p

import (
	"context"
	"encoding/json"

	"github.com/YingSuiAI/direxio-message-server/internal/productpolicy"
	"github.com/YingSuiAI/direxio-message-server/p2p/projection"
	"github.com/YingSuiAI/direxio-message-server/roomserver/types"
)

func (s *Service) projectRoomProfileState(ctx context.Context, event *types.HeaderedEvent) error {
	content := map[string]any{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		return err
	}
	kind, _ := conversationKindFromContent(content)
	if kind == "" {
		return nil
	}
	if kind == conversationKindDirect {
		if err := s.deleteGroup(ctx, event.RoomID().String()); err != nil {
			return err
		}
	}
	if err := s.projectConversationProfile(ctx, event, kind, content); err != nil {
		return err
	}
	switch kind {
	case conversationKindChannel:
		return s.projectChannelProfileContent(ctx, event, content)
	case conversationKindGroup:
		return s.projectGroupProfileContent(ctx, event, content)
	case conversationKindDirect:
		return s.projectDirectProfileContent(ctx, event, content)
	default:
		return nil
	}
}

func (s *Service) projectDirectProfileContent(ctx context.Context, event *types.HeaderedEvent, content map[string]any) error {
	roomID := event.RoomID().String()
	if err := s.deleteGroup(ctx, roomID); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:      "profile.changed",
		RoomID:    roomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("profile.changed", event.EventID(), roomID),
		Payload:   map[string]any{"room_type": DirexioRoomTypeDirect, "dissolved": boolParam(content["dissolved"])},
	})
}

func (s *Service) projectChannelProfileContent(ctx context.Context, event *types.HeaderedEvent, content map[string]any) error {
	channelID := trimString(content["channel_id"])
	if channelID == "" {
		if existing, ok, _ := s.channelByIDOrRoom(ctx, "", event.RoomID().String()); ok {
			channelID = existing.ChannelID
		}
	}
	if channelID == "" {
		channelID = event.RoomID().String()
	}
	existing, _, _ := s.channelByIDOrRoom(ctx, channelID, event.RoomID().String())
	if boolParam(content["dissolved"]) {
		return s.deleteChannel(ctx, channelID)
	}
	ch := projection.ChannelProfile(event.RoomID().String(), channelID, existing, content)
	s.mu.Lock()
	s.channels[ch.ChannelID] = ch
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.UpsertChannel(ctx, ch); err != nil {
			return err
		}
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:      "profile.changed",
		RoomID:    ch.RoomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("profile.changed", event.EventID(), ch.ChannelID),
		Payload:   map[string]any{"room_type": DirexioRoomTypeChannel, "channel_id": ch.ChannelID, "dissolved": false},
	})
}

func (s *Service) projectGroupProfileContent(ctx context.Context, event *types.HeaderedEvent, content map[string]any) error {
	roomID := event.RoomID().String()
	if boolParam(content["dissolved"]) {
		return s.deleteGroup(ctx, roomID)
	}
	existing, _, _ := s.groupByRoom(ctx, roomID)
	group := projection.GroupProfile(roomID, existing, content)
	if err := s.saveGroup(ctx, group); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:      "profile.changed",
		RoomID:    group.RoomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("profile.changed", event.EventID(), group.RoomID),
		Payload:   map[string]any{"room_type": DirexioRoomTypeGroup, "dissolved": false},
	})
}

func (s *Service) projectMemberPolicyState(ctx context.Context, event *types.HeaderedEvent) error {
	content := map[string]any{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		return err
	}
	userID := ""
	if event.StateKey() != nil {
		userID = productpolicy.UserIDFromStateKey(*event.StateKey())
	}
	if userID == "" {
		return nil
	}
	member, ok, err := s.lookupMember(ctx, event.RoomID().String(), userID)
	if err != nil {
		return err
	}
	member = projection.MemberPolicy(event.RoomID().String(), userID, member, ok, content, eventTime(event))
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
	content := map[string]any{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		return err
	}
	userID := ""
	if event.StateKey() != nil {
		userID = *event.StateKey()
	}
	if override := trimString(content["user_id"]); override != "" {
		userID = override
	}
	if userID == "" {
		return nil
	}
	roomID := event.RoomID().String()
	member, ok, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return err
	}
	member, valid := projection.JoinRequestMember(roomID, trimString(content["channel_id"]), userID, member, ok, content, eventTime(event))
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
		DedupeKey: projectedEventDedupeKey("channel.join_request.changed", event.EventID(), userID),
		Payload:   map[string]any{"user_id": userID, "status": trimString(content["status"])},
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
