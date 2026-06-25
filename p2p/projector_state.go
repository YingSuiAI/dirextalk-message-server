package p2p

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/YingSuiAI/direxio-message-server/internal/productpolicy"
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
		Type:    "profile.changed",
		RoomID:  roomID,
		Payload: map[string]any{"room_type": DirexioRoomTypeDirect, "dissolved": boolParam(content["dissolved"])},
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
	channelType := fallbackString(trimString(content["channel_type"]), fallbackString(existing.ChannelType, "chat"))
	commentsEnabled := existing.CommentsEnabled
	if _, ok := content["comments_enabled"]; ok {
		commentsEnabled = boolParam(content["comments_enabled"])
	}
	memberCount := existing.MemberCount
	if memberCount == 0 {
		memberCount = 1
	}
	description := existing.Description
	if _, ok := content["description"]; ok {
		description = trimString(content["description"])
	}
	avatarURL := existing.AvatarURL
	if _, ok := content["avatar_url"]; ok {
		avatarURL = trimString(content["avatar_url"])
	}
	muted := existing.Muted
	if _, ok := content["muted"]; ok {
		muted = boolParam(content["muted"])
	}
	ch := channel{
		ChannelID:        channelID,
		RoomID:           event.RoomID().String(),
		Name:             fallbackString(trimString(content["name"]), fallbackString(existing.Name, channelID)),
		Description:      description,
		AvatarURL:        avatarURL,
		Visibility:       fallbackString(trimString(content["visibility"]), fallbackString(existing.Visibility, "private")),
		JoinPolicy:       fallbackString(trimString(content["join_policy"]), fallbackString(existing.JoinPolicy, "invite")),
		ChannelType:      channelType,
		CommentsEnabled:  commentsEnabled,
		Muted:            muted,
		MemberCount:      memberCount,
		PendingJoinCount: existing.PendingJoinCount,
	}
	s.mu.Lock()
	s.channels[ch.ChannelID] = ch
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.UpsertChannel(ctx, ch); err != nil {
			return err
		}
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:    "profile.changed",
		RoomID:  ch.RoomID,
		Payload: map[string]any{"room_type": DirexioRoomTypeChannel, "channel_id": ch.ChannelID, "dissolved": false},
	})
}

func (s *Service) projectGroupProfileContent(ctx context.Context, event *types.HeaderedEvent, content map[string]any) error {
	roomID := event.RoomID().String()
	if boolParam(content["dissolved"]) {
		return s.deleteGroup(ctx, roomID)
	}
	existing, _, _ := s.groupByRoom(ctx, roomID)
	memberCount := existing.MemberCount
	if memberCount == 0 {
		memberCount = 1
	}
	topic := existing.Topic
	if _, ok := content["topic"]; ok {
		topic = trimString(content["topic"])
	}
	avatarURL := existing.AvatarURL
	if _, ok := content["avatar_url"]; ok {
		avatarURL = trimString(content["avatar_url"])
	}
	muted := existing.Muted
	if _, ok := content["muted"]; ok {
		muted = boolParam(content["muted"])
	}
	group := groupRecord{
		RoomID:       roomID,
		Name:         fallbackString(trimString(content["name"]), fallbackString(existing.Name, roomID)),
		Topic:        topic,
		AvatarURL:    avatarURL,
		MemberCount:  memberCount,
		InvitePolicy: fallbackString(trimString(content["invite_policy"]), fallbackString(existing.InvitePolicy, "member")),
		Muted:        muted,
	}
	if err := s.saveGroup(ctx, group); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:    "profile.changed",
		RoomID:  group.RoomID,
		Payload: map[string]any{"room_type": DirexioRoomTypeGroup, "dissolved": false},
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
	if !ok {
		member = memberRecord{
			RoomID:     event.RoomID().String(),
			UserID:     userID,
			Domain:     domainFromMXID(userID),
			Membership: "join",
			Role:       "member",
			JoinedAt:   eventTime(event).UnixMilli(),
		}
	}
	if role := trimString(content["role"]); role != "" {
		member.Role = role
	}
	if _, ok := content["muted"]; ok {
		member.Muted = boolParam(content["muted"])
	}
	if err := s.saveMember(ctx, member); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:    "room.member_policy.projected",
		RoomID:  member.RoomID,
		Payload: map[string]any{"user_id": member.UserID, "role": member.Role, "muted": member.Muted},
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
	membership := ""
	switch strings.ToLower(strings.TrimSpace(trimString(content["status"]))) {
	case "pending":
		membership = "pending"
	case "approved":
		membership = "invite"
	case "rejected":
		membership = "reject"
	default:
		return nil
	}
	roomID := event.RoomID().String()
	member, ok, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return err
	}
	if !ok {
		member = s.memberRecordFor(roomID, trimString(content["channel_id"]), userID)
	}
	member.RoomID = roomID
	member.UserID = userID
	member.Membership = membership
	member.Role = fallbackString(member.Role, "member")
	member.Domain = fallbackString(member.Domain, domainFromMXID(userID))
	if member.JoinedAt == 0 {
		member.JoinedAt = eventTime(event).UnixMilli()
	}
	if err := s.saveMember(ctx, member); err != nil {
		return err
	}
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:    "channel.join_request.changed",
		RoomID:  roomID,
		EventID: event.EventID(),
		Payload: map[string]any{"user_id": userID, "status": trimString(content["status"])},
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
