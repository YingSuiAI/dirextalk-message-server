package projector

import (
	"context"
	"errors"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkprojection"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkstate"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
)

func (m *Module) projectRoomProfileState(ctx context.Context, event *types.HeaderedEvent) error {
	profile, err := dirextalkstate.ParseRoomProfileContent(event.Content())
	if err != nil {
		return err
	}
	kind := conversationKindFromStateKind(profile.Kind)
	if kind == "" {
		return nil
	}
	if kind == dirextalkdomain.ConversationKindDirect {
		if m.dependencies.Groups == nil {
			return errors.New("group projection port is not configured")
		}
		if err := m.dependencies.Groups.Delete(ctx, event.RoomID().String()); err != nil {
			return err
		}
	}
	if err := m.projectConversationProfile(ctx, event, kind, profile.Raw); err != nil {
		return err
	}
	switch kind {
	case dirextalkdomain.ConversationKindChannel:
		return m.projectChannelProfileContent(ctx, event, profile)
	case dirextalkdomain.ConversationKindGroup:
		return m.projectGroupProfileContent(ctx, event, profile)
	case dirextalkdomain.ConversationKindDirect:
		return m.projectDirectProfileContent(ctx, event, profile)
	default:
		return nil
	}
}

func (m *Module) projectConversationProfile(ctx context.Context, event *types.HeaderedEvent, kind dirextalkdomain.ConversationKind, content map[string]any) error {
	if m.dependencies.Conversations == nil {
		return errors.New("conversation projection port is not configured")
	}
	now := m.eventTime(event).UnixMilli()
	title := fallbackText(textValue(content["name"]), textValue(content["display_name"]))
	lifecycle := dirextalkdomain.ConversationLifecycleActive
	if boolValue(content["dissolved"]) {
		lifecycle = dirextalkdomain.ConversationLifecycleDissolved
	}
	return m.dependencies.Conversations.Save(ctx, dirextalkdomain.ConversationRecord{
		MatrixRoomID:    event.RoomID().String(),
		Kind:            kind,
		Lifecycle:       lifecycle,
		CreatedByMXID:   string(event.SenderID()),
		Title:           title,
		AvatarURL:       textValue(content["avatar_url"]),
		ProjectionState: dirextalkdomain.ConversationProjectionReady,
		CreatedAt:       now,
		UpdatedAt:       now,
	})
}

func conversationKindFromStateKind(kind dirextalkstate.RoomKind) dirextalkdomain.ConversationKind {
	switch kind {
	case dirextalkstate.RoomKindDirect:
		return dirextalkdomain.ConversationKindDirect
	case dirextalkstate.RoomKindGroup:
		return dirextalkdomain.ConversationKindGroup
	case dirextalkstate.RoomKindChannel:
		return dirextalkdomain.ConversationKindChannel
	default:
		return ""
	}
}

func (m *Module) projectDirectProfileContent(ctx context.Context, event *types.HeaderedEvent, profile dirextalkstate.RoomProfileContent) error {
	if m.dependencies.Groups == nil {
		return errors.New("group projection port is not configured")
	}
	roomID := event.RoomID().String()
	// Preserve the historic second direct-room group delete after the common
	// profile path. Both durable stores implement this operation idempotently.
	if err := m.dependencies.Groups.Delete(ctx, roomID); err != nil {
		return err
	}
	if profile.Dissolved && profile.AccountDeleted {
		deletedMXID := profile.DeletedMXID
		if deletedMXID == "" {
			ownerMXID := m.identity().OwnerMXID
			switch {
			case profile.RequesterMXID != "" && profile.RequesterMXID != ownerMXID:
				deletedMXID = profile.RequesterMXID
			case profile.TargetMXID != "" && profile.TargetMXID != ownerMXID:
				deletedMXID = profile.TargetMXID
			}
		}
		if err := m.markDirectContactDeleted(ctx, roomID, deletedMXID); err != nil {
			return err
		}
	}
	return m.appendEvent(ctx, dirextalkdomain.Event{
		Type:      "profile.changed",
		RoomID:    roomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("profile.changed", event.EventID(), roomID),
		Payload:   map[string]any{"room_type": dirextalkstate.RoomTypeDirect, "dissolved": profile.Dissolved},
	})
}

func (m *Module) markDirectContactDeleted(ctx context.Context, roomID, peerMXID string) error {
	if m.dependencies.Contacts == nil {
		return errors.New("contact projection port is not configured")
	}
	mutationKey := strings.TrimSpace(peerMXID)
	if mutationKey == "" {
		contact, ok, err := m.dependencies.Contacts.LookupByRoom(ctx, roomID)
		if err != nil || !ok {
			return err
		}
		mutationKey = contact.PeerMXID
	}
	return m.dependencies.Contacts.WithPeer(mutationKey, func() error {
		contact, ok, err := m.dependencies.Contacts.LookupByRoom(ctx, roomID)
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
		return m.dependencies.Contacts.Save(ctx, contact)
	})
}

func (m *Module) projectChannelProfileContent(ctx context.Context, event *types.HeaderedEvent, profile dirextalkstate.RoomProfileContent) error {
	if m.dependencies.Channels == nil {
		return errors.New("channel projection port is not configured")
	}
	channelID := profile.ChannelID
	if channelID == "" {
		if existing, ok, _ := m.dependencies.Channels.ByIDOrRoom(ctx, "", event.RoomID().String()); ok {
			channelID = existing.ChannelID
		}
	}
	if channelID == "" {
		channelID = event.RoomID().String()
	}
	existing, _, _ := m.dependencies.Channels.ByIDOrRoom(ctx, channelID, event.RoomID().String())
	if profile.Dissolved {
		return m.dependencies.Channels.Delete(ctx, channelID)
	}
	channel := dirextalkprojection.ChannelProfile(event.RoomID().String(), channelID, existing, profile.Raw)
	if err := m.dependencies.Channels.UpsertProjection(ctx, channel); err != nil {
		return err
	}
	return m.appendEvent(ctx, dirextalkdomain.Event{
		Type:      "profile.changed",
		RoomID:    channel.RoomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("profile.changed", event.EventID(), channel.ChannelID),
		Payload: map[string]any{
			"room_type": dirextalkstate.RoomTypeChannel, "channel_id": channel.ChannelID, "dissolved": false,
		},
	})
}

func (m *Module) projectGroupProfileContent(ctx context.Context, event *types.HeaderedEvent, profile dirextalkstate.RoomProfileContent) error {
	if m.dependencies.Groups == nil {
		return errors.New("group projection port is not configured")
	}
	roomID := event.RoomID().String()
	if profile.Dissolved {
		return m.dependencies.Groups.Delete(ctx, roomID)
	}
	existing, _, _ := m.dependencies.Groups.ByRoom(ctx, roomID)
	group := dirextalkprojection.GroupProfile(roomID, existing, profile.Raw)
	if err := m.dependencies.Groups.Save(ctx, group); err != nil {
		return err
	}
	return m.appendEvent(ctx, dirextalkdomain.Event{
		Type:      "profile.changed",
		RoomID:    group.RoomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("profile.changed", event.EventID(), group.RoomID),
		Payload:   map[string]any{"room_type": dirextalkstate.RoomTypeGroup, "dissolved": false},
	})
}

func (m *Module) projectMemberPolicyState(ctx context.Context, event *types.HeaderedEvent) error {
	policy, err := dirextalkstate.ParseMemberPolicyContent(event.Content(), event.StateKey())
	if err != nil {
		return err
	}
	if policy.UserID == "" {
		return nil
	}
	if m.dependencies.Members == nil {
		return errors.New("member projection port is not configured")
	}
	member, ok, err := m.dependencies.Members.Lookup(ctx, event.RoomID().String(), policy.UserID)
	if err != nil {
		return err
	}
	member = dirextalkprojection.MemberPolicy(event.RoomID().String(), policy.UserID, member, ok, policy.Raw, m.eventTime(event))
	if err := m.dependencies.Members.Save(ctx, member); err != nil {
		return err
	}
	return m.appendEvent(ctx, dirextalkdomain.Event{
		Type:      "room.member_policy.projected",
		RoomID:    member.RoomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("room.member_policy.projected", event.EventID(), member.UserID),
		Payload:   map[string]any{"user_id": member.UserID, "role": member.Role, "muted": member.Muted},
	})
}

func (m *Module) projectJoinRequestState(ctx context.Context, event *types.HeaderedEvent) error {
	joinRequest, err := dirextalkstate.ParseJoinRequestContent(event.Content(), event.StateKey())
	if err != nil {
		return err
	}
	if joinRequest.UserID == "" {
		return nil
	}
	if m.dependencies.Members == nil {
		return errors.New("member projection port is not configured")
	}
	roomID := event.RoomID().String()
	member, ok, err := m.dependencies.Members.Lookup(ctx, roomID, joinRequest.UserID)
	if err != nil {
		return err
	}
	member, valid := dirextalkprojection.JoinRequestMember(roomID, joinRequest.ChannelID, joinRequest.UserID, member, ok, joinRequest.Raw, m.eventTime(event))
	if !valid {
		return nil
	}
	if err := m.dependencies.Members.Save(ctx, member); err != nil {
		return err
	}
	return m.appendEvent(ctx, dirextalkdomain.Event{
		Type:      "channel.join_request.changed",
		RoomID:    roomID,
		EventID:   event.EventID(),
		DedupeKey: projectedEventDedupeKey("channel.join_request.changed", event.EventID(), joinRequest.UserID),
		Payload:   map[string]any{"user_id": joinRequest.UserID, "status": joinRequest.Status},
	})
}
