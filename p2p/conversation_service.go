package p2p

import (
	"context"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	conversationmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/conversation"
)

type conversationKind = dirextalkdomain.ConversationKind

const (
	conversationKindDirect  = dirextalkdomain.ConversationKindDirect
	conversationKindGroup   = dirextalkdomain.ConversationKindGroup
	conversationKindChannel = dirextalkdomain.ConversationKindChannel
	conversationKindAgent   = dirextalkdomain.ConversationKindAgent
	conversationKindSystem  = dirextalkdomain.ConversationKindSystem
)

const (
	conversationLifecycleActive    = dirextalkdomain.ConversationLifecycleActive
	conversationLifecyclePending   = dirextalkdomain.ConversationLifecyclePending
	conversationLifecycleLeft      = dirextalkdomain.ConversationLifecycleLeft
	conversationLifecycleDissolved = dirextalkdomain.ConversationLifecycleDissolved
	conversationLifecycleDeleted   = dirextalkdomain.ConversationLifecycleDeleted
	conversationLifecycleBlocked   = dirextalkdomain.ConversationLifecycleBlocked
)

const (
	conversationProjectionReady    = dirextalkdomain.ConversationProjectionReady
	conversationProjectionPending  = dirextalkdomain.ConversationProjectionPending
	conversationProjectionConflict = dirextalkdomain.ConversationProjectionConflict
	conversationProjectionFailed   = dirextalkdomain.ConversationProjectionFailed
)

type conversationRecord = dirextalkdomain.ConversationRecord
type conversationView = dirextalkdomain.ConversationView

type conversationStore = conversationmodule.Store

type serviceConversationHydrator struct {
	service *Service
}

func (h serviceConversationHydrator) ContactByRoom(ctx context.Context, roomID string) (dirextalkdomain.ContactRecord, bool, error) {
	if h.service == nil {
		return dirextalkdomain.ContactRecord{}, false, nil
	}
	contact, ok, err := h.service.lookupContactByRoom(ctx, roomID)
	if err != nil || !ok {
		return dirextalkdomain.ContactRecord{}, ok, err
	}
	return durableContactRecord(contact), true, nil
}

func durableContactRecord(contact contactRecord) dirextalkdomain.ContactRecord {
	return dirextalkdomain.ContactRecord{
		PeerMXID:            contact.PeerMXID,
		DisplayName:         contact.DisplayName,
		DisplayNameOverride: contact.DisplayNameOverride,
		AvatarURL:           contact.AvatarURL,
		Domain:              contact.Domain,
		RoomID:              contact.RoomID,
		Status:              contact.Status,
		Remark:              contact.Remark,
	}
}

func (h serviceConversationHydrator) GroupByRoom(ctx context.Context, roomID string) (dirextalkdomain.GroupRecord, bool, error) {
	if h.service == nil {
		return dirextalkdomain.GroupRecord{}, false, nil
	}
	group, ok, err := h.service.groupByRoom(ctx, roomID)
	if err != nil || !ok {
		return dirextalkdomain.GroupRecord{}, ok, err
	}
	return durableGroupRecord(group), true, nil
}

func durableGroupRecord(group groupRecord) dirextalkdomain.GroupRecord {
	return dirextalkdomain.GroupRecord{
		RoomID:       group.RoomID,
		Name:         group.Name,
		Topic:        group.Topic,
		AvatarURL:    group.AvatarURL,
		MemberCount:  group.MemberCount,
		InvitePolicy: group.InvitePolicy,
		Muted:        group.Muted,
	}
}

func (h serviceConversationHydrator) ChannelByRoom(ctx context.Context, roomID string) (dirextalkdomain.Channel, bool, error) {
	if h.service == nil {
		return dirextalkdomain.Channel{}, false, nil
	}
	return h.service.channelByIDOrRoom(ctx, "", roomID)
}

func (h serviceConversationHydrator) CountJoinedMembers(ctx context.Context, roomID, channelID string) (int64, error) {
	if h.service == nil || h.service.memberStore() == nil {
		return 0, nil
	}
	return h.service.memberStore().CountJoinedMembers(ctx, roomID, channelID)
}

func (h serviceConversationHydrator) Member(ctx context.Context, roomID, userID string) (dirextalkdomain.MemberRecord, bool, error) {
	if h.service == nil {
		return dirextalkdomain.MemberRecord{}, false, nil
	}
	return h.service.lookupMember(ctx, roomID, userID)
}

func (h serviceConversationHydrator) OwnerMXID() string {
	if h.service == nil {
		return ""
	}
	h.service.mu.Lock()
	ownerMXID := h.service.ownerMXID
	h.service.mu.Unlock()
	return ownerMXID
}

func (s *Service) saveConversation(ctx context.Context, record conversationRecord) error {
	return s.conversationModule.Save(ctx, record)
}

func (s *Service) deleteStoredConversationKind(ctx context.Context, roomID string, kind conversationKind) error {
	return s.conversationModule.DeleteKindByRoom(ctx, roomID, kind)
}

func (s *Service) listConversations(ctx context.Context) ([]conversationRecord, error) {
	return s.conversationModule.ListRecords(ctx)
}

func (s *Service) getConversation(ctx context.Context, conversationID, roomID string) (conversationRecord, bool, error) {
	return s.conversationModule.GetRecord(ctx, conversationID, roomID)
}

func (s *Service) conversationView(ctx context.Context, record conversationRecord) (conversationView, error) {
	return s.conversationModule.View(ctx, record)
}

func (s *Service) conversationOperation(ctx context.Context, action, status, roomID string) (map[string]any, *conversationView, error) {
	return s.conversationModule.Operation(ctx, action, status, roomID)
}

func (s *Service) attachConversationOperation(ctx context.Context, result map[string]any, action, status, roomID string) error {
	return s.conversationModule.AttachOperation(ctx, result, action, status, roomID)
}

func (s *Service) attachContactConversationOperation(ctx context.Context, contact *contactRecord, action, status string) error {
	if contact == nil {
		return nil
	}
	result := map[string]any{}
	if err := s.attachConversationOperation(ctx, result, action, status, contact.RoomID); err != nil {
		return err
	}
	if operation, ok := result["operation"].(map[string]any); ok {
		contact.Operation = operation
	}
	if conversation, ok := result["conversation"].(conversationView); ok {
		contact.Conversation = &conversation
	}
	return nil
}

func conversationFromContact(contact contactRecord) conversationRecord {
	return dirextalkdomain.ConversationFromContact(durableContactRecord(contact))
}

func conversationFromGroup(group groupRecord) conversationRecord {
	return dirextalkdomain.ConversationFromGroup(durableGroupRecord(group))
}

func conversationFromChannel(ch channel) conversationRecord {
	return dirextalkdomain.ConversationFromChannel(ch)
}
