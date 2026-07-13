package p2p

import (
	"context"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	contactsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/contacts"
	conversationmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/conversation"
	groupsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/groups"
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

type conversationRecord = dirextalkdomain.ConversationRecord
type conversationView = dirextalkdomain.ConversationView

type conversationStore = conversationmodule.Store
type contactStorageRecord = dirextalkdomain.ContactRecord

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
	return contactStorageRecordFromContact(contact), true, nil
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
	return groupsmodule.RecordFromView(group)
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

func (s *Service) listConversations(ctx context.Context) ([]conversationRecord, error) {
	return s.conversationModule.ListRecords(ctx)
}

func (s *Service) getConversation(ctx context.Context, conversationID, roomID string) (conversationRecord, bool, error) {
	return s.conversationModule.GetRecord(ctx, conversationID, roomID)
}

func (s *Service) conversationView(ctx context.Context, record conversationRecord) (conversationView, error) {
	return s.conversationModule.View(ctx, record)
}

func (s *Service) listGroups(ctx context.Context) ([]groupRecord, error) {
	return s.groupsModule.List(ctx)
}

func (s *Service) listChannels(ctx context.Context) ([]channel, error) {
	return s.channelsModule.List(ctx)
}

func (s *Service) listContacts(ctx context.Context) ([]contactRecord, error) {
	contacts, err := s.contactsModule.ListVisible(ctx)
	if err != nil {
		return nil, err
	}
	return contactRecordsFromStorage(contacts), nil
}

func (s *Service) rawContacts(ctx context.Context) ([]contactRecord, error) {
	contacts, err := s.contactsModule.ListRaw(ctx)
	if err != nil {
		return nil, err
	}
	return contactRecordsFromStorage(contacts), nil
}

func (s *Service) lookupContactByRoom(ctx context.Context, roomID string) (contactRecord, bool, error) {
	contact, ok, err := s.contactsModule.LookupByRoom(ctx, roomID)
	return contactRecordFromStorage(contact), ok, err
}

func (s *Service) lookupContactByPeer(ctx context.Context, peerMXID string) (contactRecord, bool, error) {
	contact, ok, err := s.contactsModule.LookupByPeer(ctx, peerMXID)
	return contactRecordFromStorage(contact), ok, err
}

func contactStorageRecordFromContact(contact contactRecord) contactStorageRecord {
	return contactsmodule.RecordFromView(contact)
}

func contactRecordFromStorage(contact contactStorageRecord) contactRecord {
	return contactsmodule.ViewFromRecord(contact)
}

func contactRecordsFromStorage(contacts []contactStorageRecord) []contactRecord {
	return contactsmodule.ViewsFromRecords(contacts)
}

func (s *Service) saveContact(ctx context.Context, contact contactRecord) error {
	return s.contactsModule.Save(ctx, contactStorageRecordFromContact(contact))
}
