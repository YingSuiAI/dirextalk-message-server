package p2p

import (
	"context"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

type contactStorageRecord = dirextalkdomain.ContactRecord

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
	return contactStorageRecord{
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

func contactRecordFromStorage(contact contactStorageRecord) contactRecord {
	return contactRecord{
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

func contactRecordsFromStorage(contacts []contactStorageRecord) []contactRecord {
	if len(contacts) == 0 {
		return []contactRecord{}
	}
	result := make([]contactRecord, 0, len(contacts))
	for _, contact := range contacts {
		result = append(result, contactRecordFromStorage(contact))
	}
	return result
}
