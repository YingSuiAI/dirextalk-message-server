package p2p

import (
	"context"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	contactsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/contacts"
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
	return contactsmodule.RecordFromView(contact)
}

func contactRecordFromStorage(contact contactStorageRecord) contactRecord {
	return contactsmodule.ViewFromRecord(contact)
}

func contactRecordsFromStorage(contacts []contactStorageRecord) []contactRecord {
	return contactsmodule.ViewsFromRecords(contacts)
}
