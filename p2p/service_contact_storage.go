package p2p

import "github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"

type contactStorageRecord = dirextalkdomain.ContactRecord

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
