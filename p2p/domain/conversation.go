package domain

import "github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"

type ConversationKind = dirextalkdomain.ConversationKind

const (
	ConversationKindDirect  = dirextalkdomain.ConversationKindDirect
	ConversationKindGroup   = dirextalkdomain.ConversationKindGroup
	ConversationKindChannel = dirextalkdomain.ConversationKindChannel
	ConversationKindAgent   = dirextalkdomain.ConversationKindAgent
	ConversationKindSystem  = dirextalkdomain.ConversationKindSystem
)

type ConversationLifecycle = dirextalkdomain.ConversationLifecycle

const (
	ConversationLifecycleActive    = dirextalkdomain.ConversationLifecycleActive
	ConversationLifecyclePending   = dirextalkdomain.ConversationLifecyclePending
	ConversationLifecycleLeft      = dirextalkdomain.ConversationLifecycleLeft
	ConversationLifecycleDissolved = dirextalkdomain.ConversationLifecycleDissolved
	ConversationLifecycleDeleted   = dirextalkdomain.ConversationLifecycleDeleted
	ConversationLifecycleBlocked   = dirextalkdomain.ConversationLifecycleBlocked
)

type ConversationProjectionState = dirextalkdomain.ConversationProjectionState

const (
	ConversationProjectionReady    = dirextalkdomain.ConversationProjectionReady
	ConversationProjectionPending  = dirextalkdomain.ConversationProjectionPending
	ConversationProjectionConflict = dirextalkdomain.ConversationProjectionConflict
	ConversationProjectionFailed   = dirextalkdomain.ConversationProjectionFailed
)

type ConversationRecord = dirextalkdomain.ConversationRecord
type ConversationView = dirextalkdomain.ConversationView
type ConversationCapabilities = dirextalkdomain.ConversationCapabilities

func NormalizeConversationRecord(record ConversationRecord) ConversationRecord {
	return dirextalkdomain.NormalizeConversationRecord(record)
}

func ConversationIDForRoomID(roomID string) string {
	return dirextalkdomain.ConversationIDForRoomID(roomID)
}

func ConversationFromContact(contact ContactRecord) ConversationRecord {
	return dirextalkdomain.ConversationFromContact(dirextalkdomain.ContactRecord{
		PeerMXID:            contact.PeerMXID,
		DisplayName:         contact.DisplayName,
		DisplayNameOverride: contact.DisplayNameOverride,
		AvatarURL:           contact.AvatarURL,
		Domain:              contact.Domain,
		RoomID:              contact.RoomID,
		Status:              contact.Status,
		Remark:              contact.Remark,
	})
}

func ConversationFromGroup(group GroupRecord) ConversationRecord {
	return dirextalkdomain.ConversationFromGroup(dirextalkdomain.GroupRecord{
		RoomID:       group.RoomID,
		Name:         group.Name,
		Topic:        group.Topic,
		AvatarURL:    group.AvatarURL,
		MemberCount:  group.MemberCount,
		InvitePolicy: group.InvitePolicy,
		Muted:        group.Muted,
	})
}

func ConversationFromChannel(ch Channel) ConversationRecord {
	return dirextalkdomain.ConversationFromChannel(ch)
}

func ContactDeleted(status string) bool {
	return dirextalkdomain.ContactDeleted(status)
}

func FallbackString(value, fallback string) string {
	return dirextalkdomain.FallbackString(value, fallback)
}
