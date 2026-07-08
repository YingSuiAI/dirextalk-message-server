package domain

import (
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

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
	lifecycle := ConversationLifecycleActive
	if ContactDeleted(contact.Status) {
		lifecycle = ConversationLifecycleDeleted
	} else if !strings.EqualFold(contact.Status, "accepted") {
		lifecycle = ConversationLifecyclePending
	}
	return ConversationRecord{
		MatrixRoomID:    contact.RoomID,
		Kind:            ConversationKindDirect,
		Lifecycle:       lifecycle,
		PeerMXID:        contact.PeerMXID,
		Title:           FallbackString(contact.DisplayName, contact.PeerMXID),
		AvatarURL:       contact.AvatarURL,
		ProjectionState: ConversationProjectionReady,
	}
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
