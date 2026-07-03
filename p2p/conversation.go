package p2p

import "github.com/YingSuiAI/dirextalk-message-server/p2p/domain"

type conversationKind = domain.ConversationKind

const (
	conversationKindDirect  = domain.ConversationKindDirect
	conversationKindGroup   = domain.ConversationKindGroup
	conversationKindChannel = domain.ConversationKindChannel
	conversationKindAgent   = domain.ConversationKindAgent
)

type conversationLifecycle = domain.ConversationLifecycle

const (
	conversationLifecycleActive    = domain.ConversationLifecycleActive
	conversationLifecyclePending   = domain.ConversationLifecyclePending
	conversationLifecycleLeft      = domain.ConversationLifecycleLeft
	conversationLifecycleDissolved = domain.ConversationLifecycleDissolved
	conversationLifecycleDeleted   = domain.ConversationLifecycleDeleted
	conversationLifecycleBlocked   = domain.ConversationLifecycleBlocked
)

type conversationProjectionState = domain.ConversationProjectionState

const (
	conversationProjectionReady    = domain.ConversationProjectionReady
	conversationProjectionPending  = domain.ConversationProjectionPending
	conversationProjectionConflict = domain.ConversationProjectionConflict
	conversationProjectionFailed   = domain.ConversationProjectionFailed
)

type conversationRecord = domain.ConversationRecord
type conversationView = domain.ConversationView
type conversationCapabilities = domain.ConversationCapabilities

func normalizeConversationRecord(record conversationRecord) conversationRecord {
	return domain.NormalizeConversationRecord(record)
}

func conversationIDForRoomID(roomID string) string {
	return domain.ConversationIDForRoomID(roomID)
}
