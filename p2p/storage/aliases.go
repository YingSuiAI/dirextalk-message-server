package storage

import "github.com/YingSuiAI/direxio-message-server/p2p/domain"

type portalState = domain.PortalState
type readMarker = domain.ReadMarker
type channel = domain.Channel
type channelInviteGrant = domain.ChannelInviteGrant
type channelPostRecord = domain.ChannelPostRecord
type channelCommentRecord = domain.ChannelCommentRecord
type contactRecord = domain.ContactRecord
type groupRecord = domain.GroupRecord
type callRecord = domain.CallRecord
type favoriteRecord = domain.FavoriteRecord
type reportRecord = domain.ReportRecord
type followRecord = domain.FollowRecord
type reactionRecord = domain.ReactionRecord
type memberRecord = domain.MemberRecord
type p2pEvent = domain.Event
type eventBounds = domain.EventBounds
type conversationKind = domain.ConversationKind
type conversationRecord = domain.ConversationRecord

const (
	conversationKindDirect       = domain.ConversationKindDirect
	conversationKindGroup        = domain.ConversationKindGroup
	conversationKindChannel      = domain.ConversationKindChannel
	conversationLifecycleDeleted = domain.ConversationLifecycleDeleted
)

func normalizeConversationRecord(record conversationRecord) conversationRecord {
	return domain.NormalizeConversationRecord(record)
}

func conversationFromContact(contact contactRecord) conversationRecord {
	return domain.ConversationFromContact(contact)
}

func conversationFromGroup(group groupRecord) conversationRecord {
	return domain.ConversationFromGroup(group)
}

func conversationFromChannel(ch channel) conversationRecord {
	return domain.ConversationFromChannel(ch)
}

func contactDeleted(status string) bool {
	return domain.ContactDeleted(status)
}

func fallbackString(value, fallback string) string {
	return domain.FallbackString(value, fallback)
}
