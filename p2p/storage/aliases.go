package storage

import (
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkplugin"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/domain"
)

type portalState = dirextalkdomain.PortalState
type readMarker = dirextalkdomain.ReadMarker
type channel = dirextalkdomain.Channel
type channelInviteGrant = dirextalkdomain.ChannelInviteGrant
type channelPostRecord = domain.ChannelPostRecord
type channelCommentRecord = domain.ChannelCommentRecord
type contactRecord = domain.ContactRecord
type groupRecord = dirextalkdomain.GroupRecord
type callRecord = dirextalkdomain.CallRecord
type favoriteRecord = dirextalkdomain.FavoriteRecord
type followRecord = dirextalkdomain.FollowRecord
type reactionRecord = dirextalkdomain.ReactionRecord
type memberRecord = dirextalkdomain.MemberRecord
type p2pEvent = dirextalkdomain.Event
type eventBounds = dirextalkdomain.EventBounds
type blockRecord = dirextalkdomain.BlockRecord
type reportRecord = dirextalkdomain.ReportRecord
type pluginCatalogEntry = dirextalkplugin.CatalogEntry
type pluginInstance = dirextalkplugin.Instance
type pluginJob = dirextalkplugin.Job
type pluginSecret = dirextalkplugin.Secret
type conversationKind = dirextalkdomain.ConversationKind
type conversationRecord = dirextalkdomain.ConversationRecord

const (
	conversationKindDirect       = dirextalkdomain.ConversationKindDirect
	conversationKindGroup        = dirextalkdomain.ConversationKindGroup
	conversationKindChannel      = dirextalkdomain.ConversationKindChannel
	conversationKindSystem       = dirextalkdomain.ConversationKindSystem
	conversationLifecycleDeleted = dirextalkdomain.ConversationLifecycleDeleted
)

func normalizeConversationRecord(record conversationRecord) conversationRecord {
	return dirextalkdomain.NormalizeConversationRecord(record)
}

func conversationFromContact(contact contactRecord) conversationRecord {
	return domain.ConversationFromContact(contact)
}

func conversationFromGroup(group groupRecord) conversationRecord {
	return dirextalkdomain.ConversationFromGroup(group)
}

func conversationFromChannel(ch channel) conversationRecord {
	return dirextalkdomain.ConversationFromChannel(ch)
}

func contactDeleted(status string) bool {
	return dirextalkdomain.ContactDeleted(status)
}

func fallbackString(value, fallback string) string {
	return dirextalkdomain.FallbackString(value, fallback)
}
