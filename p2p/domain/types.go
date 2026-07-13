package domain

import (
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkplugin"
	channelsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/channels"
	contactsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/contacts"
	groupsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/groups"
)

type PortalState = dirextalkdomain.PortalState
type OwnerProfile = dirextalkdomain.OwnerProfile
type AgentConfig = dirextalkdomain.AgentConfig

type PluginCatalogEntry = dirextalkplugin.CatalogEntry
type PluginInstance = dirextalkplugin.Instance
type PluginJob = dirextalkplugin.Job
type PluginSecret = dirextalkplugin.Secret

type ReadMarker = dirextalkdomain.ReadMarker

type Channel = dirextalkdomain.Channel

type ChannelInviteGrant = dirextalkdomain.ChannelInviteGrant

type ChannelPostRecord = channelsmodule.Post
type ChannelCommentRecord = channelsmodule.Comment

type ContactRecord = contactsmodule.View

type BlockRecord = dirextalkdomain.BlockRecord

type GroupRecord = groupsmodule.View

type CallRecord = dirextalkdomain.CallRecord
type FavoriteRecord = dirextalkdomain.FavoriteRecord
type FollowRecord = dirextalkdomain.FollowRecord
type ReactionRecord = dirextalkdomain.ReactionRecord

type ChannelReactionHistory = channelsmodule.ReactionHistory

type ReportRecord = dirextalkdomain.ReportRecord

type MemberRecord = dirextalkdomain.MemberRecord

type Event = dirextalkdomain.Event
type EventBounds = dirextalkdomain.EventBounds
