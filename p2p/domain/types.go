package domain

import (
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkplugin"
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

type ChannelPostRecord struct {
	PostID         string            `json:"post_id"`
	ChannelID      string            `json:"channel_id"`
	RoomID         string            `json:"room_id"`
	EventID        string            `json:"event_id"`
	AuthorMXID     string            `json:"author_mxid"`
	AuthorName     string            `json:"author_name"`
	Body           string            `json:"body"`
	MessageType    string            `json:"message_type"`
	MediaJSON      string            `json:"media_json"`
	OriginServerTS int64             `json:"origin_server_ts"`
	CommentCount   int64             `json:"comment_count"`
	ReactionCount  int64             `json:"reaction_count"`
	ReactedByMe    bool              `json:"reacted_by_me"`
	Operation      map[string]any    `json:"operation,omitempty"`
	Conversation   *ConversationView `json:"conversation,omitempty"`
}

type ChannelCommentRecord struct {
	CommentID         string            `json:"comment_id"`
	PostID            string            `json:"post_id"`
	ChannelID         string            `json:"channel_id"`
	EventID           string            `json:"event_id"`
	AuthorMXID        string            `json:"author_mxid"`
	AuthorName        string            `json:"author_name"`
	Body              string            `json:"body"`
	MessageType       string            `json:"message_type"`
	MediaJSON         string            `json:"media_json"`
	ReplyToCommentID  string            `json:"reply_to_comment_id"`
	ReplyToAuthorMXID string            `json:"reply_to_author_mxid"`
	MentionsJSON      string            `json:"mentions_json"`
	OriginServerTS    int64             `json:"origin_server_ts"`
	ReactionCount     int64             `json:"reaction_count"`
	ReactedByMe       bool              `json:"reacted_by_me"`
	Operation         map[string]any    `json:"operation,omitempty"`
	Conversation      *ConversationView `json:"conversation,omitempty"`
}

type ContactRecord = contactsmodule.View

type BlockRecord = dirextalkdomain.BlockRecord

type GroupRecord = groupsmodule.View

type CallRecord = dirextalkdomain.CallRecord
type FavoriteRecord = dirextalkdomain.FavoriteRecord
type FollowRecord = dirextalkdomain.FollowRecord
type ReactionRecord = dirextalkdomain.ReactionRecord

type ChannelReactionHistory struct {
	Reaction ReactionRecord        `json:"reaction"`
	Channel  *Channel              `json:"channel,omitempty"`
	Post     *ChannelPostRecord    `json:"post,omitempty"`
	Comment  *ChannelCommentRecord `json:"comment,omitempty"`
}

type ReportRecord = dirextalkdomain.ReportRecord

type MemberRecord = dirextalkdomain.MemberRecord

type Event = dirextalkdomain.Event
type EventBounds = dirextalkdomain.EventBounds
