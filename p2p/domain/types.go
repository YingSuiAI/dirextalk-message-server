package domain

import "github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"

type PortalState = dirextalkdomain.PortalState
type OwnerProfile = dirextalkdomain.OwnerProfile
type AgentConfig = dirextalkdomain.AgentConfig

type PluginCatalogEntry struct {
	ID             string         `json:"id"`
	Name           string         `json:"name"`
	Version        string         `json:"version"`
	Description    string         `json:"description"`
	Image          string         `json:"image"`
	Digest         string         `json:"digest"`
	MinBaseVersion string         `json:"min_base_version"`
	Permissions    []string       `json:"permissions"`
	Events         []string       `json:"events"`
	Actions        []string       `json:"actions"`
	ConfigSchema   map[string]any `json:"config_schema,omitempty"`
}

type PluginInstance struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Version   string         `json:"version"`
	Image     string         `json:"image"`
	Digest    string         `json:"digest"`
	Status    string         `json:"status"`
	Enabled   bool           `json:"enabled"`
	Config    map[string]any `json:"config,omitempty"`
	LastJobID string         `json:"last_job_id,omitempty"`
	CreatedAt int64          `json:"created_at"`
	UpdatedAt int64          `json:"updated_at"`
}

type PluginJob struct {
	JobID     string `json:"job_id"`
	PluginID  string `json:"plugin_id"`
	Action    string `json:"action"`
	Status    string `json:"status"`
	Message   string `json:"message,omitempty"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type PluginSecret struct {
	PluginID  string `json:"plugin_id"`
	Name      string `json:"name"`
	Value     string `json:"-"`
	UpdatedAt int64  `json:"updated_at"`
}

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

type ContactRecord struct {
	PeerMXID            string            `json:"peer_mxid"`
	DisplayName         string            `json:"display_name"`
	DisplayNameOverride bool              `json:"display_name_override,omitempty"`
	AvatarURL           string            `json:"avatar_url"`
	Domain              string            `json:"domain"`
	RoomID              string            `json:"room_id"`
	Status              string            `json:"status"`
	Remark              string            `json:"remark,omitempty"`
	Operation           map[string]any    `json:"operation,omitempty"`
	Conversation        *ConversationView `json:"conversation,omitempty"`
}

type BlockRecord = dirextalkdomain.BlockRecord

type GroupRecord struct {
	RoomID       string            `json:"room_id"`
	Name         string            `json:"name"`
	Topic        string            `json:"topic"`
	AvatarURL    string            `json:"avatar_url"`
	MemberCount  int64             `json:"member_count"`
	InvitePolicy string            `json:"invite_policy"`
	Muted        bool              `json:"muted"`
	Operation    map[string]any    `json:"operation,omitempty"`
	Conversation *ConversationView `json:"conversation,omitempty"`
}

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
