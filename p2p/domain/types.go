package domain

import (
	"encoding/json"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

type PortalState struct {
	Initialized    bool
	Password       string
	AccessToken    string
	MatrixDeviceID string
	AgentToken     string
	OwnerMXID      string
	AgentRoomID    string
	SystemRoomID   string
	Profile        OwnerProfile
	AgentConfig    AgentConfig
}

type OwnerProfile struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name"`
	Domain      string `json:"domain"`
	AvatarURL   string `json:"avatar_url"`
	Gender      string `json:"gender"`
	Birthday    string `json:"birthday"`
	Phone       string `json:"phone"`
	Email       string `json:"email"`
}

type AgentConfig struct {
	DisplayName       string         `json:"display_name"`
	AvatarURL         string         `json:"avatar_url"`
	ContextWindow     int64          `json:"context_window"`
	Enabled           bool           `json:"enabled"`
	Model             string         `json:"model"`
	SystemPrompt      string         `json:"system_prompt"`
	MCPBlockedRoomIDs []string       `json:"mcp_blocked_room_ids"`
	Native            map[string]any `json:"-"`
}

func (c *AgentConfig) UnmarshalJSON(data []byte) error {
	type agentConfigJSON struct {
		DisplayName       string   `json:"display_name"`
		AvatarURL         string   `json:"avatar_url"`
		ContextWindow     int64    `json:"context_window"`
		Enabled           bool     `json:"enabled"`
		Model             string   `json:"model"`
		SystemPrompt      string   `json:"system_prompt"`
		MCPBlockedRoomIDs []string `json:"mcp_blocked_room_ids"`
	}
	var known agentConfigJSON
	if err := json.Unmarshal(data, &known); err != nil {
		return err
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for _, key := range agentConfigKnownJSONKeys() {
		delete(raw, key)
	}
	c.DisplayName = known.DisplayName
	c.AvatarURL = known.AvatarURL
	c.ContextWindow = known.ContextWindow
	c.Enabled = known.Enabled
	c.Model = known.Model
	c.SystemPrompt = known.SystemPrompt
	c.MCPBlockedRoomIDs = known.MCPBlockedRoomIDs
	if len(raw) > 0 {
		c.Native = raw
	} else {
		c.Native = nil
	}
	return nil
}

func (c AgentConfig) MarshalJSON() ([]byte, error) {
	out := make(map[string]any, len(c.Native)+7)
	for key, value := range c.Native {
		if !agentConfigKnownJSONKey(key) {
			out[key] = value
		}
	}
	out["display_name"] = c.DisplayName
	out["avatar_url"] = c.AvatarURL
	out["context_window"] = c.ContextWindow
	out["enabled"] = c.Enabled
	out["model"] = c.Model
	out["system_prompt"] = c.SystemPrompt
	out["mcp_blocked_room_ids"] = c.MCPBlockedRoomIDs
	return json.Marshal(out)
}

func agentConfigKnownJSONKeys() []string {
	return []string{
		"display_name",
		"avatar_url",
		"context_window",
		"enabled",
		"model",
		"system_prompt",
		"mcp_blocked_room_ids",
	}
}

func agentConfigKnownJSONKey(key string) bool {
	for _, known := range agentConfigKnownJSONKeys() {
		if key == known {
			return true
		}
	}
	return false
}

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

type ChannelInviteGrant struct {
	GrantID     string `json:"grant_id"`
	ChannelID   string `json:"channel_id"`
	RoomID      string `json:"room_id"`
	ShareRoomID string `json:"share_room_id"`
	CreatedBy   string `json:"created_by"`
	CreatedAt   int64  `json:"created_at"`
}

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

type Event struct {
	Seq       int64          `json:"seq"`
	Type      string         `json:"type"`
	RoomID    string         `json:"room_id,omitempty"`
	EventID   string         `json:"event_id,omitempty"`
	DedupeKey string         `json:"-"`
	Payload   map[string]any `json:"payload,omitempty"`
	CreatedAt string         `json:"created_at"`
}

type EventBounds struct {
	MinSeq int64 `json:"min_seq"`
	MaxSeq int64 `json:"max_seq"`
	Count  int64 `json:"count"`
}
