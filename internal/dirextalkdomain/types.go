package dirextalkdomain

import "encoding/json"

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

type Channel struct {
	ChannelID        string `json:"channel_id"`
	RoomID           string `json:"room_id"`
	Name             string `json:"name"`
	Description      string `json:"description"`
	AvatarURL        string `json:"avatar_url"`
	Visibility       string `json:"visibility"`
	JoinPolicy       string `json:"join_policy"`
	ChannelType      string `json:"channel_type"`
	CommentsEnabled  bool   `json:"comments_enabled"`
	Muted            bool   `json:"muted"`
	MemberCount      int64  `json:"member_count"`
	PendingJoinCount int64  `json:"pending_join_count"`
	IsOwned          bool   `json:"is_owned,omitempty"`
	Role             string `json:"role,omitempty"`
	MemberStatus     string `json:"member_status,omitempty"`
}

type GroupRecord struct {
	RoomID       string `json:"room_id"`
	Name         string `json:"name"`
	Topic        string `json:"topic"`
	AvatarURL    string `json:"avatar_url"`
	MemberCount  int64  `json:"member_count"`
	InvitePolicy string `json:"invite_policy"`
	Muted        bool   `json:"muted"`
}

type MemberRecord struct {
	RoomID               string `json:"room_id"`
	ChannelID            string `json:"channel_id"`
	UserID               string `json:"user_id"`
	DisplayName          string `json:"display_name"`
	AvatarURL            string `json:"avatar_url"`
	Domain               string `json:"domain"`
	Membership           string `json:"membership"`
	Role                 string `json:"role"`
	Muted                bool   `json:"muted"`
	JoinedAt             int64  `json:"joined_at"`
	RequesterNodeBaseURL string `json:"-"`
}

type ReadMarker struct {
	RoomID         string `json:"room_id"`
	EventID        string `json:"event_id"`
	OriginServerTS int64  `json:"origin_server_ts"`
}

type ChannelInviteGrant struct {
	GrantID     string `json:"grant_id"`
	ChannelID   string `json:"channel_id"`
	RoomID      string `json:"room_id"`
	ShareRoomID string `json:"share_room_id"`
	CreatedBy   string `json:"created_by"`
	CreatedAt   int64  `json:"created_at"`
}

type BlockRecord struct {
	TargetType  string `json:"target_type"`
	TargetID    string `json:"target_id"`
	RoomID      string `json:"room_id"`
	ChannelID   string `json:"channel_id,omitempty"`
	PeerMXID    string `json:"peer_mxid"`
	DisplayName string `json:"display_name"`
	AvatarURL   string `json:"avatar_url"`
	CreatedAt   int64  `json:"created_at"`
}

type CallRecord struct {
	CallID        string `json:"call_id"`
	RoomID        string `json:"room_id"`
	RoomType      string `json:"room_type"`
	MediaType     string `json:"media_type"`
	CreatedByMXID string `json:"created_by_mxid"`
	State         string `json:"state"`
	CreatedAt     string `json:"created_at"`
	AnsweredAt    string `json:"answered_at,omitempty"`
	EndedAt       string `json:"ended_at,omitempty"`
	EndedByMXID   string `json:"ended_by_mxid,omitempty"`
	EndReason     string `json:"end_reason,omitempty"`
	DurationMS    int64  `json:"duration_ms,omitempty"`
}

type FavoriteRecord struct {
	ID             int64  `json:"id"`
	EventID        string `json:"event_id"`
	RoomID         string `json:"room_id"`
	SenderID       string `json:"sender_id"`
	SenderName     string `json:"sender_name"`
	Content        string `json:"content"`
	MessageType    string `json:"message_type"`
	OriginServerTS int64  `json:"origin_server_ts"`
	CreatedAt      string `json:"created_at"`
}

type FollowRecord struct {
	Domain    string `json:"domain"`
	CreatedAt string `json:"created_at"`
}

type ReactionRecord struct {
	TargetType string `json:"target_type"`
	TargetID   string `json:"target_id"`
	ChannelID  string `json:"channel_id"`
	PostID     string `json:"post_id"`
	CommentID  string `json:"comment_id"`
	Reaction   string `json:"reaction"`
	UserID     string `json:"user_id"`
	Active     bool   `json:"active"`
	CreatedAt  string `json:"created_at"`
}

type ReportRecord struct {
	ReportID            string   `json:"report_id"`
	TargetType          string   `json:"target_type"`
	TargetRoomID        string   `json:"target_room_id"`
	TargetChannelID     string   `json:"target_channel_id,omitempty"`
	TargetName          string   `json:"target_name"`
	ReporterMXID        string   `json:"reporter_mxid"`
	ReporterDisplayName string   `json:"reporter_display_name"`
	Reason              string   `json:"reason"`
	Body                string   `json:"body"`
	ImageURLs           []string `json:"image_urls"`
	SystemRoomID        string   `json:"system_room_id"`
	EventID             string   `json:"event_id"`
	OriginServerTS      int64    `json:"origin_server_ts"`
	CreatedAt           string   `json:"created_at"`
}

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

func (m MemberRecord) MarshalJSON() ([]byte, error) {
	type memberAlias MemberRecord
	return json.Marshal(struct {
		memberAlias
		UserMXID string `json:"user_mxid"`
		Status   string `json:"status"`
	}{
		memberAlias: memberAlias(m),
		UserMXID:    m.UserID,
		Status:      m.Membership,
	})
}
