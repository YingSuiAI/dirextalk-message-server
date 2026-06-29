package domain

import "encoding/json"

type PortalState struct {
	Initialized    bool
	Password       string
	AccessToken    string
	MatrixDeviceID string
	AgentToken     string
	OwnerMXID      string
	AgentRoomID    string
	Profile        OwnerProfile
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
	DisplayName   string `json:"display_name"`
	ContextWindow int64  `json:"context_window"`
	Enabled       bool   `json:"enabled"`
	Model         string `json:"model"`
	SystemPrompt  string `json:"system_prompt"`
}

type ReadMarker struct {
	RoomID         string `json:"room_id"`
	EventID        string `json:"event_id"`
	OriginServerTS int64  `json:"origin_server_ts"`
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
	PeerMXID     string            `json:"peer_mxid"`
	DisplayName  string            `json:"display_name"`
	AvatarURL    string            `json:"avatar_url"`
	Domain       string            `json:"domain"`
	RoomID       string            `json:"room_id"`
	Status       string            `json:"status"`
	Remark       string            `json:"remark,omitempty"`
	Operation    map[string]any    `json:"operation,omitempty"`
	Conversation *ConversationView `json:"conversation,omitempty"`
}

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

type ChannelReactionHistory struct {
	Reaction ReactionRecord        `json:"reaction"`
	Channel  *Channel              `json:"channel,omitempty"`
	Post     *ChannelPostRecord    `json:"post,omitempty"`
	Comment  *ChannelCommentRecord `json:"comment,omitempty"`
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
