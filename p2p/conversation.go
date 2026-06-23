package p2p

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

type conversationKind string

const (
	conversationKindDirect  conversationKind = "direct"
	conversationKindGroup   conversationKind = "group"
	conversationKindChannel conversationKind = "channel"
	conversationKindAgent   conversationKind = "agent"
)

type conversationLifecycle string

const (
	conversationLifecycleActive    conversationLifecycle = "active"
	conversationLifecyclePending   conversationLifecycle = "pending"
	conversationLifecycleLeft      conversationLifecycle = "left"
	conversationLifecycleDissolved conversationLifecycle = "dissolved"
	conversationLifecycleDeleted   conversationLifecycle = "deleted"
	conversationLifecycleBlocked   conversationLifecycle = "blocked"
)

type conversationProjectionState string

const (
	conversationProjectionReady    conversationProjectionState = "ready"
	conversationProjectionPending  conversationProjectionState = "pending"
	conversationProjectionConflict conversationProjectionState = "conflict"
	conversationProjectionFailed   conversationProjectionState = "failed"
)

type conversationRecord struct {
	ConversationID   string                      `json:"conversation_id"`
	MatrixRoomID     string                      `json:"matrix_room_id"`
	Kind             conversationKind            `json:"kind"`
	Lifecycle        conversationLifecycle       `json:"lifecycle"`
	CreatedByMXID    string                      `json:"created_by_mxid"`
	PeerMXID         string                      `json:"peer_mxid"`
	Title            string                      `json:"title"`
	AvatarURL        string                      `json:"avatar_url"`
	LastEventID      string                      `json:"last_event_id"`
	LastMessage      string                      `json:"last_message"`
	LastActivityAt   int64                       `json:"last_activity_at"`
	ProjectionState  conversationProjectionState `json:"projection_state"`
	ProjectionReason string                      `json:"projection_reason"`
	CreatedAt        int64                       `json:"created_at"`
	UpdatedAt        int64                       `json:"updated_at"`
}

type conversationView struct {
	ConversationID     string                      `json:"conversation_id"`
	MatrixRoomID       string                      `json:"matrix_room_id"`
	Kind               conversationKind            `json:"kind"`
	Lifecycle          conversationLifecycle       `json:"lifecycle"`
	PeerMXID           string                      `json:"peer_mxid,omitempty"`
	MemberCount        int64                       `json:"member_count,omitempty"`
	Membership         string                      `json:"membership,omitempty"`
	RelationshipStatus string                      `json:"relationship_status,omitempty"`
	Role               string                      `json:"role,omitempty"`
	HydrationState     string                      `json:"hydration_state"`
	HydrationReason    string                      `json:"hydration_reason,omitempty"`
	Capabilities       conversationCapabilities    `json:"capabilities"`
	Title              string                      `json:"title"`
	AvatarURL          string                      `json:"avatar_url"`
	LastEventID        string                      `json:"last_event_id,omitempty"`
	LastMessage        string                      `json:"last_message,omitempty"`
	LastActivityAt     int64                       `json:"last_activity_at,omitempty"`
	ProjectionState    conversationProjectionState `json:"projection_state"`
	ProjectionReason   string                      `json:"projection_reason,omitempty"`
	channelType        string
	commentsEnabled    bool
}

type conversationCapabilities struct {
	Open            bool `json:"open"`
	Send            bool `json:"send"`
	SendMedia       bool `json:"send_media"`
	Call            bool `json:"call"`
	Invite          bool `json:"invite"`
	ManageMembers   bool `json:"manage_members"`
	Rename          bool `json:"rename"`
	RemoveMembers   bool `json:"remove_members"`
	Leave           bool `json:"leave"`
	Delete          bool `json:"delete"`
	PostCreate      bool `json:"post_create"`
	CommentCreate   bool `json:"comment_create"`
	ReactionToggle  bool `json:"reaction_toggle"`
	PostRecall      bool `json:"post_recall"`
	CommentRecall   bool `json:"comment_recall"`
	CommentsEnabled bool `json:"comments_enabled"`
}

func normalizeConversationRecord(record conversationRecord) conversationRecord {
	record.ConversationID = strings.TrimSpace(record.ConversationID)
	record.MatrixRoomID = strings.TrimSpace(record.MatrixRoomID)
	record.CreatedByMXID = strings.TrimSpace(record.CreatedByMXID)
	record.PeerMXID = strings.TrimSpace(record.PeerMXID)
	record.Title = strings.TrimSpace(record.Title)
	record.AvatarURL = strings.TrimSpace(record.AvatarURL)
	record.LastEventID = strings.TrimSpace(record.LastEventID)
	record.LastMessage = strings.TrimSpace(record.LastMessage)
	record.ProjectionReason = strings.TrimSpace(record.ProjectionReason)
	if record.ConversationID == "" && record.MatrixRoomID != "" {
		record.ConversationID = conversationIDForRoomID(record.MatrixRoomID)
	}
	if record.Lifecycle == "" {
		record.Lifecycle = conversationLifecycleActive
	}
	if record.ProjectionState == "" {
		record.ProjectionState = conversationProjectionReady
	}
	now := time.Now().UTC().UnixMilli()
	if record.CreatedAt <= 0 {
		record.CreatedAt = now
	}
	if record.UpdatedAt <= 0 {
		record.UpdatedAt = record.CreatedAt
	}
	return record
}

func conversationIDForRoomID(roomID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(roomID)))
	return "conv_" + hex.EncodeToString(sum[:8])
}
