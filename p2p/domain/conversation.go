package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

type ConversationKind string

const (
	ConversationKindDirect  ConversationKind = "direct"
	ConversationKindGroup   ConversationKind = "group"
	ConversationKindChannel ConversationKind = "channel"
	ConversationKindAgent   ConversationKind = "agent"
)

type ConversationLifecycle string

const (
	ConversationLifecycleActive    ConversationLifecycle = "active"
	ConversationLifecyclePending   ConversationLifecycle = "pending"
	ConversationLifecycleLeft      ConversationLifecycle = "left"
	ConversationLifecycleDissolved ConversationLifecycle = "dissolved"
	ConversationLifecycleDeleted   ConversationLifecycle = "deleted"
	ConversationLifecycleBlocked   ConversationLifecycle = "blocked"
)

type ConversationProjectionState string

const (
	ConversationProjectionReady    ConversationProjectionState = "ready"
	ConversationProjectionPending  ConversationProjectionState = "pending"
	ConversationProjectionConflict ConversationProjectionState = "conflict"
	ConversationProjectionFailed   ConversationProjectionState = "failed"
)

type ConversationRecord struct {
	ConversationID   string                      `json:"conversation_id"`
	MatrixRoomID     string                      `json:"matrix_room_id"`
	Kind             ConversationKind            `json:"kind"`
	Lifecycle        ConversationLifecycle       `json:"lifecycle"`
	CreatedByMXID    string                      `json:"created_by_mxid"`
	PeerMXID         string                      `json:"peer_mxid"`
	Title            string                      `json:"title"`
	AvatarURL        string                      `json:"avatar_url"`
	LastEventID      string                      `json:"last_event_id"`
	LastMessage      string                      `json:"last_message"`
	LastActivityAt   int64                       `json:"last_activity_at"`
	ProjectionState  ConversationProjectionState `json:"projection_state"`
	ProjectionReason string                      `json:"projection_reason"`
	CreatedAt        int64                       `json:"created_at"`
	UpdatedAt        int64                       `json:"updated_at"`
}

type ConversationView struct {
	ConversationID     string                      `json:"conversation_id"`
	MatrixRoomID       string                      `json:"matrix_room_id"`
	Kind               ConversationKind            `json:"kind"`
	Lifecycle          ConversationLifecycle       `json:"lifecycle"`
	PeerMXID           string                      `json:"peer_mxid,omitempty"`
	MemberCount        int64                       `json:"member_count,omitempty"`
	Membership         string                      `json:"membership,omitempty"`
	RelationshipStatus string                      `json:"relationship_status,omitempty"`
	Role               string                      `json:"role,omitempty"`
	HydrationState     string                      `json:"hydration_state"`
	HydrationReason    string                      `json:"hydration_reason,omitempty"`
	Capabilities       ConversationCapabilities    `json:"capabilities"`
	Title              string                      `json:"title"`
	AvatarURL          string                      `json:"avatar_url"`
	LastEventID        string                      `json:"last_event_id,omitempty"`
	LastMessage        string                      `json:"last_message,omitempty"`
	LastActivityAt     int64                       `json:"last_activity_at,omitempty"`
	ProjectionState    ConversationProjectionState `json:"projection_state"`
	ProjectionReason   string                      `json:"projection_reason,omitempty"`
	ChannelType        string                      `json:"-"`
	CommentsEnabled    bool                        `json:"-"`
}

type ConversationCapabilities struct {
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

func NormalizeConversationRecord(record ConversationRecord) ConversationRecord {
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
		record.ConversationID = ConversationIDForRoomID(record.MatrixRoomID)
	}
	if record.Lifecycle == "" {
		record.Lifecycle = ConversationLifecycleActive
	}
	if record.ProjectionState == "" {
		record.ProjectionState = ConversationProjectionReady
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

func ConversationIDForRoomID(roomID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(roomID)))
	return "conv_" + hex.EncodeToString(sum[:12])
}

func ConversationFromContact(contact ContactRecord) ConversationRecord {
	lifecycle := ConversationLifecycleActive
	if ContactDeleted(contact.Status) {
		lifecycle = ConversationLifecycleDeleted
	} else if !strings.EqualFold(contact.Status, "accepted") {
		lifecycle = ConversationLifecyclePending
	}
	return ConversationRecord{
		MatrixRoomID:    contact.RoomID,
		Kind:            ConversationKindDirect,
		Lifecycle:       lifecycle,
		PeerMXID:        contact.PeerMXID,
		Title:           FallbackString(contact.DisplayName, contact.PeerMXID),
		AvatarURL:       contact.AvatarURL,
		ProjectionState: ConversationProjectionReady,
	}
}

func ConversationFromGroup(group GroupRecord) ConversationRecord {
	return ConversationRecord{
		MatrixRoomID:    group.RoomID,
		Kind:            ConversationKindGroup,
		Lifecycle:       ConversationLifecycleActive,
		Title:           group.Name,
		AvatarURL:       group.AvatarURL,
		ProjectionState: ConversationProjectionReady,
	}
}

func ConversationFromChannel(ch Channel) ConversationRecord {
	return ConversationRecord{
		MatrixRoomID:    ch.RoomID,
		Kind:            ConversationKindChannel,
		Lifecycle:       ConversationLifecycleActive,
		Title:           ch.Name,
		AvatarURL:       ch.AvatarURL,
		ProjectionState: ConversationProjectionReady,
	}
}

func ContactDeleted(status string) bool {
	return strings.EqualFold(strings.TrimSpace(status), "deleted")
}

func FallbackString(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}
