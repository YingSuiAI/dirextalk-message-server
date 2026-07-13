package dirextalkstate

import (
	"encoding/json"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
)

type RoomKind string

const (
	RoomKindDirect  RoomKind = "direct"
	RoomKindGroup   RoomKind = "group"
	RoomKindChannel RoomKind = "channel"
)

type RoomProfileContent struct {
	Raw map[string]any

	Kind     RoomKind
	RoomType string

	RoomID       string
	ChannelID    string
	Name         string
	Topic        string
	Description  string
	AvatarURL    string
	Visibility   string
	JoinPolicy   string
	InvitePolicy string
	ChannelType  string
	DisplayName  string
	Domain       string
	Remark       string

	RequesterMXID string
	TargetMXID    string
	Dissolved     bool

	Muted              bool
	HasMuted           bool
	CommentsEnabled    bool
	HasCommentsEnabled bool

	AccountDeleted bool
	DeletedMXID    string
}

type MemberPolicyContent struct {
	Raw map[string]any

	UserID   string
	Role     string
	Muted    bool
	HasMuted bool
}

type JoinRequestContent struct {
	Raw map[string]any

	UserID    string
	ChannelID string
	Status    string
	Reason    string
	RequestID string
}

func ParseRoomProfileContent(data []byte) (RoomProfileContent, error) {
	content, err := unmarshalContent(data)
	if err != nil {
		return RoomProfileContent{}, err
	}
	return RoomProfileFromContent(content), nil
}

func RoomProfileFromContent(content map[string]any) RoomProfileContent {
	if content == nil {
		content = map[string]any{}
	}
	roomType := trimAny(content["room_type"])
	profile := RoomProfileContent{
		Raw:             content,
		Kind:            RoomKindFromRoomType(roomType),
		RoomType:        strings.TrimSpace(roomType),
		RoomID:          trimAny(content["room_id"]),
		ChannelID:       trimAny(content["channel_id"]),
		Name:            trimAny(content["name"]),
		Topic:           trimAny(content["topic"]),
		Description:     trimAny(content["description"]),
		AvatarURL:       trimAny(content["avatar_url"]),
		Visibility:      trimAny(content["visibility"]),
		JoinPolicy:      trimAny(content["join_policy"]),
		InvitePolicy:    trimAny(content["invite_policy"]),
		ChannelType:     trimAny(content["channel_type"]),
		DisplayName:     trimAny(content["display_name"]),
		Domain:          trimAny(content["domain"]),
		Remark:          trimAny(content["remark"]),
		RequesterMXID:   trimAny(content["requester_mxid"]),
		TargetMXID:      trimAny(content["target_mxid"]),
		Dissolved:       boolValue(content["dissolved"]),
		AccountDeleted:  boolValue(content["account_deleted"]),
		DeletedMXID:     trimAny(content["deleted_mxid"]),
		CommentsEnabled: boolValue(content["comments_enabled"]),
		Muted:           boolValue(content["muted"]),
	}
	_, profile.HasCommentsEnabled = content["comments_enabled"]
	_, profile.HasMuted = content["muted"]
	return profile
}

func RoomKindFromRoomType(roomType string) RoomKind {
	switch strings.ToLower(strings.TrimSpace(roomType)) {
	case RoomTypeDirect:
		return RoomKindDirect
	case RoomTypeGroup:
		return RoomKindGroup
	case RoomTypeChannel:
		return RoomKindChannel
	default:
		return ""
	}
}

func ParseMemberPolicyContent(data []byte, stateKey *string) (MemberPolicyContent, error) {
	content, err := unmarshalContent(data)
	if err != nil {
		return MemberPolicyContent{}, err
	}
	return MemberPolicyFromContent(content, stateKey), nil
}

func MemberPolicyFromContent(content map[string]any, stateKey *string) MemberPolicyContent {
	if content == nil {
		content = map[string]any{}
	}
	policy := MemberPolicyContent{
		Raw:    content,
		UserID: userIDFromStateKey(stateKey),
		Role:   trimAny(content["role"]),
		Muted:  boolValue(content["muted"]),
	}
	_, policy.HasMuted = content["muted"]
	return policy
}

func ParseJoinRequestContent(data []byte, stateKey *string) (JoinRequestContent, error) {
	content, err := unmarshalContent(data)
	if err != nil {
		return JoinRequestContent{}, err
	}
	return JoinRequestFromContent(content, stateKey), nil
}

func JoinRequestFromContent(content map[string]any, stateKey *string) JoinRequestContent {
	if content == nil {
		content = map[string]any{}
	}
	userID := userIDFromStateKey(stateKey)
	if override := trimAny(content["user_id"]); override != "" {
		userID = override
	}
	return JoinRequestContent{
		Raw:       content,
		UserID:    userID,
		ChannelID: trimAny(content["channel_id"]),
		Status:    strings.ToLower(strings.TrimSpace(trimAny(content["status"]))),
		Reason:    trimAny(content["reason"]),
		RequestID: trimAny(content["request_id"]),
	}
}

func unmarshalContent(data []byte) (map[string]any, error) {
	content := map[string]any{}
	if err := json.Unmarshal(data, &content); err != nil {
		return nil, err
	}
	return content, nil
}

func userIDFromStateKey(stateKey *string) string {
	if stateKey == nil {
		return ""
	}
	return strings.TrimSpace(productpolicy.UserIDFromStateKey(*stateKey))
}

func trimAny(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	default:
		return ""
	}
}

func boolValue(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		trimmed := strings.TrimSpace(v)
		return strings.EqualFold(trimmed, "true") || trimmed == "1"
	case float64:
		return v != 0
	case int:
		return v != 0
	case int64:
		return v != 0
	default:
		return false
	}
}
