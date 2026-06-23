package p2p

import "strings"

func conversationKindFromContent(content map[string]any) (conversationKind, string) {
	if kind := conversationKindFromRoomType(trimString(content["room_type"])); kind != "" {
		return kind, ""
	}
	return "", "missing explicit room_type"
}

func conversationKindFromRoomType(roomType string) conversationKind {
	switch strings.ToLower(strings.TrimSpace(roomType)) {
	case DirexioRoomTypeDirect:
		return conversationKindDirect
	case DirexioRoomTypeGroup:
		return conversationKindGroup
	case DirexioRoomTypeChannel:
		return conversationKindChannel
	default:
		return ""
	}
}
