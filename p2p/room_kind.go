package p2p

import "github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkprojection"

func conversationKindFromContent(content map[string]any) (conversationKind, string) {
	kind := conversationKindFromStateKind(dirextalkprojection.ConversationKindFromRoomType(trimString(content["room_type"])))
	if kind != "" {
		return kind, ""
	}
	return "", "missing explicit room_type"
}
