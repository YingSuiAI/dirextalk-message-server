package p2p

import "github.com/YingSuiAI/dirextalk-message-server/p2p/projection"

func conversationKindFromContent(content map[string]any) (conversationKind, string) {
	return projection.ConversationKindFromContent(content)
}

func conversationKindFromRoomType(roomType string) conversationKind {
	return projection.ConversationKindFromRoomType(roomType)
}
