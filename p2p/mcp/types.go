package mcp

import (
	"context"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
)

const DefaultLimit = dirextalkmcp.DefaultLimit
const MaxLimit = dirextalkmcp.MaxLimit

type RoomSummary = dirextalkmcp.RoomSummary
type ContactSummary = dirextalkmcp.ContactSummary
type MessageSummary = dirextalkmcp.MessageSummary
type MessagePage = dirextalkmcp.Page
type MessagePageResult = dirextalkmcp.MessagePageResult
type MemberSummary = dirextalkmcp.MemberSummary
type PostSummary = dirextalkmcp.PostSummary
type CommentSummary = dirextalkmcp.CommentSummary

type MessageReader interface {
	ListOrdinaryMessages(ctx context.Context, roomID string, page MessagePage) (MessagePageResult, error)
}

func InTimeRange(ts, fromTS, toTS int64) bool {
	return dirextalkmcp.InTimeRange(ts, fromTS, toTS)
}
