package matrixhistory

import (
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmatrix"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
)

type MessageSummary = dirextalkmcp.MessageSummary
type Page = dirextalkmcp.Page
type MessagePageResult = dirextalkmcp.MessagePageResult
type Event = dirextalkmcp.Event

type StatusError = dirextalkmatrix.StatusError
type HTTPMessageReader = dirextalkmatrix.HTTPMessageReader

func SortMessageSummaries(messages []MessageSummary) {
	dirextalkmcp.SortMessageSummaries(messages)
}

func InTimeRange(ts, fromTS, toTS int64) bool {
	return dirextalkmcp.InTimeRange(ts, fromTS, toTS)
}

func InPage(ts int64, id string, page Page) bool {
	return dirextalkmcp.InPage(ts, id, page)
}

func FormatTime(ts int64) string {
	return dirextalkmcp.FormatTime(ts)
}
