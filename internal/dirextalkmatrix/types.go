package dirextalkmatrix

import "github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"

type MessageSummary = dirextalkmcp.MessageSummary
type Page = dirextalkmcp.Page
type MessagePageResult = dirextalkmcp.MessagePageResult

func SortMessageSummaries(messages []MessageSummary) {
	dirextalkmcp.SortMessageSummaries(messages)
}

type Event = dirextalkmcp.Event

func InTimeRange(ts, fromTS, toTS int64) bool {
	return dirextalkmcp.InTimeRange(ts, fromTS, toTS)
}

func InPage(ts int64, id string, page Page) bool {
	return dirextalkmcp.InPage(ts, id, page)
}

func FormatTime(ts int64) string {
	return dirextalkmcp.FormatTime(ts)
}

func OrdinaryMessageSummary(eventType, eventID string, originServerTS int64, sender string, content map[string]any, page Page) (MessageSummary, bool) {
	return dirextalkmcp.OrdinaryMessageSummary(eventType, eventID, originServerTS, sender, content, page)
}

func trimString(value any) string {
	return dirextalkmcp.TrimString(value)
}
