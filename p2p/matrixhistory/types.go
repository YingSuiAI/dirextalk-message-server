package matrixhistory

import (
	"fmt"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
)

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

func trimString(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return ""
	}
}

func displayNameFromMXID(mxid string) string {
	localpart, _ := splitMXID(mxid)
	if strings.TrimSpace(localpart) == "" {
		return strings.TrimSpace(mxid)
	}
	return localpart
}

func splitMXID(mxid string) (localpart, domain string) {
	trimmed := strings.TrimSpace(mxid)
	withoutSigil := strings.TrimPrefix(trimmed, "@")
	if idx := strings.Index(withoutSigil, ":"); idx >= 0 {
		localpart = strings.TrimSpace(withoutSigil[:idx])
		domain = strings.TrimSpace(withoutSigil[idx+1:])
		return localpart, domain
	}
	return strings.TrimSpace(withoutSigil), ""
}
