package matrixhistory

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type MessageSummary struct {
	EventID           string `json:"-"`
	OriginServerTS    int64  `json:"-"`
	CreatedAt         string `json:"created_at"`
	Sender            string `json:"sender"`
	SenderMXID        string `json:"sender_mxid,omitempty"`
	SenderDisplayName string `json:"sender_display_name,omitempty"`
	SenderDomain      string `json:"sender_domain,omitempty"`
	SenderLocalpart   string `json:"sender_localpart,omitempty"`
	Msg               string `json:"msg"`
}

type Page struct {
	FromTS     int64
	SnapshotTS int64
	CursorTS   int64
	CursorID   string
	Limit      int
}

type MessagePageResult struct {
	Messages []MessageSummary
	HasMore  bool
}

func SortMessageSummaries(messages []MessageSummary) {
	sort.SliceStable(messages, func(i, j int) bool {
		if messages[i].OriginServerTS == messages[j].OriginServerTS {
			return messages[i].EventID > messages[j].EventID
		}
		return messages[i].OriginServerTS > messages[j].OriginServerTS
	})
}

type Event struct {
	RoomID         string         `json:"room_id"`
	EventID        string         `json:"event_id"`
	Type           string         `json:"type"`
	Sender         string         `json:"sender"`
	OriginServerTS int64          `json:"origin_server_ts"`
	Content        map[string]any `json:"content"`
}

func InTimeRange(ts, fromTS, toTS int64) bool {
	if fromTS > 0 && ts < fromTS {
		return false
	}
	if toTS > 0 && ts > toTS {
		return false
	}
	return true
}

func InPage(ts int64, id string, page Page) bool {
	if !InTimeRange(ts, page.FromTS, page.SnapshotTS) {
		return false
	}
	if page.CursorTS <= 0 {
		return true
	}
	if ts < page.CursorTS {
		return true
	}
	return ts == page.CursorTS && strings.TrimSpace(id) < strings.TrimSpace(page.CursorID)
}

func FormatTime(ts int64) string {
	if ts <= 0 {
		return ""
	}
	return time.UnixMilli(ts).UTC().Format(time.RFC3339Nano)
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
