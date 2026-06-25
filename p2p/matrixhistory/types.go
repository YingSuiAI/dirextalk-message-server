package matrixhistory

import (
	"fmt"
	"strings"
)

type MessageSummary struct {
	TS         int64  `json:"ts"`
	Sender     string `json:"sender"`
	Msg        string `json:"msg"`
	SenderMXID string `json:"-"`
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
	original := strings.TrimSpace(mxid)
	localpart := strings.TrimPrefix(original, "@")
	if idx := strings.Index(localpart, ":"); idx >= 0 {
		localpart = localpart[:idx]
	}
	if strings.TrimSpace(localpart) == "" {
		return original
	}
	return localpart
}
