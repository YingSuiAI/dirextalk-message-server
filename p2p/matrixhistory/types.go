package matrixhistory

import (
	"fmt"
	"strings"
)

type MessageSummary struct {
	TS              int64  `json:"ts"`
	Sender          string `json:"sender"`
	SenderMXID      string `json:"sender_mxid,omitempty"`
	SenderDomain    string `json:"sender_domain,omitempty"`
	SenderLocalpart string `json:"sender_localpart,omitempty"`
	Msg             string `json:"msg"`
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
