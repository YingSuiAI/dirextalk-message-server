package mcp

import (
	"context"
	"fmt"
	"strings"
)

const DefaultLimit = 50
const MaxLimit = 100

type RoomSummary struct {
	Type     string `json:"type"`
	Name     string `json:"name"`
	RoomID   string `json:"room_id"`
	Subtitle string `json:"subtitle,omitempty"`
	LastMsg  string `json:"last_msg,omitempty"`
	LastTS   int64  `json:"last_ts,omitempty"`
}

type MessageSummary struct {
	TS         int64  `json:"ts"`
	Sender     string `json:"sender"`
	Msg        string `json:"msg"`
	SenderMXID string `json:"-"`
}

type PostSummary struct {
	PostID       string `json:"post_id"`
	TS           int64  `json:"ts"`
	Sender       string `json:"sender"`
	Msg          string `json:"msg"`
	CommentCount int64  `json:"comment_count"`
}

type CommentSummary struct {
	CommentID string `json:"comment_id"`
	TS        int64  `json:"ts"`
	Sender    string `json:"sender"`
	Msg       string `json:"msg"`
}

type MessageReader interface {
	ListOrdinaryMessages(ctx context.Context, roomID string, fromTS, toTS int64, limit int) ([]MessageSummary, error)
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
