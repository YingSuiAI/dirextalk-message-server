package mcp

import (
	"context"

	"github.com/YingSuiAI/direxio-message-server/p2p/matrixhistory"
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

type MessageSummary = matrixhistory.MessageSummary

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
	return matrixhistory.InTimeRange(ts, fromTS, toTS)
}
