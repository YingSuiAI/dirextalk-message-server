package dirextalkmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

const DefaultLimit = 50
const MaxLimit = 100

type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func BadRequest(message string) *Error {
	return StatusError(http.StatusBadRequest, message)
}

func InternalError(err error) *Error {
	if err == nil {
		return nil
	}
	return StatusError(http.StatusInternalServerError, err.Error())
}

func StatusError(status int, message string) *Error {
	return &Error{Status: status, Message: message}
}

type RoomSummary struct {
	Type           string `json:"type"`
	Name           string `json:"name"`
	RoomID         string `json:"room_id"`
	Subtitle       string `json:"subtitle,omitempty"`
	LastMsg        string `json:"last_msg,omitempty"`
	LastMessageAt  string `json:"last_message_at,omitempty"`
	LastActivityTS int64  `json:"-"`
}

type ContactSummary struct {
	PeerMXID    string `json:"peer_mxid"`
	DisplayName string `json:"display_name"`
	AvatarURL   string `json:"avatar_url,omitempty"`
	Domain      string `json:"domain,omitempty"`
	RoomID      string `json:"room_id"`
	Status      string `json:"status"`
	Remark      string `json:"remark,omitempty"`
}

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

type MessageReader interface {
	ListOrdinaryMessages(ctx context.Context, roomID string, page Page) (MessagePageResult, error)
}

type MemberSummary struct {
	UserID      string `json:"user_id"`
	UserMXID    string `json:"user_mxid"`
	Localpart   string `json:"localpart,omitempty"`
	Domain      string `json:"domain,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	AvatarURL   string `json:"avatar_url,omitempty"`
	Membership  string `json:"membership,omitempty"`
	Role        string `json:"role,omitempty"`
	JoinedAt    string `json:"joined_at,omitempty"`
}

type PostSummary struct {
	PostID        string `json:"post_id"`
	CreatedAt     string `json:"created_at"`
	Sender        string `json:"sender"`
	Msg           string `json:"msg"`
	CommentCount  int64  `json:"comment_count"`
	LikeCount     int64  `json:"like_count"`
	FavoriteCount int64  `json:"favorite_count"`
	FavoritedByMe bool   `json:"favorited_by_me"`
}

type CommentSummary struct {
	CommentID string `json:"comment_id"`
	CreatedAt string `json:"created_at"`
	Sender    string `json:"sender"`
	Msg       string `json:"msg"`
}

type Event struct {
	RoomID         string         `json:"room_id"`
	EventID        string         `json:"event_id"`
	Type           string         `json:"type"`
	Sender         string         `json:"sender"`
	OriginServerTS int64          `json:"origin_server_ts"`
	Content        map[string]any `json:"content"`
}

func Limit(params map[string]any) int {
	limit := int(Int64Param(params["limit"]))
	if limit <= 0 {
		return DefaultLimit
	}
	if limit > MaxLimit {
		return MaxLimit
	}
	return limit
}

func Int64Param(value any) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case json.Number:
		i, _ := v.Int64()
		return i
	case string:
		i, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		return i
	default:
		return 0
	}
}

func TrimString(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return ""
	}
}

func OrdinaryMessageSummary(eventType, eventID string, originServerTS int64, sender string, content map[string]any, page Page) (MessageSummary, bool) {
	if eventType != "m.room.message" || !InPage(originServerTS, eventID, page) {
		return MessageSummary{}, false
	}
	if TrimString(content["p2p_kind"]) != "" {
		return MessageSummary{}, false
	}
	body := TrimString(content["body"])
	if body == "" {
		return MessageSummary{}, false
	}
	sender = strings.TrimSpace(sender)
	localpart, domain := splitMXID(sender)
	return MessageSummary{
		EventID:         eventID,
		OriginServerTS:  originServerTS,
		CreatedAt:       FormatTime(originServerTS),
		Sender:          displayNameFromMXID(sender),
		SenderMXID:      sender,
		SenderDomain:    domain,
		SenderLocalpart: localpart,
		Msg:             body,
	}, true
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

func SortMessageSummaries(messages []MessageSummary) {
	sort.SliceStable(messages, func(i, j int) bool {
		if messages[i].OriginServerTS == messages[j].OriginServerTS {
			return messages[i].EventID > messages[j].EventID
		}
		return messages[i].OriginServerTS > messages[j].OriginServerTS
	})
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
