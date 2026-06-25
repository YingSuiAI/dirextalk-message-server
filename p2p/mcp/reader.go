package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type HTTPMessageReader struct {
	BaseURL string
	Token   func(context.Context) (string, error)
	Client  *http.Client
}

func NewHTTPMessageReader(baseURL string, token func(context.Context) (string, error), client *http.Client) *HTTPMessageReader {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &HTTPMessageReader{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, Client: client}
}

func (r *HTTPMessageReader) ListOrdinaryMessages(ctx context.Context, roomID string, fromTS, toTS int64, limit int) ([]MessageSummary, error) {
	token, err := r.accessToken(ctx)
	if err != nil {
		return nil, err
	}
	values := url.Values{}
	values.Set("dir", "b")
	values.Set("limit", strconv.Itoa(limit))
	payload, err := r.getMessages(ctx, token, roomID, values)
	if err != nil {
		return nil, err
	}
	messages := make([]MessageSummary, 0, len(payload.Chunk))
	for _, event := range payload.Chunk {
		if event.Type != "m.room.message" || !InTimeRange(event.OriginServerTS, fromTS, toTS) {
			continue
		}
		if trimString(event.Content["p2p_kind"]) != "" {
			continue
		}
		body := trimString(event.Content["body"])
		if body == "" {
			continue
		}
		messages = append(messages, MessageSummary{
			TS:         event.OriginServerTS,
			Sender:     displayNameFromMXID(event.Sender),
			Msg:        body,
			SenderMXID: event.Sender,
		})
		if len(messages) >= limit {
			break
		}
	}
	return messages, nil
}

func (r *HTTPMessageReader) ListChannelContent(ctx context.Context, roomID string, limit int) ([]ChannelContentEvent, error) {
	token, err := r.accessToken(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	const pageLimit = 100
	events := make([]ChannelContentEvent, 0)
	seenTokens := map[string]struct{}{}
	next := ""
	for len(events) < limit {
		values := url.Values{}
		values.Set("dir", "b")
		values.Set("limit", strconv.Itoa(pageLimit))
		if next != "" {
			values.Set("from", next)
		}
		payload, err := r.getMessages(ctx, token, roomID, values)
		if err != nil {
			return nil, err
		}
		if len(payload.Chunk) == 0 {
			break
		}
		for _, event := range payload.Chunk {
			if event.Type != "m.room.message" && event.Type != "m.reaction" {
				continue
			}
			if event.Type == "m.room.message" {
				switch trimString(event.Content["p2p_kind"]) {
				case "channel_post", "channel_comment":
				default:
					continue
				}
			}
			if event.Type == "m.reaction" {
				relatesTo, _ := event.Content["m.relates_to"].(map[string]any)
				if trimString(relatesTo["event_id"]) == "" && trimString(event.Content["post_id"]) == "" && trimString(event.Content["comment_id"]) == "" {
					continue
				}
			}
			event.RoomID = roomID
			events = append(events, event)
			if len(events) >= limit {
				break
			}
		}
		if strings.TrimSpace(payload.End) == "" || payload.End == next {
			break
		}
		if _, ok := seenTokens[payload.End]; ok {
			break
		}
		seenTokens[payload.End] = struct{}{}
		next = payload.End
	}
	return events, nil
}

func (r *HTTPMessageReader) accessToken(ctx context.Context) (string, error) {
	if strings.TrimSpace(r.BaseURL) == "" {
		return "", fmt.Errorf("matrix base URL is required")
	}
	if r.Token == nil {
		return "", fmt.Errorf("matrix token provider is required")
	}
	return r.Token(ctx)
}

type messagesResponse struct {
	Chunk []ChannelContentEvent `json:"chunk"`
	End   string                `json:"end"`
}

func (r *HTTPMessageReader) getMessages(ctx context.Context, token, roomID string, values url.Values) (messagesResponse, error) {
	path := r.BaseURL + "/_matrix/client/v3/rooms/" + url.PathEscape(roomID) + "/messages?" + values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return messagesResponse{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := r.Client.Do(req)
	if err != nil {
		return messagesResponse{}, err
	}
	defer func() {
		_ = res.Body.Close()
	}()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return messagesResponse{}, fmt.Errorf("matrix messages failed with status %d", res.StatusCode)
	}
	var payload messagesResponse
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return messagesResponse{}, err
	}
	return payload, nil
}
