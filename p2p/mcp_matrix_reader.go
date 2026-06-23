package p2p

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

type HTTPMCPMessageReader struct {
	BaseURL string
	Token   func(context.Context) (string, error)
	Client  *http.Client
}

func NewHTTPMCPMessageReader(baseURL string, token func(context.Context) (string, error), client *http.Client) *HTTPMCPMessageReader {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &HTTPMCPMessageReader{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, Client: client}
}

func (r *HTTPMCPMessageReader) ListOrdinaryMessages(ctx context.Context, roomID string, fromTS, toTS int64, limit int) ([]mcpMessageSummary, error) {
	if strings.TrimSpace(r.BaseURL) == "" {
		return nil, fmt.Errorf("matrix base URL is required")
	}
	if r.Token == nil {
		return nil, fmt.Errorf("matrix token provider is required")
	}
	token, err := r.Token(ctx)
	if err != nil {
		return nil, err
	}
	values := url.Values{}
	values.Set("dir", "b")
	values.Set("limit", strconv.Itoa(limit))
	path := r.BaseURL + "/_matrix/client/v3/rooms/" + url.PathEscape(roomID) + "/messages?" + values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := r.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = res.Body.Close()
	}()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("matrix messages failed with status %d", res.StatusCode)
	}
	var payload struct {
		Chunk []struct {
			Type           string         `json:"type"`
			Sender         string         `json:"sender"`
			OriginServerTS int64          `json:"origin_server_ts"`
			Content        map[string]any `json:"content"`
		} `json:"chunk"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return nil, err
	}
	messages := make([]mcpMessageSummary, 0, len(payload.Chunk))
	for _, event := range payload.Chunk {
		if event.Type != "m.room.message" || !inMCPTimeRange(event.OriginServerTS, fromTS, toTS) {
			continue
		}
		if trimString(event.Content["p2p_kind"]) != "" {
			continue
		}
		body := trimString(event.Content["body"])
		if body == "" {
			continue
		}
		messages = append(messages, mcpMessageSummary{
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
