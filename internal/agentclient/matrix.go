package agentclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultAgentDeviceID = "DIREXIO_CLI"

type MatrixSession struct {
	AccessToken string `json:"access_token"`
	DeviceID    string `json:"device_id"`
	UserID      string `json:"user_id"`
	Homeserver  string `json:"homeserver"`
}

func (c *Client) CreateMatrixSession(ctx context.Context) (MatrixSession, error) {
	resp, err := c.CallP2PAction(ctx, "agent.matrix_session.create", map[string]any{"device_id": defaultAgentDeviceID}, P2PCommand)
	if err != nil {
		return MatrixSession{}, err
	}
	session := MatrixSession{
		AccessToken: stringValue(resp["access_token"]),
		DeviceID:    stringValue(resp["device_id"]),
		UserID:      stringValue(resp["user_id"]),
		Homeserver:  stringValue(resp["homeserver"]),
	}
	if session.AccessToken == "" {
		return MatrixSession{}, fmt.Errorf("agent.matrix_session.create did not return access_token")
	}
	return session, nil
}

func (c *Client) SendTextMessage(ctx context.Context, session MatrixSession, roomID, text string) (map[string]any, error) {
	if strings.TrimSpace(roomID) == "" {
		return nil, fmt.Errorf("room-id is required")
	}
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("text is required")
	}
	txnID := "direxio-cli-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	path := "/v3/rooms/" + url.PathEscape(roomID) + "/send/m.room.message/" + url.PathEscape(txnID)
	return c.matrixJSON(ctx, session, http.MethodPut, path, map[string]any{
		"msgtype": "m.text",
		"body":    text,
	})
}

func (c *Client) RoomMessages(ctx context.Context, session MatrixSession, roomID string, limit int) (map[string]any, error) {
	if strings.TrimSpace(roomID) == "" {
		return nil, fmt.Errorf("room-id is required")
	}
	if limit <= 0 {
		limit = 50
	}
	path := "/v3/rooms/" + url.PathEscape(roomID) + "/messages?dir=b&limit=" + strconv.Itoa(limit)
	return c.matrixJSON(ctx, session, http.MethodGet, path, nil)
}

func (c *Client) Sync(ctx context.Context, session MatrixSession, timeoutMS int, since string) (map[string]any, error) {
	values := url.Values{}
	if timeoutMS > 0 {
		values.Set("timeout", strconv.Itoa(timeoutMS))
	}
	if strings.TrimSpace(since) != "" {
		values.Set("since", strings.TrimSpace(since))
	}
	path := "/v3/sync"
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return c.matrixJSON(ctx, session, http.MethodGet, path, nil)
}

func ExtractSyncTimelineEvents(sync map[string]any) []map[string]any {
	rooms, _ := sync["rooms"].(map[string]any)
	join, _ := rooms["join"].(map[string]any)
	var out []map[string]any
	for roomID, rawRoom := range join {
		room, _ := rawRoom.(map[string]any)
		timeline, _ := room["timeline"].(map[string]any)
		events, _ := timeline["events"].([]any)
		for _, rawEvent := range events {
			event, ok := rawEvent.(map[string]any)
			if !ok {
				continue
			}
			copy := make(map[string]any, len(event)+1)
			for key, value := range event {
				copy[key] = value
			}
			copy["room_id"] = roomID
			out = append(out, copy)
		}
	}
	return out
}

func (c *Client) matrixJSON(ctx context.Context, session MatrixSession, method, path string, body any) (map[string]any, error) {
	if strings.TrimSpace(session.AccessToken) == "" {
		return nil, fmt.Errorf("matrix session access token is required")
	}
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.MatrixBaseURL()+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+session.AccessToken)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = res.Body.Close()
	}()
	return decodeObjectResponse(res, "matrix "+method+" "+path)
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}
