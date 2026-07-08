package dirextalkmcp

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
)

type CursorPayload struct {
	Version        int    `json:"v"`
	Action         string `json:"action"`
	TargetID       string `json:"target_id"`
	FromTimeMS     int64  `json:"from_time_ms,omitempty"`
	SnapshotTimeMS int64  `json:"snapshot_time_ms"`
	LastTimeMS     int64  `json:"last_time_ms"`
	LastID         string `json:"last_id"`
}

func PageFromParams(params map[string]any, action, targetID string) (Page, *Error) {
	if apiErr := RejectLegacyTimeParams(params); apiErr != nil {
		return Page{}, apiErr
	}
	limit := Limit(params)
	if cursor := TrimString(params["cursor"]); cursor != "" {
		payload, apiErr := DecodeCursor(cursor)
		if apiErr != nil {
			return Page{}, apiErr
		}
		if payload.Action != action || payload.TargetID != targetID || payload.SnapshotTimeMS <= 0 || payload.LastTimeMS <= 0 || strings.TrimSpace(payload.LastID) == "" {
			return Page{}, BadRequest("cursor is invalid for this query")
		}
		return Page{
			FromTS:     payload.FromTimeMS,
			SnapshotTS: payload.SnapshotTimeMS,
			CursorTS:   payload.LastTimeMS,
			CursorID:   payload.LastID,
			Limit:      limit,
		}, nil
	}
	fromTS, _, apiErr := TimeParam(params, "from_time")
	if apiErr != nil {
		return Page{}, apiErr
	}
	toTS, hasTo, apiErr := TimeParam(params, "to_time")
	if apiErr != nil {
		return Page{}, apiErr
	}
	if !hasTo {
		toTS = time.Now().UTC().UnixMilli()
	}
	if fromTS > 0 && fromTS > toTS {
		return Page{}, BadRequest("from_time must be less than or equal to to_time")
	}
	return Page{FromTS: fromTS, SnapshotTS: toTS, Limit: limit}, nil
}

func RejectLegacyTimeParams(params map[string]any) *Error {
	if _, ok := params["from_ts"]; ok {
		return BadRequest("use from_time/to_time instead of from_ts/to_ts")
	}
	if _, ok := params["to_ts"]; ok {
		return BadRequest("use from_time/to_time instead of from_ts/to_ts")
	}
	return nil
}

func TimeParam(params map[string]any, key string) (int64, bool, *Error) {
	value, ok := params[key]
	if !ok {
		return 0, false, nil
	}
	text := TrimString(value)
	if text == "" {
		return 0, false, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return 0, false, BadRequest(key + " must be RFC3339 UTC")
	}
	if _, offset := parsed.Zone(); offset != 0 {
		return 0, false, BadRequest(key + " must be RFC3339 UTC")
	}
	return parsed.UTC().UnixMilli(), true, nil
}

func DecodeCursor(cursor string) (CursorPayload, *Error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(cursor))
	if err != nil {
		return CursorPayload{}, BadRequest("cursor is invalid")
	}
	var payload CursorPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return CursorPayload{}, BadRequest("cursor is invalid")
	}
	if payload.Version != 1 {
		return CursorPayload{}, BadRequest("cursor is invalid")
	}
	return payload, nil
}

func EncodeCursor(action, targetID string, page Page, lastTS int64, lastID string) (string, error) {
	raw, err := json.Marshal(CursorPayload{
		Version:        1,
		Action:         action,
		TargetID:       targetID,
		FromTimeMS:     page.FromTS,
		SnapshotTimeMS: page.SnapshotTS,
		LastTimeMS:     lastTS,
		LastID:         lastID,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func AttachPagination(payload map[string]any, action, targetID string, page Page, hasMore bool, lastTS int64, lastID string) *Error {
	payload["has_more"] = hasMore
	if !hasMore || lastTS <= 0 || strings.TrimSpace(lastID) == "" {
		return nil
	}
	cursor, err := EncodeCursor(action, targetID, page, lastTS, lastID)
	if err != nil {
		return InternalError(err)
	}
	payload["next_cursor"] = cursor
	return nil
}
