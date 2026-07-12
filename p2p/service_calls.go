package p2p

import (
	"context"
	"strings"
	"time"
)

type callStore interface {
	UpsertCall(ctx context.Context, call callRecord) error
	ListCalls(ctx context.Context, roomID string, activeOnly bool) ([]callRecord, error)
}

func (s *Service) callStore() callStore {
	if s.store == nil {
		return nil
	}
	return s.store
}

func (s *Service) callSession(ctx context.Context, params map[string]any) (any, *apiError) {
	now := time.Now().UTC()
	callID := trimString(params["call_id"])
	if callID == "" {
		callID = "call_" + randomToken("p2p")
	}
	roomID := trimString(params["room_id"])
	if roomID == "" {
		roomID = "!call:" + s.serverName
	}
	state := "ringing"
	if event := trimString(params["event"]); event != "" {
		state = event
	}
	createdAt := callTimeParam(params["created_at"], params["created_at_ms"])
	if createdAt == "" {
		createdAt = now.Format(time.RFC3339Nano)
	}
	call := callRecord{
		CallID:        callID,
		RoomID:        roomID,
		RoomType:      "direct",
		MediaType:     fallbackString(trimString(params["media_type"]), "voice"),
		CreatedByMXID: fallbackString(trimString(params["created_by_mxid"]), s.ownerMXID),
		State:         state,
		CreatedAt:     createdAt,
	}
	if existing, ok, err := s.callByID(ctx, callID); err != nil {
		return nil, internalError(err)
	} else if ok && terminalCallState(existing.State) {
		return existing, nil
	}
	applyCallLifecycle(&call, state, params, now, s.ownerMXID)
	s.mu.Lock()
	s.calls[call.CallID] = call
	s.mu.Unlock()
	if store := s.callStore(); store != nil {
		if err := store.UpsertCall(ctx, call); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.publishCallChanged(ctx, call); err != nil {
		return nil, internalError(err)
	}
	return call, nil
}

func (s *Service) callGet(ctx context.Context, params map[string]any) (any, *apiError) {
	callID := trimString(params["call_id"])
	if callID == "" {
		return nil, badRequest("call_id is required")
	}
	call, ok, err := s.callByID(ctx, callID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "call not found")
	}
	return call, nil
}

func (s *Service) callEvent(ctx context.Context, params map[string]any) (any, *apiError) {
	callID := trimString(params["call_id"])
	if callID == "" {
		return nil, badRequest("call_id is required")
	}
	event := trimString(params["event"])
	switch event {
	case "connected", "ended", "rejected", "missed", "failed":
	default:
		return nil, badRequest("event must be connected, ended, rejected, missed, or failed")
	}
	call, ok, err := s.callByID(ctx, callID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(404, "call not found")
	}
	if terminalCallState(call.State) && call.State != event {
		return call, nil
	}
	call.State = event
	if mediaType := trimString(params["media_type"]); mediaType != "" {
		call.MediaType = mediaType
	}
	applyCallLifecycle(&call, event, params, time.Now().UTC(), s.ownerMXID)
	s.mu.Lock()
	s.calls[call.CallID] = call
	s.mu.Unlock()
	if store := s.callStore(); store != nil {
		if err := store.UpsertCall(ctx, call); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.publishCallChanged(ctx, call); err != nil {
		return nil, internalError(err)
	}
	return call, nil
}

func (s *Service) publishCallChanged(ctx context.Context, call callRecord) error {
	return s.appendP2PEvent(ctx, p2pEvent{
		Type:   "call.changed",
		RoomID: call.RoomID,
		Payload: map[string]any{
			"call": call,
		},
	})
}

func applyCallLifecycle(call *callRecord, event string, params map[string]any, now time.Time, localUserID string) {
	switch event {
	case "connected":
		if answeredAt := callTimeParam(params["answered_at"], params["answered_at_ms"]); answeredAt != "" {
			call.AnsweredAt = answeredAt
		} else if call.AnsweredAt == "" {
			call.AnsweredAt = now.Format(time.RFC3339Nano)
		}
	case "ended", "rejected", "missed", "failed":
		if endedAt := callTimeParam(params["ended_at"], params["ended_at_ms"]); endedAt != "" {
			call.EndedAt = endedAt
		} else if call.EndedAt == "" {
			call.EndedAt = now.Format(time.RFC3339Nano)
		}
		call.EndedByMXID = fallbackString(trimString(params["ended_by_mxid"]), localUserID)
		call.EndReason = trimString(params["reason"])
		if durationMS := int64Param(params["duration_ms"]); durationMS > 0 {
			call.DurationMS = durationMS
		} else if call.DurationMS <= 0 {
			call.DurationMS = callDurationMS(call.AnsweredAt, call.EndedAt)
		}
	}
}

func callDurationMS(start, end string) int64 {
	startTime, startErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(start))
	endTime, endErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(end))
	if startErr != nil || endErr != nil {
		return 0
	}
	duration := endTime.Sub(startTime)
	if duration < 0 {
		return 0
	}
	return duration.Milliseconds()
}

func (s *Service) callByID(ctx context.Context, callID string) (callRecord, bool, error) {
	if store := s.callStore(); store != nil {
		calls, err := store.ListCalls(ctx, "", false)
		if err != nil {
			return callRecord{}, false, err
		}
		for _, call := range calls {
			if call.CallID == callID {
				return call, true, nil
			}
		}
		return callRecord{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	call, ok := s.calls[callID]
	return call, ok, nil
}

func (s *Service) callList(ctx context.Context, params map[string]any, activeOnly bool) any {
	roomID := trimString(params["room_id"])
	if store := s.callStore(); store != nil {
		calls, err := store.ListCalls(ctx, roomID, activeOnly)
		if err == nil {
			return map[string]any{"calls": calls}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	calls := make([]callRecord, 0, len(s.calls))
	for _, call := range s.calls {
		if roomID != "" && call.RoomID != roomID {
			continue
		}
		if activeOnly && terminalCallState(call.State) {
			continue
		}
		calls = append(calls, call)
	}
	return map[string]any{"calls": calls}
}

func terminalCallState(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "ended", "rejected", "missed", "failed":
		return true
	default:
		return false
	}
}
