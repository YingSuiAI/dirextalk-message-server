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

type socialStore interface {
	UpsertFavorite(ctx context.Context, favorite favoriteRecord) error
	FindFavoriteByEvent(ctx context.Context, eventID, roomID string) (favoriteRecord, bool, error)
	ListFavorites(ctx context.Context, messageType string) ([]favoriteRecord, error)
	DeleteFavorite(ctx context.Context, id int64) error
	UpsertFollow(ctx context.Context, follow followRecord) error
	ListFollows(ctx context.Context) ([]followRecord, error)
	DeleteFollow(ctx context.Context, domain string) error
}

func (s *Service) socialStore() socialStore {
	if s.store == nil {
		return nil
	}
	return s.store
}

func (s *Service) favoriteMessage(ctx context.Context, params map[string]any) (any, *apiError) {
	now := time.Now().UTC()
	favorite := favoriteRecord{
		ID:             now.UnixMilli(),
		EventID:        trimString(params["event_id"]),
		RoomID:         trimString(params["room_id"]),
		SenderID:       trimString(params["sender_id"]),
		SenderName:     trimString(params["sender_name"]),
		Content:        trimString(params["content"]),
		MessageType:    trimString(params["message_type"]),
		OriginServerTS: int64Param(params["origin_server_ts"]),
		CreatedAt:      now.Format(time.RFC3339Nano),
	}
	reuseFavoriteID := false
	if favorite.EventID != "" {
		if store := s.socialStore(); store != nil {
			existing, ok, err := store.FindFavoriteByEvent(ctx, favorite.EventID, favorite.RoomID)
			if err != nil {
				return nil, internalError(err)
			}
			if ok {
				favorite.ID = existing.ID
				reuseFavoriteID = true
				if existing.CreatedAt != "" {
					favorite.CreatedAt = existing.CreatedAt
				}
			}
		} else {
			s.mu.Lock()
			for _, existing := range s.favorites {
				if sameFavoriteTarget(existing, favorite) {
					favorite.ID = existing.ID
					reuseFavoriteID = true
					if existing.CreatedAt != "" {
						favorite.CreatedAt = existing.CreatedAt
					}
					break
				}
			}
			s.mu.Unlock()
		}
	}
	s.mu.Lock()
	for !reuseFavoriteID && s.favorites[favorite.ID].ID != 0 {
		favorite.ID++
	}
	s.favorites[favorite.ID] = favorite
	s.mu.Unlock()
	if store := s.socialStore(); store != nil {
		if err := store.UpsertFavorite(ctx, favorite); err != nil {
			return nil, internalError(err)
		}
	}
	return favorite, nil
}

func sameFavoriteTarget(existing, incoming favoriteRecord) bool {
	if incoming.EventID == "" || existing.EventID != incoming.EventID {
		return false
	}
	if incoming.RoomID != "" && existing.RoomID != "" && incoming.RoomID != existing.RoomID {
		return false
	}
	return true
}

func (s *Service) favoriteList(ctx context.Context, params map[string]any) any {
	messageType := trimString(params["message_type"])
	if store := s.socialStore(); store != nil {
		favorites, err := store.ListFavorites(ctx, messageType)
		if err == nil {
			return map[string]any{"favorites": favorites}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	favorites := make([]favoriteRecord, 0, len(s.favorites))
	for _, favorite := range s.favorites {
		if messageType == "" || favorite.MessageType == messageType {
			favorites = append(favorites, favorite)
		}
	}
	return map[string]any{"favorites": favorites}
}

func (s *Service) favoriteDelete(ctx context.Context, params map[string]any) (any, *apiError) {
	id := int64Param(params["id"])
	s.mu.Lock()
	delete(s.favorites, id)
	s.mu.Unlock()
	if store := s.socialStore(); store != nil {
		if err := store.DeleteFavorite(ctx, id); err != nil {
			return nil, internalError(err)
		}
	}
	return map[string]any{"status": "ok"}, nil
}

func (s *Service) favoriteDeleteBatch(ctx context.Context, params map[string]any) (any, *apiError) {
	ids := int64SliceParam(params["ids"])
	if len(ids) == 0 {
		return nil, badRequest("ids is required")
	}
	if len(ids) > 500 {
		return nil, badRequest("ids is too large")
	}
	s.mu.Lock()
	for _, id := range ids {
		delete(s.favorites, id)
	}
	s.mu.Unlock()
	if store := s.socialStore(); store != nil {
		for _, id := range ids {
			if err := store.DeleteFavorite(ctx, id); err != nil {
				return nil, internalError(err)
			}
		}
	}
	return map[string]any{"status": "ok", "deleted": ids}, nil
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

func (s *Service) followAdd(ctx context.Context, params map[string]any) (any, *apiError) {
	domain := trimString(params["domain"])
	if domain == "" {
		return nil, badRequest("domain is required")
	}
	follow := followRecord{Domain: domain, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	s.mu.Lock()
	s.follows[domain] = follow
	s.mu.Unlock()
	if store := s.socialStore(); store != nil {
		if err := store.UpsertFollow(ctx, follow); err != nil {
			return nil, internalError(err)
		}
	}
	return follow, nil
}

func (s *Service) followRemove(ctx context.Context, params map[string]any) (any, *apiError) {
	domain := trimString(params["domain"])
	s.mu.Lock()
	delete(s.follows, domain)
	s.mu.Unlock()
	if store := s.socialStore(); store != nil {
		if err := store.DeleteFollow(ctx, domain); err != nil {
			return nil, internalError(err)
		}
	}
	return map[string]any{"status": "ok"}, nil
}

func (s *Service) followList(ctx context.Context) any {
	if store := s.socialStore(); store != nil {
		follows, err := store.ListFollows(ctx)
		if err == nil {
			return map[string]any{"follows": follows}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	follows := make([]followRecord, 0, len(s.follows))
	for _, follow := range s.follows {
		follows = append(follows, follow)
	}
	return map[string]any{"follows": follows}
}
