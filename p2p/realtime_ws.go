package p2p

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/realtime"
	"github.com/YingSuiAI/direxio-message-server/p2p/serviceapi"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const (
	realtimeWSTicketAction = "realtime.ws_ticket.create"
	realtimeWSTicketTTL    = 30 * time.Second
	realtimeWSBatchLimit   = 100
)

type realtimeWSTicket struct {
	Role      string
	UserID    string
	ExpiresAt time.Time
}

type realtimeWSSubscriber struct {
	Role   string
	Frames chan<- map[string]any
}

type realtimeWSConnection struct {
	sessionID string
	record    realtimeWSTicket
	outbound  chan map[string]any
}

func newRealtimeWSConnection(sessionID string, record realtimeWSTicket) *realtimeWSConnection {
	return &realtimeWSConnection{
		sessionID: sessionID,
		record:    record,
		outbound:  make(chan map[string]any, 32),
	}
}

func (c *realtimeWSConnection) send(frame map[string]any) {
	if c == nil || frame == nil {
		return
	}
	select {
	case c.outbound <- frame:
	default:
	}
}

func realtimeWSHandler(service *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		ticket := strings.TrimSpace(r.URL.Query().Get("ticket"))
		record, err := service.consumeRealtimeWSTicketRecord(ticket)
		if err != nil {
			writeError(w, statusError(http.StatusUnauthorized, "M_UNKNOWN_TOKEN"))
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusInternalError, "connection closed")

		sessionID := randomToken("ws_session")
		service.upsertRealtimeWSSession(sessionID, realtime.SessionState{
			UserID:   record.UserID,
			Role:     record.Role,
			LastSeen: time.Now().UTC(),
		})
		defer service.removeRealtimeWSSession(sessionID)

		ctx := r.Context()
		hello, err := readRealtimeWSHello(ctx, conn)
		if err != nil {
			_ = wsjson.Write(ctx, conn, map[string]any{
				"type":  "server.error",
				"error": err.Error(),
			})
			return
		}
		since := int64Param(hello["since"])
		if since < 0 {
			since = 0
		}
		service.touchRealtimeWSSession(sessionID)
		if err := wsjson.Write(ctx, conn, map[string]any{
			"type":                  "server.ready",
			"role":                  record.Role,
			"heartbeat_interval_ms": int64(eventStreamHeartbeat / time.Millisecond),
		}); err != nil {
			return
		}

		wsConn := newRealtimeWSConnection(sessionID, record)
		service.registerRealtimeWSSubscriber(wsConn)
		defer service.unregisterRealtimeWSSubscriber(sessionID)

		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			service.readRealtimeWSFrames(ctx, conn, wsConn)
		}()

		service.streamRealtimeWSEvents(ctx, conn, record.Role, since, readDone, wsConn.outbound)
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}
}

func readRealtimeWSHello(ctx context.Context, conn *websocket.Conn) (map[string]any, error) {
	var frame map[string]any
	if err := wsjson.Read(ctx, conn, &frame); err != nil {
		return nil, err
	}
	if trimString(frame["type"]) != "client.hello" {
		return nil, errors.New("client.hello is required")
	}
	return frame, nil
}

func (s *Service) readRealtimeWSFrames(ctx context.Context, conn *websocket.Conn, wsConn *realtimeWSConnection) {
	for {
		var frame map[string]any
		if err := wsjson.Read(ctx, conn, &frame); err != nil {
			return
		}
		sessionID := wsConn.sessionID
		switch trimString(frame["type"]) {
		case "client.lifecycle":
			s.updateRealtimeWSSession(sessionID, func(state *realtime.SessionState) {
				state.Foreground = boolParam(frame["foreground"])
			})
		case "client.focus":
			s.updateRealtimeWSSession(sessionID, func(state *realtime.SessionState) {
				state.FocusedRoomID = trimString(frame["room_id"])
			})
		case "client.ack":
			s.updateRealtimeWSSession(sessionID, func(state *realtime.SessionState) {
				if seq := int64Param(frame["seq"]); seq > state.LastAckSeq {
					state.LastAckSeq = seq
				}
			})
		case "client.ping":
			s.touchRealtimeWSSession(sessionID)
		case "client.request":
			wsConn.send(s.handleRealtimeWSRequest(ctx, wsConn.record, frame))
		case "client.command":
			wsConn.send(s.handleRealtimeWSRequest(ctx, wsConn.record, frame))
		case "client.agent_stream":
			if response := s.handleRealtimeWSAgentStream(wsConn.record, frame); response != nil {
				wsConn.send(response)
			}
		default:
			s.touchRealtimeWSSession(sessionID)
		}
	}
}

func (s *Service) streamRealtimeWSEvents(ctx context.Context, conn *websocket.Conn, role string, since int64, readDone <-chan struct{}, outbound <-chan map[string]any) {
	cursorStatus, err := s.p2pEventCursorStatus(ctx, since)
	if err != nil {
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "server.error", "error": err.Error()})
		return
	}
	if cursorStatus.Expired {
		if err := wsjson.Write(ctx, conn, map[string]any{
			"type":     "server.cursor_reset",
			"since":    cursorStatus.Since,
			"min_seq":  cursorStatus.Bounds.MinSeq,
			"max_seq":  cursorStatus.Bounds.MaxSeq,
			"count":    cursorStatus.Bounds.Count,
			"recovery": "bootstrap_required",
		}); err != nil {
			return
		}
	}
	for {
		events, err := s.listP2PEvents(ctx, since, realtimeWSBatchLimit)
		if err != nil {
			_ = wsjson.Write(ctx, conn, map[string]any{"type": "server.error", "error": err.Error()})
			return
		}
		if len(events) > 0 {
			for _, event := range events {
				if realtimeWSEventVisible(role, event) {
					if err := wsjson.Write(ctx, conn, map[string]any{
						"type":  "server.event",
						"event": event,
					}); err != nil {
						return
					}
				}
				since = event.Seq
			}
			continue
		}
		waitForEvent := s.p2pEventWaiter()
		select {
		case <-ctx.Done():
			return
		case <-readDone:
			return
		case frame := <-outbound:
			if err := wsjson.Write(ctx, conn, frame); err != nil {
				return
			}
		case <-waitForEvent:
		}
	}
}

func realtimeWSEventVisible(role string, event p2pEvent) bool {
	if role == "agent" {
		return event.Type == AgentRoomMessageEventType
	}
	return true
}

func (s *Service) handleRealtimeWSRequest(ctx context.Context, record realtimeWSTicket, frame map[string]any) map[string]any {
	id := trimString(frame["id"])
	action := trimString(frame["action"])
	if id == "" {
		return realtimeWSResponseError(id, action, http.StatusBadRequest, "id is required")
	}
	if action == "" {
		return realtimeWSResponseError(id, action, http.StatusBadRequest, "action is required")
	}
	params := map[string]any{}
	if rawParams, ok := frame["params"].(map[string]any); ok {
		params = rawParams
	} else if rawParams, ok := frame["params"].(map[string]interface{}); ok {
		params = rawParams
	} else if frame["params"] != nil {
		return realtimeWSResponseError(id, action, http.StatusBadRequest, "params must be an object")
	}
	if action == realtimeWSTicketAction {
		return realtimeWSResponseError(id, action, http.StatusForbidden, "M_FORBIDDEN")
	}
	if record.Role != "owner" && !(record.Role == "agent" && serviceapi.AgentAction(action)) {
		return realtimeWSResponseError(id, action, http.StatusForbidden, "M_FORBIDDEN")
	}
	result, apiErr := s.Handle(ctx, action, params)
	if apiErr != nil {
		return realtimeWSResponseError(id, action, apiErr.Status, apiErr.Error)
	}
	return map[string]any{
		"type":   "server.response",
		"id":     id,
		"action": action,
		"ok":     true,
		"result": result,
	}
}

func realtimeWSResponseError(id, action string, status int, message string) map[string]any {
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	if strings.TrimSpace(message) == "" {
		message = "M_UNKNOWN"
	}
	return map[string]any{
		"type":   "server.response",
		"id":     id,
		"action": action,
		"ok":     false,
		"status": status,
		"error":  message,
	}
}

func realtimeWSCommandError(id string, status int, message string) map[string]any {
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	if strings.TrimSpace(message) == "" {
		message = "M_UNKNOWN"
	}
	return map[string]any{
		"type":   "server.command_error",
		"id":     id,
		"status": status,
		"error":  message,
	}
}

func (s *Service) handleRealtimeWSAgentStream(record realtimeWSTicket, frame map[string]any) map[string]any {
	if record.Role != "agent" {
		return realtimeWSCommandError(trimString(frame["id"]), http.StatusForbidden, "M_FORBIDDEN")
	}
	streamID := trimString(frame["stream_id"])
	if streamID == "" {
		return realtimeWSCommandError(trimString(frame["id"]), http.StatusBadRequest, "stream_id is required")
	}
	s.mu.Lock()
	agentRoomID := strings.TrimSpace(s.agentRoomID)
	agentMXID := s.agentMXIDLocked()
	s.mu.Unlock()
	roomID := fallbackString(trimString(frame["room_id"]), agentRoomID)
	if agentRoomID == "" || roomID != agentRoomID {
		return realtimeWSCommandError(trimString(frame["id"]), http.StatusForbidden, "agent stream room is forbidden")
	}
	out := map[string]any{
		"type":        "server.agent_stream",
		"room_id":     roomID,
		"stream_id":   streamID,
		"seq":         int64Param(frame["seq"]),
		"delta":       trimString(frame["delta"]),
		"body":        trimString(frame["body"]),
		"final_body":  trimString(frame["final_body"]),
		"done":        boolParam(frame["done"]) || boolParam(frame["complete"]),
		"replace":     boolParam(frame["replace"]),
		"sender_mxid": fallbackString(strings.TrimSpace(record.UserID), agentMXID),
		"created_at":  time.Now().UTC().Format(time.RFC3339Nano),
	}
	s.broadcastRealtimeWSAgentStream(out)
	return nil
}

func (s *Service) registerRealtimeWSSubscriber(conn *realtimeWSConnection) {
	if s == nil || conn == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.realtimeWSSubscribers == nil {
		s.realtimeWSSubscribers = map[string]realtimeWSSubscriber{}
	}
	s.realtimeWSSubscribers[conn.sessionID] = realtimeWSSubscriber{
		Role:   conn.record.Role,
		Frames: conn.outbound,
	}
}

func (s *Service) unregisterRealtimeWSSubscriber(sessionID string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.realtimeWSSubscribers, strings.TrimSpace(sessionID))
}

func (s *Service) broadcastRealtimeWSAgentStream(frame map[string]any) {
	if s == nil || frame == nil {
		return
	}
	s.mu.Lock()
	subscribers := make([]chan<- map[string]any, 0, len(s.realtimeWSSubscribers))
	for _, subscriber := range s.realtimeWSSubscribers {
		if subscriber.Role == "owner" {
			subscribers = append(subscribers, subscriber.Frames)
		}
	}
	s.mu.Unlock()
	for _, ch := range subscribers {
		select {
		case ch <- frame:
		default:
		}
	}
}

func (s *Service) createRealtimeWSTicketForToken(token string) (map[string]any, *apiError) {
	token = strings.TrimSpace(token)
	s.mu.Lock()
	defer s.mu.Unlock()
	role := ""
	userID := ""
	switch {
	case token != "" && token == s.accessToken:
		role = "owner"
		userID = s.profile.UserID
	case token != "" && token == s.agentToken:
		role = "agent"
		userID = s.agentMXIDLocked()
	default:
		return nil, statusError(http.StatusUnauthorized, "M_UNKNOWN_TOKEN")
	}
	ticket := randomToken("ws_ticket")
	if s.realtimeWSTickets == nil {
		s.realtimeWSTickets = map[string]realtimeWSTicket{}
	}
	s.realtimeWSTickets[ticket] = realtimeWSTicket{
		Role:      role,
		UserID:    userID,
		ExpiresAt: time.Now().UTC().Add(realtimeWSTicketTTL),
	}
	return map[string]any{
		"ticket":        ticket,
		"expires_in_ms": int64(realtimeWSTicketTTL / time.Millisecond),
	}, nil
}

func (s *Service) consumeRealtimeWSTicket(ticket string) error {
	_, err := s.consumeRealtimeWSTicketRecord(ticket)
	return err
}

func (s *Service) consumeRealtimeWSTicketRecord(ticket string) (realtimeWSTicket, error) {
	ticket = strings.TrimSpace(ticket)
	if ticket == "" {
		return realtimeWSTicket{}, errors.New("ticket is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.realtimeWSTickets[ticket]
	if !ok {
		return realtimeWSTicket{}, errors.New("ticket invalid")
	}
	delete(s.realtimeWSTickets, ticket)
	if time.Now().UTC().After(record.ExpiresAt) {
		return realtimeWSTicket{}, errors.New("ticket expired")
	}
	return record, nil
}

func (s *Service) upsertRealtimeWSSession(sessionID string, state realtime.SessionState) {
	s.realtimeSessions.Upsert(sessionID, state)
}

func (s *Service) updateRealtimeWSSession(sessionID string, update func(*realtime.SessionState)) {
	s.realtimeSessions.Update(sessionID, update)
}

func (s *Service) touchRealtimeWSSession(sessionID string) {
	s.updateRealtimeWSSession(sessionID, func(_ *realtime.SessionState) {})
}

func (s *Service) removeRealtimeWSSession(sessionID string) {
	s.realtimeSessions.Remove(sessionID)
}

func (s *Service) shouldSuppressPushForRoom(roomID string) bool {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return false
	}
	s.mu.Lock()
	userID := s.profile.UserID
	s.mu.Unlock()
	return s.realtimeSessions.ShouldSuppressPush(userID, roomID, time.Now().UTC())
}
