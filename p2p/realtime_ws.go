package p2p

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/realtime"
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

		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			service.readRealtimeWSFrames(ctx, conn, sessionID)
		}()

		service.streamRealtimeWSEvents(ctx, conn, record.Role, since, readDone)
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

func (s *Service) readRealtimeWSFrames(ctx context.Context, conn *websocket.Conn, sessionID string) {
	for {
		var frame map[string]any
		if err := wsjson.Read(ctx, conn, &frame); err != nil {
			return
		}
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
		default:
			s.touchRealtimeWSSession(sessionID)
		}
	}
}

func (s *Service) streamRealtimeWSEvents(ctx context.Context, conn *websocket.Conn, role string, since int64, readDone <-chan struct{}) {
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
