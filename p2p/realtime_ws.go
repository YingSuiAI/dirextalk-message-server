package p2p

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/realtime"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/nativeagent"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/serviceapi"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const (
	realtimeWSTicketAction = serviceapi.RealtimeWSTicketAction
	realtimeWSTicketTTL    = 120 * time.Second
	realtimeWSBatchLimit   = 100
)

type realtimeWSTicket struct {
	Role      string
	UserID    string
	ExpiresAt time.Time
}

type realtimeWSConnection struct {
	sessionID     string
	record        realtimeWSTicket
	outbound      chan map[string]any
	streamMu      sync.Mutex
	streamCancels map[string]context.CancelFunc
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

func (c *realtimeWSConnection) sendBlocking(ctx context.Context, frame map[string]any) error {
	if c == nil || frame == nil {
		return nil
	}
	select {
	case c.outbound <- frame:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *realtimeWSConnection) startStream(id string, cancel context.CancelFunc) bool {
	id = strings.TrimSpace(id)
	if c == nil || id == "" || cancel == nil {
		return false
	}
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	if c.streamCancels == nil {
		c.streamCancels = map[string]context.CancelFunc{}
	}
	if _, exists := c.streamCancels[id]; exists {
		return false
	}
	c.streamCancels[id] = cancel
	return true
}

func (c *realtimeWSConnection) finishStream(id string) {
	if c == nil {
		return
	}
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	delete(c.streamCancels, strings.TrimSpace(id))
}

func (c *realtimeWSConnection) cancelStream(id string) bool {
	if c == nil {
		return false
	}
	c.streamMu.Lock()
	cancel, ok := c.streamCancels[strings.TrimSpace(id)]
	if ok {
		delete(c.streamCancels, strings.TrimSpace(id))
	}
	c.streamMu.Unlock()
	if ok {
		cancel()
	}
	return ok
}

func (c *realtimeWSConnection) cancelAllStreams() {
	if c == nil {
		return
	}
	c.streamMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(c.streamCancels))
	for _, cancel := range c.streamCancels {
		cancels = append(cancels, cancel)
	}
	c.streamCancels = map[string]context.CancelFunc{}
	c.streamMu.Unlock()
	for _, cancel := range cancels {
		cancel()
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
		if _, err := service.lookupRealtimeWSTicketRecord(ticket); err != nil {
			writeError(w, statusError(http.StatusUnauthorized, "M_UNKNOWN_TOKEN"))
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			return
		}
		record, err := service.consumeRealtimeWSTicketRecord(ticket)
		if err != nil {
			_ = conn.Close(websocket.StatusPolicyViolation, "M_UNKNOWN_TOKEN")
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
		defer wsConn.cancelAllStreams()

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
				foreground := boolParam(frame["foreground"])
				hidden := boolParam(frame["hidden"])
				state.Foreground = foreground
				state.Hidden = hidden
				state.AppState = trimString(frame["state"])
				updateRealtimeWSSessionFlags(state, frame, map[string]bool{
					"foreground": foreground,
					"background": !foreground,
					"hidden":     hidden,
				})
			})
		case "client.focus":
			s.updateRealtimeWSSession(sessionID, func(state *realtime.SessionState) {
				roomID := trimString(frame["room_id"])
				state.FocusedRoomID = roomID
				updateRealtimeWSSessionFlags(state, frame, map[string]bool{
					"focused": roomID != "",
				})
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
			wsConn.send(realtimeWSResponseError(trimString(frame["id"]), trimString(frame["action"]), http.StatusBadRequest, "unsupported frame type"))
		case "client.plugin_stream":
			s.startRealtimeWSPluginStream(ctx, wsConn, frame)
		case "client.plugin_stream.cancel":
			s.cancelRealtimeWSPluginStream(wsConn, frame)
		case "client.native_agent_stream":
			s.startRealtimeWSNativeAgentStream(ctx, wsConn, frame)
		case "client.native_agent_stream.cancel":
			s.cancelRealtimeWSNativeAgentStream(wsConn, frame)
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
	return true
}

func (s *Service) startRealtimeWSPluginStream(ctx context.Context, wsConn *realtimeWSConnection, frame map[string]any) {
	id := trimString(frame["id"])
	action := trimString(frame["action"])
	if id == "" {
		wsConn.send(realtimeWSPluginStreamError(id, action, http.StatusBadRequest, "id is required"))
		return
	}
	if action == "" {
		wsConn.send(realtimeWSPluginStreamError(id, action, http.StatusBadRequest, "action is required"))
		return
	}
	if wsConn.record.Role != "owner" {
		wsConn.send(realtimeWSPluginStreamError(id, action, http.StatusForbidden, "M_FORBIDDEN"))
		return
	}
	params := map[string]any{
		"plugin_id": frame["plugin_id"],
		"action":    action,
	}
	if rawParams, ok := frame["params"].(map[string]any); ok {
		params["params"] = rawParams
	} else if frame["params"] != nil {
		wsConn.send(realtimeWSPluginStreamError(id, action, http.StatusBadRequest, "params must be an object"))
		return
	}
	req, clientAction, apiErr := s.pluginInvokeRequest(ctx, params, true)
	if apiErr != nil {
		wsConn.send(realtimeWSPluginStreamError(id, action, apiErr.Status, apiErr.Error))
		return
	}
	streamCtx, cancel := context.WithCancel(ctx)
	if !wsConn.startStream(id, cancel) {
		cancel()
		wsConn.send(realtimeWSPluginStreamError(id, action, http.StatusConflict, "stream id is already active"))
		return
	}
	go func() {
		defer wsConn.finishStream(id)
		doneSent := false
		err := s.pluginRunner.StreamPlugin(streamCtx, req, func(event PluginStreamEvent) error {
			eventName := strings.TrimSpace(event.Event)
			if eventName == "" {
				eventName = "message"
			}
			if eventName == "done" {
				doneSent = true
			}
			data := event.Data
			if data == nil {
				data = map[string]any{}
			}
			return wsConn.sendBlocking(streamCtx, map[string]any{
				"type":      "server.plugin_stream.event",
				"id":        id,
				"plugin_id": req.PluginID,
				"action":    clientAction,
				"event":     eventName,
				"data":      data,
			})
		})
		if err != nil {
			if streamCtx.Err() != nil {
				return
			}
			_ = wsConn.sendBlocking(ctx, realtimeWSPluginStreamError(id, clientAction, http.StatusBadGateway, err.Error()))
			return
		}
		if !doneSent {
			_ = wsConn.sendBlocking(ctx, map[string]any{
				"type":      "server.plugin_stream.event",
				"id":        id,
				"plugin_id": req.PluginID,
				"action":    clientAction,
				"event":     "done",
				"data":      map[string]any{},
			})
		}
	}()
}

func (s *Service) cancelRealtimeWSPluginStream(wsConn *realtimeWSConnection, frame map[string]any) {
	id := trimString(frame["id"])
	if id == "" {
		wsConn.send(realtimeWSPluginStreamError(id, "", http.StatusBadRequest, "id is required"))
		return
	}
	if !wsConn.cancelStream(id) {
		wsConn.send(realtimeWSPluginStreamError(id, "", http.StatusNotFound, "stream is not active"))
		return
	}
	wsConn.send(map[string]any{
		"type": "server.plugin_stream.cancelled",
		"id":   id,
		"ok":   true,
	})
}

func realtimeWSPluginStreamError(id, action string, status int, message string) map[string]any {
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	if strings.TrimSpace(message) == "" {
		message = "M_UNKNOWN"
	}
	return map[string]any{
		"type":   "server.plugin_stream.error",
		"id":     id,
		"action": action,
		"ok":     false,
		"status": status,
		"error":  message,
	}
}

func (s *Service) startRealtimeWSNativeAgentStream(ctx context.Context, wsConn *realtimeWSConnection, frame map[string]any) {
	id := trimString(frame["id"])
	action := trimString(frame["action"])
	if id == "" {
		wsConn.send(realtimeWSNativeAgentStreamError(id, action, http.StatusBadRequest, "id is required"))
		return
	}
	if action == "" {
		wsConn.send(realtimeWSNativeAgentStreamError(id, action, http.StatusBadRequest, "action is required"))
		return
	}
	if wsConn.record.Role != "owner" {
		wsConn.send(realtimeWSNativeAgentStreamError(id, action, http.StatusForbidden, "M_FORBIDDEN"))
		return
	}
	params := map[string]any{}
	if rawParams, ok := frame["params"].(map[string]any); ok {
		params = cloneAnyMap(rawParams)
	} else if frame["params"] != nil {
		wsConn.send(realtimeWSNativeAgentStreamError(id, action, http.StatusBadRequest, "params must be an object"))
		return
	}
	runnerAction := action
	if !strings.HasSuffix(runnerAction, ".stream") {
		runnerAction += ".stream"
	}
	spec, ok := serviceapi.ActionSpecFor(runnerAction)
	if !ok || spec.Transport != serviceapi.ActionTransportWSStreamOnly || !strings.HasPrefix(runnerAction, "agent.") {
		wsConn.send(realtimeWSNativeAgentStreamError(id, action, http.StatusBadRequest, "action is not a native agent stream action"))
		return
	}
	if s.nativeAgentRunner == nil {
		wsConn.send(realtimeWSNativeAgentStreamError(id, action, http.StatusBadGateway, "native agent runtime is not configured"))
		return
	}
	streamCtx, cancel := context.WithCancel(ctx)
	if !wsConn.startStream(id, cancel) {
		cancel()
		wsConn.send(realtimeWSNativeAgentStreamError(id, action, http.StatusConflict, "stream id is already active"))
		return
	}
	go func() {
		defer wsConn.finishStream(id)
		doneSent := false
		err := s.nativeAgentRunner.Stream(streamCtx, runnerAction, params, func(event nativeagent.Event) error {
			eventName := strings.TrimSpace(event.Event)
			if eventName == "" {
				eventName = "message"
			}
			if eventName == "done" {
				doneSent = true
			}
			data := event.Data
			if data == nil {
				data = map[string]any{}
			}
			return wsConn.sendBlocking(streamCtx, map[string]any{
				"type":   "server.native_agent_stream.event",
				"id":     id,
				"action": strings.TrimSuffix(action, ".stream"),
				"event":  eventName,
				"data":   data,
			})
		})
		if err != nil {
			if streamCtx.Err() != nil {
				return
			}
			_ = wsConn.sendBlocking(ctx, realtimeWSNativeAgentStreamError(id, action, http.StatusBadGateway, err.Error()))
			return
		}
		if !doneSent {
			_ = wsConn.sendBlocking(ctx, map[string]any{
				"type":   "server.native_agent_stream.event",
				"id":     id,
				"action": strings.TrimSuffix(action, ".stream"),
				"event":  "done",
				"data":   map[string]any{},
			})
		}
	}()
}

func (s *Service) cancelRealtimeWSNativeAgentStream(wsConn *realtimeWSConnection, frame map[string]any) {
	id := trimString(frame["id"])
	if id == "" {
		wsConn.send(realtimeWSNativeAgentStreamError(id, "", http.StatusBadRequest, "id is required"))
		return
	}
	if !wsConn.cancelStream(id) {
		wsConn.send(realtimeWSNativeAgentStreamError(id, "", http.StatusNotFound, "stream is not active"))
		return
	}
	wsConn.send(map[string]any{
		"type": "server.native_agent_stream.cancelled",
		"id":   id,
		"ok":   true,
	})
}

func realtimeWSNativeAgentStreamError(id, action string, status int, message string) map[string]any {
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	if strings.TrimSpace(message) == "" {
		message = "M_UNKNOWN"
	}
	return map[string]any{
		"type":   "server.native_agent_stream.error",
		"id":     id,
		"action": action,
		"ok":     false,
		"status": status,
		"error":  message,
	}
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
	if message := realtimeWSClientRequestBlockedMessage(action); message != "" {
		return realtimeWSResponseError(id, action, http.StatusBadRequest, message)
	}
	if record.Role != "owner" {
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

func realtimeWSHTTPOnlyAction(action string) bool {
	return serviceapi.HTTPOnlyAction(action)
}

func realtimeWSClientRequestBlockedMessage(action string) string {
	if serviceapi.HTTPOnlyAction(action) {
		return "action requires http"
	}
	if serviceapi.WSStreamOnlyAction(action) {
		return "action requires websocket"
	}
	return ""
}

func updateRealtimeWSSessionFlags(state *realtime.SessionState, frame map[string]any, defaults map[string]bool) {
	if state == nil {
		return
	}
	flags := make(map[string]bool, len(state.Flags)+len(defaults)+4)
	for key, value := range state.Flags {
		key = strings.TrimSpace(key)
		if key != "" {
			flags[key] = value
		}
	}
	for key, value := range defaults {
		key = strings.TrimSpace(key)
		if key != "" {
			flags[key] = value
		}
	}
	for key, value := range boolMapParam(frame["flags"]) {
		flags[key] = value
	}
	if len(flags) == 0 {
		state.Flags = nil
		return
	}
	state.Flags = flags
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

func (s *Service) lookupRealtimeWSTicketRecord(ticket string) (realtimeWSTicket, error) {
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
