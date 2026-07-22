package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/nativeagent"
	"github.com/sirupsen/logrus"
)

const voiceSessionTTL = time.Hour

var volcRTCIDPattern = regexp.MustCompile(`^[A-Za-z0-9@._-]{1,128}$`)

type voiceConfig struct {
	AppID                  string
	AppKey                 string
	VoiceChatAppID         string
	AIUserID               string
	WebhookSecret          string
	WebhookURL             string
	OpenAPIAccessKeyID     string
	OpenAPISecretAccessKey string
	OpenAPIHost            string
	OpenAPIRegion          string
	OpenAPIVersion         string
	VoiceChatConfigJSON    string
}

func voiceConfigFromEnv() voiceConfig {
	return voiceConfig{
		AppID:                  strings.TrimSpace(os.Getenv("VOLC_RTC_APP_ID")),
		AppKey:                 strings.TrimSpace(os.Getenv("VOLC_RTC_APP_KEY")),
		VoiceChatAppID:         strings.TrimSpace(os.Getenv("VOLC_VOICE_CHAT_APP_ID")),
		AIUserID:               strings.TrimSpace(os.Getenv("VOLC_RTC_AI_APP_ID")),
		WebhookSecret:          strings.TrimSpace(os.Getenv("VOLC_VOICE_WEBHOOK_SECRET")),
		WebhookURL:             strings.TrimSpace(os.Getenv("VOLC_VOICE_WEBHOOK_URL")),
		OpenAPIAccessKeyID:     strings.TrimSpace(os.Getenv("VOLC_ACCESS_KEY_ID")),
		OpenAPISecretAccessKey: strings.TrimSpace(os.Getenv("VOLC_SECRET_ACCESS_KEY")),
		OpenAPIHost:            strings.TrimSpace(os.Getenv("VOLC_RTC_OPENAPI_HOST")),
		OpenAPIRegion:          strings.TrimSpace(os.Getenv("VOLC_RTC_OPENAPI_REGION")),
		OpenAPIVersion:         strings.TrimSpace(os.Getenv("VOLC_VOICE_CHAT_OPENAPI_VERSION")),
		VoiceChatConfigJSON:    strings.TrimSpace(os.Getenv("VOLC_VOICE_CHAT_CONFIG_JSON")),
	}
}

type voiceCoordinator struct {
	mu       sync.Mutex
	cfg      voiceConfig
	signer   voiceTokenSigner
	client   voiceChatClient
	sessions map[string]*voiceSession
	streams  map[string]map[chan nativeagent.Event]struct{}
}

type voiceSession struct {
	SessionID      string
	TaskID         string
	AppID          string
	VoiceChatAppID string
	RoomID         string
	UserID         string
	Token          string
	AIUserID       string
	ExpiresAt      time.Time
	Params         map[string]any
	Started        bool
	Ended          bool
	LastTranscript string
}

func newVoiceCoordinator(cfg voiceConfig) *voiceCoordinator {
	coordinator := &voiceCoordinator{
		cfg:      cfg,
		signer:   volcRTCTokenSigner{},
		sessions: map[string]*voiceSession{},
		streams:  map[string]map[chan nativeagent.Event]struct{}{},
	}
	if client := newVolcVoiceChatOpenAPIClient(cfg); client != nil {
		coordinator.client = client
	}
	return coordinator
}

func (m *Module) createVoiceSession(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if m == nil || m.voice == nil {
		return nil, actionbase.StatusError(http.StatusBadGateway, "native agent voice service is not configured")
	}
	return m.voice.create(ctx, params)
}

func (m *Module) startVoiceSession(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if m == nil || m.voice == nil {
		return nil, actionbase.StatusError(http.StatusBadGateway, "native agent voice service is not configured")
	}
	return m.voice.start(ctx, params)
}

func (m *Module) submitVoiceTranscript(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if m == nil || m.voice == nil {
		return nil, actionbase.StatusError(http.StatusBadGateway, "native agent voice service is not configured")
	}
	return m.submitVoiceTranscriptForSession(ctx, params)
}

func (m *Module) interruptVoiceSession(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if m == nil || m.voice == nil {
		return nil, actionbase.StatusError(http.StatusBadGateway, "native agent voice service is not configured")
	}
	return m.voice.interrupt(ctx, params)
}

func (m *Module) endVoiceSession(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if m == nil || m.voice == nil {
		return nil, actionbase.StatusError(http.StatusBadGateway, "native agent voice service is not configured")
	}
	return m.voice.end(ctx, params)
}

func (v *voiceCoordinator) create(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	source := actionbase.String(params["source"])
	if source == "" {
		source = "native_agent"
	}
	if source != "native_agent" {
		return nil, actionbase.BadRequest("source must be native_agent")
	}
	if strings.TrimSpace(v.cfg.AppID) == "" {
		return nil, actionbase.CodedError(http.StatusServiceUnavailable, "volc_rtc_not_configured", "VOLC_RTC_APP_ID is required")
	}
	if strings.TrimSpace(v.cfg.AppKey) == "" {
		return nil, actionbase.CodedError(http.StatusServiceUnavailable, "volc_rtc_app_key_not_configured", "VOLC_RTC_APP_KEY is required")
	}
	voiceChatAppID := fallback(v.cfg.VoiceChatAppID, v.cfg.AppID)
	if v.signer == nil {
		return nil, actionbase.CodedError(http.StatusServiceUnavailable, "volc_rtc_token_signer_not_configured", "Volc RTC token signer is not configured")
	}
	if v.client == nil && (strings.TrimSpace(v.cfg.OpenAPIAccessKeyID) == "" || strings.TrimSpace(v.cfg.OpenAPISecretAccessKey) == "") {
		return nil, actionbase.CodedError(http.StatusServiceUnavailable, "volc_openapi_not_configured", "VOLC_ACCESS_KEY_ID and VOLC_SECRET_ACCESS_KEY are required")
	}
	sessionID := "voice_" + randomHex(12)
	roomID := "dirextalk_voice_" + randomHex(12)
	userID := "owner_" + randomHex(12)
	aiUserID := fallback(v.cfg.AIUserID, "dirextalk_ai_"+randomHex(8))
	if !volcRTCIDPattern.MatchString(roomID) || !volcRTCIDPattern.MatchString(userID) || !volcRTCIDPattern.MatchString(aiUserID) {
		return nil, actionbase.StatusError(http.StatusInternalServerError, "generated RTC identity is invalid")
	}
	expiresAt := time.Now().UTC().Add(voiceSessionTTL)
	token, err := v.signer.SignRTC(v.cfg.AppID, v.cfg.AppKey, roomID, userID, expiresAt)
	if err != nil {
		return nil, actionbase.CodedError(http.StatusServiceUnavailable, "volc_rtc_token_sign_failed", err.Error())
	}
	session := &voiceSession{
		SessionID:      sessionID,
		TaskID:         sessionID,
		AppID:          v.cfg.AppID,
		VoiceChatAppID: voiceChatAppID,
		RoomID:         roomID,
		UserID:         userID,
		Token:          token,
		AIUserID:       aiUserID,
		ExpiresAt:      expiresAt,
		Params:         cloneMap(params),
	}
	v.mu.Lock()
	v.sessions[sessionID] = session
	v.mu.Unlock()
	v.emit(sessionID, nativeagent.Event{Event: "listening", Data: map[string]any{"status": "listening"}})
	return session.response(), nil
}

func (v *voiceCoordinator) start(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	sessionID := actionbase.String(params["session_id"])
	if sessionID == "" {
		return nil, actionbase.BadRequest("session_id is required")
	}
	v.mu.Lock()
	session, ok := v.sessions[sessionID]
	if !ok || session.Ended {
		v.mu.Unlock()
		return nil, actionbase.StatusError(http.StatusNotFound, "voice session not found")
	}
	if session.Started {
		v.mu.Unlock()
		return map[string]any{"ok": true, "session_id": sessionID, "started": true, "already_started": true}, nil
	}
	session.Started = true
	sessionCopy := *session
	sessionCopy.Params = cloneMap(session.Params)
	v.mu.Unlock()
	if v.client != nil {
		logrus.WithFields(logrus.Fields{
			"component":         "native_agent_voice",
			"action":            "StartVoiceChat",
			"session_id":        sessionCopy.SessionID,
			"task_id":           sessionCopy.TaskID,
			"rtc_app_id":        sessionCopy.AppID,
			"voice_chat_app_id": sessionCopy.VoiceChatAppID,
			"room_id":           sessionCopy.RoomID,
			"user_id":           sessionCopy.UserID,
			"ai_user_id":        sessionCopy.AIUserID,
		}).Info("starting Volc VoiceChat session")
		if err := v.client.StartVoiceChat(ctx, sessionCopy); err != nil {
			v.mu.Lock()
			if current := v.sessions[sessionID]; current != nil {
				current.Started = false
			}
			v.mu.Unlock()
			logrus.WithError(err).WithFields(logrus.Fields{
				"component":         "native_agent_voice",
				"action":            "StartVoiceChat",
				"session_id":        sessionCopy.SessionID,
				"task_id":           sessionCopy.TaskID,
				"rtc_app_id":        sessionCopy.AppID,
				"voice_chat_app_id": sessionCopy.VoiceChatAppID,
				"room_id":           sessionCopy.RoomID,
				"user_id":           sessionCopy.UserID,
				"ai_user_id":        sessionCopy.AIUserID,
			}).Warn("Volc VoiceChat session failed to start")
			return nil, actionbase.CodedError(http.StatusBadGateway, "volc_voice_chat_start_failed", err.Error())
		}
		logrus.WithFields(logrus.Fields{
			"component":         "native_agent_voice",
			"action":            "StartVoiceChat",
			"session_id":        sessionCopy.SessionID,
			"task_id":           sessionCopy.TaskID,
			"rtc_app_id":        sessionCopy.AppID,
			"voice_chat_app_id": sessionCopy.VoiceChatAppID,
			"room_id":           sessionCopy.RoomID,
		}).Info("Volc VoiceChat session started")
	}
	v.emit(sessionID, nativeagent.Event{Event: "listening", Data: map[string]any{"status": "listening"}})
	return map[string]any{"ok": true, "session_id": sessionID, "started": true}, nil
}

func (m *Module) HandleVoiceWebhook(ctx context.Context, token string, params map[string]any) (map[string]any, *actionbase.Error) {
	if m == nil || m.voice == nil {
		return nil, actionbase.StatusError(http.StatusBadGateway, "native agent voice service is not configured")
	}
	if strings.TrimSpace(m.voice.cfg.WebhookSecret) == "" {
		return nil, actionbase.CodedError(http.StatusServiceUnavailable, "volc_voice_webhook_not_configured", "VOLC_VOICE_WEBHOOK_SECRET is required")
	}
	if token == "" || token != m.voice.cfg.WebhookSecret {
		return nil, actionbase.StatusError(http.StatusUnauthorized, "M_UNKNOWN_TOKEN")
	}
	sessionID := actionbase.String(params["session_id"])
	if sessionID == "" {
		return nil, actionbase.BadRequest("session_id is required")
	}
	_, ok := m.voice.session(sessionID)
	if !ok {
		return nil, actionbase.StatusError(http.StatusNotFound, "voice session not found")
	}
	status := actionbase.String(params["status"])
	if status == "" {
		status = actionbase.String(params["event"])
	}
	data := map[string]any{}
	for _, key := range []string{"status", "transcript_delta", "transcript_final", "answer_delta", "summary", "references", "volume", "error"} {
		if value, exists := params[key]; exists {
			data[key] = value
		}
	}
	if status != "" {
		data["status"] = status
	}
	if len(data) > 0 {
		m.voice.emit(sessionID, nativeagent.Event{Event: fallback(status, "message"), Data: data})
	}
	transcript := strings.TrimSpace(actionbase.String(params["transcript_final"]))
	if transcript != "" {
		if session, accepted := m.voice.acceptTranscript(sessionID, transcript); accepted {
			go m.runVoiceAgent(context.Background(), session, transcript)
		}
	}
	return map[string]any{"ok": true, "session_id": sessionID}, nil
}

func (m *Module) submitVoiceTranscriptForSession(ctx context.Context, params map[string]any) (map[string]any, *actionbase.Error) {
	sessionID := actionbase.String(params["session_id"])
	if sessionID == "" {
		return nil, actionbase.BadRequest("session_id is required")
	}
	_, ok := m.voice.session(sessionID)
	if !ok {
		return nil, actionbase.StatusError(http.StatusNotFound, "voice session not found")
	}
	delta := strings.TrimSpace(actionbase.String(params["transcript_delta"]))
	final := strings.TrimSpace(actionbase.String(params["transcript_final"]))
	if delta == "" && final == "" {
		return nil, actionbase.BadRequest("transcript_delta or transcript_final is required")
	}
	data := map[string]any{"status": "transcribing"}
	if delta != "" {
		data["transcript_delta"] = delta
	}
	if final != "" {
		data["transcript_final"] = final
	}
	m.voice.emit(sessionID, nativeagent.Event{Event: "transcribing", Data: data})
	if final != "" {
		if session, accepted := m.voice.acceptTranscript(sessionID, final); accepted {
			go m.runVoiceAgent(context.Background(), session, final)
		}
	}
	_ = ctx
	return map[string]any{"ok": true, "session_id": sessionID, "accepted": true}, nil
}

func (m *Module) runVoiceAgent(ctx context.Context, session voiceSession, transcript string) {
	if m == nil || m.runner == nil || m.voice == nil {
		return
	}
	m.voice.emit(session.SessionID, nativeagent.Event{Event: "thinking", Data: map[string]any{"status": "thinking", "transcript_final": transcript}})
	params := cloneMap(session.Params)
	params["prompt"] = transcript
	delete(params, "source")
	err := m.runner.Stream(ctx, "agent.chat.stream", params, func(event nativeagent.Event) error {
		switch event.Event {
		case "delta":
			text := actionbase.String(event.Data["text"])
			if text == "" {
				return nil
			}
			m.voice.emit(session.SessionID, nativeagent.Event{Event: "answer", Data: map[string]any{"status": "speaking", "answer_delta": text}})
		case "done":
			data := map[string]any{"status": "done"}
			if text := actionbase.String(event.Data["text"]); text != "" {
				data["summary"] = text
			}
			if refs, ok := event.Data["references"]; ok {
				data["references"] = refs
			}
			m.voice.emit(session.SessionID, nativeagent.Event{Event: "done", Data: data})
		case "error":
			data := map[string]any{"status": "error", "error": actionbase.String(event.Data["error"])}
			m.voice.emit(session.SessionID, nativeagent.Event{Event: "error", Data: data})
		}
		return nil
	})
	if err != nil {
		m.voice.emit(session.SessionID, nativeagent.Event{Event: "error", Data: map[string]any{"status": "error", "error": err.Error()}})
	}
}

func (v *voiceCoordinator) interrupt(_ context.Context, params map[string]any) (any, *actionbase.Error) {
	sessionID := actionbase.String(params["session_id"])
	if sessionID == "" {
		return nil, actionbase.BadRequest("session_id is required")
	}
	if !v.exists(sessionID) {
		return nil, actionbase.StatusError(http.StatusNotFound, "voice session not found")
	}
	v.emit(sessionID, nativeagent.Event{Event: "listening", Data: map[string]any{"status": "listening"}})
	return map[string]any{"ok": true, "session_id": sessionID, "interrupted": true}, nil
}

func (v *voiceCoordinator) end(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	sessionID := actionbase.String(params["session_id"])
	if sessionID == "" {
		return nil, actionbase.BadRequest("session_id is required")
	}
	v.mu.Lock()
	session, ok := v.sessions[sessionID]
	if ok {
		session.Ended = true
		delete(v.sessions, sessionID)
	}
	streams := v.streams[sessionID]
	delete(v.streams, sessionID)
	v.mu.Unlock()
	for ch := range streams {
		ch <- nativeagent.Event{Event: "done", Data: map[string]any{"status": "done"}}
		close(ch)
	}
	if ok && v.client != nil {
		logrus.WithFields(logrus.Fields{
			"component":         "native_agent_voice",
			"action":            "StopVoiceChat",
			"session_id":        session.SessionID,
			"task_id":           session.TaskID,
			"rtc_app_id":        session.AppID,
			"voice_chat_app_id": session.VoiceChatAppID,
			"room_id":           session.RoomID,
		}).Info("stopping Volc VoiceChat session")
		if err := v.client.StopVoiceChat(ctx, *session); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"component":         "native_agent_voice",
				"action":            "StopVoiceChat",
				"session_id":        session.SessionID,
				"task_id":           session.TaskID,
				"rtc_app_id":        session.AppID,
				"voice_chat_app_id": session.VoiceChatAppID,
				"room_id":           session.RoomID,
			}).Warn("Volc VoiceChat session failed to stop")
		}
	}
	return map[string]any{"ok": true, "session_id": sessionID, "ended": ok}, nil
}

func (v *voiceCoordinator) stream(ctx context.Context, params map[string]any, emit func(nativeagent.Event) error) error {
	sessionID := actionbase.String(params["session_id"])
	if sessionID == "" {
		return emit(nativeagent.Event{Event: "error", Data: map[string]any{"status": "error", "error": "session_id is required"}})
	}
	ch := make(chan nativeagent.Event, 16)
	v.mu.Lock()
	session, ok := v.sessions[sessionID]
	if ok && !session.Ended {
		if v.streams[sessionID] == nil {
			v.streams[sessionID] = map[chan nativeagent.Event]struct{}{}
		}
		v.streams[sessionID][ch] = struct{}{}
	}
	v.mu.Unlock()
	if !ok || session.Ended {
		return emit(nativeagent.Event{Event: "error", Data: map[string]any{"status": "error", "error": "voice session not found"}})
	}
	defer func() {
		v.mu.Lock()
		if streams := v.streams[sessionID]; streams != nil {
			delete(streams, ch)
			if len(streams) == 0 {
				delete(v.streams, sessionID)
			}
		}
		v.mu.Unlock()
	}()
	if err := emit(nativeagent.Event{Event: "listening", Data: map[string]any{"status": "listening"}}); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-ch:
			if !ok {
				return nil
			}
			if err := emit(event); err != nil {
				return err
			}
			if event.Event == "done" || event.Event == "error" {
				return nil
			}
		}
	}
}

func (v *voiceCoordinator) exists(sessionID string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	session, ok := v.sessions[sessionID]
	return ok && !session.Ended
}

func (v *voiceCoordinator) session(sessionID string) (voiceSession, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	session, ok := v.sessions[sessionID]
	if !ok || session.Ended {
		return voiceSession{}, false
	}
	copy := *session
	copy.Params = cloneMap(session.Params)
	return copy, true
}

func (v *voiceCoordinator) acceptTranscript(sessionID, transcript string) (voiceSession, bool) {
	transcript = strings.TrimSpace(transcript)
	if transcript == "" {
		return voiceSession{}, false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	session, ok := v.sessions[sessionID]
	if !ok || session.Ended {
		return voiceSession{}, false
	}
	if session.LastTranscript == transcript {
		return voiceSession{}, false
	}
	session.LastTranscript = transcript
	copy := *session
	copy.Params = cloneMap(session.Params)
	return copy, true
}

func (v *voiceCoordinator) emit(sessionID string, event nativeagent.Event) {
	v.mu.Lock()
	streams := make([]chan nativeagent.Event, 0, len(v.streams[sessionID]))
	for ch := range v.streams[sessionID] {
		streams = append(streams, ch)
	}
	v.mu.Unlock()
	for _, ch := range streams {
		select {
		case ch <- event:
		default:
		}
	}
}

func (s voiceSession) response() map[string]any {
	return map[string]any{
		"session_id": s.SessionID,
		"task_id":    s.TaskID,
		"app_id":     s.AppID,
		"room_id":    s.RoomID,
		"user_id":    s.UserID,
		"token":      s.Token,
		"expires_at": s.ExpiresAt.Unix(),
		"ai_user_id": s.AIUserID,
	}
}

func randomHex(bytes int) string {
	buf := make([]byte, bytes)
	if _, err := rand.Read(buf); err != nil {
		return strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000000"), ".", "")
	}
	return hex.EncodeToString(buf)
}

func fallback(value, defaultValue string) string {
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return defaultValue
}
