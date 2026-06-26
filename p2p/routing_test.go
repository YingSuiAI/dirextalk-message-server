package p2p

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

func TestCommandUsesBodyActionAndBearerAuth(t *testing.T) {
	service := NewService(Config{
		ServerName: "example.com",
	})
	router := newP2PTestRouter(service)

	reqBody := map[string]any{
		"action": "profile.get",
		"params": map[string]any{},
	}
	req := jsonRequest(t, "/_p2p/command", reqBody)
	req.Header.Set("Authorization", "Bearer "+service.AccessToken())
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["user_id"] == "" {
		t.Fatalf("expected owner profile response, got %#v", got)
	}
}

func TestAgentMatrixSessionCreateRejectsAgentToken(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	issuer := &recordingMatrixSessionIssuer{}
	service.SetMatrixSessionIssuer(issuer)
	router := newP2PTestRouter(service)

	req := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "agent.matrix_session.create",
		"params": map[string]any{"device_id": "DIREXIO_CLI"},
	})
	req.Header.Set("Authorization", "Bearer "+service.AgentToken())
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestProtectedQueryRejectsMissingBearer(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	req := jsonRequest(t, "/_p2p/query", map[string]any{
		"action": "profile.get",
	})
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestEventsEndpointKeepsConnectionOpenUntilClientDisconnect(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	rec, cancel, done := startEventStreamTest(t, router, service, "/_p2p/events?since=0")

	select {
	case <-done:
		t.Fatalf("events endpoint returned before client disconnect; body=%s", rec.BodyString())
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	waitForEventStreamDone(t, done)
	if rec.Code() != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code(), rec.BodyString())
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("expected SSE content type, got %q", got)
	}
}

func TestEventsEndpointStreamsAppendedEvents(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	rec, cancel, done := startEventStreamTest(t, router, service, "/_p2p/events?since=0")
	defer cancel()

	if err := service.appendP2PEvent(context.Background(), p2pEvent{
		Type:    "test.event",
		RoomID:  "!room:example.com",
		EventID: "$event",
		Payload: map[string]any{"ok": true},
	}); err != nil {
		t.Fatal(err)
	}

	waitForEventStreamBody(t, rec, "event: test.event")
	cancel()
	waitForEventStreamDone(t, done)
}

func TestBootstrapAndAuthAreBodyActions(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	bootstrap := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "portal.bootstrap",
		"params": map[string]any{"password": service.password},
	})
	bootstrapRec := httptest.NewRecorder()
	router.ServeHTTP(bootstrapRec, bootstrap)
	if bootstrapRec.Code != http.StatusOK {
		t.Fatalf("bootstrap expected 200, got %d body=%s", bootstrapRec.Code, bootstrapRec.Body.String())
	}

	auth := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "portal.auth",
		"params": map[string]any{"password": service.password},
	})
	authRec := httptest.NewRecorder()
	router.ServeHTTP(authRec, auth)
	if authRec.Code != http.StatusOK {
		t.Fatalf("auth expected 200, got %d body=%s", authRec.Code, authRec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(authRec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["access_token"] == "" {
		t.Fatalf("expected access token, got %#v", got)
	}
}

func TestAutoHomeserverResponseUsesRequestHost(t *testing.T) {
	service := NewService(Config{
		ServerName: "example.com",
		Homeserver: "http://auto",
	})
	router := newP2PTestRouter(service)

	req := jsonRequest(t, "/_p2p/query", map[string]any{
		"action": "portal.auth",
		"params": map[string]any{"password": service.password},
	})
	req.Host = "10.0.2.2:18008"
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("auth expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["homeserver"] != "http://10.0.2.2:18008" {
		t.Fatalf("expected request host homeserver, got %#v", got["homeserver"])
	}
}

func TestAutoHomeserverResponseUsesForwardedHost(t *testing.T) {
	service := NewService(Config{
		ServerName: "example.com",
		Homeserver: "https://auto",
	})
	router := newP2PTestRouter(service)

	req := jsonRequest(t, "/_p2p/query", map[string]any{
		"action": "portal.auth",
		"params": map[string]any{"password": service.password},
	})
	req.Host = "127.0.0.1:18008"
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Forwarded-Host", "portal.example.test")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("auth expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["homeserver"] != "https://portal.example.test" {
		t.Fatalf("expected forwarded homeserver, got %#v", got["homeserver"])
	}
}

func TestPortalOwnerWellKnownIsPublic(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := mux.NewRouter()
	Register(router.PathPrefix(PathPrefix).Subrouter(), service)
	RegisterWellKnown(router.PathPrefix("/.well-known/portal/").Subrouter(), service)

	mustRoute(t, router, service, "/_p2p/command", map[string]any{
		"action": "profile.update",
		"params": map[string]any{
			"display_name": "Alice",
			"avatar_url":   "mxc://example.com/avatar",
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/.well-known/portal/owner.json", nil)
	req.Header.Set("Origin", "http://127.0.0.1:3001")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "http://127.0.0.1:3001" {
		t.Fatalf("expected CORS origin echo, got %q", rec.Header().Get("Access-Control-Allow-Origin"))
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["matrix_user_id"] != "@owner:example.com" || got["display_name"] != "Alice" || got["avatar_url"] != "mxc://example.com/avatar" {
		t.Fatalf("unexpected owner well-known response %#v", got)
	}
}

func TestChannelMembersQueryFiltersStatusAndReturnsLegacyFields(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	mustRoute(t, router, service, "/_p2p/command", map[string]any{
		"action": "portal.bootstrap",
		"params": map[string]any{"password": service.password},
	})
	channelRaw := mustRoute(t, router, service, "/_p2p/command", map[string]any{
		"action": "channels.create",
		"params": map[string]any{
			"channel_id":  "approval",
			"room_id":     "!approval:example.com",
			"name":        "Approval",
			"join_policy": "approval",
		},
	})
	channelID := channelRaw["channel_id"].(string)
	roomID := channelRaw["room_id"].(string)
	mustRoute(t, router, service, "/_p2p/command", map[string]any{
		"action": "channels.public.join_request",
		"params": map[string]any{"channel_id": channelID, "room_id": roomID, "user_mxid": "@alice:example.com"},
	})
	mustRoute(t, router, service, "/_p2p/command", map[string]any{
		"action": "channels.public.join_request",
		"params": map[string]any{"channel_id": channelID, "room_id": roomID, "user_mxid": "@bob:example.com"},
	})
	mustRoute(t, router, service, "/_p2p/command", map[string]any{
		"action": "channels.join_request.approve",
		"params": map[string]any{"channel_id": channelID, "room_id": roomID, "user_mxid": "@alice:example.com"},
	})

	list := mustRoute(t, router, service, "/_p2p/query", map[string]any{
		"action": "channels.members",
		"params": map[string]any{"channel_id": channelID, "status": "pending"},
	})
	members := list["members"].([]any)
	if len(members) != 1 {
		t.Fatalf("expected one pending member, got %#v", list)
	}
	member := members[0].(map[string]any)
	if member["user_mxid"] != "@bob:example.com" || member["status"] != "pending" || member["user_id"] != "@bob:example.com" || member["membership"] != "pending" {
		t.Fatalf("expected legacy and unified member fields, got %#v", member)
	}
}

func TestChannelJoinRequestResolutionReturnsChannelForClientRefresh(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	mustRoute(t, router, service, "/_p2p/command", map[string]any{
		"action": "portal.bootstrap",
		"params": map[string]any{"password": service.password},
	})
	channelRaw := mustRoute(t, router, service, "/_p2p/command", map[string]any{
		"action": "channels.create",
		"params": map[string]any{
			"channel_id":  "approval",
			"room_id":     "!approval:example.com",
			"name":        "Approval",
			"join_policy": "approval",
		},
	})
	channelID := channelRaw["channel_id"].(string)
	roomID := channelRaw["room_id"].(string)
	mustRoute(t, router, service, "/_p2p/command", map[string]any{
		"action": "channels.public.join_request",
		"params": map[string]any{"channel_id": channelID, "room_id": roomID, "user_mxid": "@alice:example.com"},
	})

	approved := mustRoute(t, router, service, "/_p2p/command", map[string]any{
		"action": "channels.join_request.approve",
		"params": map[string]any{"channel_id": channelID, "room_id": roomID, "user_mxid": "@alice:example.com"},
	})
	channel := approved["channel"].(map[string]any)
	if approved["status"] != "approved" || channel["channel_id"] != channelID || channel["pending_join_count"] != float64(0) {
		t.Fatalf("expected approved status and refreshed channel, got %#v", approved)
	}
}

func TestPublicChannelActionsDoNotRequireBearer(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	channelRaw := mustRoute(t, router, service, "/_p2p/command", map[string]any{
		"action": "channels.create",
		"params": map[string]any{
			"channel_id":  "public-news",
			"room_id":     "!public-news:example.com",
			"name":        "Public News",
			"visibility":  "public",
			"join_policy": "approval",
		},
	})
	roomID := channelRaw["room_id"].(string)

	detailReq := jsonRequest(t, "/_p2p/query", map[string]any{
		"action": "channels.public.get",
		"params": map[string]any{"room_id": roomID},
	})
	detailRec := httptest.NewRecorder()
	router.ServeHTTP(detailRec, detailReq)
	if detailRec.Code != http.StatusOK {
		t.Fatalf("expected public channel detail without bearer, got %d body=%s", detailRec.Code, detailRec.Body.String())
	}
	var detail map[string]any
	if err := json.Unmarshal(detailRec.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail["channel_id"] != "public-news" {
		t.Fatalf("expected public channel detail, got %#v", detail)
	}

	joinReq := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "channels.public.join_request",
		"params": map[string]any{"room_id": roomID, "user_mxid": "@guest:remote.example"},
	})
	joinRec := httptest.NewRecorder()
	router.ServeHTTP(joinRec, joinReq)
	if joinRec.Code != http.StatusOK {
		t.Fatalf("expected public join request without bearer, got %d body=%s", joinRec.Code, joinRec.Body.String())
	}
	var joined map[string]any
	if err := json.Unmarshal(joinRec.Body.Bytes(), &joined); err != nil {
		t.Fatal(err)
	}
	if joined["status"] != "pending" {
		t.Fatalf("expected pending join request, got %#v", joined)
	}

	userChannelsReq := jsonRequest(t, "/_p2p/query", map[string]any{
		"action": "users.public_channels",
		"params": map[string]any{"user_mxid": "@owner:example.com"},
	})
	userChannelsRec := httptest.NewRecorder()
	router.ServeHTTP(userChannelsRec, userChannelsReq)
	if userChannelsRec.Code != http.StatusOK {
		t.Fatalf("expected public user channels without bearer, got %d body=%s", userChannelsRec.Code, userChannelsRec.Body.String())
	}
	var userChannels map[string]any
	if err := json.Unmarshal(userChannelsRec.Body.Bytes(), &userChannels); err != nil {
		t.Fatal(err)
	}
	channels, ok := userChannels["channels"].([]any)
	if !ok || len(channels) != 1 || channels[0].(map[string]any)["room_id"] != roomID {
		t.Fatalf("expected owner public channel list, got %#v", userChannels)
	}
}

func TestAgentTokenCanOnlyCallMCPActions(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	session := mustRoute(t, router, service, "/_p2p/command", map[string]any{
		"action": "portal.auth",
		"params": map[string]any{"password": service.password},
	})
	agentToken := session["agent_token"].(string)

	mcpReq := jsonRequest(t, "/_p2p/query", map[string]any{
		"action": "mcp.rooms.search",
		"params": map[string]any{"q": "none"},
	})
	mcpReq.Header.Set("Authorization", "Bearer "+agentToken)
	mcpRec := httptest.NewRecorder()
	router.ServeHTTP(mcpRec, mcpReq)
	if mcpRec.Code != http.StatusOK {
		t.Fatalf("expected Agent token to call MCP action, got %d body=%s", mcpRec.Code, mcpRec.Body.String())
	}

	agentRequest := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "contacts.request",
		"params": map[string]any{"mxid": "@agent-ok:example.com"},
	})
	agentRequest.Header.Set("Authorization", "Bearer "+agentToken)
	agentRec := httptest.NewRecorder()
	router.ServeHTTP(agentRec, agentRequest)
	if agentRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected non-MCP Agent action to fail, got %d body=%s", agentRec.Code, agentRec.Body.String())
	}

	agentEventsRec, cancel, done := startEventStreamTestWithToken(t, router, "/_p2p/events?since=0", agentToken)
	cancel()
	waitForEventStreamDone(t, done)
	if agentEventsRec.Code() != http.StatusOK {
		t.Fatalf("expected Agent token to subscribe to events stream, got %d body=%s", agentEventsRec.Code(), agentEventsRec.BodyString())
	}
	if got := agentEventsRec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("expected Agent token events request to receive SSE content type, got %q", got)
	}

	adminRequest := mustRoute(t, router, service, "/_p2p/command", map[string]any{
		"action": "contacts.request",
		"params": map[string]any{"mxid": "@admin-still-ok:example.com"},
	})
	if adminRequest["room_id"] == "" {
		t.Fatalf("expected access token to call non-MCP action, got %#v", adminRequest)
	}

	removed := mustRouteError(t, router, service, "/_p2p/command", map[string]any{"action": "apis.list"})
	if removed.Status != http.StatusBadRequest {
		t.Fatalf("expected removed apis.list to be unknown, got %#v", removed)
	}
}

func TestAgentStatusConnectedReflectsAgentEventStream(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	status := mustRoute(t, router, service, "/_p2p/query", map[string]any{"action": "agent.status"})
	if status["connected"] != false || status["online"] != false {
		t.Fatalf("expected disconnected agent before SSE subscription, got %#v", status)
	}

	ownerRec, cancelOwner, ownerDone := startEventStreamTest(t, router, service, "/_p2p/events?since=0")
	defer cancelOwner()
	status = mustRoute(t, router, service, "/_p2p/query", map[string]any{"action": "agent.status"})
	if status["connected"] != false || status["online"] != false {
		t.Fatalf("expected owner event stream not to mark agent connected, got %#v body=%s", status, ownerRec.BodyString())
	}
	cancelOwner()
	waitForEventStreamDone(t, ownerDone)

	agentRec, cancelAgent, agentDone := startEventStreamTestWithToken(t, router, "/_p2p/events?since=0", service.AgentToken())
	defer cancelAgent()
	status = mustRoute(t, router, service, "/_p2p/query", map[string]any{"action": "agent.status"})
	if status["connected"] != true || status["online"] != true {
		t.Fatalf("expected agent token event stream to mark agent connected, got %#v body=%s", status, agentRec.BodyString())
	}

	_ = mustRoute(t, router, service, "/_p2p/command", map[string]any{
		"action": "agent.config.update",
		"params": map[string]any{"enabled": false},
	})
	status = mustRoute(t, router, service, "/_p2p/query", map[string]any{"action": "agent.status"})
	if status["connected"] != true || status["online"] != false {
		t.Fatalf("expected disabled agent to stay connected but not online, got %#v", status)
	}

	cancelAgent()
	waitForEventStreamDone(t, agentDone)
	status = mustRoute(t, router, service, "/_p2p/query", map[string]any{"action": "agent.status"})
	if status["connected"] != false || status["online"] != false {
		t.Fatalf("expected agent disconnect after SSE closes, got %#v", status)
	}
}

func mustRoute(t *testing.T, router http.Handler, service *Service, path string, body map[string]any) map[string]any {
	t.Helper()
	req := jsonRequest(t, path, body)
	req.Header.Set("Authorization", "Bearer "+service.AccessToken())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s expected 200, got %d body=%s", path, rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func mustRouteError(t *testing.T, router http.Handler, service *Service, path string, body map[string]any) apiError {
	t.Helper()
	req := jsonRequest(t, path, body)
	req.Header.Set("Authorization", "Bearer "+service.AccessToken())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatalf("%s expected error, got 200 body=%s", path, rec.Body.String())
	}
	var got apiError
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	got.Status = rec.Code
	return got
}

func jsonRequest(t *testing.T, path string, body map[string]any) *http.Request {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func newP2PTestRouter(service *Service) *mux.Router {
	router := mux.NewRouter()
	Register(router.PathPrefix(PathPrefix).Subrouter(), service)
	return router
}

type sseTestResponseWriter struct {
	header    http.Header
	mu        sync.Mutex
	status    int
	body      bytes.Buffer
	flushed   chan struct{}
	flushOnce sync.Once
}

func newSSETestResponseWriter() *sseTestResponseWriter {
	return &sseTestResponseWriter{
		header:  make(http.Header),
		flushed: make(chan struct{}),
	}
}

func (w *sseTestResponseWriter) Header() http.Header {
	return w.header
}

func (w *sseTestResponseWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = status
	}
}

func (w *sseTestResponseWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(data)
}

func (w *sseTestResponseWriter) Flush() {
	w.flushOnce.Do(func() {
		close(w.flushed)
	})
}

func (w *sseTestResponseWriter) Code() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (w *sseTestResponseWriter) BodyString() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.String()
}

func startEventStreamTest(t *testing.T, router http.Handler, service *Service, path string) (*sseTestResponseWriter, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	return startEventStreamTestWithToken(t, router, path, service.AccessToken())
}

func startEventStreamTestWithToken(t *testing.T, router http.Handler, path, token string) (*sseTestResponseWriter, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := newSSETestResponseWriter()
	done := make(chan struct{})
	go func() {
		router.ServeHTTP(rec, req)
		close(done)
	}()
	select {
	case <-rec.flushed:
	case <-done:
		cancel()
		t.Fatalf("events endpoint returned before flushing SSE headers; body=%s", rec.BodyString())
	case <-time.After(time.Second):
		cancel()
		t.Fatal("events endpoint did not flush SSE headers")
	}
	return rec, cancel, done
}

func waitForEventStreamBody(t *testing.T, rec *sseTestResponseWriter, want string) {
	t.Helper()
	deadline := time.After(time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if strings.Contains(rec.BodyString(), want) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("event stream body did not contain %q; body=%s", want, rec.BodyString())
		case <-tick.C:
		}
	}
}

func waitForEventStreamDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("events endpoint did not stop after client disconnect")
	}
}
