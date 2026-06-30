package p2p

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
)

func TestHTTPProductActionRequiresWebSocketAfterLogin(t *testing.T) {
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

	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusBadRequest || got["error"] != "action requires websocket" {
		t.Fatalf("expected websocket-only error, got %d body=%#v", rec.Code, got)
	}
}

func TestAgentMatrixSessionCreateAllowsAgentToken(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	agentToken := service.AgentToken()

	if !service.Authorize(agentToken, "agent.matrix_session.create") {
		t.Fatal("expected agent token to create an agent Matrix session")
	}

	req := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "agent.matrix_session.create",
		"params": map[string]any{"device_id": "DIREXIO_AGENT_GATEWAY"},
	})
	req.Header.Set("Authorization", "Bearer "+agentToken)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected agent Matrix session create to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["user_id"] != "@agent:example.com" || got["device_id"] != "DIREXIO_AGENT_GATEWAY" {
		t.Fatalf("expected local agent Matrix session metadata, got %#v", got)
	}
}

func TestProtectedHTTPRetainedActionRejectsMissingBearer(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	req := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "portal.password",
		"params": map[string]any{},
	})
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestEventsEndpointIsRemoved(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	req := httptest.NewRequest(http.MethodGet, "/_p2p/events?since=0", nil)
	req.Header.Set("Authorization", "Bearer "+service.AccessToken())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected removed events endpoint to return 404, got %d body=%s", rec.Code, rec.Body.String())
	}
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

	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Alice",
		"avatar_url":   "mxc://example.com/avatar",
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

	mustHandle[map[string]any](t, service, "portal.bootstrap", map[string]any{"password": service.password})
	channelRaw := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "approval",
		"room_id":     "!approval:example.com",
		"name":        "Approval",
		"join_policy": "approval",
	})
	channelID := channelRaw.ChannelID
	roomID := channelRaw.RoomID
	mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{"channel_id": channelID, "room_id": roomID, "user_mxid": "@alice:example.com"})
	mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{"channel_id": channelID, "room_id": roomID, "user_mxid": "@bob:example.com"})
	mustHandle[map[string]any](t, service, "channels.join_request.approve", map[string]any{"channel_id": channelID, "room_id": roomID, "user_mxid": "@alice:example.com"})

	list := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": channelID, "status": "pending"})
	members := list["members"].([]memberRecord)
	if len(members) != 1 {
		t.Fatalf("expected one pending member, got %#v", list)
	}
	member := members[0]
	if member.UserID != "@bob:example.com" || member.Membership != "pending" {
		t.Fatalf("expected legacy and unified member fields, got %#v", member)
	}
}

func TestChannelJoinRequestResolutionReturnsChannelForClientRefresh(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})

	mustHandle[map[string]any](t, service, "portal.bootstrap", map[string]any{"password": service.password})
	channelRaw := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "approval",
		"room_id":     "!approval:example.com",
		"name":        "Approval",
		"join_policy": "approval",
	})
	channelID := channelRaw.ChannelID
	roomID := channelRaw.RoomID
	mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{"channel_id": channelID, "room_id": roomID, "user_mxid": "@alice:example.com"})

	approved := mustHandle[map[string]any](t, service, "channels.join_request.approve", map[string]any{"channel_id": channelID, "room_id": roomID, "user_mxid": "@alice:example.com"})
	channel := approved["channel"].(channel)
	if approved["status"] != "approved" || channel.ChannelID != channelID || channel.PendingJoinCount != 0 {
		t.Fatalf("expected approved status and refreshed channel, got %#v", approved)
	}
}

func TestPublicChannelActionsDoNotRequireBearer(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	channelRaw := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id":  "public-news",
		"room_id":     "!public-news:example.com",
		"name":        "Public News",
		"visibility":  "public",
		"join_policy": "approval",
	})
	roomID := channelRaw.RoomID

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

func TestAgentTokenCanOnlyCallAgentBootstrapAndMCPActions(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)
	agentToken := service.AgentToken()

	if !service.Authorize(agentToken, "agent.matrix_session.create") {
		t.Fatal("expected agent token to authorize agent Matrix session bootstrap")
	}
	if !service.Authorize(agentToken, "mcp.rooms.search") {
		t.Fatal("expected agent token to authorize MCP actions")
	}
	for _, action := range []string{
		"contacts.request",
		"agent.config.get",
		"agent.config.update",
		"agent.password",
		realtimeWSTicketAction,
	} {
		if service.Authorize(agentToken, action) {
			t.Fatalf("expected agent token to reject %s", action)
		}
	}

	mcpReq := jsonRequest(t, "/_p2p/query", map[string]any{
		"action": "mcp.rooms.search",
		"params": map[string]any{"q": "none"},
	})
	mcpReq.Header.Set("Authorization", "Bearer "+agentToken)
	mcpRec := httptest.NewRecorder()
	router.ServeHTTP(mcpRec, mcpReq)
	if mcpRec.Code != http.StatusOK {
		t.Fatalf("expected HTTP MCP action to succeed, got %d body=%s", mcpRec.Code, mcpRec.Body.String())
	}

	wsTicketReq := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": realtimeWSTicketAction,
	})
	wsTicketReq.Header.Set("Authorization", "Bearer "+agentToken)
	wsTicketRec := httptest.NewRecorder()
	router.ServeHTTP(wsTicketRec, wsTicketReq)
	if wsTicketRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected agent token realtime WS ticket creation to be unauthorized, got %d body=%s", wsTicketRec.Code, wsTicketRec.Body.String())
	}

	agentRequest := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "contacts.request",
		"params": map[string]any{"mxid": "@agent-ok:example.com"},
	})
	agentRequest.Header.Set("Authorization", "Bearer "+agentToken)
	agentRec := httptest.NewRecorder()
	router.ServeHTTP(agentRec, agentRequest)
	if agentRec.Code != http.StatusBadRequest {
		t.Fatalf("expected owner action over HTTP to require websocket, got %d body=%s", agentRec.Code, agentRec.Body.String())
	}

	removed := mustRouteError(t, router, service, "/_p2p/command", map[string]any{"action": "apis.list"})
	if removed.Status != http.StatusBadRequest {
		t.Fatalf("expected removed apis.list to be unknown, got %#v", removed)
	}
}

func TestAgentStatusActionRemoved(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	err := mustRouteError(t, router, service, "/_p2p/query", map[string]any{"action": "agent.status"})
	if err.Status != http.StatusBadRequest {
		t.Fatalf("expected removed agent.status action to be unknown, got %#v", err)
	}
}

func TestSyncBootstrapOmitsDeprecatedAgentOnline(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})

	bootstrap := mustHandle[map[string]any](t, service, "sync.bootstrap", nil)
	if _, ok := bootstrap["agent_online"]; ok {
		t.Fatalf("expected sync.bootstrap to omit deprecated agent_online, got %#v", bootstrap)
	}
	if _, ok := bootstrap["agent_presence"]; ok {
		t.Fatalf("expected sync.bootstrap to omit deprecated agent_presence, got %#v", bootstrap["agent_presence"])
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
