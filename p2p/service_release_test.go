package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/releasecontrol"
)

type recordingReleaseController struct {
	statusRequest releasecontrol.StatusRequest
	applyRequest  releasecontrol.ApplyRequest
	desiredStates []releasecontrol.DesiredState
	desiredErrors map[releasecontrol.DesiredState]error
	status        releasecontrol.UpdaterStatus
	ticket        releasecontrol.JobTicket
	statusErr     error
	applyErr      error
	desiredErr    error
}

type blockingClientBuildStore struct {
	Store
	mu             sync.Mutex
	state          portalState
	narrowEntered  chan struct{}
	releaseNarrow  chan struct{}
	fullReportSave chan struct{}
	portalSaved    chan portalState
}

func (s *blockingClientBuildStore) SavePortal(_ context.Context, state portalState) error {
	if state.ClientBuild.Version != "" {
		select {
		case s.fullReportSave <- struct{}{}:
		default:
		}
	}
	s.mu.Lock()
	if cleanMatrixDeviceID(s.state.MatrixDeviceID) == cleanMatrixDeviceID(state.MatrixDeviceID) {
		state.ClientBuild = s.state.ClientBuild
	}
	s.state = state
	s.mu.Unlock()
	if s.portalSaved != nil {
		s.portalSaved <- state
	}
	return nil
}

func (s *blockingClientBuildStore) SaveClientBuild(_ context.Context, expectedDeviceID string, build clientBuild) (bool, error) {
	close(s.narrowEntered)
	<-s.releaseNarrow
	s.mu.Lock()
	defer s.mu.Unlock()
	if cleanMatrixDeviceID(s.state.MatrixDeviceID) != cleanMatrixDeviceID(expectedDeviceID) {
		return false, nil
	}
	s.state.ClientBuild = build
	return true, nil
}

func (c *recordingReleaseController) Status(_ context.Context, request releasecontrol.StatusRequest) (releasecontrol.UpdaterStatus, error) {
	c.statusRequest = request
	return c.status, c.statusErr
}

func (c *recordingReleaseController) Apply(_ context.Context, request releasecontrol.ApplyRequest) (releasecontrol.JobTicket, error) {
	c.applyRequest = request
	return c.ticket, c.applyErr
}

func (c *recordingReleaseController) SetDesiredState(_ context.Context, state releasecontrol.DesiredState) error {
	c.desiredStates = append(c.desiredStates, state)
	if c.desiredErrors != nil && c.desiredErrors[state] != nil {
		return c.desiredErrors[state]
	}
	return c.desiredErr
}

func TestReleaseActionsOwnerAuthTransportAndPersistence(t *testing.T) {
	controller := &recordingReleaseController{
		status: releasecontrol.UpdaterStatus{
			Available:        true,
			ReleaseAvailable: true,
			UpdateAvailable:  true,
			DiscoveryStatus:  "fresh",
			CurrentVersion:   "v1.0.1",
			LatestVersion:    "v1.1.0",
			ClientVersion:    "v2.3.4",
			Compatibility:    "compatible",
			Operations:       []releasecontrol.Operation{{Kind: "upgrade", PlanToken: "opaque-plan", TargetVersion: "v1.1.0"}},
			Watchdog: releasecontrol.WatchdogStatus{
				Status:         "degraded",
				Degraded:       true,
				CooldownUntil:  "2026-07-10T12:15:00Z",
				LastObservedAt: "2026-07-10T12:00:00Z",
				ErrorCode:      "repair_failed",
			},
		},
		ticket: releasecontrol.JobTicket{JobID: "job_test", JobToken: "job-secret", StatusURL: "/_dirextalk/updater/v1/jobs/job_test"},
	}
	service := NewService(Config{ServerName: "example.com", ReleaseController: controller})
	router := newP2PTestRouter(service)

	report := releaseRoute(t, router, service.AccessToken(), "client.version.report", map[string]any{
		"client_version": "2.3.4",
		"build_number":   "42",
		"platform":       "android",
	})
	if report["client_version"] != "v2.3.4" || report["device_id"] == "" || report["reported_at"] == "" {
		t.Fatalf("unexpected report response: %#v", report)
	}
	service.mu.Lock()
	persisted := service.portalStateLocked().ClientBuild
	service.mu.Unlock()
	if persisted.Version != "v2.3.4" || persisted.BuildNumber != "42" || persisted.Platform != "android" {
		t.Fatalf("client report was not persisted on portal state: %#v", persisted)
	}

	status := releaseRoute(t, router, service.AccessToken(), "release.v1.status", nil)
	if status["available"] != true || status["release_available"] != true || status["compatibility"] != "compatible" {
		t.Fatalf("unexpected release status: %#v", status)
	}
	watchdog, ok := status["watchdog"].(map[string]any)
	if !ok || watchdog["status"] != "degraded" || watchdog["degraded"] != true || watchdog["error_code"] != "repair_failed" {
		t.Fatalf("unexpected public watchdog status: %#v", status["watchdog"])
	}
	if controller.statusRequest.CurrentVersion != "v1.0.2" || controller.statusRequest.CurrentSchemaVersion != 1 || controller.statusRequest.CurrentSchemaCompatVersion != 1 || controller.statusRequest.ClientVersion != "v2.3.4" {
		t.Fatalf("unexpected controller status request: %#v", controller.statusRequest)
	}

	apply := releaseRoute(t, router, service.AccessToken(), "release.v1.apply", map[string]any{
		"plan_token":      "opaque-plan",
		"idempotency_key": "31a20813-c5d9-4f6d-b4f0-cdf8cfc75c6e",
		"confirm":         releasecontrol.ApplyConfirmation,
	})
	if apply["job_id"] != "job_test" || apply["job_token"] != "job-secret" {
		t.Fatalf("unexpected apply response: %#v", apply)
	}
	stateRaw, _ := json.Marshal(persisted)
	if strings.Contains(string(stateRaw), "job-secret") || strings.Contains(string(stateRaw), "opaque-plan") {
		t.Fatalf("job credentials entered durable portal state: %s", stateRaw)
	}

	for _, action := range []string{"client.version.report", "release.v1.status", "release.v1.apply"} {
		for _, token := range []string{"", service.AgentToken()} {
			response := releaseRouteRaw(t, router, token, action, map[string]any{})
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("%s token=%q expected 401, got %d body=%s", action, token, response.Code, response.Body.String())
			}
		}
	}

	wsIdentity, authorized := service.authorizeProductAction(service.AccessToken(), "client.version.report")
	if !authorized {
		t.Fatal("expected current owner session")
	}
	record := realtimeWSTicket{Role: "owner", UserID: service.OwnerMXID(), DeviceID: wsIdentity.DeviceID, Generation: wsIdentity.Generation}
	for _, action := range []string{"client.version.report", "release.v1.status"} {
		frame := service.handleRealtimeWSRequest(context.Background(), record, map[string]any{
			"id": "ws-" + action, "action": action, "params": map[string]any{"client_version": "v2.3.4"},
		})
		if frame["ok"] != true {
			t.Fatalf("%s must be available over owner WS: %#v", action, frame)
		}
	}
	applyWS := service.handleRealtimeWSRequest(context.Background(), record, map[string]any{
		"id": "ws-apply", "action": "release.v1.apply", "params": map[string]any{},
	})
	if applyWS["ok"] != false || applyWS["error"] != "action requires http" {
		t.Fatalf("release apply must be HTTP-only: %#v", applyWS)
	}
}

func TestReleaseStatusUnavailableIsParseable(t *testing.T) {
	controller := &recordingReleaseController{statusErr: errors.New("socket unavailable: secret-token")}
	service := NewService(Config{ServerName: "example.com", ReleaseController: controller})
	status := mustHandle[map[string]any](t, service, "release.v1.status", nil)
	if status["available"] != false || status["release_available"] != false || status["discovery_status"] != "unavailable" || status["compatibility"] != "unknown" {
		t.Fatalf("unexpected unavailable status: %#v", status)
	}
	raw, _ := json.Marshal(status)
	if strings.Contains(string(raw), "secret-token") {
		t.Fatalf("controller error leaked into status: %s", raw)
	}
}

func TestReleaseStatusKeepsLocalCurrentAndClientVersionsAuthoritative(t *testing.T) {
	controller := &recordingReleaseController{status: releasecontrol.UpdaterStatus{
		Available:       true,
		DiscoveryStatus: "fresh",
		CurrentVersion:  "v999.0.0",
		ClientVersion:   "v888.0.0",
		Compatibility:   "compatible",
	}}
	service := NewService(Config{ServerName: "example.com", ReleaseController: controller})
	service.mu.Lock()
	service.clientBuild = clientBuild{Version: "v2.3.4"}
	service.mu.Unlock()

	status := mustHandle[map[string]any](t, service, "release.v1.status", nil)
	if status["current_version"] != "v1.0.2" || status["client_version"] != "v2.3.4" {
		t.Fatalf("updater echo replaced authoritative local versions: %#v", status)
	}
}

func TestClientVersionReportFollowsCurrentPortalDevice(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	issuer := &recordingMatrixSessionIssuer{}
	service.SetMatrixSessionIssuer(issuer)

	mustReportClientVersion(t, service, map[string]any{"client_version": "v1.2.3"})
	mustHandle[map[string]any](t, service, "portal.auth", map[string]any{"password": service.password, "device_id": "NEW_DEVICE"})
	if service.clientBuild.Version != "" {
		t.Fatalf("new portal device must clear previous device report: %#v", service.clientBuild)
	}

	mustReportClientVersion(t, service, map[string]any{"client_version": "v1.2.4"})
	mustHandle[map[string]any](t, service, "portal.auth", map[string]any{"password": service.password, "device_id": "NEW_DEVICE"})
	if service.clientBuild.Version != "v1.2.4" {
		t.Fatalf("same portal device must retain its report: %#v", service.clientBuild)
	}
}

func TestClientVersionReportRejectsHTTPAuthorizationCapturedBeforeDeviceSwitch(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	service.SetMatrixSessionIssuer(&recordingMatrixSessionIssuer{})
	oldToken := service.AccessToken()
	identity, authorized := service.authorizeProductAction(oldToken, "client.version.report")
	if !authorized {
		t.Fatal("expected current owner token to authorize")
	}

	switched := make(chan *apiError, 1)
	go func() {
		_, apiErr := service.Handle(context.Background(), "portal.auth", map[string]any{
			"password": service.password, "device_id": "NEW_DEVICE",
		})
		switched <- apiErr
	}()
	if apiErr := <-switched; apiErr != nil {
		t.Fatalf("switch portal device: %#v", apiErr)
	}

	_, apiErr := service.reportClientVersion(withPortalActionSession(context.Background(), identity), map[string]any{
		"client_version": "v9.9.9",
	})
	if apiErr == nil || apiErr.Status != http.StatusUnauthorized || apiErr.Code != "client_session_stale" {
		t.Fatalf("expected stale authorized HTTP request to be rejected, got %#v", apiErr)
	}
	if service.clientBuild.Version != "" {
		t.Fatalf("stale HTTP request wrote the new device build: %#v", service.clientBuild)
	}
}

func TestClientVersionReportRejectsConnectedOwnerWSAfterDeviceSwitch(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	service.SetMatrixSessionIssuer(&recordingMatrixSessionIssuer{})
	ticketResult, apiErr := service.createRealtimeWSTicketForToken(service.AccessToken())
	if apiErr != nil {
		t.Fatalf("create WS ticket: %#v", apiErr)
	}
	record, consumeErr := service.consumeRealtimeWSTicketRecord(trimString(ticketResult["ticket"]))
	if consumeErr != nil {
		t.Fatalf("consume WS ticket: %v", consumeErr)
	}
	if _, apiErr := service.Handle(context.Background(), "portal.auth", map[string]any{
		"password": service.password, "device_id": "NEW_DEVICE",
	}); apiErr != nil {
		t.Fatalf("switch portal device: %#v", apiErr)
	}

	frame := service.handleRealtimeWSRequest(context.Background(), record, map[string]any{
		"id": "stale-ws", "action": "client.version.report", "params": map[string]any{"client_version": "v9.9.9"},
	})
	if frame["ok"] != false || frame["status"] != http.StatusUnauthorized || frame["code"] != "client_session_stale" {
		t.Fatalf("expected stale connected WS to be rejected, got %#v", frame)
	}
	if service.clientBuild.Version != "" {
		t.Fatalf("stale WS wrote the new device build: %#v", service.clientBuild)
	}
}

func TestClientVersionReportUsesNarrowDeviceCASWithoutLosingConcurrentPortalFields(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	service.mu.Lock()
	initial := service.portalStateLocked()
	service.mu.Unlock()
	store := &blockingClientBuildStore{
		Store:          service.store,
		state:          initial,
		narrowEntered:  make(chan struct{}),
		releaseNarrow:  make(chan struct{}),
		fullReportSave: make(chan struct{}, 1),
	}
	service.store = store
	identity, authorized := service.authorizeProductAction(service.AccessToken(), "client.version.report")
	if !authorized {
		t.Fatal("expected owner action authorization")
	}
	reportDone := make(chan *apiError, 1)
	go func() {
		_, apiErr := service.reportClientVersion(withPortalActionSession(context.Background(), identity), map[string]any{
			"client_version": "v2.3.4", "build_number": "42", "platform": "android",
		})
		reportDone <- apiErr
	}()

	select {
	case <-store.fullReportSave:
		t.Fatal("client version report used stale full-row SavePortal")
	case <-store.narrowEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("client version report did not reach narrow persistence")
	}
	if _, apiErr := service.updateProfile(context.Background(), map[string]any{"display_name": "Concurrent Profile"}); apiErr != nil {
		t.Fatalf("concurrent profile update: %#v", apiErr)
	}
	if _, apiErr := service.updateAgentConfig(context.Background(), map[string]any{"system_prompt": "Concurrent Agent Config"}); apiErr != nil {
		t.Fatalf("concurrent agent update: %#v", apiErr)
	}
	close(store.releaseNarrow)
	if apiErr := <-reportDone; apiErr != nil {
		t.Fatalf("report client version: %#v", apiErr)
	}

	store.mu.Lock()
	durable := store.state
	store.mu.Unlock()
	if durable.Profile.DisplayName != "Concurrent Profile" || durable.AgentConfig.SystemPrompt != "Concurrent Agent Config" {
		t.Fatalf("narrow report lost concurrent portal fields: %#v", durable)
	}
	if durable.ClientBuild.Version != "v2.3.4" || durable.ClientBuild.BuildNumber != "42" {
		t.Fatalf("narrow report did not persist client build: %#v", durable.ClientBuild)
	}
}

func TestClientVersionReportSerializesSameDevicePasswordRotation(t *testing.T) {
	for _, transport := range []string{"http", "ws"} {
		t.Run(transport, func(t *testing.T) {
			service := NewService(Config{ServerName: "example.com"})
			service.mu.Lock()
			initial := service.portalStateLocked()
			service.mu.Unlock()
			store := &blockingClientBuildStore{
				Store:          service.store,
				state:          initial,
				narrowEntered:  make(chan struct{}),
				releaseNarrow:  make(chan struct{}),
				fullReportSave: make(chan struct{}, 1),
				portalSaved:    make(chan portalState, 1),
			}
			service.store = store
			identity, authorized := service.authorizeProductAction(service.AccessToken(), "client.version.report")
			if !authorized {
				t.Fatal("expected current owner session")
			}
			record := realtimeWSTicket{Role: "owner", UserID: service.OwnerMXID(), DeviceID: identity.DeviceID, Generation: identity.Generation}
			reportDone := make(chan *apiError, 1)
			go func() {
				if transport == "ws" {
					frame := service.handleRealtimeWSRequest(context.Background(), record, map[string]any{
						"id": "same-device-race", "action": "client.version.report", "params": map[string]any{"client_version": "v2.3.4"},
					})
					if frame["ok"] != true {
						status, _ := frame["status"].(int)
						reportDone <- codedError(status, trimString(frame["code"]), trimString(frame["error"]))
						return
					}
					reportDone <- nil
					return
				}
				_, apiErr := service.reportClientVersion(withPortalActionSession(context.Background(), identity), map[string]any{"client_version": "v2.3.4"})
				reportDone <- apiErr
			}()
			<-store.narrowEntered

			passwordDone := make(chan *apiError, 1)
			go func() {
				_, apiErr := service.Handle(context.Background(), "portal.password", map[string]any{
					"old_password": service.password,
					"new_password": "rotated-password",
					"device_id":    identity.DeviceID,
				})
				passwordDone <- apiErr
			}()
			passwordPersistedBeforeReport := false
			select {
			case <-store.portalSaved:
				passwordPersistedBeforeReport = true
			case <-time.After(300 * time.Millisecond):
			}
			close(store.releaseNarrow)
			if apiErr := <-reportDone; apiErr != nil {
				t.Fatalf("report client version: %#v", apiErr)
			}
			if apiErr := <-passwordDone; apiErr != nil {
				t.Fatalf("rotate portal password: %#v", apiErr)
			}
			if passwordPersistedBeforeReport {
				t.Fatal("same-device password token/generation mutation overtook an already-validated client report")
			}

			if transport == "ws" {
				frame := service.handleRealtimeWSRequest(context.Background(), record, map[string]any{
					"id": "stale-after-password", "action": "client.version.report", "params": map[string]any{"client_version": "v9.9.9"},
				})
				if frame["code"] != clientSessionStaleCode {
					t.Fatalf("old WS remained valid after same-device password rotation: %#v", frame)
				}
				return
			}
			_, apiErr := service.reportClientVersion(withPortalActionSession(context.Background(), identity), map[string]any{"client_version": "v9.9.9"})
			if apiErr == nil || apiErr.Code != clientSessionStaleCode {
				t.Fatalf("old HTTP session remained valid after same-device password rotation: %#v", apiErr)
			}
		})
	}
}

func TestReleaseApplyRejectsUnknownOrInfrastructureParamsWithStructuredCode(t *testing.T) {
	controller := &recordingReleaseController{}
	service := NewService(Config{ServerName: "example.com", ReleaseController: controller})
	router := newP2PTestRouter(service)
	base := map[string]any{
		"plan_token": "opaque-plan", "idempotency_key": "31a20813-c5d9-4f6d-b4f0-cdf8cfc75c6e", "confirm": releasecontrol.ApplyConfirmation,
	}
	for _, key := range []string{"image", "digest", "version", "shell", "compose_path", "service", "unknown"} {
		params := cloneAnyMap(base)
		params[key] = "attacker-controlled"
		response := releaseRouteRaw(t, router, service.AccessToken(), "release.v1.apply", params)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s expected 400, got %d body=%s", key, response.Code, response.Body.String())
		}
		var body map[string]any
		_ = json.Unmarshal(response.Body.Bytes(), &body)
		if body["code"] != "release_apply_invalid_params" {
			t.Fatalf("%s missing structured error code: %#v", key, body)
		}
	}
	if controller.applyRequest.PlanToken != "" {
		t.Fatalf("invalid request reached controller: %#v", controller.applyRequest)
	}
}

func releaseRoute(t *testing.T, router http.Handler, token, action string, params map[string]any) map[string]any {
	t.Helper()
	response := releaseRouteRaw(t, router, token, action, params)
	if response.Code != http.StatusOK {
		t.Fatalf("%s expected 200, got %d body=%s", action, response.Code, response.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

func releaseRouteRaw(t *testing.T, router http.Handler, token, action string, params map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	req := jsonRequest(t, "/_p2p/command", map[string]any{"action": action, "params": params})
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	return recorder
}

func mustReportClientVersion(t *testing.T, service *Service, params map[string]any) map[string]any {
	t.Helper()
	identity, authorized := service.authorizeProductAction(service.AccessToken(), "client.version.report")
	if !authorized {
		t.Fatal("expected current owner session")
	}
	result, apiErr := service.reportClientVersion(withPortalActionSession(context.Background(), identity), params)
	if apiErr != nil {
		t.Fatalf("client.version.report failed: %#v", apiErr)
	}
	return result.(map[string]any)
}
