package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/releasecontrol"
)

type recordingReleaseController struct {
	statusRequest releasecontrol.StatusRequest
	applyRequest  releasecontrol.ApplyRequest
	desiredStates []releasecontrol.DesiredState
	status        releasecontrol.UpdaterStatus
	ticket        releasecontrol.JobTicket
	statusErr     error
	applyErr      error
	desiredErr    error
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
	return c.desiredErr
}

func TestReleaseActionsOwnerAuthTransportAndPersistence(t *testing.T) {
	controller := &recordingReleaseController{
		status: releasecontrol.UpdaterStatus{
			Available:        true,
			ReleaseAvailable: true,
			UpdateAvailable:  true,
			DiscoveryStatus:  "fresh",
			CurrentVersion:   "v1.0.0",
			LatestVersion:    "v1.1.0",
			ClientVersion:    "v2.3.4",
			Compatibility:    "compatible",
			Operations:       []releasecontrol.Operation{{Kind: "upgrade", PlanToken: "opaque-plan", TargetVersion: "v1.1.0"}},
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
	if controller.statusRequest.CurrentVersion != "v1.0.0" || controller.statusRequest.CurrentSchemaVersion != 1 || controller.statusRequest.CurrentSchemaCompatVersion != 1 || controller.statusRequest.ClientVersion != "v2.3.4" {
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

	record := realtimeWSTicket{Role: "owner", UserID: service.OwnerMXID()}
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

func TestClientVersionReportFollowsCurrentPortalDevice(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	issuer := &recordingMatrixSessionIssuer{}
	service.SetMatrixSessionIssuer(issuer)

	mustHandle[map[string]any](t, service, "client.version.report", map[string]any{"client_version": "v1.2.3"})
	mustHandle[map[string]any](t, service, "portal.auth", map[string]any{"password": service.password, "device_id": "NEW_DEVICE"})
	if service.clientBuild.Version != "" {
		t.Fatalf("new portal device must clear previous device report: %#v", service.clientBuild)
	}

	mustHandle[map[string]any](t, service, "client.version.report", map[string]any{"client_version": "v1.2.4"})
	mustHandle[map[string]any](t, service, "portal.auth", map[string]any{"password": service.password, "device_id": "NEW_DEVICE"})
	if service.clientBuild.Version != "v1.2.4" {
		t.Fatalf("same portal device must retain its report: %#v", service.clientBuild)
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
