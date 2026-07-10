package p2p

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal"
	"github.com/YingSuiAI/dirextalk-message-server/internal/releasecontrol"
	"github.com/google/uuid"
)

const (
	releaseApplyInvalidParamsCode = "release_apply_invalid_params"
	updaterUnavailableCode        = "updater_unavailable"
	clientSessionStaleCode        = "client_session_stale"
)

func (s *Service) reportClientVersion(ctx context.Context, params map[string]any) (any, *apiError) {
	s.matrixSessionMu.Lock()
	defer s.matrixSessionMu.Unlock()
	identity, ok := portalActionSessionFromContext(ctx)
	if !ok {
		return nil, codedError(http.StatusUnauthorized, clientSessionStaleCode, "client session is stale")
	}
	s.mu.Lock()
	currentDeviceID := cleanMatrixDeviceID(s.matrixDeviceID)
	currentGeneration := s.portalSessionGeneration
	s.mu.Unlock()
	if identity.DeviceID != currentDeviceID || identity.Generation != currentGeneration {
		return nil, codedError(http.StatusUnauthorized, clientSessionStaleCode, "client session is stale")
	}
	version, err := releasecontrol.NormalizeClientVersion(trimString(params["client_version"]))
	if err != nil {
		return nil, codedError(http.StatusBadRequest, "client_version_invalid", "client_version must be a stable semantic version")
	}
	buildNumber, ok := optionalReleaseText(params["build_number"], 64)
	if !ok {
		return nil, codedError(http.StatusBadRequest, "client_build_invalid", "build_number is invalid")
	}
	platform, ok := optionalReleaseText(params["platform"], 64)
	if !ok {
		return nil, codedError(http.StatusBadRequest, "client_platform_invalid", "platform is invalid")
	}
	reportedAt := time.Now().UTC().Format(time.RFC3339Nano)
	build := clientBuild{Version: version, BuildNumber: buildNumber, Platform: platform, ReportedAt: reportedAt}
	if store := s.portalStore(); store != nil {
		updated, err := store.SaveClientBuild(ctx, identity.DeviceID, build)
		if err != nil {
			return nil, internalError(err)
		}
		if !updated {
			return nil, codedError(http.StatusUnauthorized, clientSessionStaleCode, "client session is stale")
		}
	}
	s.mu.Lock()
	s.clientBuild = build
	deviceID := cleanMatrixDeviceID(s.matrixDeviceID)
	s.mu.Unlock()
	return map[string]any{
		"client_version": version,
		"build_number":   buildNumber,
		"platform":       platform,
		"device_id":      deviceID,
		"reported_at":    reportedAt,
	}, nil
}

func (s *Service) releaseStatus(ctx context.Context, _ map[string]any) (any, *apiError) {
	buildInfo := internal.CurrentBuildInfo()
	s.mu.Lock()
	controller := s.releaseController
	client := s.clientBuild
	deviceID := cleanMatrixDeviceID(s.matrixDeviceID)
	s.mu.Unlock()
	request := releasecontrol.StatusRequest{
		CurrentVersion:             buildInfo.Version,
		CurrentSchemaVersion:       buildInfo.SchemaVersion,
		CurrentSchemaCompatVersion: buildInfo.SchemaCompatVersion,
		ClientVersion:              client.Version,
	}
	status := unavailableUpdaterStatus(request)
	if controller != nil {
		if current, err := controller.Status(ctx, request); err == nil {
			status = current
		}
	}
	status.CurrentVersion = request.CurrentVersion
	status.ClientVersion = request.ClientVersion
	return releaseStatusMap(status, buildInfo.SchemaVersion, buildInfo.SchemaCompatVersion, client, deviceID), nil
}

func (s *Service) applyRelease(ctx context.Context, params map[string]any) (any, *apiError) {
	allowed := map[string]struct{}{"plan_token": {}, "idempotency_key": {}, "confirm": {}}
	for key := range params {
		if _, ok := allowed[key]; !ok {
			return nil, codedError(http.StatusBadRequest, releaseApplyInvalidParamsCode, "release apply accepts only plan_token, idempotency_key, and confirm")
		}
	}
	request := releasecontrol.ApplyRequest{
		PlanToken:      trimString(params["plan_token"]),
		IdempotencyKey: trimString(params["idempotency_key"]),
		Confirm:        trimString(params["confirm"]),
	}
	if request.PlanToken == "" || len(request.PlanToken) > 4096 || strings.ContainsAny(request.PlanToken, "\r\n\t") || request.Confirm != releasecontrol.ApplyConfirmation {
		return nil, codedError(http.StatusBadRequest, releaseApplyInvalidParamsCode, "release apply parameters are invalid")
	}
	if _, err := uuid.Parse(request.IdempotencyKey); err != nil {
		return nil, codedError(http.StatusBadRequest, releaseApplyInvalidParamsCode, "idempotency_key must be a UUID")
	}
	s.mu.Lock()
	controller := s.releaseController
	s.mu.Unlock()
	if controller == nil {
		return nil, codedError(http.StatusServiceUnavailable, updaterUnavailableCode, "updater is unavailable")
	}
	ticket, err := controller.Apply(ctx, request)
	if err != nil {
		return nil, releaseControllerAPIError(err)
	}
	return map[string]any{
		"job_id":     ticket.JobID,
		"job_token":  ticket.JobToken,
		"status_url": ticket.StatusURL,
	}, nil
}

func unavailableUpdaterStatus(request releasecontrol.StatusRequest) releasecontrol.UpdaterStatus {
	return releasecontrol.UpdaterStatus{
		Available:        false,
		ReleaseAvailable: false,
		UpdateAvailable:  false,
		DiscoveryStatus:  "unavailable",
		CurrentVersion:   request.CurrentVersion,
		ClientVersion:    request.ClientVersion,
		Compatibility:    "unknown",
		Reasons:          []string{updaterUnavailableCode},
		Operations:       []releasecontrol.Operation{},
	}
}

func releaseStatusMap(status releasecontrol.UpdaterStatus, schemaVersion, schemaCompatVersion int, client clientBuild, deviceID string) map[string]any {
	operations := status.Operations
	if operations == nil {
		operations = []releasecontrol.Operation{}
	}
	reasons := status.Reasons
	if reasons == nil {
		reasons = []string{}
	}
	return map[string]any{
		"available":                     status.Available,
		"release_available":             status.ReleaseAvailable,
		"update_available":              status.UpdateAvailable,
		"discovery_status":              status.DiscoveryStatus,
		"checked_at":                    status.CheckedAt,
		"current_version":               status.CurrentVersion,
		"current_schema_version":        schemaVersion,
		"current_schema_compat_version": schemaCompatVersion,
		"latest_version":                status.LatestVersion,
		"client_version":                status.ClientVersion,
		"client_build_number":           client.BuildNumber,
		"client_platform":               client.Platform,
		"client_device_id":              deviceID,
		"client_reported_at":            client.ReportedAt,
		"compatibility":                 status.Compatibility,
		"reasons":                       reasons,
		"release_notes_url":             status.ReleaseNotesURL,
		"operations":                    operations,
	}
}

func optionalReleaseText(value any, limit int) (string, bool) {
	text := trimString(value)
	if text == "" {
		return "", true
	}
	if len(text) > limit || strings.ContainsAny(text, "\r\n\t") {
		return "", false
	}
	return text, true
}

func releaseControllerAPIError(err error) *apiError {
	if controllerErr, ok := releasecontrol.AsControllerError(err); ok {
		status := controllerErr.Status
		if status < 400 || status > 599 {
			status = http.StatusBadGateway
		}
		code := controllerErr.Code
		if code == "" {
			code = "updater_rejected"
		}
		message := "updater rejected the request"
		if code == updaterUnavailableCode {
			message = "updater is unavailable"
		}
		return codedError(status, code, message)
	}
	return codedError(http.StatusServiceUnavailable, updaterUnavailableCode, "updater is unavailable")
}
