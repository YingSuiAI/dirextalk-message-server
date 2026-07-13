// Package release owns the ProductCore release status, apply, and client-build
// reporting workflows.
package release

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/releasecontrol"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	"github.com/google/uuid"
)

const (
	ClientSessionStaleCode = "client_session_stale"
	UpdaterUnavailableCode = "updater_unavailable"

	actionClientVersionReport = "client.version.report"
	actionReleaseStatus       = "release.v1.status"
	actionReleaseApply        = "release.v1.apply"
	applyInvalidParamsCode    = "release_apply_invalid_params"
)

type Session struct {
	DeviceID   string
	Generation uint64
}

type Snapshot struct {
	DeviceID   string
	Generation uint64
	Client     dirextalkdomain.ClientBuild
	Controller releasecontrol.Controller
}

// StatePort keeps the module on the Service's single portal-state instance and
// preserves the existing device-scoped durable CAS.
type StatePort interface {
	Session(context.Context) (Session, bool)
	Snapshot() Snapshot
	SaveClientBuild(context.Context, string, dirextalkdomain.ClientBuild) (bool, error)
	CommitClientBuild(dirextalkdomain.ClientBuild) string
}

type Config struct {
	SessionLocker sync.Locker
	Now           func() time.Time
}

type Module struct {
	state StatePort
	cfg   Config
}

func New(state StatePort, cfg Config) *Module {
	return &Module{state: state, cfg: cfg}
}

func (m *Module) Handlers() map[string]actionbase.Handler {
	return map[string]actionbase.Handler{
		actionClientVersionReport: m.reportClientVersion,
		actionReleaseStatus:       m.status,
		actionReleaseApply:        m.apply,
	}
}

func (m *Module) reportClientVersion(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if m.cfg.SessionLocker != nil {
		m.cfg.SessionLocker.Lock()
		defer m.cfg.SessionLocker.Unlock()
	}
	identity, ok := m.state.Session(ctx)
	if !ok {
		return nil, staleSessionError()
	}
	snapshot := m.state.Snapshot()
	if identity.DeviceID != snapshot.DeviceID || identity.Generation != snapshot.Generation {
		return nil, staleSessionError()
	}
	values := actionbase.Params(params)
	version, err := releasecontrol.NormalizeClientVersion(values.String("client_version"))
	if err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, "client_version_invalid", "client_version must be a stable semantic version")
	}
	buildNumber, ok := optionalText(values.Raw("build_number"), 64)
	if !ok {
		return nil, actionbase.CodedError(http.StatusBadRequest, "client_build_invalid", "build_number is invalid")
	}
	platform, ok := optionalText(values.Raw("platform"), 64)
	if !ok {
		return nil, actionbase.CodedError(http.StatusBadRequest, "client_platform_invalid", "platform is invalid")
	}
	reportedAt := m.now().Format(time.RFC3339Nano)
	build := dirextalkdomain.ClientBuild{Version: version, BuildNumber: buildNumber, Platform: platform, ReportedAt: reportedAt}
	updated, err := m.state.SaveClientBuild(ctx, identity.DeviceID, build)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !updated {
		return nil, staleSessionError()
	}
	deviceID := m.state.CommitClientBuild(build)
	return map[string]any{
		"client_version": version,
		"build_number":   buildNumber,
		"platform":       platform,
		"device_id":      deviceID,
		"reported_at":    reportedAt,
	}, nil
}

func (m *Module) status(ctx context.Context, _ map[string]any) (any, *actionbase.Error) {
	buildInfo := internal.CurrentBuildInfo()
	snapshot := m.state.Snapshot()
	request := releasecontrol.StatusRequest{
		CurrentVersion:             buildInfo.Version,
		CurrentSchemaVersion:       buildInfo.SchemaVersion,
		CurrentSchemaCompatVersion: buildInfo.SchemaCompatVersion,
		ClientVersion:              snapshot.Client.Version,
	}
	status := unavailableStatus(request)
	if snapshot.Controller != nil {
		if current, err := snapshot.Controller.Status(ctx, request); err == nil {
			status = current
		}
	}
	status.CurrentVersion = request.CurrentVersion
	status.ClientVersion = request.ClientVersion
	return statusMap(status, buildInfo.SchemaVersion, buildInfo.SchemaCompatVersion, snapshot.Client, snapshot.DeviceID), nil
}

func (m *Module) apply(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	allowed := map[string]struct{}{"plan_token": {}, "idempotency_key": {}, "confirm": {}}
	for key := range params {
		if _, ok := allowed[key]; !ok {
			return nil, actionbase.CodedError(http.StatusBadRequest, applyInvalidParamsCode, "release apply accepts only plan_token, idempotency_key, and confirm")
		}
	}
	values := actionbase.Params(params)
	request := releasecontrol.ApplyRequest{
		PlanToken:      values.String("plan_token"),
		IdempotencyKey: values.String("idempotency_key"),
		Confirm:        values.String("confirm"),
	}
	if request.PlanToken == "" || len(request.PlanToken) > 4096 || strings.ContainsAny(request.PlanToken, "\r\n\t") || request.Confirm != releasecontrol.ApplyConfirmation {
		return nil, actionbase.CodedError(http.StatusBadRequest, applyInvalidParamsCode, "release apply parameters are invalid")
	}
	if _, err := uuid.Parse(request.IdempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, applyInvalidParamsCode, "idempotency_key must be a UUID")
	}
	controller := m.state.Snapshot().Controller
	if controller == nil {
		return nil, unavailableError()
	}
	ticket, err := controller.Apply(ctx, request)
	if err != nil {
		return nil, controllerError(err)
	}
	return map[string]any{"job_id": ticket.JobID, "job_token": ticket.JobToken, "status_url": ticket.StatusURL}, nil
}

func (m *Module) SetDesiredState(ctx context.Context, state releasecontrol.DesiredState) *actionbase.Error {
	controller := m.state.Snapshot().Controller
	if controller == nil {
		return unavailableError()
	}
	if err := controller.SetDesiredState(ctx, state); err != nil {
		return controllerError(err)
	}
	return nil
}

func (m *Module) now() time.Time {
	if m.cfg.Now == nil {
		return time.Now().UTC()
	}
	return m.cfg.Now().UTC()
}

func staleSessionError() *actionbase.Error {
	return actionbase.CodedError(http.StatusUnauthorized, ClientSessionStaleCode, "client session is stale")
}

func unavailableError() *actionbase.Error {
	return actionbase.CodedError(http.StatusServiceUnavailable, UpdaterUnavailableCode, "updater is unavailable")
}

func unavailableStatus(request releasecontrol.StatusRequest) releasecontrol.UpdaterStatus {
	return releasecontrol.UpdaterStatus{
		Available: false, ReleaseAvailable: false, UpdateAvailable: false,
		DiscoveryStatus: "unavailable", CurrentVersion: request.CurrentVersion,
		ClientVersion: request.ClientVersion, Compatibility: "unknown",
		Reasons: []string{UpdaterUnavailableCode}, Operations: []releasecontrol.Operation{},
	}
}

func statusMap(status releasecontrol.UpdaterStatus, schemaVersion, schemaCompatVersion int, client dirextalkdomain.ClientBuild, deviceID string) map[string]any {
	operations := status.Operations
	if operations == nil {
		operations = []releasecontrol.Operation{}
	}
	reasons := status.Reasons
	if reasons == nil {
		reasons = []string{}
	}
	return map[string]any{
		"available": status.Available, "release_available": status.ReleaseAvailable,
		"update_available": status.UpdateAvailable, "discovery_status": status.DiscoveryStatus,
		"checked_at": status.CheckedAt, "current_version": status.CurrentVersion,
		"current_schema_version": schemaVersion, "current_schema_compat_version": schemaCompatVersion,
		"latest_version": status.LatestVersion, "client_version": status.ClientVersion,
		"client_build_number": client.BuildNumber, "client_platform": client.Platform,
		"client_device_id": deviceID, "client_reported_at": client.ReportedAt,
		"compatibility": status.Compatibility, "reasons": reasons,
		"release_notes_url": status.ReleaseNotesURL, "operations": operations,
		"watchdog": watchdogMap(status.Watchdog),
	}
}

func watchdogMap(status releasecontrol.WatchdogStatus) map[string]any {
	watchdogStatus := status.Status
	switch watchdogStatus {
	case "healthy", "observing", "repairing", "degraded", "suppressed":
	default:
		watchdogStatus = "unknown"
	}
	errorCode := status.ErrorCode
	switch errorCode {
	case "", "observation_failed", "repair_failed":
	default:
		errorCode = ""
	}
	return map[string]any{
		"status": watchdogStatus, "degraded": watchdogStatus == "degraded",
		"cooldown_until":   normalizedTime(status.CooldownUntil),
		"last_observed_at": normalizedTime(status.LastObservedAt), "error_code": errorCode,
	}
}

func normalizedTime(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return ""
	}
	return parsed.UTC().Format(time.RFC3339)
}

func optionalText(value any, limit int) (string, bool) {
	text := actionbase.String(value)
	if text == "" {
		return "", true
	}
	if len(text) > limit || strings.ContainsAny(text, "\r\n\t") {
		return "", false
	}
	return text, true
}

func controllerError(err error) *actionbase.Error {
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
		if code == UpdaterUnavailableCode {
			message = "updater is unavailable"
		}
		return actionbase.CodedError(status, code, message)
	}
	return unavailableError()
}
