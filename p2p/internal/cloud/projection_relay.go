package cloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	InvalidCloudProjectionCode   = "invalid_cloud_projection"
	ProjectionPublishFailureCode = "cloud_projection_publish_failed"
)

var ErrProjectionLeaseLost = errors.New("cloud projection claim lease was lost")

// ProjectionClaim is a lease-fenced Cloud event waiting to enter the
// ProductCore event stream. Its payload is the durable Cloud audit summary,
// never a Goal prompt, a Worker log, or a secret-delivery record.
type ProjectionClaim struct {
	ProjectionID string
	CloudEventID string
	Type         string
	PayloadJSON  string
	LeaseToken   string
	Attempt      int
}

// ProjectionStore is intentionally separate from Store. Only the persistent
// Message Server storage implements it; the Cloud Orchestrator can never use
// it to write ProductCore events or websocket state.
type ProjectionStore interface {
	ClaimCloudProjection(context.Context, string, time.Duration, string) (ProjectionClaim, bool, error)
	CompleteCloudProjection(context.Context, ProjectionClaim) error
	DeferCloudProjection(context.Context, ProjectionClaim, string, time.Time) error
	RejectCloudProjection(context.Context, ProjectionClaim, string) error
}

// ProjectionPublisher must persist through the Message Server's events
// module. The Cloud event ID is supplied separately so the publisher can use
// it as a durable dedupe key before the projection outbox is acknowledged.
type ProjectionPublisher func(context.Context, string, string, map[string]any) error

type ProjectionRelayConfig struct {
	WorkerID      string
	Lease         time.Duration
	PollInterval  time.Duration
	RetryDelay    time.Duration
	Now           func() time.Time
	NewLeaseToken func() string
}

type ProjectionRelay struct {
	store   ProjectionStore
	publish ProjectionPublisher
	cfg     ProjectionRelayConfig
}

func NewProjectionRelay(store ProjectionStore, publish ProjectionPublisher, cfg ProjectionRelayConfig) *ProjectionRelay {
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = 15 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.NewLeaseToken == nil {
		cfg.NewLeaseToken = uuid.NewString
	}
	return &ProjectionRelay{store: store, publish: publish, cfg: cfg}
}

// Run drains available projections and keeps polling until the caller's
// lifecycle context ends. Failures are retried with a bounded delay and are
// intentionally not logged with payload detail here.
func (r *ProjectionRelay) Run(ctx context.Context) error {
	if err := r.validateConfig(); err != nil {
		return err
	}
	for {
		processed, err := r.RunOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			if !waitForCloudRelay(ctx, r.cfg.RetryDelay) {
				return nil
			}
			continue
		}
		if processed {
			continue
		}
		if !waitForCloudRelay(ctx, r.cfg.PollInterval) {
			return nil
		}
	}
}

// RunOnce claims at most one projection. It validates a fixed, de-secretsed
// payload schema before publishing and only acknowledges after ProductCore has
// persisted the matching dedupe key.
func (r *ProjectionRelay) RunOnce(ctx context.Context) (bool, error) {
	if err := r.validateConfig(); err != nil {
		return false, err
	}
	token := strings.TrimSpace(r.cfg.NewLeaseToken())
	if !validProjectionToken(token) {
		return false, errors.New("cloud projection relay lease token is invalid")
	}
	claim, found, err := r.store.ClaimCloudProjection(ctx, strings.TrimSpace(r.cfg.WorkerID), r.cfg.Lease, token)
	if err != nil || !found {
		return found, err
	}
	if !validProjectionClaim(claim) {
		return true, r.store.RejectCloudProjection(ctx, claim, InvalidCloudProjectionCode)
	}
	payload, err := decodeProjectionPayload(claim.Type, claim.PayloadJSON)
	if err != nil {
		return true, r.store.RejectCloudProjection(ctx, claim, InvalidCloudProjectionCode)
	}
	if err := r.publish(ctx, claim.CloudEventID, claim.Type, payload); err != nil {
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		return true, r.store.DeferCloudProjection(ctx, claim, ProjectionPublishFailureCode, r.now().Add(r.cfg.RetryDelay))
	}
	if err := r.store.CompleteCloudProjection(ctx, claim); err != nil {
		return true, fmt.Errorf("complete cloud projection: %w", err)
	}
	return true, nil
}

func (r *ProjectionRelay) validateConfig() error {
	if r == nil || r.store == nil || r.publish == nil {
		return errors.New("cloud projection relay is unavailable")
	}
	workerID := strings.TrimSpace(r.cfg.WorkerID)
	if workerID == "" || utf8.RuneCountInString(workerID) > 128 || strings.ContainsAny(workerID, "\r\n\t\x00") {
		return errors.New("cloud projection relay worker id is invalid")
	}
	if r.cfg.Lease <= 0 || r.cfg.Lease > 5*time.Minute {
		return errors.New("cloud projection relay lease is invalid")
	}
	if r.cfg.PollInterval <= 0 || r.cfg.RetryDelay <= 0 {
		return errors.New("cloud projection relay timing is invalid")
	}
	return nil
}

func (r *ProjectionRelay) now() time.Time {
	if r != nil && r.cfg.Now != nil {
		return r.cfg.Now().UTC()
	}
	return time.Now().UTC()
}

func waitForCloudRelay(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func validProjectionToken(value string) bool {
	return value != "" && utf8.RuneCountInString(value) <= 128 && !strings.ContainsAny(value, "\r\n\t\x00")
}

func validProjectionClaim(claim ProjectionClaim) bool {
	return validProjectionIdentifier(claim.ProjectionID) && validProjectionIdentifier(claim.CloudEventID) &&
		validProjectionToken(claim.LeaseToken) && strings.TrimSpace(claim.PayloadJSON) != "" &&
		utf8.RuneCountInString(claim.PayloadJSON) <= 16_000
}

func validProjectionIdentifier(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && utf8.RuneCountInString(value) <= 200 && !strings.ContainsAny(value, "\r\n\t\x00")
}

type goalProjectionPayload struct {
	GoalID       string `json:"goal_id"`
	PlanID       string `json:"plan_id"`
	ConnectionID string `json:"cloud_connection_id"`
	Status       string `json:"status"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

type planProjectionPayload struct {
	PlanID       string `json:"plan_id"`
	GoalID       string `json:"goal_id"`
	ConnectionID string `json:"cloud_connection_id"`
	Status       string `json:"status"`
	Title        string `json:"title"`
	Summary      string `json:"summary"`
	RecipeDigest string `json:"recipe_digest"`
	QuoteID      string `json:"quote_id"`
	PlanHash     string `json:"plan_hash"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

type jobProjectionPayload struct {
	DeploymentID    string `json:"deployment_id"`
	JobID           string `json:"job_id"`
	PlanID          string `json:"plan_id"`
	Kind            string `json:"kind"`
	ExecutionStatus string `json:"execution_status"`
	OutcomeStatus   string `json:"outcome_status"`
	Checkpoint      string `json:"checkpoint"`
	ErrorCode       string `json:"error_code"`
	Revision        int64  `json:"revision"`
	CreatedAt       int64  `json:"created_at"`
	UpdatedAt       int64  `json:"updated_at"`
}

type deploymentProjectionPayload struct {
	DeploymentID string `json:"deployment_id"`
	PlanID       string `json:"plan_id"`
	ConnectionID string `json:"cloud_connection_id"`
	Execution    string `json:"execution_status"`
	Outcome      string `json:"outcome_status"`
	Resource     string `json:"resource_status"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

type serviceProjectionPayload struct {
	ServiceID    string                            `json:"service_id"`
	DeploymentID string                            `json:"deployment_id"`
	RecipeID     string                            `json:"recipe_id"`
	Name         string                            `json:"name"`
	Status       string                            `json:"service_status"`
	Integration  string                            `json:"integration_status"`
	Revision     int64                             `json:"revision"`
	CreatedAt    int64                             `json:"created_at"`
	UpdatedAt    int64                             `json:"updated_at"`
	Backups      []serviceBackupProjectionPayload  `json:"backups,omitempty"`
	Restores     []serviceRestoreProjectionPayload `json:"restores,omitempty"`
}

type serviceBackupProjectionPayload struct {
	BackupID        string   `json:"backup_id"`
	ServiceID       string   `json:"service_id"`
	DeploymentID    string   `json:"deployment_id"`
	Status          string   `json:"status"`
	RetentionPolicy string   `json:"retention_policy"`
	ImageID         string   `json:"image_id,omitempty"`
	SnapshotIDs     []string `json:"snapshot_ids,omitempty"`
	Revision        int64    `json:"revision"`
	CreatedAt       int64    `json:"created_at"`
	UpdatedAt       int64    `json:"updated_at"`
}

type serviceRestoreProjectionPayload struct {
	RestoreID            string   `json:"restore_id"`
	RestorePlanID        string   `json:"restore_plan_id"`
	ServiceID            string   `json:"service_id"`
	DeploymentID         string   `json:"deployment_id"`
	BackupID             string   `json:"backup_id"`
	Status               string   `json:"status"`
	OriginalVolumeIDs    []string `json:"original_volume_ids,omitempty"`
	ReplacementVolumeIDs []string `json:"replacement_volume_ids,omitempty"`
	Revision             int64    `json:"revision"`
	CreatedAt            int64    `json:"created_at"`
	UpdatedAt            int64    `json:"updated_at"`
}

type connectionProjectionPayload struct {
	ConnectionID string `json:"cloud_connection_id"`
	Provider     string `json:"provider"`
	AccountID    string `json:"account_id"`
	Region       string `json:"region"`
	Mode         string `json:"mode"`
	Status       string `json:"status"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

type alertProjectionPayload struct {
	AlertID      string `json:"alert_id"`
	DeploymentID string `json:"deployment_id"`
	ServiceID    string `json:"service_id"`
	Severity     string `json:"severity"`
	Code         string `json:"code"`
	Message      string `json:"message"`
	Acknowledged bool   `json:"acknowledged"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

func decodeProjectionPayload(eventType, raw string) (map[string]any, error) {
	switch eventType {
	case "cloud.goal.changed":
		var value goalProjectionPayload
		if err := decodeStrictProjectionJSON(raw, &value); err != nil || !validGoalProjection(value) {
			return nil, errors.New("invalid cloud goal projection")
		}
		return map[string]any{
			"goal_id": value.GoalID, "plan_id": value.PlanID, "cloud_connection_id": value.ConnectionID,
			"status": value.Status, "revision": value.Revision, "created_at": value.CreatedAt, "updated_at": value.UpdatedAt,
		}, nil
	case "cloud.plan.changed":
		var value planProjectionPayload
		if err := decodeStrictProjectionJSON(raw, &value); err != nil || !validPlanProjection(value) {
			return nil, errors.New("invalid cloud plan projection")
		}
		return map[string]any{
			"plan_id": value.PlanID, "goal_id": value.GoalID, "cloud_connection_id": value.ConnectionID,
			"status": value.Status, "title": value.Title, "summary": value.Summary, "recipe_digest": value.RecipeDigest,
			"quote_id": value.QuoteID, "plan_hash": value.PlanHash, "revision": value.Revision,
			"created_at": value.CreatedAt, "updated_at": value.UpdatedAt,
		}, nil
	case "cloud.job.changed":
		var value jobProjectionPayload
		if err := decodeStrictProjectionJSON(raw, &value); err != nil || !validJobProjection(value) {
			return nil, errors.New("invalid cloud job projection")
		}
		return map[string]any{
			"job_id": value.JobID, "plan_id": value.PlanID, "deployment_id": value.DeploymentID, "kind": value.Kind,
			"execution_status": value.ExecutionStatus, "outcome_status": value.OutcomeStatus,
			"checkpoint": value.Checkpoint, "error_code": value.ErrorCode, "revision": value.Revision,
			"created_at": value.CreatedAt, "updated_at": value.UpdatedAt,
		}, nil
	case "cloud.deployment.changed":
		var value deploymentProjectionPayload
		if err := decodeStrictProjectionJSON(raw, &value); err != nil || !validDeploymentProjection(value) {
			return nil, errors.New("invalid cloud deployment projection")
		}
		return map[string]any{
			"deployment_id": value.DeploymentID, "plan_id": value.PlanID, "cloud_connection_id": value.ConnectionID,
			"execution_status": value.Execution, "outcome_status": value.Outcome, "resource_status": value.Resource,
			"revision": value.Revision, "created_at": value.CreatedAt, "updated_at": value.UpdatedAt,
		}, nil
	case "cloud.service.changed":
		var value serviceProjectionPayload
		if err := decodeStrictProjectionJSON(raw, &value); err != nil || !validServiceProjection(value) {
			return nil, errors.New("invalid cloud service projection")
		}
		payload := map[string]any{
			"service_id": value.ServiceID, "deployment_id": value.DeploymentID, "recipe_id": value.RecipeID, "name": value.Name,
			"service_status": value.Status, "integration_status": value.Integration, "revision": value.Revision,
			"created_at": value.CreatedAt, "updated_at": value.UpdatedAt,
		}
		if value.Backups != nil {
			payload["backups"] = serviceBackupProjectionSummaries(value.Backups)
		}
		if value.Restores != nil {
			payload["restores"] = serviceRestoreProjectionSummaries(value.Restores)
		}
		return payload, nil
	case "cloud.connection.changed":
		var value connectionProjectionPayload
		if err := decodeStrictProjectionJSON(raw, &value); err != nil || !validConnectionProjection(value) {
			return nil, errors.New("invalid cloud connection projection")
		}
		return map[string]any{
			"cloud_connection_id": value.ConnectionID, "provider": value.Provider, "account_id": value.AccountID,
			"region": value.Region, "mode": value.Mode, "status": value.Status, "revision": value.Revision,
			"created_at": value.CreatedAt, "updated_at": value.UpdatedAt,
		}, nil
	case "cloud.alert.raised":
		var value alertProjectionPayload
		if err := decodeStrictProjectionJSON(raw, &value); err != nil || !validAlertProjection(value) {
			return nil, errors.New("invalid cloud alert projection")
		}
		return map[string]any{
			"alert_id": value.AlertID, "deployment_id": value.DeploymentID, "service_id": value.ServiceID,
			"severity": value.Severity, "code": value.Code, "message": value.Message, "acknowledged": value.Acknowledged,
			"revision": value.Revision, "created_at": value.CreatedAt, "updated_at": value.UpdatedAt,
		}, nil
	default:
		return nil, errors.New("cloud event type is not projectable")
	}
}

func decodeStrictProjectionJSON(raw string, target any) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("cloud projection contains trailing JSON")
	}
	return nil
}

func validGoalProjection(value goalProjectionPayload) bool {
	return validProjectionIdentifier(value.GoalID) && validProjectionIdentifier(value.PlanID) && validOptionalProjectionIdentifier(value.ConnectionID) &&
		allowedProjectionValue(value.Status, "researching") && value.Revision > 0 && value.CreatedAt > 0 && value.UpdatedAt >= value.CreatedAt
}

func validPlanProjection(value planProjectionPayload) bool {
	return validProjectionIdentifier(value.PlanID) && validProjectionIdentifier(value.GoalID) && validOptionalProjectionIdentifier(value.ConnectionID) &&
		allowedProjectionValue(value.Status, PlanStatusResearching, PlanStatusQuoting, PlanStatusReadyForConfirmation, PlanStatusApproved, PlanStatusExpired, PlanStatusSuperseded) &&
		validVisibleProjectionText(value.Title, 160, true) && validVisibleProjectionText(value.Summary, 2_000, true) &&
		validOptionalProjectionIdentifier(value.RecipeDigest) && validOptionalProjectionIdentifier(value.QuoteID) && validOptionalProjectionIdentifier(value.PlanHash) &&
		value.Revision > 0 && value.CreatedAt > 0 && value.UpdatedAt >= value.CreatedAt
}

func validJobProjection(value jobProjectionPayload) bool {
	planBindingValid := validProjectionIdentifier(value.PlanID)
	if value.Kind == "connection_registration" {
		// A Connection Stack verification happens before any deployment Plan
		// exists. It can be projected as progress, but must not invent a Plan or
		// Deployment identity just to fit the generic job view.
		planBindingValid = value.PlanID == "" && value.DeploymentID == ""
	}
	return validProjectionIdentifier(value.JobID) && planBindingValid && validOptionalProjectionIdentifier(value.DeploymentID) &&
		allowedProjectionValue(value.Kind, "research", "quote", "provision", "install", "verify", "backup", "destroy", "connection_registration") &&
		allowedProjectionValue(value.ExecutionStatus, "queued", "provisioning", "installing", "waiting_user", "verifying", "finished") &&
		allowedProjectionValue(value.OutcomeStatus, "pending", "succeeded", "failed", "canceled", "interrupted") &&
		validVisibleProjectionText(value.Checkpoint, 128, true) && validProjectionErrorCode(value.ErrorCode) &&
		value.Revision > 0 && value.CreatedAt > 0 && value.UpdatedAt >= value.CreatedAt
}

func validDeploymentProjection(value deploymentProjectionPayload) bool {
	return validProjectionIdentifier(value.DeploymentID) && validProjectionIdentifier(value.PlanID) && validProjectionIdentifier(value.ConnectionID) &&
		allowedProjectionValue(value.Execution, "queued", "provisioning", "installing", "waiting_user", "waiting_user_pairing", "verifying", "finished") &&
		allowedProjectionValue(value.Outcome, "pending", "succeeded", "failed", "canceled", "interrupted") &&
		allowedProjectionValue(value.Resource, "none", "active", "retained_tracked", "destroying", "verified_destroyed", "blocked", "orphaned") &&
		value.Revision > 0 && value.CreatedAt > 0 && value.UpdatedAt >= value.CreatedAt
}

func validServiceProjection(value serviceProjectionPayload) bool {
	if !(validProjectionIdentifier(value.ServiceID) && validProjectionIdentifier(value.DeploymentID) && validProjectionIdentifier(value.RecipeID) &&
		validVisibleProjectionText(value.Name, 160, false) &&
		allowedProjectionValue(value.Status, "experimental", "awaiting_management_acceptance", "active", "stopped", "degraded", "destroying", "destroyed") &&
		allowedProjectionValue(value.Integration, "not_requested", "pending", "connected", "degraded", "failed", "disconnected") &&
		value.Revision > 0 && value.CreatedAt > 0 && value.UpdatedAt >= value.CreatedAt) {
		return false
	}
	seen := make(map[string]struct{}, len(value.Backups))
	for _, backup := range value.Backups {
		if !validServiceBackupProjection(backup, value.ServiceID, value.DeploymentID) {
			return false
		}
		if _, exists := seen[backup.BackupID]; exists {
			return false
		}
		seen[backup.BackupID] = struct{}{}
	}
	seenRestores := make(map[string]struct{}, len(value.Restores))
	for _, restore := range value.Restores {
		if !validServiceRestoreProjection(restore, value.ServiceID, value.DeploymentID) {
			return false
		}
		if _, exists := seenRestores[restore.RestoreID]; exists {
			return false
		}
		seenRestores[restore.RestoreID] = struct{}{}
	}
	return true
}

func validServiceRestoreProjection(value serviceRestoreProjectionPayload, serviceID, deploymentID string) bool {
	if !(validProjectionIdentifier(value.RestoreID) && validProjectionIdentifier(value.RestorePlanID) && validProjectionIdentifier(value.BackupID) &&
		value.ServiceID == serviceID && value.DeploymentID == deploymentID &&
		allowedProjectionValue(value.Status, "queued", "running", "verifying", "succeeded", "failed", "restore_blocked") &&
		value.Revision > 0 && value.CreatedAt > 0 && value.UpdatedAt >= value.CreatedAt) {
		return false
	}
	if (value.Status == "queued" || value.Status == "running") && len(value.OriginalVolumeIDs) == 0 && len(value.ReplacementVolumeIDs) == 0 {
		return true
	}
	if len(value.OriginalVolumeIDs) == 0 || len(value.ReplacementVolumeIDs) == 0 {
		return false
	}
	seen := map[string]struct{}{}
	for _, volumeID := range append(append([]string(nil), value.OriginalVolumeIDs...), value.ReplacementVolumeIDs...) {
		if !validEC2ResourceID(volumeID, "vol-") {
			return false
		}
		if _, exists := seen[volumeID]; exists {
			return false
		}
		seen[volumeID] = struct{}{}
	}
	return true
}

func validServiceBackupProjection(value serviceBackupProjectionPayload, serviceID, deploymentID string) bool {
	if !validProjectionIdentifier(value.BackupID) || value.ServiceID != serviceID || value.DeploymentID != deploymentID ||
		value.RetentionPolicy != "manual" || value.Revision <= 0 || value.CreatedAt <= 0 || value.UpdatedAt < value.CreatedAt {
		return false
	}
	switch value.Status {
	case "available":
		if !validEC2ResourceID(value.ImageID, "ami-") || len(value.SnapshotIDs) == 0 {
			return false
		}
		seen := make(map[string]struct{}, len(value.SnapshotIDs))
		for _, snapshotID := range value.SnapshotIDs {
			if !validEC2ResourceID(snapshotID, "snap-") {
				return false
			}
			if _, exists := seen[snapshotID]; exists {
				return false
			}
			seen[snapshotID] = struct{}{}
		}
		return true
	case "failed":
		return value.ImageID == "" && len(value.SnapshotIDs) == 0
	default:
		return false
	}
}

func validEC2ResourceID(value, prefix string) bool {
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(value, prefix)
	if len(suffix) < 8 || len(suffix) > 17 {
		return false
	}
	for _, char := range suffix {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func serviceBackupProjectionSummaries(values []serviceBackupProjectionPayload) []any {
	result := make([]any, 0, len(values))
	for _, value := range values {
		result = append(result, map[string]any{
			"backup_id": value.BackupID, "service_id": value.ServiceID, "deployment_id": value.DeploymentID,
			"status": value.Status, "retention_policy": value.RetentionPolicy, "image_id": value.ImageID,
			"snapshot_ids": append([]string(nil), value.SnapshotIDs...), "revision": value.Revision,
			"created_at": value.CreatedAt, "updated_at": value.UpdatedAt,
		})
	}
	return result
}

func serviceRestoreProjectionSummaries(values []serviceRestoreProjectionPayload) []any {
	result := make([]any, 0, len(values))
	for _, value := range values {
		result = append(result, map[string]any{
			"restore_id": value.RestoreID, "restore_plan_id": value.RestorePlanID, "service_id": value.ServiceID,
			"deployment_id": value.DeploymentID, "backup_id": value.BackupID, "status": value.Status,
			"original_volume_ids": append([]string(nil), value.OriginalVolumeIDs...), "replacement_volume_ids": append([]string(nil), value.ReplacementVolumeIDs...),
			"revision": value.Revision, "created_at": value.CreatedAt, "updated_at": value.UpdatedAt,
		})
	}
	return result
}

func validConnectionProjection(value connectionProjectionPayload) bool {
	return validProjectionIdentifier(value.ConnectionID) && value.Provider == "aws" &&
		validVisibleProjectionText(value.AccountID, 12, false) && validVisibleProjectionText(value.Region, 32, false) &&
		value.Mode == "connection_stack_v2" && value.Status == "active" && value.Revision > 0 &&
		value.CreatedAt > 0 && value.UpdatedAt >= value.CreatedAt
}

func validAlertProjection(value alertProjectionPayload) bool {
	return validProjectionIdentifier(value.AlertID) && validOptionalProjectionIdentifier(value.DeploymentID) && validOptionalProjectionIdentifier(value.ServiceID) &&
		(value.DeploymentID != "" || value.ServiceID != "") && allowedProjectionValue(value.Severity, "warning", "critical") &&
		validProjectionErrorCode(value.Code) && value.Code != "" && validVisibleProjectionText(value.Message, 500, false) &&
		value.Revision > 0 && value.CreatedAt > 0 && value.UpdatedAt >= value.CreatedAt
}

func validOptionalProjectionIdentifier(value string) bool {
	return value == "" || validProjectionIdentifier(value)
}

func validVisibleProjectionText(value string, maximum int, optional bool) bool {
	if value == "" && optional {
		return true
	}
	return strings.TrimSpace(value) == value && utf8.RuneCountInString(value) > 0 && utf8.RuneCountInString(value) <= maximum &&
		!strings.ContainsAny(value, "\r\n\t\x00") && !ContainsSensitiveGoalMaterial(value)
}

func validProjectionErrorCode(value string) bool {
	if value == "" {
		return true
	}
	if len(value) > 96 || strings.TrimSpace(value) != value || ContainsSensitiveGoalMaterial(value) {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' && character != '-' {
			return false
		}
	}
	return true
}

func allowedProjectionValue(value string, allowed ...string) bool {
	for _, item := range allowed {
		if value == item {
			return true
		}
	}
	return false
}
