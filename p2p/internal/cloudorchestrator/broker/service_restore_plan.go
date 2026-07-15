package broker

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
)

const ServiceRestorePlanAction = "service.restore.plan"
const ServiceRestorePlanSchema = "dirextalk.aws.service-restore-plan/v1"
const ServiceRestorePlanResultSchema = "dirextalk.aws.service-restore-plan-result/v1"

var restoreSnapshotIDPattern = regexp.MustCompile(`^snap-[0-9a-f]{8,17}$`)
var restoreDevicePattern = regexp.MustCompile(`^/dev/(?:xvd|sd)[a-z][0-9]*$`)

type ServiceRestoreSnapshotRef struct {
	OriginalVolumeID string `json:"original_volume_id"`
	SnapshotID       string `json:"snapshot_id"`
}

type ServiceRestorePlanRequest struct {
	Schema, RestorePlanID, ServiceID, DeploymentID, BackupID string
	InstanceID, Region, ImageID                              string
	SnapshotRefs                                             []ServiceRestoreSnapshotRef
}

func (r ServiceRestorePlanRequest) MarshalJSON() ([]byte, error) {
	type wire struct {
		Schema        string                      `json:"schema"`
		RestorePlanID string                      `json:"restore_plan_id"`
		ServiceID     string                      `json:"service_id"`
		DeploymentID  string                      `json:"deployment_id"`
		BackupID      string                      `json:"backup_id"`
		InstanceID    string                      `json:"instance_id"`
		Region        string                      `json:"region"`
		ImageID       string                      `json:"image_id"`
		SnapshotRefs  []ServiceRestoreSnapshotRef `json:"snapshot_refs"`
	}
	return json.Marshal(wire{r.Schema, r.RestorePlanID, r.ServiceID, r.DeploymentID, r.BackupID, r.InstanceID, r.Region, r.ImageID, r.SnapshotRefs})
}

func (r *ServiceRestorePlanRequest) UnmarshalJSON(raw []byte) error {
	type wire struct {
		Schema        string                      `json:"schema"`
		RestorePlanID string                      `json:"restore_plan_id"`
		ServiceID     string                      `json:"service_id"`
		DeploymentID  string                      `json:"deployment_id"`
		BackupID      string                      `json:"backup_id"`
		InstanceID    string                      `json:"instance_id"`
		Region        string                      `json:"region"`
		ImageID       string                      `json:"image_id"`
		SnapshotRefs  []ServiceRestoreSnapshotRef `json:"snapshot_refs"`
	}
	var v wire
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	*r = ServiceRestorePlanRequest{v.Schema, v.RestorePlanID, v.ServiceID, v.DeploymentID, v.BackupID, v.InstanceID, v.Region, v.ImageID, v.SnapshotRefs}
	return nil
}

type ServiceRestorePlanCommand struct {
	Schema             string `json:"schema"`
	ConnectionID       string `json:"connection_id"`
	CommandID          string `json:"command_id"`
	NodeKeyID          string `json:"node_key_id"`
	IssuedAt           string `json:"issued_at"`
	ExpiresAt          string `json:"expires_at"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	Action             string `json:"action"`
	PayloadB64         string `json:"payload_b64"`
	PayloadSHA256      string `json:"payload_sha256"`
	SignatureB64       string `json:"signature_b64"`
}
type ServiceRestorePlanCommandInput struct {
	ConnectionID, CommandID, NodeKeyID string
	ExpectedGeneration, NodeCounter    int64
	IssuedAt, ExpiresAt                time.Time
	Request                            ServiceRestorePlanRequest
	PrivateKey                         ed25519.PrivateKey
}
type ServiceRestorePlanCommandBinding struct {
	ConnectionID, CommandID, NodeKeyID string
	ExpectedGeneration, NodeCounter    int64
	IssuedAt, ExpiresAt                time.Time
	Request                            ServiceRestorePlanRequest
}

type ServiceRestoreVolumeSwap struct {
	OriginalVolumeID    string `json:"original_volume_id"`
	SnapshotID          string `json:"snapshot_id"`
	DeviceName          string `json:"device_name"`
	VolumeType          string `json:"volume_type"`
	SizeGiB             int64  `json:"size_gib"`
	IOPS                int64  `json:"iops"`
	ThroughputMiB       int64  `json:"throughput_mib"`
	Encrypted           bool   `json:"encrypted"`
	DeleteOnTermination bool   `json:"delete_on_termination"`
}
type ServiceRestorePlan struct {
	Schema                  string                     `json:"schema"`
	RestorePlanID           string                     `json:"restore_plan_id"`
	ConnectionID            string                     `json:"connection_id"`
	CommandID               string                     `json:"command_id"`
	RequestSHA256           string                     `json:"request_sha256"`
	ServiceID               string                     `json:"service_id"`
	DeploymentID            string                     `json:"deployment_id"`
	BackupID                string                     `json:"backup_id"`
	InstanceID              string                     `json:"instance_id"`
	Region                  string                     `json:"region"`
	AvailabilityZone        string                     `json:"availability_zone"`
	RestoreMode             string                     `json:"restore_mode"`
	DowntimeRequired        bool                       `json:"downtime_required"`
	OriginalVolumeRetention string                     `json:"original_volume_retention"`
	FailurePolicy           string                     `json:"failure_policy"`
	QuoteID                 string                     `json:"quote_id"`
	Currency                string                     `json:"currency"`
	EstimatedHourlyMinor    int64                      `json:"estimated_hourly_minor"`
	EstimatedThirtyDayMinor int64                      `json:"estimated_thirty_day_minor"`
	QuotedAt                string                     `json:"quoted_at"`
	ValidUntil              string                     `json:"valid_until"`
	Unincluded              []string                   `json:"unincluded"`
	VolumeSwaps             []ServiceRestoreVolumeSwap `json:"volume_swaps"`
}
type ServiceRestorePlanResult struct {
	Schema  string                   `json:"schema"`
	Status  string                   `json:"status"`
	Receipt DeploymentCommandReceipt `json:"receipt"`
	Plan    ServiceRestorePlan       `json:"plan"`
}

func NewServiceRestorePlanCommand(i ServiceRestorePlanCommandInput) (ServiceRestorePlanCommand, error) {
	if len(i.PrivateKey) != ed25519.PrivateKeySize {
		return ServiceRestorePlanCommand{}, newError("invalid_node_private_key", nil)
	}
	i.Request = normalizeServiceRestorePlanRequest(i.Request)
	if validateServiceRestorePlanRequest(i.Request) != nil {
		return ServiceRestorePlanCommand{}, newError("invalid_service_restore_plan_request", nil)
	}
	payload, _ := json.Marshal(i.Request)
	c := ServiceRestorePlanCommand{Schema: CommandSchema, ConnectionID: i.ConnectionID, CommandID: i.CommandID, NodeKeyID: i.NodeKeyID, IssuedAt: canonicalInstant(i.IssuedAt), ExpiresAt: canonicalInstant(i.ExpiresAt), ExpectedGeneration: i.ExpectedGeneration, NodeCounter: i.NodeCounter, Action: ServiceRestorePlanAction, PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: sha256Hex(payload)}
	if validateServiceRestorePlanCommand(c, false) != nil {
		return ServiceRestorePlanCommand{}, newError("invalid_command", nil)
	}
	c.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(i.PrivateKey, []byte(c.SignatureBase())))
	return c, c.Validate()
}
func (c ServiceRestorePlanCommand) SignatureBase() string {
	return nodeSignatureBase(nodeSignatureFields{Schema: c.Schema, ConnectionID: c.ConnectionID, CommandID: c.CommandID, NodeKeyID: c.NodeKeyID, IssuedAt: c.IssuedAt, ExpiresAt: c.ExpiresAt, ExpectedGeneration: c.ExpectedGeneration, NodeCounter: c.NodeCounter, Action: c.Action, PayloadSHA256: c.PayloadSHA256})
}
func (c ServiceRestorePlanCommand) RequestSHA256() string {
	return sha256Hex([]byte(c.SignatureBase()))
}
func (c ServiceRestorePlanCommand) Validate() error {
	return validateServiceRestorePlanCommand(c, true)
}
func (c ServiceRestorePlanCommand) Request() (ServiceRestorePlanRequest, error) {
	if c.Validate() != nil {
		return ServiceRestorePlanRequest{}, newError("invalid_command", nil)
	}
	raw, _ := base64.StdEncoding.DecodeString(c.PayloadB64)
	return decodeServiceRestorePlanRequest(raw)
}
func (c ServiceRestorePlanCommand) ValidateBinding(b ServiceRestorePlanCommandBinding) error {
	r, e := c.Request()
	if e != nil || c.ConnectionID != b.ConnectionID || c.CommandID != b.CommandID || c.NodeKeyID != b.NodeKeyID || c.ExpectedGeneration != b.ExpectedGeneration || c.NodeCounter != b.NodeCounter || c.IssuedAt != canonicalInstant(b.IssuedAt) || c.ExpiresAt != canonicalInstant(b.ExpiresAt) || !reflect.DeepEqual(r, normalizeServiceRestorePlanRequest(b.Request)) {
		return newError("invalid_service_restore_plan_request", e)
	}
	return nil
}
func ParseServiceRestorePlanCommand(raw []byte) (ServiceRestorePlanCommand, error) {
	if _, e := exactJSONObject(raw, commandFields); e != nil {
		return ServiceRestorePlanCommand{}, newError("invalid_command", e)
	}
	var c ServiceRestorePlanCommand
	if decodeStrictJSON(raw, &c) != nil || c.Validate() != nil {
		return c, newError("invalid_command", nil)
	}
	return c, nil
}
func validateServiceRestorePlanCommand(c ServiceRestorePlanCommand, sig bool) error {
	if c.Schema != CommandSchema || c.Action != ServiceRestorePlanAction || !idPattern.MatchString(c.ConnectionID) || !idPattern.MatchString(c.CommandID) || !keyIDPattern.MatchString(c.NodeKeyID) || !safePositive(c.ExpectedGeneration) || !safeNonnegative(c.NodeCounter) || !sha256Pattern.MatchString(c.PayloadSHA256) {
		return newError("invalid_command", nil)
	}
	issued, e := parseCanonicalInstant(c.IssuedAt)
	if e != nil {
		return e
	}
	expires, e := parseCanonicalInstant(c.ExpiresAt)
	if e != nil || !expires.After(issued) || expires.Sub(issued) > maxCommandLifetime {
		return newError("invalid_command", e)
	}
	raw, e := decodeCanonicalBase64(c.PayloadB64)
	if e != nil || sha256Hex(raw) != c.PayloadSHA256 {
		return newError("invalid_payload", e)
	}
	r, e := decodeServiceRestorePlanRequest(raw)
	if e != nil {
		return e
	}
	canonical, _ := json.Marshal(r)
	if !bytes.Equal(raw, canonical) {
		return newError("noncanonical_payload", nil)
	}
	if sig {
		s, e := decodeCanonicalBase64(c.SignatureB64)
		if e != nil || len(s) != ed25519.SignatureSize {
			return newError("invalid_command", e)
		}
	}
	return nil
}
func validateServiceRestorePlanRequest(r ServiceRestorePlanRequest) error {
	if r.Schema != ServiceRestorePlanSchema || !idPattern.MatchString(r.RestorePlanID) || !idPattern.MatchString(r.ServiceID) || !idPattern.MatchString(r.DeploymentID) || !idPattern.MatchString(r.BackupID) || !instanceIDPattern.MatchString(r.InstanceID) || !regionPattern.MatchString(r.Region) || !backupImageIDPattern.MatchString(r.ImageID) || len(r.SnapshotRefs) == 0 {
		return newError("invalid_service_restore_plan_request", nil)
	}
	volumes, snaps := map[string]bool{}, map[string]bool{}
	for _, x := range r.SnapshotRefs {
		if !volumeIDPattern.MatchString(x.OriginalVolumeID) || !restoreSnapshotIDPattern.MatchString(x.SnapshotID) || volumes[x.OriginalVolumeID] || snaps[x.SnapshotID] {
			return newError("invalid_service_restore_plan_request", nil)
		}
		volumes[x.OriginalVolumeID] = true
		snaps[x.SnapshotID] = true
	}
	return nil
}
func normalizeServiceRestorePlanRequest(r ServiceRestorePlanRequest) ServiceRestorePlanRequest {
	sort.Slice(r.SnapshotRefs, func(i, j int) bool { return r.SnapshotRefs[i].OriginalVolumeID < r.SnapshotRefs[j].OriginalVolumeID })
	return r
}
func decodeServiceRestorePlanRequest(raw []byte) (ServiceRestorePlanRequest, error) {
	o, e := exactJSONObject(raw, []string{"schema", "restore_plan_id", "service_id", "deployment_id", "backup_id", "instance_id", "region", "image_id", "snapshot_refs"})
	if e != nil {
		return ServiceRestorePlanRequest{}, e
	}
	if _, e = exactJSONArray(o["snapshot_refs"]); e != nil {
		return ServiceRestorePlanRequest{}, e
	}
	var r ServiceRestorePlanRequest
	if decodeStrictJSON(raw, &r) != nil {
		return r, newError("invalid_payload", nil)
	}
	r = normalizeServiceRestorePlanRequest(r)
	return r, validateServiceRestorePlanRequest(r)
}

func decodeServiceRestorePlanResult(raw []byte) (ServiceRestorePlanResult, error) {
	o, e := exactJSONObject(raw, []string{"schema", "status", "receipt", "plan"})
	if e != nil {
		return ServiceRestorePlanResult{}, e
	}
	if _, e = exactJSONObject(o["receipt"], deploymentCommandReceiptFields); e != nil {
		return ServiceRestorePlanResult{}, e
	}
	p, e := exactJSONObject(o["plan"], []string{"schema", "restore_plan_id", "connection_id", "command_id", "request_sha256", "service_id", "deployment_id", "backup_id", "instance_id", "region", "availability_zone", "restore_mode", "downtime_required", "original_volume_retention", "failure_policy", "quote_id", "currency", "estimated_hourly_minor", "estimated_thirty_day_minor", "quoted_at", "valid_until", "unincluded", "volume_swaps"})
	if e != nil {
		return ServiceRestorePlanResult{}, e
	}
	swaps, e := exactJSONArray(p["volume_swaps"])
	if e != nil || len(swaps) == 0 {
		return ServiceRestorePlanResult{}, e
	}
	for _, rawSwap := range swaps {
		if _, e = exactJSONObject(rawSwap, []string{"original_volume_id", "snapshot_id", "device_name", "volume_type", "size_gib", "iops", "throughput_mib", "encrypted", "delete_on_termination"}); e != nil {
			return ServiceRestorePlanResult{}, e
		}
	}
	var r ServiceRestorePlanResult
	if decodeStrictJSON(raw, &r) != nil {
		return r, newError("invalid_broker_response", nil)
	}
	return r, nil
}
func ValidateServiceRestorePlanResult(c ServiceRestorePlanCommand, r ServiceRestorePlanResult) error {
	if c.Validate() != nil || r.Schema != ServiceRestorePlanResultSchema || (r.Status != "restore_plan_ready" && r.Status != "idempotent") || r.Receipt.Schema != ReceiptSchema || (r.Receipt.Disposition != "committed" && r.Receipt.Disposition != "idempotent") || (r.Status == "restore_plan_ready") != (r.Receipt.Disposition == "committed") || r.Receipt.ConnectionID != c.ConnectionID || r.Receipt.ExpectedGeneration != c.ExpectedGeneration || r.Receipt.NodeCounter != c.NodeCounter || r.Receipt.CommandID != c.CommandID || r.Receipt.RequestSHA256 != c.RequestSHA256() || r.Receipt.Action != ServiceRestorePlanAction {
		return newError("invalid_service_restore_plan_result", nil)
	}
	q, e := c.Request()
	if e != nil {
		return e
	}
	p := r.Plan
	sort.Strings(p.Unincluded)
	sort.Slice(p.VolumeSwaps, func(i, j int) bool { return p.VolumeSwaps[i].OriginalVolumeID < p.VolumeSwaps[j].OriginalVolumeID })
	if p.Schema != ServiceRestorePlanSchema || p.RestorePlanID != q.RestorePlanID || p.ConnectionID != c.ConnectionID || p.CommandID != c.CommandID || p.RequestSHA256 != c.RequestSHA256() || p.ServiceID != q.ServiceID || p.DeploymentID != q.DeploymentID || p.BackupID != q.BackupID || p.InstanceID != q.InstanceID || p.Region != q.Region || !availabilityZonePattern.MatchString(p.AvailabilityZone) || !strings.HasPrefix(p.AvailabilityZone, p.Region) || len(p.AvailabilityZone) != len(p.Region)+1 || p.RestoreMode != "in_place" || !p.DowntimeRequired || p.OriginalVolumeRetention != "manual" || p.FailurePolicy != "reattach_original" || p.Currency != "USD" || p.EstimatedHourlyMinor < 0 || p.EstimatedThirtyDayMinor <= 0 || !idPattern.MatchString(p.QuoteID) || len(p.VolumeSwaps) != len(q.SnapshotRefs) {
		return newError("invalid_service_restore_plan_result", nil)
	}
	quoted, e := parseCanonicalInstant(p.QuotedAt)
	if e != nil {
		return e
	}
	valid, e := parseCanonicalInstant(p.ValidUntil)
	if e != nil || !valid.After(quoted) || valid.Sub(quoted) != QuoteValidity {
		return newError("invalid_service_restore_plan_result", e)
	}
	for i, x := range p.VolumeSwaps {
		ref := q.SnapshotRefs[i]
		if x.OriginalVolumeID != ref.OriginalVolumeID || x.SnapshotID != ref.SnapshotID || !restoreDevicePattern.MatchString(x.DeviceName) || !validRestoreVolumeType(x.VolumeType) || x.SizeGiB <= 0 || x.IOPS < 0 || x.ThroughputMiB < 0 || !x.Encrypted {
			return newError("invalid_service_restore_plan_result", nil)
		}
	}
	return nil
}
func validRestoreVolumeType(v string) bool {
	switch v {
	case "gp2", "gp3", "io1", "io2", "st1", "sc1", "standard":
		return true
	}
	return false
}
