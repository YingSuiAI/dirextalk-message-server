package contract

import (
	"encoding/json"
	"regexp"
	"sort"
	"time"
)

const (
	ServiceRestorePlanSchema       = "dirextalk.aws.service-restore-plan/v1"
	ServiceRestorePlanResultSchema = "dirextalk.aws.service-restore-plan-result/v1"
	ServiceRestorePlanValidity     = 15 * time.Minute
)

var (
	restorePlanSnapshotPattern = regexp.MustCompile(`^snap-[0-9a-f]{8,17}$`)
	restorePlanImagePattern    = regexp.MustCompile(`^ami-[0-9a-f]{8,17}$`)
	restorePlanDevicePattern   = regexp.MustCompile(`^/dev/(?:xvd|sd)[a-z][0-9]*$`)
)

type ServiceRestoreSnapshotRef struct {
	OriginalVolumeID string `json:"original_volume_id"`
	SnapshotID       string `json:"snapshot_id"`
}

type ServiceRestorePlanRequest struct {
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

func (c Command) ServiceRestorePlanRequest() (ServiceRestorePlanRequest, error) {
	if c.Action != ActionServiceRestorePlan {
		return ServiceRestorePlanRequest{}, errCode("invalid_payload")
	}
	raw, err := decodeCanonicalBase64(c.PayloadB64)
	if err != nil {
		return ServiceRestorePlanRequest{}, errCode("invalid_payload")
	}
	object, err := exactJSONObject(raw)
	if err != nil || !exactFields(object, []string{"schema", "restore_plan_id", "service_id", "deployment_id", "backup_id", "instance_id", "region", "image_id", "snapshot_refs"}) {
		return ServiceRestorePlanRequest{}, errCode("invalid_payload")
	}
	var request ServiceRestorePlanRequest
	if decodeSingle(raw, &request) != nil || request.validate() != nil {
		return ServiceRestorePlanRequest{}, errCode("invalid_payload")
	}
	request.normalize()
	return request, nil
}

func (r ServiceRestorePlanRequest) validate() error {
	if r.Schema != ServiceRestorePlanSchema || !idPattern.MatchString(r.RestorePlanID) || !idPattern.MatchString(r.ServiceID) || !idPattern.MatchString(r.DeploymentID) || !idPattern.MatchString(r.BackupID) || !destroyInstanceIDPattern.MatchString(r.InstanceID) || !regionPattern.MatchString(r.Region) || !restorePlanImagePattern.MatchString(r.ImageID) || len(r.SnapshotRefs) == 0 {
		return errCode("invalid_payload")
	}
	volumes, snapshots := map[string]bool{}, map[string]bool{}
	for _, ref := range r.SnapshotRefs {
		if !destroyVolumeIDPattern.MatchString(ref.OriginalVolumeID) || !restorePlanSnapshotPattern.MatchString(ref.SnapshotID) || volumes[ref.OriginalVolumeID] || snapshots[ref.SnapshotID] {
			return errCode("invalid_payload")
		}
		volumes[ref.OriginalVolumeID], snapshots[ref.SnapshotID] = true, true
	}
	return nil
}

func (r *ServiceRestorePlanRequest) normalize() {
	sort.Slice(r.SnapshotRefs, func(i, j int) bool { return r.SnapshotRefs[i].OriginalVolumeID < r.SnapshotRefs[j].OriginalVolumeID })
}

func MarshalCommittedServiceRestorePlanResult(command Command, plan ServiceRestorePlan) ([]byte, error) {
	request, err := command.ServiceRestorePlanRequest()
	if err != nil {
		return nil, err
	}
	requestSHA, _ := command.RequestSHA256()
	plan.normalize()
	if plan.validate(command, request) != nil {
		return nil, errCode("provider_readback_invalid")
	}
	return json.Marshal(ServiceRestorePlanResult{Schema: ServiceRestorePlanResultSchema, Status: "restore_plan_ready", Receipt: DeploymentCommandReceipt{Schema: ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, CommandID: command.CommandID, RequestSHA256: requestSHA, Action: ActionServiceRestorePlan}, Plan: plan})
}

func ValidateServiceRestorePlanResult(command Command, result ServiceRestorePlanResult) error {
	requestSHA, err := command.RequestSHA256()
	if err != nil || result.Schema != ServiceRestorePlanResultSchema || (result.Status != "restore_plan_ready" && result.Status != "idempotent") || result.Receipt.Schema != ReceiptSchema || result.Receipt.ConnectionID != command.ConnectionID || result.Receipt.ExpectedGeneration != command.ExpectedGeneration || result.Receipt.NodeCounter != command.NodeCounter || result.Receipt.CommandID != command.CommandID || result.Receipt.RequestSHA256 != requestSHA || result.Receipt.Action != ActionServiceRestorePlan {
		return errCode("invalid_result")
	}
	request, err := command.ServiceRestorePlanRequest()
	if err != nil {
		return err
	}
	result.Plan.normalize()
	return result.Plan.validate(command, request)
}

func decodeServiceRestorePlanResult(raw []byte, result *ServiceRestorePlanResult) error {
	object, err := exactJSONObject(raw)
	if err != nil || !exactFields(object, []string{"schema", "status", "receipt", "plan"}) {
		return errCode("receipt_store_invalid")
	}
	receipt, err := exactJSONObject(object["receipt"])
	if err != nil || !exactFields(receipt, []string{"schema", "disposition", "connection_id", "expected_generation", "node_counter", "command_id", "request_sha256", "action"}) {
		return errCode("receipt_store_invalid")
	}
	plan, err := exactJSONObject(object["plan"])
	if err != nil || !exactFields(plan, []string{"schema", "restore_plan_id", "connection_id", "command_id", "request_sha256", "service_id", "deployment_id", "backup_id", "instance_id", "region", "availability_zone", "restore_mode", "downtime_required", "original_volume_retention", "failure_policy", "quote_id", "currency", "estimated_hourly_minor", "estimated_thirty_day_minor", "quoted_at", "valid_until", "unincluded", "volume_swaps"}) {
		return errCode("receipt_store_invalid")
	}
	var swaps []json.RawMessage
	if decodeSingle(plan["volume_swaps"], &swaps) != nil || len(swaps) == 0 {
		return errCode("receipt_store_invalid")
	}
	for _, rawSwap := range swaps {
		swap, swapErr := exactJSONObject(rawSwap)
		if swapErr != nil || !exactFields(swap, []string{"original_volume_id", "snapshot_id", "device_name", "volume_type", "size_gib", "iops", "throughput_mib", "encrypted", "delete_on_termination"}) {
			return errCode("receipt_store_invalid")
		}
	}
	if decodeSingle(raw, result) != nil {
		return errCode("receipt_store_invalid")
	}
	return nil
}

func (p *ServiceRestorePlan) normalize() {
	sort.Strings(p.Unincluded)
	sort.Slice(p.VolumeSwaps, func(i, j int) bool { return p.VolumeSwaps[i].OriginalVolumeID < p.VolumeSwaps[j].OriginalVolumeID })
}

func (p ServiceRestorePlan) validate(command Command, request ServiceRestorePlanRequest) error {
	if p.Schema != ServiceRestorePlanSchema || p.RestorePlanID != request.RestorePlanID || p.ConnectionID != command.ConnectionID || p.CommandID != command.CommandID || p.ServiceID != request.ServiceID || p.DeploymentID != request.DeploymentID || p.BackupID != request.BackupID || p.InstanceID != request.InstanceID || p.Region != request.Region || !ValidAvailabilityZone(p.Region, p.AvailabilityZone) || p.RestoreMode != "in_place" || !p.DowntimeRequired || p.OriginalVolumeRetention != "manual" || p.FailurePolicy != "reattach_original" || p.Currency != "USD" || p.EstimatedHourlyMinor < 0 || p.EstimatedThirtyDayMinor <= 0 || !idPattern.MatchString(p.QuoteID) || len(p.VolumeSwaps) != len(request.SnapshotRefs) {
		return errCode("invalid_result")
	}
	requestSHA, _ := command.RequestSHA256()
	if p.RequestSHA256 != requestSHA {
		return errCode("invalid_result")
	}
	quoted, err := parseCanonicalInstant(p.QuotedAt)
	if err != nil {
		return errCode("invalid_result")
	}
	valid, err := parseCanonicalInstant(p.ValidUntil)
	if err != nil || !valid.After(quoted) || valid.Sub(quoted) != ServiceRestorePlanValidity {
		return errCode("invalid_result")
	}
	for i, swap := range p.VolumeSwaps {
		ref := request.SnapshotRefs[i]
		if swap.OriginalVolumeID != ref.OriginalVolumeID || swap.SnapshotID != ref.SnapshotID || !restorePlanDevicePattern.MatchString(swap.DeviceName) || !validRestorePlanVolumeType(swap.VolumeType) || swap.SizeGiB <= 0 || swap.IOPS < 0 || swap.ThroughputMiB < 0 || !swap.Encrypted {
			return errCode("invalid_result")
		}
	}
	return nil
}

func validRestorePlanVolumeType(value string) bool {
	switch value {
	case "gp2", "gp3", "io1", "io2", "st1", "sc1", "standard":
		return true
	default:
		return false
	}
}
