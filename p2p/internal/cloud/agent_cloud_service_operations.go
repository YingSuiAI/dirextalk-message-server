package cloud

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"

	"github.com/fxamacker/cbor/v2"
)

const (
	agentCloudVolumeScopeSchemaV1        = "dirextalk.agent.cloud.volume-scope/v1"
	agentCloudServiceOperationUsageLimit = uint64(1 << 50)
	agentCloudThirtyDayMonthSeconds      = uint64(30 * 24 * 60 * 60)
	agentCloudYearSeconds                = uint64(365 * 24 * 60 * 60)
)

var agentCloudServiceOperationKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func (value AgentCloudServiceOperationScope) empty() bool {
	return len(value.PrivateEndpoints) == 0 && len(value.Snapshots) == 0
}

// AgentCloudServiceOperationsRequired reports whether the supplied version
// carries the v2 service-operation extension and whether it is recognized.
func AgentCloudServiceOperationsRequired(schemaVersion string) (required, known bool) {
	switch schemaVersion {
	case AgentCloudPlanSchemaV1, AgentCloudQuoteScopeSchemaV1, AgentCloudApprovalSchemaV1:
		return false, true
	case AgentCloudPlanSchemaV2, AgentCloudQuoteScopeSchemaV2, AgentCloudApprovalSchemaV2:
		return true, true
	default:
		return false, false
	}
}

// NormalizeAgentCloudServiceOperations preserves the Agent's deterministic
// public ordering without changing the caller's slices.
func NormalizeAgentCloudServiceOperations(value AgentCloudServiceOperationScope) AgentCloudServiceOperationScope {
	result := AgentCloudServiceOperationScope{
		PrivateEndpoints: append([]AgentCloudPrivateEndpointOperation(nil), value.PrivateEndpoints...),
		Snapshots:        append([]AgentCloudSnapshotOperation(nil), value.Snapshots...),
	}
	sort.Slice(result.PrivateEndpoints, func(left, right int) bool {
		return result.PrivateEndpoints[left].OperationKey < result.PrivateEndpoints[right].OperationKey
	})
	sort.Slice(result.Snapshots, func(left, right int) bool {
		return result.Snapshots[left].OperationKey < result.Snapshots[right].OperationKey
	})
	return result
}

// ValidateAgentCloudServiceOperations applies the closed v1/v2 contract to an
// approval-visible scope. It never resolves or accepts provider resource IDs.
func ValidateAgentCloudServiceOperations(schemaVersion string, value AgentCloudServiceOperationScope, resource AgentCloudResourceScope, network AgentCloudNetworkScope, retention AgentCloudRetentionScope) error {
	required, known := AgentCloudServiceOperationsRequired(schemaVersion)
	if !known {
		return fmt.Errorf("unknown service-operation schema version")
	}
	if !required {
		if !value.empty() {
			return fmt.Errorf("service operations require a v2 schema")
		}
		return nil
	}
	if value.empty() {
		return fmt.Errorf("v2 service operations are required")
	}
	if len(value.PrivateEndpoints) > 4 || len(value.Snapshots) > len(resource.VolumeScopes) || len(value.PrivateEndpoints)+len(value.Snapshots) > 16 {
		return fmt.Errorf("service operations exceed the supported operation budget")
	}

	network = normalizeAgentCloudNetworkScope(network)
	seenKeys := make(map[string]struct{}, len(value.PrivateEndpoints)+len(value.Snapshots))
	for _, endpoint := range value.PrivateEndpoints {
		if !agentCloudServiceOperationKeyPattern.MatchString(endpoint.OperationKey) {
			return fmt.Errorf("private endpoint operation key is invalid")
		}
		if _, duplicate := seenKeys[endpoint.OperationKey]; duplicate {
			return fmt.Errorf("service operations contain duplicate operation keys")
		}
		seenKeys[endpoint.OperationKey] = struct{}{}
		if endpoint.Service != "s3" {
			return fmt.Errorf("private endpoint service is not approved")
		}
		switch endpoint.SecurityGroupSource {
		case "plan_existing":
			if network.SecurityGroupMode != "existing" || network.SecurityGroupID == "" {
				return fmt.Errorf("private endpoint must use the planned existing security group")
			}
		case "worker_dedicated":
			if network.SecurityGroupMode != "create_dedicated" || network.SecurityGroupID != "" {
				return fmt.Errorf("private endpoint must use the planned dedicated worker security group")
			}
		default:
			return fmt.Errorf("private endpoint security group source is invalid")
		}
		if endpoint.MonthlyHours == 0 || endpoint.MonthlyHours > 744 || endpoint.DataMiBPerMonth > agentCloudServiceOperationUsageLimit {
			return fmt.Errorf("private endpoint usage assumptions are invalid")
		}
	}

	volumes := make(map[string]AgentCloudVolumeScope, len(resource.VolumeScopes))
	for _, volume := range resource.VolumeScopes {
		volumes[volume.SlotID] = volume
	}
	seenVolumes := make(map[string]struct{}, len(value.Snapshots))
	for _, snapshot := range value.Snapshots {
		if !agentCloudServiceOperationKeyPattern.MatchString(snapshot.OperationKey) {
			return fmt.Errorf("snapshot operation key is invalid")
		}
		if _, duplicate := seenKeys[snapshot.OperationKey]; duplicate {
			return fmt.Errorf("service operations contain duplicate operation keys")
		}
		seenKeys[snapshot.OperationKey] = struct{}{}
		volume, found := volumes[snapshot.SourceVolumeSlotID]
		if !found || !volume.Persistent {
			return fmt.Errorf("snapshot source must be a persistent approved volume")
		}
		if _, duplicate := seenVolumes[snapshot.SourceVolumeSlotID]; duplicate {
			return fmt.Errorf("service operations contain duplicate snapshot source volumes")
		}
		seenVolumes[snapshot.SourceVolumeSlotID] = struct{}{}
		expectedDigest, err := agentCloudVolumeScopeDigest(volume)
		if err != nil || snapshot.SourceVolumeSpecDigest != expectedDigest {
			return fmt.Errorf("snapshot source digest does not bind the approved volume")
		}
		switch retention.Class {
		case "ephemeral":
			if volume.Disposition != "delete_with_deployment" || snapshot.Disposition != "delete_with_deployment" || snapshot.MaxRetentionSeconds != retention.MaxLifetimeSeconds {
				return fmt.Errorf("ephemeral snapshot retention is invalid")
			}
		case "managed":
			if volume.Disposition != "retain_with_managed_service" || snapshot.Disposition != "retain_with_managed_service" || snapshot.MaxRetentionSeconds == 0 || snapshot.MaxRetentionSeconds > agentCloudYearSeconds {
				return fmt.Errorf("managed snapshot retention is invalid")
			}
		default:
			return fmt.Errorf("snapshot plan retention is invalid")
		}
	}
	return nil
}

// ValidateAgentCloudServiceOperationUsage binds quote usage to the exact
// v2 operation scope. V1 quotes cannot carry endpoint usage fields.
func ValidateAgentCloudServiceOperationUsage(schemaVersion string, operations AgentCloudServiceOperationScope, resource AgentCloudResourceScope, network AgentCloudNetworkScope, retention AgentCloudRetentionScope, usage AgentCloudUsageEstimate) error {
	if !ValidAgentCloudUsageEstimate(usage) {
		return fmt.Errorf("quote usage assumptions are invalid")
	}
	if err := ValidateAgentCloudServiceOperations(schemaVersion, operations, resource, network, retention); err != nil {
		return err
	}
	required, _ := AgentCloudServiceOperationsRequired(schemaVersion)
	if !required {
		if usage.PrivateEndpointHours != 0 || usage.PrivateEndpointDataMiB != 0 {
			return fmt.Errorf("private endpoint usage requires a v2 schema")
		}
		return nil
	}

	var expectedHours, expectedData uint64
	for _, endpoint := range operations.PrivateEndpoints {
		if expectedHours > ^uint64(0)-uint64(endpoint.MonthlyHours) || expectedData > ^uint64(0)-endpoint.DataMiBPerMonth {
			return fmt.Errorf("private endpoint usage overflows")
		}
		expectedHours += uint64(endpoint.MonthlyHours)
		expectedData += endpoint.DataMiBPerMonth
	}
	if expectedHours > uint64(^uint32(0)) || usage.PrivateEndpointHours != uint32(expectedHours) || usage.PrivateEndpointDataMiB != expectedData {
		return fmt.Errorf("private endpoint usage does not match service operations")
	}

	volumes := make(map[string]AgentCloudVolumeScope, len(resource.VolumeScopes))
	for _, volume := range resource.VolumeScopes {
		volumes[volume.SlotID] = volume
	}
	var expectedSnapshotGiBMonths uint64
	for _, snapshot := range operations.Snapshots {
		volume := volumes[snapshot.SourceVolumeSlotID]
		if snapshot.MaxRetentionSeconds == 0 {
			return fmt.Errorf("snapshot usage retention is invalid")
		}
		if uint64(volume.SizeGiB) > ^uint64(0)/snapshot.MaxRetentionSeconds {
			return fmt.Errorf("snapshot usage overflows")
		}
		units := uint64(volume.SizeGiB) * snapshot.MaxRetentionSeconds
		months := units / agentCloudThirtyDayMonthSeconds
		if units%agentCloudThirtyDayMonthSeconds != 0 {
			months++
		}
		if expectedSnapshotGiBMonths > ^uint64(0)-months {
			return fmt.Errorf("snapshot usage overflows")
		}
		expectedSnapshotGiBMonths += months
	}
	if usage.SnapshotGiBMonths != expectedSnapshotGiBMonths {
		return fmt.Errorf("snapshot usage does not match service operations")
	}
	return nil
}

// ValidAgentCloudUsageEstimate validates the range shared by the Agent quote
// wire mapper and the owner-facing quote projection.
func ValidAgentCloudUsageEstimate(value AgentCloudUsageEstimate) bool {
	return value.RuntimeHoursPerMonth > 0 && value.RuntimeHoursPerMonth <= 744 && value.PublicIPv4Hours <= 744 && value.EntryHours <= 744 && value.PrivateEndpointHours <= 16*744 &&
		value.LogIngestMiB <= agentCloudServiceOperationUsageLimit && value.LogStoredMiBMonths <= agentCloudServiceOperationUsageLimit &&
		value.SnapshotGiBMonths <= agentCloudServiceOperationUsageLimit && value.InternetEgressMiB <= agentCloudServiceOperationUsageLimit && value.PrivateEndpointDataMiB <= agentCloudServiceOperationUsageLimit
}

func agentCloudVolumeScopeDigest(value AgentCloudVolumeScope) (string, error) {
	mode, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return "", err
	}
	volume := map[string]any{
		"slot_id":          value.SlotID,
		"size_gib":         value.SizeGiB,
		"volume_type":      value.VolumeType,
		"encrypted":        value.Encrypted,
		"kms_key_id":       value.KMSKeyID,
		"device_name":      value.DeviceName,
		"mount_path":       value.MountPath,
		"read_only":        value.ReadOnly,
		"persistent":       value.Persistent,
		"disposition":      value.Disposition,
		"iops":             value.IOPS,
		"throughput_mibps": value.ThroughputMiBPS,
	}
	encoded, err := mode.Marshal(map[string]any{
		"schema_version": agentCloudVolumeScopeSchemaV1,
		"volume":         volume,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
