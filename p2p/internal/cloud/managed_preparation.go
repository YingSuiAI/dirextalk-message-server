package cloud

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path"
	"strings"
	"time"
)

const (
	managedPreparationChallengeSchema = "dirextalk.agent.cloud.service-operation-challenge/v1"
	managedPreparationScopeSchema     = "dirextalk.agent.cloud.service-operation-scope/v1"
	managedPreparationIntent          = "MANAGED_PREPARATION"
)

var managedPreparationPhases = [...]string{
	"restart", "backup", "restore_create", "restore_swap", "semantic_health", "finalize",
}

// AgentCloudManagedPreparationClient is the protocol-neutral, optional
// compatibility boundary for Agent-owned managed preparation. Message Server
// resolves only the owner-scoped legacy Service mapping and active signer.
type AgentCloudManagedPreparationClient interface {
	CreateCloudManagedPreparation(context.Context, AgentCloudManagedPreparationCreateRequest) (AgentCloudManagedPreparationChallenge, error)
	ApproveCloudManagedPreparation(context.Context, AgentCloudManagedPreparationApproveRequest) (AgentCloudManagedPreparationOperation, error)
	GetCloudManagedPreparation(context.Context, AgentCloudManagedPreparationGetRequest) (AgentCloudManagedPreparationOperation, bool, error)
}

type AgentCloudManagedPreparationCreateRequest struct {
	IdempotencyKey, DeploymentID, SignerKeyID string
	ExpectedDeploymentRevision                int64
	CostAlertAmountMinor                      int64
}

type AgentCloudManagedPreparationApproveRequest struct {
	IdempotencyKey, OperationID, DeploymentID, ScopeDigest string
	ExpectedRevision                                       int64
	Approval                                               AgentCloudManagedPreparationSignature
}

type AgentCloudManagedPreparationGetRequest struct{ OperationID string }

type AgentCloudManagedPreparationSignature struct {
	ApprovalID, ChallengeID, OperationID, SignerKeyID string
	ExpiresAt                                         time.Time
	Signature                                         []byte
}

type AgentCloudManagedPreparationResourceFact struct {
	ResourceID string `json:"resource_id"`
	ProviderID string `json:"provider_id"`
	Revision   int64  `json:"revision"`
	SpecDigest string `json:"spec_digest"`
	TagDigest  string `json:"tag_digest"`
}

type AgentCloudManagedPreparationRestart struct {
	OperationID             string `json:"operation_id"`
	ExpectedInitialRevision int64  `json:"expected_initial_revision"`
	Action                  string `json:"action"`
	LifecycleRestartRef     string `json:"lifecycle_restart_ref"`
	ExecutionBundleDigest   string `json:"execution_bundle_digest"`
}

type AgentCloudManagedPreparationVolume struct {
	SlotID                      string                                   `json:"slot_id"`
	SourceVolume                AgentCloudManagedPreparationResourceFact `json:"source_volume"`
	SnapshotResourceID          string                                   `json:"snapshot_resource_id"`
	ReplacementVolumeResourceID string                                   `json:"replacement_volume_resource_id"`
	AvailabilityZone            string                                   `json:"availability_zone"`
	SizeGiB                     uint32                                   `json:"size_gib"`
	VolumeType                  string                                   `json:"volume_type"`
	IOPS                        uint32                                   `json:"iops"`
	ThroughputMiBPS             uint32                                   `json:"throughput_mibps"`
	KMSKeyID                    string                                   `json:"kms_key_id"`
	DeviceName                  string                                   `json:"device_name"`
	MountPath                   string                                   `json:"mount_path"`
	ReadOnly                    bool                                     `json:"read_only"`
	Persistent                  bool                                     `json:"persistent"`
	Disposition                 string                                   `json:"disposition"`
}

// ManagedPreparationVolumeSourceSpecDigest reconstructs the Agent-owned
// canonical AWS EBS source spec used by managed-preparation scope validation.
func ManagedPreparationVolumeSourceSpecDigest(volume AgentCloudManagedPreparationVolume) (string, error) {
	if volume.AvailabilityZone == "" || volume.SizeGiB == 0 || volume.SizeGiB > 65_536 ||
		volume.VolumeType != "gp3" || volume.IOPS < 3_000 || volume.IOPS > 80_000 ||
		volume.ThroughputMiBPS < 125 || volume.ThroughputMiBPS > 2_000 ||
		volume.KMSKeyID == "" || volume.SlotID == "" || volume.DeviceName == "" ||
		!strings.HasPrefix(volume.MountPath, "/") || volume.MountPath == "/" ||
		path.Clean(volume.MountPath) != volume.MountPath || strings.Contains(volume.MountPath, "\\") ||
		!volume.Persistent || volume.Disposition != "retain_with_managed_service" {
		return "", ErrAgentCloudControlInvalidResponse
	}
	for _, denied := range []string{"/dev", "/proc", "/sys", "/run/secrets"} {
		if volume.MountPath == denied || strings.HasPrefix(volume.MountPath, denied+"/") {
			return "", ErrAgentCloudControlInvalidResponse
		}
	}
	type ebsVolumeSpec struct {
		AvailabilityZone         string `json:"availability_zone"`
		SizeGiB                  uint32 `json:"size_gib"`
		VolumeType               string `json:"volume_type"`
		IOPS                     uint32 `json:"iops,omitempty"`
		ThroughputMiBPS          uint32 `json:"throughput_mibps,omitempty"`
		KMSKeyID                 string `json:"kms_key_id"`
		SourceSnapshotResourceID string `json:"source_snapshot_resource_id,omitempty"`
		SlotID                   string `json:"slot_id,omitempty"`
		DeviceName               string `json:"device_name,omitempty"`
		MountPath                string `json:"mount_path,omitempty"`
		ReadOnly                 bool   `json:"read_only,omitempty"`
		Persistent               bool   `json:"persistent,omitempty"`
		Disposition              string `json:"disposition,omitempty"`
	}
	document := struct {
		SchemaVersion string         `json:"schema_version"`
		Volume        *ebsVolumeSpec `json:"volume,omitempty"`
	}{
		SchemaVersion: "dirextalk.agent.aws-resource/v1",
		Volume: &ebsVolumeSpec{
			AvailabilityZone: volume.AvailabilityZone, SizeGiB: volume.SizeGiB, VolumeType: volume.VolumeType,
			IOPS: volume.IOPS, ThroughputMiBPS: volume.ThroughputMiBPS, KMSKeyID: volume.KMSKeyID,
			SlotID: volume.SlotID, DeviceName: volume.DeviceName, MountPath: volume.MountPath,
			ReadOnly: volume.ReadOnly, Persistent: volume.Persistent, Disposition: volume.Disposition,
		},
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return "", ErrAgentCloudControlInvalidResponse
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

type AgentCloudManagedPreparationScope struct {
	SchemaVersion                   string                                     `json:"schema_version"`
	Intent                          string                                     `json:"intent"`
	PreparationOperationID          string                                     `json:"preparation_operation_id"`
	OwnerID                         string                                     `json:"owner_id"`
	AgentInstanceID                 string                                     `json:"agent_instance_id"`
	DeploymentID                    string                                     `json:"deployment_id"`
	DeploymentRevision              int64                                      `json:"deployment_revision"`
	ConnectionID                    string                                     `json:"connection_id"`
	ConnectionRevision              int64                                      `json:"connection_revision"`
	PlanID                          string                                     `json:"plan_id"`
	PlanRevision                    int64                                      `json:"plan_revision"`
	PlanHash                        string                                     `json:"plan_hash"`
	RecipeID                        string                                     `json:"recipe_id"`
	RecipeDigest                    string                                     `json:"recipe_digest"`
	RecipeRevision                  int64                                      `json:"recipe_revision"`
	EC2                             AgentCloudManagedPreparationResourceFact   `json:"ec2"`
	SourceVolumes                   []AgentCloudManagedPreparationResourceFact `json:"source_volumes"`
	Restart                         AgentCloudManagedPreparationRestart        `json:"restart"`
	Volumes                         []AgentCloudManagedPreparationVolume       `json:"volumes"`
	ServiceMonitorRevision          int64                                      `json:"service_monitor_revision"`
	ServiceMonitorSuiteDigest       string                                     `json:"service_monitor_suite_digest"`
	Currency                        string                                     `json:"currency"`
	CostAlertAmountMinor            int64                                      `json:"cost_alert_amount_minor"`
	ExpectedInstalledManifestDigest string                                     `json:"expected_installed_manifest_digest"`
}

type AgentCloudManagedPreparationChallenge struct {
	SchemaVersion                                      string
	ChallengeID, OperationID, SignerKeyID, ScopeDigest string
	Scope                                              AgentCloudManagedPreparationScope
	IssuedAt, ExpiresAt                                time.Time
	SigningPayloadCBOR                                 []byte
	Revision                                           int64
}

type AgentCloudManagedPreparationStep struct {
	Phase        string
	Ordinal      int32
	Status       string
	Revision     int64
	IntentDigest string
	StartedAt    *time.Time
	CompletedAt  *time.Time
}

type AgentCloudManagedPreparationResult struct {
	PreparationID         string
	PreparationDigest     string
	FreshHealthDigest     string
	FreshHealthRevision   int64
	FreshHealthObservedAt time.Time
	CostDigest            string
	CostPolicyRevision    int64
	CostObservedAt        time.Time
	StackDigest           string
	StackRevision         int64
	StackObservedAt       time.Time
}

type AgentCloudManagedPreparationOperation struct {
	OperationID  string
	Challenge    AgentCloudManagedPreparationChallenge
	Status       string
	CurrentPhase string
	Revision     int64
	Steps        []AgentCloudManagedPreparationStep
	CreatedAt    time.Time
	UpdatedAt    time.Time
	ApprovedAt   *time.Time
	Result       *AgentCloudManagedPreparationResult
}

type managedPreparationApprovalV1 struct {
	SchemaVersion     string                            `json:"schema_version"`
	ChallengeID       string                            `json:"challenge_id"`
	OperationID       string                            `json:"operation_id"`
	SignerKeyID       string                            `json:"signer_key_id"`
	Scope             AgentCloudManagedPreparationScope `json:"scope"`
	ScopeDigest       string                            `json:"scope_digest"`
	IssuedAt          time.Time                         `json:"issued_at"`
	ExpiresAt         time.Time                         `json:"expires_at"`
	OperationRevision int64                             `json:"operation_revision"`
	Signature         string                            `json:"signature,omitempty"`
}
