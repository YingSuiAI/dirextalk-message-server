package cloudorchestrator

import "errors"

const (
	FixedProbeManagedArtifactDigest = "sha256:ad88e50776ac1b308a0e385dd5f9cbf847ae431d50b20b82b04ec74c75995d93"
	FixedProbeInstallActionID       = "dirextalk_fixed_probe_service_install_v1"
	FixedProbeStartActionID         = "dirextalk_fixed_probe_service_start_v1"
	FixedProbeStopActionID          = "dirextalk_fixed_probe_service_stop_v1"
	FixedProbeRestartActionID       = "dirextalk_fixed_probe_service_restart_v1"
)

// ServiceOperationCapabilityV1 is registered only from a trusted compiled
// Recipe artifact after installation/readiness. ProductCore may select the
// operation name but never supply an action id, artifact digest, checkpoint,
// command, path or root scope.
type ServiceOperationCapabilityV1 struct {
	ArtifactDigest          string                   `json:"artifact_digest"`
	InstalledManifestDigest string                   `json:"installed_manifest_digest"`
	RootRequired            bool                     `json:"root_required"`
	Start                   ServiceOperationActionV1 `json:"start"`
	Stop                    ServiceOperationActionV1 `json:"stop"`
	Restart                 ServiceOperationActionV1 `json:"restart"`
}

type ServiceOperationActionV1 struct {
	ActionID           string   `json:"action_id"`
	TimeoutSeconds     uint32   `json:"timeout_seconds"`
	CheckpointSequence []string `json:"checkpoint_sequence"`
}

func FixedProbeManagedCapability(installedManifestDigest string) ServiceOperationCapabilityV1 {
	return ServiceOperationCapabilityV1{
		ArtifactDigest:          FixedProbeManagedArtifactDigest,
		InstalledManifestDigest: installedManifestDigest,
		RootRequired:            true,
		Start: ServiceOperationActionV1{
			ActionID: FixedProbeStartActionID, TimeoutSeconds: 120,
			CheckpointSequence: []string{"probe_service_started", "probe_health_verified"},
		},
		Stop: ServiceOperationActionV1{
			ActionID: FixedProbeStopActionID, TimeoutSeconds: 120,
			CheckpointSequence: []string{"probe_service_stopped"},
		},
		Restart: ServiceOperationActionV1{
			ActionID: FixedProbeRestartActionID, TimeoutSeconds: 120,
			CheckpointSequence: []string{"probe_service_restarted", "probe_health_verified"},
		},
	}
}

func (capability ServiceOperationCapabilityV1) Validate() error {
	if validateDigest("artifact_digest", capability.ArtifactDigest) != nil || validateDigest("installed_manifest_digest", capability.InstalledManifestDigest) != nil || !capability.RootRequired {
		return ErrServiceOperationApprovalBinding
	}
	seen := map[string]struct{}{}
	for operation, action := range map[ServiceOperation]ServiceOperationActionV1{
		ServiceOperationStart: capability.Start, ServiceOperationStop: capability.Stop, ServiceOperationRestart: capability.Restart,
	} {
		if _, exists := seen[action.ActionID]; exists {
			return ErrServiceOperationApprovalBinding
		}
		seen[action.ActionID] = struct{}{}
		target := ServiceOperationTargetV1{
			Operation: operation, ServiceID: "service-capability", ServiceRevision: 1,
			ExpectedServiceStatus: map[ServiceOperation]string{ServiceOperationStart: "stopped", ServiceOperationStop: "active", ServiceOperationRestart: "active"}[operation],
			DeploymentID:          "deployment-capability", DeploymentRevision: 1,
			CloudConnectionID: "connection-capability", RecipeID: "recipe-capability",
			RecipeDigest: capability.ArtifactDigest, InstalledManifestDigest: capability.InstalledManifestDigest,
			ArtifactDigest: capability.ArtifactDigest, ActionID: action.ActionID, RootRequired: capability.RootRequired,
			TimeoutSeconds: action.TimeoutSeconds, CheckpointSequence: action.CheckpointSequence,
		}
		if err := target.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// ManagedCapabilityFromCompiledArtifact derives the lifecycle capability only
// from a trusted, immutable compiled descriptor. No caller-selected action ID,
// timeout, checkpoint, command, path, or root scope enters the approval.
func ManagedCapabilityFromCompiledArtifact(artifact CompiledRecipeArtifactV1, installedManifestDigest string) (ServiceOperationCapabilityV1, error) {
	if artifact.Validate() != nil || validateDigest("installed_manifest_digest", installedManifestDigest) != nil {
		return ServiceOperationCapabilityV1{}, ErrServiceOperationApprovalBinding
	}
	capability := ServiceOperationCapabilityV1{
		ArtifactDigest: artifact.ArtifactDigest, InstalledManifestDigest: installedManifestDigest, RootRequired: true,
	}
	seen := map[CompiledRecipeActionKind]bool{}
	for _, compiled := range artifact.Actions {
		var target *ServiceOperationActionV1
		switch compiled.Kind {
		case CompiledRecipeActionStart:
			target = &capability.Start
		case CompiledRecipeActionStop:
			target = &capability.Stop
		case CompiledRecipeActionRestart:
			target = &capability.Restart
		default:
			continue
		}
		if seen[compiled.Kind] || !compiled.RootRequired {
			return ServiceOperationCapabilityV1{}, ErrServiceOperationApprovalBinding
		}
		seen[compiled.Kind] = true
		*target = ServiceOperationActionV1{ActionID: compiled.ActionID, TimeoutSeconds: compiled.TimeoutSeconds, CheckpointSequence: append([]string(nil), compiled.CheckpointSequence...)}
	}
	if !seen[CompiledRecipeActionStart] || !seen[CompiledRecipeActionStop] || !seen[CompiledRecipeActionRestart] || capability.Validate() != nil {
		return ServiceOperationCapabilityV1{}, errors.New("compiled Recipe does not provide a managed lifecycle capability")
	}
	return capability, nil
}

func (capability ServiceOperationCapabilityV1) Action(operation ServiceOperation) (ServiceOperationActionV1, bool) {
	switch operation {
	case ServiceOperationStart:
		return capability.Start, true
	case ServiceOperationStop:
		return capability.Stop, true
	case ServiceOperationRestart:
		return capability.Restart, true
	default:
		return ServiceOperationActionV1{}, false
	}
}
