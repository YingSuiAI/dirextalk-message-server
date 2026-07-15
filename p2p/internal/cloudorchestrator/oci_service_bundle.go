package cloudorchestrator

import (
	"encoding/json"
	"errors"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

const (
	OCIServiceBundleV1Schema = "dirextalk.oci-service-bundle/v1"

	OCIServiceCheckpointArtifactVerified = "artifact_verified"
	OCIServiceCheckpointContainerCreated = "container_created"
	OCIServiceCheckpointContainerStarted = "container_started"
	OCIServiceCheckpointContainerStopped = "container_stopped"
	OCIServiceCheckpointHealthVerified   = "health_verified"
)

var ociServiceInstallCheckpointSequenceV1 = []string{
	OCIServiceCheckpointArtifactVerified,
	OCIServiceCheckpointContainerCreated,
	OCIServiceCheckpointContainerStarted,
	OCIServiceCheckpointHealthVerified,
}

var (
	ociServiceStartCheckpointSequenceV1   = []string{OCIServiceCheckpointContainerStarted, OCIServiceCheckpointHealthVerified}
	ociServiceStopCheckpointSequenceV1    = []string{OCIServiceCheckpointContainerStopped}
	ociServiceRestartCheckpointSequenceV1 = []string{OCIServiceCheckpointContainerStopped, OCIServiceCheckpointContainerStarted, OCIServiceCheckpointHealthVerified}
)

// OCIServiceInstallCheckpointSequenceV1 returns an isolated copy of the only
// checkpoint sequence accepted for a typed OCI install. Compiler, executor
// manifests, and Worker drivers must all derive their sequence from this API.
func OCIServiceInstallCheckpointSequenceV1() []string {
	return append([]string(nil), ociServiceInstallCheckpointSequenceV1...)
}

func OCIServiceStartCheckpointSequenceV1() []string {
	return append([]string(nil), ociServiceStartCheckpointSequenceV1...)
}

func OCIServiceStopCheckpointSequenceV1() []string {
	return append([]string(nil), ociServiceStopCheckpointSequenceV1...)
}

func OCIServiceRestartCheckpointSequenceV1() []string {
	return append([]string(nil), ociServiceRestartCheckpointSequenceV1...)
}

// OCIServiceActionCheckpointSequenceV1 returns the only typed checkpoint
// sequence executable by the OCI Worker for one supported action kind.
func OCIServiceActionCheckpointSequenceV1(kind CompiledRecipeActionKind) ([]string, bool) {
	sequence, ok := ociServiceActionCheckpointSequenceV1(kind)
	return append([]string(nil), sequence...), ok
}

func ociServiceActionCheckpointSequenceV1(kind CompiledRecipeActionKind) ([]string, bool) {
	switch kind {
	case CompiledRecipeActionInstall:
		return ociServiceInstallCheckpointSequenceV1, true
	case CompiledRecipeActionStart:
		return ociServiceStartCheckpointSequenceV1, true
	case CompiledRecipeActionStop:
		return ociServiceStopCheckpointSequenceV1, true
	case CompiledRecipeActionRestart:
		return ociServiceRestartCheckpointSequenceV1, true
	default:
		return nil, false
	}
}

type OCIServiceProbeScheme string

// OCIImageSourceReferenceV1 is the only network image source accepted by the
// trusted compiler and Worker. It is always a fully qualified public registry
// repository pinned to an immutable OCI manifest digest.
type OCIImageSourceReferenceV1 string

var ociImageSourceReferencePattern = regexp.MustCompile(`^(?:ghcr\.io|docker\.io|quay\.io|public\.ecr\.aws)/[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*@sha256:([0-9a-f]{64})$`)

func (reference OCIImageSourceReferenceV1) Validate() error {
	if len(reference) == 0 || len(reference) > 512 || !ociImageSourceReferencePattern.MatchString(string(reference)) {
		return errors.New("OCI image source reference is invalid")
	}
	return nil
}

func (reference OCIImageSourceReferenceV1) PinnedDigest() (string, error) {
	match := ociImageSourceReferencePattern.FindStringSubmatch(string(reference))
	if len(reference) == 0 || len(reference) > 512 || len(match) != 2 {
		return "", errors.New("OCI image source reference is invalid")
	}
	return "sha256:" + match[1], nil
}

const (
	OCIServiceProbeHTTP  OCIServiceProbeScheme = "http"
	OCIServiceProbeHTTPS OCIServiceProbeScheme = "https"
)

// OCIServiceLoopbackProbeV1 intentionally has no configurable host. Workers
// always probe the service through loopback and cannot be redirected by a
// compiled recipe to an arbitrary URL.
type OCIServiceLoopbackProbeV1 struct {
	Scheme         OCIServiceProbeScheme `json:"scheme"`
	Port           uint16                `json:"port"`
	Path           string                `json:"path"`
	ExpectedStatus uint16                `json:"expected_status"`
	BodySHA256     string                `json:"body_sha256"`
}

// Validate keeps the executable readiness boundary closed: callers may choose
// only a loopback scheme, port, safe request path, expected status, and pinned
// response-body digest. A host, URL, command, or credential is never accepted.
func (probe OCIServiceLoopbackProbeV1) Validate() error {
	return validateOCIServiceHealth(OCIServiceHealthV1{Liveness: probe, Readiness: probe, Semantic: probe})
}

type OCIServiceHealthV1 struct {
	Liveness  OCIServiceLoopbackProbeV1 `json:"liveness"`
	Readiness OCIServiceLoopbackProbeV1 `json:"readiness"`
	Semantic  OCIServiceLoopbackProbeV1 `json:"semantic"`
}

// OCIServiceMountTargetV1 is compiler-owned input. It maps one reviewed Recipe
// slot to a container path and can never carry a host path or opaque ref.
type OCIServiceMountTargetV1 struct {
	SlotID          string `json:"slot_id"`
	ContainerTarget string `json:"container_target"`
	OwnerUID        uint32 `json:"owner_uid,omitempty"`
	OwnerGID        uint32 `json:"owner_gid,omitempty"`
	DirectoryMode   uint32 `json:"directory_mode,omitempty"`
}

// OCIServiceStorageTargetV1 is the immutable Worker descriptor binding. Its
// read-only bit is derived from the reviewed Recipe, never from BuildSpec.
type OCIServiceStorageTargetV1 struct {
	SlotID          string `json:"slot_id"`
	ContainerTarget string `json:"container_target"`
	ReadOnly        bool   `json:"read_only"`
	OwnerUID        uint32 `json:"owner_uid,omitempty"`
	OwnerGID        uint32 `json:"owner_gid,omitempty"`
	DirectoryMode   uint32 `json:"directory_mode,omitempty"`
}

// OCIServiceBundleV1 is a strict, de-secreted Worker input. ImageSource is a
// public allowlisted repository pinned to ImageDigest; neither field can carry
// a mutable tag, credential, query, or arbitrary URL.
type OCIServiceBundleV1 struct {
	SchemaVersion           string                      `json:"schema_version"`
	ArtifactDigest          string                      `json:"artifact_digest"`
	ImageSource             OCIImageSourceReferenceV1   `json:"image_source"`
	ImageDigest             string                      `json:"image_digest"`
	ImageSizeBytes          uint64                      `json:"image_size_bytes"`
	Architecture            Architecture                `json:"architecture"`
	Actions                 []CompiledRecipeActionV1    `json:"actions"`
	Health                  OCIServiceHealthV1          `json:"health"`
	HealthContractDigest    string                      `json:"health_contract_digest"`
	LifecycleContractDigest string                      `json:"lifecycle_contract_digest"`
	VolumeTargets           []OCIServiceStorageTargetV1 `json:"volume_targets,omitempty"`
	DataTargets             []OCIServiceStorageTargetV1 `json:"data_targets,omitempty"`
	RuntimeProfile          *OCIServiceRuntimeProfileV1 `json:"runtime_profile,omitempty"`
}

func (bundle OCIServiceBundleV1) Validate() error {
	if bundle.SchemaVersion != OCIServiceBundleV1Schema {
		return errors.New("OCI service bundle schema is invalid")
	}
	for label, value := range map[string]string{
		"artifact_digest": bundle.ArtifactDigest, "image_digest": bundle.ImageDigest,
		"health_contract_digest": bundle.HealthContractDigest, "lifecycle_contract_digest": bundle.LifecycleContractDigest,
	} {
		if validateDigest(label, value) != nil {
			return errors.New("OCI service bundle digest is invalid")
		}
	}
	pinnedDigest, sourceErr := bundle.ImageSource.PinnedDigest()
	if sourceErr != nil || pinnedDigest != bundle.ImageDigest || bundle.ArtifactDigest != bundle.ImageDigest || bundle.ImageSizeBytes == 0 || bundle.ImageSizeBytes > 1<<40 {
		return errors.New("OCI service bundle image binding is invalid")
	}
	if bundle.Architecture != ArchitectureAMD64 && bundle.Architecture != ArchitectureARM64 {
		return errors.New("OCI service bundle architecture is invalid")
	}
	if len(bundle.Actions) == 0 || len(bundle.Actions) > 32 {
		return errors.New("OCI service bundle actions are invalid")
	}
	seenKinds, seenIDs, hasInstall := map[CompiledRecipeActionKind]struct{}{}, map[string]struct{}{}, false
	for _, action := range bundle.Actions {
		expectedCheckpoints, supported := ociServiceActionCheckpointSequenceV1(action.Kind)
		if !supported || !action.RootRequired || validateCompiledRecipeIdentifier("action_id", action.ActionID) != nil || action.TimeoutSeconds == 0 || action.TimeoutSeconds > 86400 || validateCheckpointSequence(action.CheckpointSequence) != nil || !sameOCIServiceCheckpointSequence(action.CheckpointSequence, expectedCheckpoints) {
			return errors.New("OCI service bundle action is invalid")
		}
		if _, exists := seenKinds[action.Kind]; exists {
			return errors.New("OCI service bundle action kind is duplicated")
		}
		if _, exists := seenIDs[action.ActionID]; exists {
			return errors.New("OCI service bundle action id is duplicated")
		}
		seenKinds[action.Kind], seenIDs[action.ActionID] = struct{}{}, struct{}{}
		hasInstall = hasInstall || action.Kind == CompiledRecipeActionInstall
	}
	if !hasInstall || validateOCIServiceHealth(bundle.Health) != nil {
		return errors.New("OCI service bundle install action or health contract is invalid")
	}
	if validateOCIServiceStorageTargets(bundle.VolumeTargets, bundle.DataTargets) != nil || bundle.RuntimeProfile != nil && bundle.RuntimeProfile.Validate() != nil || ociServiceRuntimeMountsConflict(bundle) {
		return errors.New("OCI service bundle storage targets are invalid")
	}
	return nil
}

func sameOCIServiceCheckpointSequence(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (bundle OCIServiceBundleV1) CanonicalOCIServiceBundleCBOR() ([]byte, error) {
	if err := bundle.Validate(); err != nil {
		return nil, err
	}
	return canonicalCBOR(normalizeOCIServiceBundle(bundle))
}

func (bundle OCIServiceBundleV1) Digest() (string, error) {
	canonical, err := bundle.CanonicalOCIServiceBundleCBOR()
	if err != nil {
		return "", err
	}
	return digestCanonicalCBOR(canonical), nil
}

func (bundle OCIServiceBundleV1) Action(kind CompiledRecipeActionKind) (CompiledRecipeActionV1, bool) {
	for _, action := range bundle.Actions {
		if action.Kind == kind {
			return action, true
		}
	}
	return CompiledRecipeActionV1{}, false
}

func (bundle OCIServiceBundleV1) ActionByID(actionID string) (CompiledRecipeActionV1, bool) {
	for _, action := range bundle.Actions {
		if action.ActionID == actionID {
			return action, true
		}
	}
	return CompiledRecipeActionV1{}, false
}

func ParseOCIServiceBundleV1(raw []byte) (OCIServiceBundleV1, error) {
	baseFields := []string{"schema_version", "artifact_digest", "image_source", "image_digest", "image_size_bytes", "architecture", "actions", "health", "health_contract_digest", "lifecycle_contract_digest"}
	var present map[string]json.RawMessage
	if err := json.Unmarshal(raw, &present); err != nil {
		return OCIServiceBundleV1{}, errors.New("OCI service bundle JSON is invalid")
	}
	fields := append([]string(nil), baseFields...)
	for _, optional := range []string{"volume_targets", "data_targets", "runtime_profile"} {
		if _, ok := present[optional]; ok {
			fields = append(fields, optional)
		}
	}
	top, err := compiledRecipeExactObject(raw, fields)
	if err != nil {
		return OCIServiceBundleV1{}, errors.New("OCI service bundle JSON is invalid")
	}
	if err := compiledRecipeExactArray(top["actions"], []string{"kind", "action_id", "root_required", "timeout_seconds", "checkpoint_sequence"}); err != nil {
		return OCIServiceBundleV1{}, errors.New("OCI service bundle JSON is invalid")
	}
	health, err := compiledRecipeExactObject(top["health"], []string{"liveness", "readiness", "semantic"})
	if err != nil {
		return OCIServiceBundleV1{}, errors.New("OCI service bundle JSON is invalid")
	}
	for _, field := range []string{"liveness", "readiness", "semantic"} {
		if _, err := compiledRecipeExactObject(health[field], []string{"scheme", "port", "path", "expected_status", "body_sha256"}); err != nil {
			return OCIServiceBundleV1{}, errors.New("OCI service bundle JSON is invalid")
		}
	}
	for _, field := range []string{"volume_targets", "data_targets"} {
		if value, exists := top[field]; exists {
			if err := validateOCIServiceStorageTargetsJSON(value); err != nil {
				return OCIServiceBundleV1{}, errors.New("OCI service bundle JSON is invalid")
			}
		}
	}
	if value, exists := top["runtime_profile"]; exists && validateOCIServiceRuntimeProfileJSON(value) != nil {
		return OCIServiceBundleV1{}, errors.New("OCI service bundle JSON is invalid")
	}
	var bundle OCIServiceBundleV1
	if err := json.Unmarshal(raw, &bundle); err != nil || bundle.Validate() != nil {
		return OCIServiceBundleV1{}, errors.New("OCI service bundle JSON is invalid")
	}
	return normalizeOCIServiceBundle(bundle), nil
}

func normalizeOCIServiceBundle(bundle OCIServiceBundleV1) OCIServiceBundleV1 {
	bundle.Actions = append([]CompiledRecipeActionV1(nil), bundle.Actions...)
	for index := range bundle.Actions {
		bundle.Actions[index].CheckpointSequence = append([]string(nil), bundle.Actions[index].CheckpointSequence...)
	}
	sort.Slice(bundle.Actions, func(i, j int) bool {
		if bundle.Actions[i].Kind == bundle.Actions[j].Kind {
			return bundle.Actions[i].ActionID < bundle.Actions[j].ActionID
		}
		return bundle.Actions[i].Kind < bundle.Actions[j].Kind
	})
	bundle.VolumeTargets = normalizeOCIServiceStorageTargets(bundle.VolumeTargets)
	bundle.DataTargets = normalizeOCIServiceStorageTargets(bundle.DataTargets)
	bundle.RuntimeProfile, _ = NormalizeOCIServiceRuntimeProfileV1(bundle.RuntimeProfile)
	return bundle
}

func normalizeOCIServiceStorageTargets(targets []OCIServiceStorageTargetV1) []OCIServiceStorageTargetV1 {
	if len(targets) == 0 {
		return nil
	}
	normalized := append([]OCIServiceStorageTargetV1(nil), targets...)
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].SlotID < normalized[j].SlotID })
	return normalized
}

func validateOCIServiceStorageTargets(volumes, data []OCIServiceStorageTargetV1) error {
	if len(volumes) > 32 || len(data) > 32 {
		return errors.New("OCI service storage target count is invalid")
	}
	seenSlots := make(map[string]struct{}, len(volumes)+len(data))
	seenTargets := make(map[string]struct{}, len(volumes)+len(data))
	for _, targets := range [][]OCIServiceStorageTargetV1{volumes, data} {
		for _, target := range targets {
			if validateCompiledRecipeIdentifier("slot_id", target.SlotID) != nil || ValidateOCIServiceContainerTarget(target.ContainerTarget) != nil || target.OwnerUID > maxOCIServiceRuntimeID || target.OwnerGID > maxOCIServiceRuntimeID {
				return errors.New("OCI service storage target is invalid")
			}
			if _, err := NormalizeOCIServiceStorageDirectoryMode(target.DirectoryMode); err != nil {
				return errors.New("OCI service storage target is invalid")
			}
			if _, exists := seenSlots[target.SlotID]; exists {
				return errors.New("OCI service storage target slot is duplicated")
			}
			if _, exists := seenTargets[target.ContainerTarget]; exists {
				return errors.New("OCI service container target is duplicated")
			}
			seenSlots[target.SlotID], seenTargets[target.ContainerTarget] = struct{}{}, struct{}{}
		}
	}
	return nil
}

// ValidateOCIServiceContainerTarget accepts only one clean, absolute Linux
// container path outside system and secret-delivery boundaries.
func ValidateOCIServiceContainerTarget(value string) error {
	if len(value) < 2 || len(value) > 512 || !path.IsAbs(value) || path.Clean(value) != value || value == "/" || strings.ContainsAny(value, ",\\") {
		return errors.New("OCI service container target is invalid")
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return errors.New("OCI service container target is invalid")
		}
	}
	for _, boundary := range []string{"/proc", "/sys", "/dev", "/run/secrets", "/run/dirextalk"} {
		if value == boundary || strings.HasPrefix(value, boundary+"/") {
			return errors.New("OCI service container target crosses a system boundary")
		}
	}
	if value == "/run" {
		return errors.New("OCI service container target conflicts with secret delivery")
	}
	return nil
}

func validateOCIServiceHealth(health OCIServiceHealthV1) error {
	for _, probe := range []OCIServiceLoopbackProbeV1{health.Liveness, health.Readiness, health.Semantic} {
		if probe.Scheme != OCIServiceProbeHTTP && probe.Scheme != OCIServiceProbeHTTPS || probe.Port == 0 || probe.ExpectedStatus < 100 || probe.ExpectedStatus > 599 || validateDigest("body_sha256", probe.BodySHA256) != nil || len(probe.Path) == 0 || len(probe.Path) > 256 || !strings.HasPrefix(probe.Path, "/") || strings.HasPrefix(probe.Path, "//") || strings.ContainsAny(probe.Path, "?#\\") || strings.Contains(probe.Path, "..") || rejectSecretMaterial("OCI service probe path", probe.Path) != nil || compiledRecipeSecretPattern.MatchString(probe.Path) {
			return errors.New("OCI service loopback probe is invalid")
		}
		parsed, err := url.ParseRequestURI(probe.Path)
		if err != nil || parsed.IsAbs() || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return errors.New("OCI service loopback probe is invalid")
		}
	}
	return nil
}
