package cloudorchestrator

import (
	"encoding/json"
	"errors"
	"net/url"
	"sort"
	"strings"
)

const (
	OCIServiceBundleV1Schema = "dirextalk.oci-service-bundle/v1"

	OCIServiceCheckpointArtifactVerified = "artifact_verified"
	OCIServiceCheckpointContainerCreated = "container_created"
	OCIServiceCheckpointContainerStarted = "container_started"
	OCIServiceCheckpointHealthVerified   = "health_verified"
)

var ociServiceInstallCheckpointSequenceV1 = []string{
	OCIServiceCheckpointArtifactVerified,
	OCIServiceCheckpointContainerCreated,
	OCIServiceCheckpointContainerStarted,
	OCIServiceCheckpointHealthVerified,
}

// OCIServiceInstallCheckpointSequenceV1 returns an isolated copy of the only
// checkpoint sequence accepted for a typed OCI install. Compiler, executor
// manifests, and Worker drivers must all derive their sequence from this API.
func OCIServiceInstallCheckpointSequenceV1() []string {
	return append([]string(nil), ociServiceInstallCheckpointSequenceV1...)
}

type OCIServiceProbeScheme string

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

type OCIServiceHealthV1 struct {
	Liveness  OCIServiceLoopbackProbeV1 `json:"liveness"`
	Readiness OCIServiceLoopbackProbeV1 `json:"readiness"`
	Semantic  OCIServiceLoopbackProbeV1 `json:"semantic"`
}

// OCIServiceBundleV1 is a strict, de-secreted Worker input. ImageDigest is an
// OCI manifest digest, never an image reference, registry URL, or mutable tag.
type OCIServiceBundleV1 struct {
	SchemaVersion           string                   `json:"schema_version"`
	ArtifactDigest          string                   `json:"artifact_digest"`
	ImageDigest             string                   `json:"image_digest"`
	ImageSizeBytes          uint64                   `json:"image_size_bytes"`
	Architecture            Architecture             `json:"architecture"`
	Actions                 []CompiledRecipeActionV1 `json:"actions"`
	Health                  OCIServiceHealthV1       `json:"health"`
	HealthContractDigest    string                   `json:"health_contract_digest"`
	LifecycleContractDigest string                   `json:"lifecycle_contract_digest"`
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
	if bundle.ArtifactDigest != bundle.ImageDigest || bundle.ImageSizeBytes == 0 || bundle.ImageSizeBytes > 1<<40 {
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
		if !validCompiledRecipeActionKind(action.Kind) || validateCompiledRecipeIdentifier("action_id", action.ActionID) != nil || action.TimeoutSeconds == 0 || action.TimeoutSeconds > 86400 || validateCheckpointSequence(action.CheckpointSequence) != nil {
			return errors.New("OCI service bundle action is invalid")
		}
		if _, exists := seenKinds[action.Kind]; exists {
			return errors.New("OCI service bundle action kind is duplicated")
		}
		if _, exists := seenIDs[action.ActionID]; exists {
			return errors.New("OCI service bundle action id is duplicated")
		}
		seenKinds[action.Kind], seenIDs[action.ActionID] = struct{}{}, struct{}{}
		if action.Kind == CompiledRecipeActionInstall {
			hasInstall = sameOCIServiceCheckpointSequence(action.CheckpointSequence, ociServiceInstallCheckpointSequenceV1)
		}
	}
	if !hasInstall || validateOCIServiceHealth(bundle.Health) != nil {
		return errors.New("OCI service bundle install action or health contract is invalid")
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

func ParseOCIServiceBundleV1(raw []byte) (OCIServiceBundleV1, error) {
	top, err := compiledRecipeExactObject(raw, []string{"schema_version", "artifact_digest", "image_digest", "image_size_bytes", "architecture", "actions", "health", "health_contract_digest", "lifecycle_contract_digest"})
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
	return bundle
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
