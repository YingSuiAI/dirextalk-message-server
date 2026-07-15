package cloudorchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

const CompiledRecipeArtifactV1Schema = "dirextalk.compiled-recipe-artifact/v1"

type CompiledRecipeActionKind string

const (
	CompiledRecipeActionInstall  CompiledRecipeActionKind = "install"
	CompiledRecipeActionStart    CompiledRecipeActionKind = "start"
	CompiledRecipeActionStop     CompiledRecipeActionKind = "stop"
	CompiledRecipeActionRestart  CompiledRecipeActionKind = "restart"
	CompiledRecipeActionUpgrade  CompiledRecipeActionKind = "upgrade"
	CompiledRecipeActionRollback CompiledRecipeActionKind = "rollback"
	CompiledRecipeActionBackup   CompiledRecipeActionKind = "backup"
	CompiledRecipeActionRestore  CompiledRecipeActionKind = "restore"
	CompiledRecipeActionDestroy  CompiledRecipeActionKind = "destroy"
)

type CompiledRecipeActionV1 struct {
	Kind               CompiledRecipeActionKind `json:"kind"`
	ActionID           string                   `json:"action_id"`
	RootRequired       bool                     `json:"root_required"`
	TimeoutSeconds     uint32                   `json:"timeout_seconds"`
	CheckpointSequence []string                 `json:"checkpoint_sequence"`
}

type CompiledVolumeSlotSchemaV1 = RecipeVolumeSlotRequirementV1
type CompiledDataSlotSchemaV1 = RecipeDataSlotRequirementV1
type CompiledSecretSlotSchemaV1 = RecipeSecretSlotRequirementV1

// CompiledRecipeArtifactV1 is the de-secreted, content-addressed descriptor
// emitted by a trusted compiler. It describes only typed capabilities and
// opaque slot requirements; executable content remains in the separately
// authenticated artifact named by ArtifactDigest.
type CompiledRecipeArtifactV1 struct {
	SchemaVersion                 string                          `json:"schema_version"`
	RecipeID                      string                          `json:"recipe_id"`
	RecipeDigest                  string                          `json:"recipe_digest"`
	RecipeRevision                uint64                          `json:"recipe_revision"`
	OfficialSourceArtifactDigests []string                        `json:"official_source_artifact_digests"`
	Architecture                  Architecture                    `json:"architecture"`
	Requirements                  ResourceRequirementsV1          `json:"requirements"`
	WorkerResourceManifestDigest  string                          `json:"worker_resource_manifest_digest"`
	ArtifactDigest                string                          `json:"artifact_digest"`
	MediaType                     string                          `json:"media_type"`
	SizeBytes                     uint64                          `json:"size_bytes"`
	Actions                       []CompiledRecipeActionV1        `json:"actions"`
	HealthContractDigest          string                          `json:"health_contract_digest"`
	LifecycleContractDigest       string                          `json:"lifecycle_contract_digest"`
	VolumeSlots                   []RecipeVolumeSlotRequirementV1 `json:"volume_slots"`
	DataSlots                     []RecipeDataSlotRequirementV1   `json:"data_slots"`
	SecretSlots                   []RecipeSecretSlotRequirementV1 `json:"secret_slots"`
}

var (
	compiledRecipeMediaTypePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9!#$&^_.+-]{0,63}/[a-z0-9][a-z0-9!#$&^_.+-]{0,127}$`)
	compiledRecipeCommandPattern   = regexp.MustCompile(`(?i)^\s*(?:sudo|sh|bash|curl|wget|rm|cp|mv|chmod|chown|systemctl|docker|podman|apt|yum|dnf|apk)(?:\s|$)`)
	compiledRecipeSecretPattern    = regexp.MustCompile(`\b(?:sk-[A-Za-z0-9_-]{20,}|Bearer\s+[A-Za-z0-9._~+/=-]{20,})\b`)
	compiledRecipeEnvNamePattern   = regexp.MustCompile(`^[A-Z][A-Z0-9_]{1,127}$`)
)

func (artifact CompiledRecipeArtifactV1) Validate() error {
	if artifact.SchemaVersion != CompiledRecipeArtifactV1Schema {
		return errors.New("compiled recipe artifact schema is invalid")
	}
	if err := validateCompiledRecipeIdentifier("recipe_id", artifact.RecipeID); err != nil {
		return err
	}
	for label, value := range map[string]string{
		"recipe_digest": artifact.RecipeDigest, "worker_resource_manifest_digest": artifact.WorkerResourceManifestDigest,
		"artifact_digest": artifact.ArtifactDigest, "health_contract_digest": artifact.HealthContractDigest,
		"lifecycle_contract_digest": artifact.LifecycleContractDigest,
	} {
		if err := validateDigest(label, value); err != nil {
			return err
		}
	}
	if artifact.RecipeRevision == 0 || artifact.SizeBytes == 0 || artifact.SizeBytes > 1<<40 {
		return errors.New("compiled recipe artifact revision or size is invalid")
	}
	if artifact.Architecture != ArchitectureAMD64 && artifact.Architecture != ArchitectureARM64 {
		return errors.New("compiled recipe artifact architecture is invalid")
	}
	if artifact.Requirements.validate() != nil || artifact.Requirements.Architecture != artifact.Architecture {
		return errors.New("compiled recipe artifact requirements are invalid")
	}
	if !compiledRecipeMediaTypePattern.MatchString(artifact.MediaType) {
		return errors.New("compiled recipe artifact media type is invalid")
	}
	if len(artifact.OfficialSourceArtifactDigests) == 0 || len(artifact.OfficialSourceArtifactDigests) > 32 {
		return errors.New("compiled recipe artifact sources are invalid")
	}
	seenSources := make(map[string]struct{}, len(artifact.OfficialSourceArtifactDigests))
	for _, digest := range artifact.OfficialSourceArtifactDigests {
		if validateDigest("official_source_artifact_digest", digest) != nil {
			return errors.New("compiled recipe artifact source digest is invalid")
		}
		if _, exists := seenSources[digest]; exists {
			return errors.New("compiled recipe artifact source digest is duplicated")
		}
		seenSources[digest] = struct{}{}
	}
	if len(artifact.Actions) == 0 || len(artifact.Actions) > 32 {
		return errors.New("compiled recipe artifact actions are invalid")
	}
	seenKinds, seenActions := map[CompiledRecipeActionKind]struct{}{}, map[string]struct{}{}
	for _, action := range artifact.Actions {
		if !validCompiledRecipeActionKind(action.Kind) || validateCompiledRecipeIdentifier("action_id", action.ActionID) != nil || action.TimeoutSeconds == 0 || action.TimeoutSeconds > 86400 || validateCheckpointSequence(action.CheckpointSequence) != nil {
			return errors.New("compiled recipe artifact action is invalid")
		}
		if _, exists := seenKinds[action.Kind]; exists {
			return errors.New("compiled recipe artifact action kind is duplicated")
		}
		if _, exists := seenActions[action.ActionID]; exists {
			return errors.New("compiled recipe artifact action id is duplicated")
		}
		seenKinds[action.Kind], seenActions[action.ActionID] = struct{}{}, struct{}{}
	}
	if err := validateCompiledRecipeSlots(artifact); err != nil {
		return err
	}
	return nil
}

func (artifact CompiledRecipeArtifactV1) CanonicalCompiledRecipeArtifactCBOR() ([]byte, error) {
	if err := artifact.Validate(); err != nil {
		return nil, err
	}
	return canonicalCBOR(normalizeCompiledRecipeArtifact(artifact))
}

func (artifact CompiledRecipeArtifactV1) Digest() (string, error) {
	canonical, err := artifact.CanonicalCompiledRecipeArtifactCBOR()
	if err != nil {
		return "", err
	}
	return digestCanonicalCBOR(canonical), nil
}

func HealthContractDigestV1(contract HealthContractV1) (string, error) {
	if err := contract.validate(); err != nil {
		return "", err
	}
	canonical, err := canonicalCBOR(contract)
	if err != nil {
		return "", err
	}
	return digestCanonicalCBOR(canonical), nil
}

func LifecycleContractDigestV1(contract LifecycleContractV1) (string, error) {
	if err := contract.validate(); err != nil {
		return "", err
	}
	canonical, err := canonicalCBOR(contract)
	if err != nil {
		return "", err
	}
	return digestCanonicalCBOR(canonical), nil
}

func ParseCompiledRecipeArtifactV1(raw []byte) (CompiledRecipeArtifactV1, error) {
	top, err := compiledRecipeExactObject(raw, []string{"schema_version", "recipe_id", "recipe_digest", "recipe_revision", "official_source_artifact_digests", "architecture", "requirements", "worker_resource_manifest_digest", "artifact_digest", "media_type", "size_bytes", "actions", "health_contract_digest", "lifecycle_contract_digest", "volume_slots", "data_slots", "secret_slots"})
	if err != nil {
		return CompiledRecipeArtifactV1{}, err
	}
	if err := compiledRecipeExactArray(top["actions"], []string{"kind", "action_id", "root_required", "timeout_seconds", "checkpoint_sequence"}); err != nil {
		return CompiledRecipeArtifactV1{}, err
	}
	var requirementValues map[string]json.RawMessage
	if err := json.Unmarshal(top["requirements"], &requirementValues); err != nil {
		return CompiledRecipeArtifactV1{}, errors.New("compiled recipe artifact requirements are invalid")
	}
	requirementFields := []string{"min_vcpu", "min_memory_mib", "min_disk_gib", "architecture"}
	for _, optional := range []string{"min_gpu_count", "min_gpu_memory_mib"} {
		if _, found := requirementValues[optional]; found {
			requirementFields = append(requirementFields, optional)
		}
	}
	if _, err := compiledRecipeExactObject(top["requirements"], requirementFields); err != nil {
		return CompiledRecipeArtifactV1{}, err
	}
	for field, fields := range map[string][]string{
		"volume_slots": {"slot_id", "purpose", "read_only"}, "data_slots": {"slot_id", "purpose", "read_only"},
		"secret_slots": {"slot_id", "purpose", "delivery"},
	} {
		if err := compiledRecipeExactArray(top[field], fields); err != nil {
			return CompiledRecipeArtifactV1{}, err
		}
	}
	var artifact CompiledRecipeArtifactV1
	if err := json.Unmarshal(raw, &artifact); err != nil || artifact.Validate() != nil {
		return CompiledRecipeArtifactV1{}, errors.New("compiled recipe artifact JSON is invalid")
	}
	return normalizeCompiledRecipeArtifact(artifact), nil
}

func normalizeCompiledRecipeArtifact(artifact CompiledRecipeArtifactV1) CompiledRecipeArtifactV1 {
	normalized := artifact
	normalized.OfficialSourceArtifactDigests = append([]string(nil), artifact.OfficialSourceArtifactDigests...)
	sort.Strings(normalized.OfficialSourceArtifactDigests)
	normalized.Actions = append([]CompiledRecipeActionV1(nil), artifact.Actions...)
	for index := range normalized.Actions {
		normalized.Actions[index].CheckpointSequence = append([]string(nil), normalized.Actions[index].CheckpointSequence...)
	}
	sort.Slice(normalized.Actions, func(i, j int) bool {
		if normalized.Actions[i].Kind == normalized.Actions[j].Kind {
			return normalized.Actions[i].ActionID < normalized.Actions[j].ActionID
		}
		return normalized.Actions[i].Kind < normalized.Actions[j].Kind
	})
	normalized.VolumeSlots = append([]RecipeVolumeSlotRequirementV1{}, artifact.VolumeSlots...)
	sort.Slice(normalized.VolumeSlots, func(i, j int) bool { return normalized.VolumeSlots[i].SlotID < normalized.VolumeSlots[j].SlotID })
	normalized.DataSlots = append([]RecipeDataSlotRequirementV1{}, artifact.DataSlots...)
	sort.Slice(normalized.DataSlots, func(i, j int) bool { return normalized.DataSlots[i].SlotID < normalized.DataSlots[j].SlotID })
	normalized.SecretSlots = append([]RecipeSecretSlotRequirementV1{}, artifact.SecretSlots...)
	sort.Slice(normalized.SecretSlots, func(i, j int) bool { return normalized.SecretSlots[i].SlotID < normalized.SecretSlots[j].SlotID })
	return normalized
}

func validateCompiledRecipeSlots(artifact CompiledRecipeArtifactV1) error {
	if err := validateRecipeSlotRequirements(artifact.VolumeSlots, artifact.DataSlots, artifact.SecretSlots); err != nil {
		return fmt.Errorf("compiled recipe artifact slots are invalid: %w", err)
	}
	return nil
}

func validateCompiledRecipeIdentifier(label, value string) error {
	if err := validateIdentifier(label, value); err != nil {
		return err
	}
	return rejectCompiledRecipeExecutableText(label, value)
}

func validateCompiledRecipePurpose(value string) error {
	if err := validateText("slot purpose", value, 160); err != nil {
		return err
	}
	return rejectCompiledRecipeExecutableText("slot purpose", value)
}

func rejectCompiledRecipeExecutableText(label, value string) error {
	if err := rejectSecretMaterial(label, value); err != nil {
		return err
	}
	if compiledRecipeSecretPattern.MatchString(value) {
		return fmt.Errorf("%s must not contain credential material", label)
	}
	lower := strings.ToLower(value)
	if strings.Contains(lower, "://") || strings.HasPrefix(value, "/") || strings.HasPrefix(value, `\`) || strings.Contains(value, `\`) || strings.Contains(value, "../") ||
		strings.Contains(value, "=") || strings.Contains(value, ";") || strings.Contains(value, "&&") || strings.Contains(value, "||") || strings.Contains(value, "`") || strings.Contains(value, "$(") || strings.Contains(value, "|") || strings.Contains(value, ">") || strings.Contains(value, "<") ||
		strings.HasPrefix(lower, "secret_ref:") || strings.HasPrefix(lower, "volume_ref:") || strings.HasPrefix(lower, "data_ref:") || compiledRecipeCommandPattern.MatchString(value) {
		return fmt.Errorf("%s must not contain a reference, path, URL, or command", label)
	}
	if strings.HasPrefix(lower, "env:") || strings.HasPrefix(lower, "environment:") || strings.HasPrefix(lower, "value:") || compiledRecipeEnvNamePattern.MatchString(value) {
		return fmt.Errorf("%s must not contain an environment name or value", label)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return fmt.Errorf("%s must not contain control characters", label)
		}
	}
	return nil
}

func validCompiledRecipeActionKind(kind CompiledRecipeActionKind) bool {
	switch kind {
	case CompiledRecipeActionInstall, CompiledRecipeActionStart, CompiledRecipeActionStop, CompiledRecipeActionRestart, CompiledRecipeActionUpgrade, CompiledRecipeActionRollback, CompiledRecipeActionBackup, CompiledRecipeActionRestore, CompiledRecipeActionDestroy:
		return true
	default:
		return false
	}
}

func compiledRecipeExactObject(raw []byte, expected []string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("compiled recipe artifact object is invalid")
	}
	values := make(map[string]json.RawMessage, len(expected))
	for decoder.More() {
		keyToken, err := decoder.Token()
		key, ok := keyToken.(string)
		if err != nil || !ok {
			return nil, errors.New("compiled recipe artifact field is invalid")
		}
		if _, duplicate := values[key]; duplicate {
			return nil, errors.New("compiled recipe artifact field is duplicated")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, errors.New("compiled recipe artifact value is invalid")
		}
		values[key] = value
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, errors.New("compiled recipe artifact object is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("compiled recipe artifact JSON has trailing data")
	}
	if len(values) != len(expected) {
		return nil, errors.New("compiled recipe artifact fields are invalid")
	}
	for _, field := range expected {
		if _, ok := values[field]; !ok {
			return nil, errors.New("compiled recipe artifact fields are invalid")
		}
	}
	return values, nil
}

func compiledRecipeExactArray(raw []byte, fields []string) error {
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '[' {
		return errors.New("compiled recipe artifact array is invalid")
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return errors.New("compiled recipe artifact array is invalid")
	}
	for _, item := range items {
		if _, err := compiledRecipeExactObject(item, fields); err != nil {
			return err
		}
	}
	return nil
}
