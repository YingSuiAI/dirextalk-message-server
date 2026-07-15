package cloudorchestrator

import (
	"encoding/json"
	"errors"
	"path"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxOCIServiceRuntimeArguments   = 32
	maxOCIServiceRuntimeEnvironment = 32
	maxOCIServiceRuntimeTmpfs       = 16
	maxOCIServiceRuntimeTextBytes   = 512
	maxOCIServiceRuntimeID          = 65535
	minOCIServiceTmpfsBytes         = 1 << 20
	maxOCIServiceTmpfsBytes         = 16 << 30
)

type OCIServiceCapability string

const (
	OCIServiceCapabilityChown       OCIServiceCapability = "CHOWN"
	OCIServiceCapabilityDACOverride OCIServiceCapability = "DAC_OVERRIDE"
	OCIServiceCapabilityFOwner      OCIServiceCapability = "FOWNER"
	OCIServiceCapabilitySetGID      OCIServiceCapability = "SETGID"
	OCIServiceCapabilitySetUID      OCIServiceCapability = "SETUID"
)

type OCIServiceEnvironmentV1 struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type OCIServiceTmpfsV1 struct {
	ContainerTarget string `json:"container_target"`
	SizeBytes       uint64 `json:"size_bytes"`
	Mode            uint32 `json:"mode"`
}

type OCIServiceSecretEnvironmentV1 struct {
	SlotID         string `json:"slot_id"`
	EnvironmentKey string `json:"environment_key"`
}

type OCIServiceRunAsV1 struct {
	UID uint32 `json:"uid"`
	GID uint32 `json:"gid"`
}

// OCIServiceRuntimeProfileV1 is compiler-owned, immutable container runtime
// data. It can select only a direct executable/argv, bounded non-secret
// environment, fixed tmpfs mounts, one uid:gid, and a minimal capability set.
type OCIServiceRuntimeProfileV1 struct {
	Entrypoint        string                          `json:"entrypoint,omitempty"`
	Argv              []string                        `json:"argv,omitempty"`
	Environment       []OCIServiceEnvironmentV1       `json:"environment,omitempty"`
	SecretEnvironment []OCIServiceSecretEnvironmentV1 `json:"secret_environment,omitempty"`
	Tmpfs             []OCIServiceTmpfsV1             `json:"tmpfs,omitempty"`
	RunAs             *OCIServiceRunAsV1              `json:"run_as,omitempty"`
	Capabilities      []OCIServiceCapability          `json:"capabilities,omitempty"`
	SecretReadGID     uint32                          `json:"secret_read_gid,omitempty"`
}

var (
	ociRuntimeEnvironmentNamePattern = regexp.MustCompile(`^[A-Z_][A-Z0-9_]{0,63}$`)
	ociRuntimeInterpolationPattern   = regexp.MustCompile("(?:\\$\\{|\\$\\(|\\$[A-Za-z_]|\\{\\{|\\}\\}|%[A-Za-z_][A-Za-z0-9_]*%|`)")
	ociRuntimeSecretFilePattern      = regexp.MustCompile(`^/run/secrets/[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
)

func (profile OCIServiceRuntimeProfileV1) IsZero() bool {
	return profile.Entrypoint == "" && len(profile.Argv) == 0 && len(profile.Environment) == 0 && len(profile.SecretEnvironment) == 0 && len(profile.Tmpfs) == 0 && profile.RunAs == nil && len(profile.Capabilities) == 0 && profile.SecretReadGID == 0
}

func (profile OCIServiceRuntimeProfileV1) Validate() error {
	if profile.IsZero() {
		return nil
	}
	if err := validateOCIServiceEntrypoint(profile.Entrypoint); err != nil {
		return err
	}
	if len(profile.Argv) > maxOCIServiceRuntimeArguments || len(profile.Argv) != 0 && profile.Entrypoint == "" {
		return errors.New("OCI service runtime argv is invalid")
	}
	totalArguments := 0
	for _, argument := range profile.Argv {
		if validateOCIServiceRuntimeText("OCI service runtime argument", argument, false) != nil || len(argument) == 0 || len(argument) > 256 {
			return errors.New("OCI service runtime argument is invalid")
		}
		totalArguments += len(argument)
	}
	if totalArguments > 4096 {
		return errors.New("OCI service runtime argv is too large")
	}
	if len(profile.Environment) > maxOCIServiceRuntimeEnvironment {
		return errors.New("OCI service runtime environment is too large")
	}
	seenEnvironment := make(map[string]struct{}, len(profile.Environment))
	for _, variable := range profile.Environment {
		if !ociRuntimeEnvironmentNamePattern.MatchString(variable.Name) || validateOCIServiceEnvironmentValue(variable.Name, variable.Value) != nil {
			return errors.New("OCI service runtime environment is invalid")
		}
		if _, duplicate := seenEnvironment[variable.Name]; duplicate {
			return errors.New("OCI service runtime environment is duplicated")
		}
		seenEnvironment[variable.Name] = struct{}{}
	}
	if len(profile.SecretEnvironment) > maxOCIServiceRuntimeEnvironment || len(profile.Environment)+len(profile.SecretEnvironment) > maxOCIServiceRuntimeEnvironment || len(profile.SecretEnvironment) != 0 && profile.Entrypoint == "" {
		return errors.New("OCI service runtime secret environment is invalid")
	}
	seenSecretSlots := make(map[string]struct{}, len(profile.SecretEnvironment))
	for _, variable := range profile.SecretEnvironment {
		if validateCompiledRecipeIdentifier("secret slot_id", variable.SlotID) != nil || !ociRuntimeEnvironmentNamePattern.MatchString(variable.EnvironmentKey) || strings.HasSuffix(variable.EnvironmentKey, "_FILE") {
			return errors.New("OCI service runtime secret environment is invalid")
		}
		if _, duplicate := seenSecretSlots[variable.SlotID]; duplicate {
			return errors.New("OCI service runtime secret slot is duplicated")
		}
		if _, duplicate := seenEnvironment[variable.EnvironmentKey]; duplicate {
			return errors.New("OCI service runtime environment key is duplicated")
		}
		seenSecretSlots[variable.SlotID], seenEnvironment[variable.EnvironmentKey] = struct{}{}, struct{}{}
	}
	if len(profile.Tmpfs) > maxOCIServiceRuntimeTmpfs {
		return errors.New("OCI service runtime tmpfs is too large")
	}
	seenTmpfs := make(map[string]struct{}, len(profile.Tmpfs))
	for _, mount := range profile.Tmpfs {
		if validateOCIServiceTmpfsTarget(mount.ContainerTarget) != nil || mount.SizeBytes < minOCIServiceTmpfsBytes || mount.SizeBytes > maxOCIServiceTmpfsBytes || !validOCIServiceTmpfsMode(mount.Mode) {
			return errors.New("OCI service runtime tmpfs is invalid")
		}
		if _, duplicate := seenTmpfs[mount.ContainerTarget]; duplicate {
			return errors.New("OCI service runtime tmpfs target is duplicated")
		}
		seenTmpfs[mount.ContainerTarget] = struct{}{}
	}
	if profile.RunAs != nil && (profile.RunAs.UID > maxOCIServiceRuntimeID || profile.RunAs.GID > maxOCIServiceRuntimeID) {
		return errors.New("OCI service runtime uid or gid is invalid")
	}
	seenCapabilities := make(map[OCIServiceCapability]struct{}, len(profile.Capabilities))
	for _, capability := range profile.Capabilities {
		if !validOCIServiceCapability(capability) {
			return errors.New("OCI service runtime capability is invalid")
		}
		if _, duplicate := seenCapabilities[capability]; duplicate {
			return errors.New("OCI service runtime capability is duplicated")
		}
		seenCapabilities[capability] = struct{}{}
	}
	if profile.SecretReadGID > maxOCIServiceRuntimeID || profile.SecretReadGID != 0 && profile.RunAs != nil && profile.RunAs.GID != profile.SecretReadGID {
		return errors.New("OCI service runtime secret read gid is invalid")
	}
	if len(profile.SecretEnvironment) != 0 {
		_, setUID := seenCapabilities[OCIServiceCapabilitySetUID]
		_, setGID := seenCapabilities[OCIServiceCapabilitySetGID]
		if profile.RunAs == nil || profile.RunAs.UID == 0 || profile.RunAs.GID == 0 || profile.SecretReadGID != profile.RunAs.GID || !setUID || !setGID {
			return errors.New("OCI service container-init profile is invalid")
		}
	}
	return nil
}

func NormalizeOCIServiceRuntimeProfileV1(profile *OCIServiceRuntimeProfileV1) (*OCIServiceRuntimeProfileV1, error) {
	if profile == nil || profile.IsZero() {
		return nil, nil
	}
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	normalized := *profile
	normalized.Argv = append([]string(nil), profile.Argv...)
	normalized.Environment = append([]OCIServiceEnvironmentV1(nil), profile.Environment...)
	sort.Slice(normalized.Environment, func(i, j int) bool { return normalized.Environment[i].Name < normalized.Environment[j].Name })
	normalized.SecretEnvironment = append([]OCIServiceSecretEnvironmentV1(nil), profile.SecretEnvironment...)
	sort.Slice(normalized.SecretEnvironment, func(i, j int) bool {
		return normalized.SecretEnvironment[i].EnvironmentKey < normalized.SecretEnvironment[j].EnvironmentKey
	})
	normalized.Tmpfs = append([]OCIServiceTmpfsV1(nil), profile.Tmpfs...)
	sort.Slice(normalized.Tmpfs, func(i, j int) bool { return normalized.Tmpfs[i].ContainerTarget < normalized.Tmpfs[j].ContainerTarget })
	normalized.Capabilities = append([]OCIServiceCapability(nil), profile.Capabilities...)
	sort.Slice(normalized.Capabilities, func(i, j int) bool { return normalized.Capabilities[i] < normalized.Capabilities[j] })
	if profile.RunAs != nil {
		runAs := *profile.RunAs
		normalized.RunAs = &runAs
	}
	return &normalized, nil
}

func CloneOCIServiceRuntimeProfileV1(profile *OCIServiceRuntimeProfileV1) *OCIServiceRuntimeProfileV1 {
	cloned, err := NormalizeOCIServiceRuntimeProfileV1(profile)
	if err != nil {
		return nil
	}
	return cloned
}

func NormalizeOCIServiceStorageDirectoryMode(mode uint32) (uint32, error) {
	if mode == 0 {
		return 0o700, nil
	}
	switch mode {
	case 0o500, 0o550, 0o555, 0o700, 0o750, 0o755, 0o770, 0o775:
		return mode, nil
	default:
		return 0, errors.New("OCI service storage directory mode is invalid")
	}
}

func validateOCIServiceEntrypoint(value string) error {
	if value == "" {
		return nil
	}
	if len(value) > maxOCIServiceRuntimeTextBytes || !utf8.ValidString(value) || !path.IsAbs(value) || path.Clean(value) != value || value == "/" {
		return errors.New("OCI service runtime entrypoint is invalid")
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return errors.New("OCI service runtime entrypoint is invalid")
		}
	}
	switch strings.ToLower(path.Base(value)) {
	case "sh", "bash", "dash", "ash", "zsh", "fish", "busybox", "env", "sudo":
		return errors.New("OCI service runtime shell entrypoint is forbidden")
	}
	return nil
}

func validateOCIServiceRuntimeText(label, value string, allowEmpty bool) error {
	if !allowEmpty && value == "" || len(value) > maxOCIServiceRuntimeTextBytes || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") || rejectSecretMaterial(label, value) != nil || compiledRecipeSecretPattern.MatchString(value) {
		return errors.New("OCI service runtime text is invalid")
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return errors.New("OCI service runtime text is invalid")
		}
	}
	return nil
}

func validateOCIServiceEnvironmentValue(name, value string) error {
	if validateOCIServiceRuntimeText("OCI service runtime environment", value, true) != nil || ociRuntimeInterpolationPattern.MatchString(value) {
		return errors.New("OCI service runtime environment value is invalid")
	}
	fileVariable := strings.HasSuffix(name, "_FILE")
	secretPath := strings.HasPrefix(value, "/run/secrets/")
	if fileVariable != secretPath || secretPath && (!ociRuntimeSecretFilePattern.MatchString(value) || path.Clean(value) != value) {
		return errors.New("OCI service runtime secret file environment is invalid")
	}
	return nil
}

func validOCIServiceTmpfsMode(mode uint32) bool {
	switch mode {
	case 0o700, 0o750, 0o755, 0o770, 0o775, 0o1770, 0o1777:
		return true
	default:
		return false
	}
}

func validateOCIServiceTmpfsTarget(target string) error {
	// A whole /run tmpfs is required by fixed, root-init images such as
	// s6-overlay. Secret and measured-init paths remain separate nested,
	// read-only bind mounts; storage targets still cannot cross /run.
	if target == "/run" {
		return nil
	}
	return ValidateOCIServiceContainerTarget(target)
}

func validOCIServiceCapability(capability OCIServiceCapability) bool {
	switch capability {
	case OCIServiceCapabilityChown, OCIServiceCapabilityDACOverride, OCIServiceCapabilityFOwner, OCIServiceCapabilitySetGID, OCIServiceCapabilitySetUID:
		return true
	default:
		return false
	}
}

func validateOCIServiceRuntimeProfileJSON(raw []byte) error {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return errors.New("OCI service runtime profile JSON is invalid")
	}
	allowed := map[string]bool{"entrypoint": true, "argv": true, "environment": true, "secret_environment": true, "tmpfs": true, "run_as": true, "capabilities": true, "secret_read_gid": true}
	fields := make([]string, 0, len(values))
	for field := range values {
		if !allowed[field] {
			return errors.New("OCI service runtime profile JSON is invalid")
		}
		fields = append(fields, field)
	}
	if _, err := compiledRecipeExactObject(raw, fields); err != nil {
		return err
	}
	if value, ok := values["environment"]; ok {
		if err := compiledRecipeExactArray(value, []string{"name", "value"}); err != nil {
			return err
		}
	}
	if value, ok := values["secret_environment"]; ok {
		if err := compiledRecipeExactArray(value, []string{"slot_id", "environment_key"}); err != nil {
			return err
		}
	}
	if value, ok := values["tmpfs"]; ok {
		if err := compiledRecipeExactArray(value, []string{"container_target", "size_bytes", "mode"}); err != nil {
			return err
		}
	}
	if value, ok := values["run_as"]; ok {
		if _, err := compiledRecipeExactObject(value, []string{"uid", "gid"}); err != nil {
			return err
		}
	}
	return nil
}

func validateOCIServiceStorageTargetsJSON(raw []byte) error {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return errors.New("OCI service storage targets JSON is invalid")
	}
	allowed := map[string]bool{"slot_id": true, "container_target": true, "read_only": true, "owner_uid": true, "owner_gid": true, "directory_mode": true}
	for _, item := range items {
		var values map[string]json.RawMessage
		if err := json.Unmarshal(item, &values); err != nil {
			return errors.New("OCI service storage target JSON is invalid")
		}
		for _, required := range []string{"slot_id", "container_target", "read_only"} {
			if _, ok := values[required]; !ok {
				return errors.New("OCI service storage target JSON is invalid")
			}
		}
		fields := make([]string, 0, len(values))
		for field := range values {
			if !allowed[field] {
				return errors.New("OCI service storage target JSON is invalid")
			}
			fields = append(fields, field)
		}
		if _, err := compiledRecipeExactObject(item, fields); err != nil {
			return err
		}
	}
	return nil
}

func ociServiceRuntimeMountsConflict(bundle OCIServiceBundleV1) bool {
	if bundle.RuntimeProfile == nil {
		return false
	}
	storageTargets := make([]string, 0, len(bundle.VolumeTargets)+len(bundle.DataTargets))
	for _, target := range bundle.VolumeTargets {
		storageTargets = append(storageTargets, target.ContainerTarget)
	}
	for _, target := range bundle.DataTargets {
		storageTargets = append(storageTargets, target.ContainerTarget)
	}
	for _, mount := range bundle.RuntimeProfile.Tmpfs {
		for _, target := range storageTargets {
			if ociServiceContainerPathsOverlap(mount.ContainerTarget, target) {
				return true
			}
		}
	}
	return false
}

func ociServiceContainerPathsOverlap(left, right string) bool {
	return left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/")
}
