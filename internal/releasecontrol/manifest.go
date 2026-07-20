package releasecontrol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/distribution/reference"
)

const SupportedManifestVersion = 1

var (
	canonicalVersionPattern = regexp.MustCompile(`^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$`)
	digestPattern           = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

type Manifest struct {
	ManifestVersion               int      `json:"manifest_version"`
	Version                       string   `json:"version"`
	Image                         string   `json:"image"`
	ImageDigest                   string   `json:"image_digest"`
	BaselineResetFromVersion      string   `json:"baseline_reset_from_version,omitempty"`
	UpgradeFrom                   []string `json:"upgrade_from"`
	SchemaVersion                 int      `json:"schema_version"`
	SchemaCompatVersion           int      `json:"schema_compat_version"`
	MinimumClientVersion          string   `json:"minimum_client_version"`
	MaximumClientVersionExclusive string   `json:"maximum_client_version_exclusive"`
	BackupRequired                bool     `json:"backup_required"`
	RollbackSupported             bool     `json:"rollback_supported"`
	RollbackMode                  string   `json:"rollback_mode"`
	ReleaseNotesURL               string   `json:"release_notes_url"`
}

func ValidateManifest(data []byte) (Manifest, error) {
	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode release manifest: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return Manifest{}, err
	}
	if err := manifest.validate(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode release manifest: multiple JSON values")
		}
		return fmt.Errorf("decode release manifest: %w", err)
	}
	return nil
}

func (manifest Manifest) validate() error {
	if manifest.ManifestVersion != SupportedManifestVersion {
		return fmt.Errorf("manifest_version %d is not supported", manifest.ManifestVersion)
	}
	target, err := parseCanonicalVersion("version", manifest.Version)
	if err != nil {
		return err
	}
	if err := validateImage(manifest.Image, manifest.Version); err != nil {
		return err
	}
	if !digestPattern.MatchString(manifest.ImageDigest) {
		return fmt.Errorf("image_digest must be a lowercase sha256 digest")
	}
	if manifest.BaselineResetFromVersion != "" {
		baseline, err := parseCanonicalVersion("baseline_reset_from_version", manifest.BaselineResetFromVersion)
		if err != nil {
			return err
		}
		if !baseline.LessThan(target) {
			return fmt.Errorf("baseline_reset_from_version must be lower than release version %s", manifest.Version)
		}
	}
	for index, value := range manifest.UpgradeFrom {
		constraint, err := semver.NewConstraint(strings.TrimSpace(value))
		if err != nil {
			return fmt.Errorf("upgrade_from[%d] is invalid: %w", index, err)
		}
		if constraint.Check(target) {
			return fmt.Errorf("upgrade_from[%d] must not include target version %s", index, manifest.Version)
		}
	}
	if manifest.SchemaVersion < 1 {
		return fmt.Errorf("schema_version must be positive")
	}
	if manifest.SchemaCompatVersion < 1 || manifest.SchemaCompatVersion > manifest.SchemaVersion {
		return fmt.Errorf("schema_compat_version must be between 1 and schema_version")
	}
	minimumClient, err := parseCanonicalVersion("minimum_client_version", manifest.MinimumClientVersion)
	if err != nil {
		return err
	}
	maximumClient, err := parseCanonicalVersion("maximum_client_version_exclusive", manifest.MaximumClientVersionExclusive)
	if err != nil {
		return err
	}
	if !minimumClient.LessThan(maximumClient) {
		return fmt.Errorf("minimum_client_version must be lower than maximum_client_version_exclusive")
	}
	if !manifest.BackupRequired {
		return fmt.Errorf("backup_required must be true")
	}
	if manifest.RollbackSupported && manifest.RollbackMode != "restore_backup" {
		return fmt.Errorf("rollback_mode must be restore_backup when rollback is supported")
	}
	if !manifest.RollbackSupported && manifest.RollbackMode != "" {
		return fmt.Errorf("rollback_mode must be empty when rollback is not supported")
	}
	if err := validateReleaseNotesURL(manifest.ReleaseNotesURL, manifest.Version); err != nil {
		return err
	}
	return nil
}

// ValidateUpgradeFrom verifies one explicit source-to-target release edge.
// Release indexes call this for every edge instead of inferring compatibility
// from version ordering alone.
func (manifest Manifest) ValidateUpgradeFrom(currentVersion string) error {
	current, err := parseCanonicalVersion("current_version", currentVersion)
	if err != nil {
		return err
	}
	target, err := parseCanonicalVersion("version", manifest.Version)
	if err != nil {
		return err
	}
	if !current.LessThan(target) {
		return fmt.Errorf("current_version %s must be lower than upgrade target %s", currentVersion, manifest.Version)
	}
	for _, value := range manifest.UpgradeFrom {
		constraint, constraintErr := semver.NewConstraint(strings.TrimSpace(value))
		if constraintErr != nil {
			return fmt.Errorf("upgrade_from is invalid: %w", constraintErr)
		}
		if constraint.Check(current) {
			return nil
		}
	}
	return fmt.Errorf("current_version %s is not an allowed upgrade source", currentVersion)
}

func parseCanonicalVersion(field, value string) (*semver.Version, error) {
	value = strings.TrimSpace(value)
	if !canonicalVersionPattern.MatchString(value) {
		return nil, fmt.Errorf("%s must be a canonical stable version such as v1.0.0", field)
	}
	parsed, err := semver.NewVersion(value)
	if err != nil {
		return nil, fmt.Errorf("%s is invalid: %w", field, err)
	}
	return parsed, nil
}

func validateImage(image, version string) error {
	if strings.TrimSpace(image) != image || strings.Contains(image, "@") {
		return fmt.Errorf("image must be a tagged reference without a digest")
	}
	named, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return fmt.Errorf("image is invalid: %w", err)
	}
	tagged, ok := named.(reference.Tagged)
	if !ok || tagged.Tag() != version {
		return fmt.Errorf("image tag must equal manifest version %s", version)
	}
	return nil
}

func validateReleaseNotesURL(value, version string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || !strings.EqualFold(parsed.Host, "github.com") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("release_notes_url must be an HTTPS github.com release URL")
	}
	if !strings.HasSuffix(parsed.EscapedPath(), "/releases/tag/"+version) {
		return fmt.Errorf("release_notes_url path must end with /releases/tag/%s", version)
	}
	return nil
}
