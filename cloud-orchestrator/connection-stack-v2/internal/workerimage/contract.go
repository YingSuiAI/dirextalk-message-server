package workerimage

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/fxamacker/cbor/v2"
)

const (
	TrustedCatalogSchema  = "dirextalk.trusted-oci-artifact-catalog/v1"
	ImageManifestSchema   = "dirextalk.worker-image-manifest/v1"
	RuntimeIdentity       = "dirextalk-podman-v1"
	RecipeArtifactStatic  = "static"
	RecipeArtifactDynamic = "dynamic"
	controllerCatalogPath = "controller-trusted-artifact-catalog.json"
)

var (
	ErrInvalidArtifact = errors.New("worker image artifact is invalid")
	ErrInvalidConfig   = errors.New("worker image configuration is invalid")
	ErrBuildFailed     = errors.New("worker image build failed")
	digestPattern      = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	versionPattern     = regexp.MustCompile(`^v?(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*$`)
	awsIDPattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{1,255}$`)
	objectKeyPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{1,511}$`)
	ociSourcePattern   = regexp.MustCompile(`^(ghcr\.io|docker\.io|quay\.io|public\.ecr\.aws)/[a-z0-9][a-z0-9._/-]*@(sha256:[0-9a-f]{64})$`)
)

type ArtifactFile struct {
	Path      string `json:"path"`
	SHA256    string `json:"sha256"`
	SizeBytes uint64 `json:"size_bytes"`
	Mode      uint32 `json:"mode"`
}

type TrustedCatalog struct {
	SchemaVersion                string         `json:"schema_version"`
	ArtifactVersion              string         `json:"artifact_version"`
	RecipeID                     string         `json:"recipe_id"`
	RecipeDigest                 string         `json:"recipe_digest"`
	RecipeRevision               uint64         `json:"recipe_revision"`
	ImageSource                  string         `json:"image_source"`
	CompiledRecipeArtifactDigest string         `json:"compiled_recipe_artifact_digest"`
	WorkerResourceManifestDigest string         `json:"worker_resource_manifest_digest"`
	WorkerOCICatalogDigest       string         `json:"worker_oci_catalog_digest"`
	WorkerBinaryDigest           string         `json:"worker_binary_digest"`
	RuntimeIdentity              string         `json:"runtime_identity"`
	Files                        []ArtifactFile `json:"files"`
}

type BuildConfig struct {
	Region          string
	BaseAMIID       string
	SubnetID        string
	SecurityGroupID string
	Bucket          string
	ObjectKey       string
	ArtifactVersion string
	InstanceType    string
	OCISource       string
	// DynamicRecipeArtifacts is deliberately default-off. When enabled the AMI
	// contains only the measured Worker runtime and resolves approved Recipe
	// artifacts at task time instead of binding the bundled static catalog.
	DynamicRecipeArtifacts bool
	Timeout                time.Duration
}

func (config BuildConfig) Validate(artifact ValidatedArtifact) error {
	if !validVersion(config.ArtifactVersion) || config.ArtifactVersion != artifact.Catalog.ArtifactVersion ||
		!awsIDPattern.MatchString(config.Region) || !strings.HasPrefix(config.BaseAMIID, "ami-") ||
		!strings.HasPrefix(config.SubnetID, "subnet-") || !strings.HasPrefix(config.SecurityGroupID, "sg-") ||
		!awsIDPattern.MatchString(config.Bucket) || !objectKeyPattern.MatchString(config.ObjectKey) ||
		!awsIDPattern.MatchString(config.InstanceType) || config.Timeout < 5*time.Minute || config.Timeout > 2*time.Hour {
		return ErrInvalidConfig
	}
	match := ociSourcePattern.FindStringSubmatch(config.OCISource)
	if len(match) != 3 || match[2] != artifact.ImageDigest || config.OCISource != artifact.Catalog.ImageSource || strings.Contains(config.OCISource, "//") || strings.Contains(config.OCISource, "/../") || strings.Contains(config.OCISource, "/./") || strings.Contains(config.OCISource, "/@") {
		return ErrInvalidConfig
	}
	return nil
}

type ImageManifest struct {
	SchemaVersion                string   `json:"schema_version"`
	ArtifactVersion              string   `json:"artifact_version"`
	Region                       string   `json:"region"`
	ImageID                      string   `json:"image_id"`
	ImageName                    string   `json:"image_name"`
	BaseAMIID                    string   `json:"base_ami_id"`
	OCISource                    string   `json:"oci_source"`
	ArchiveSHA256                string   `json:"archive_sha256"`
	TrustedCatalogDigest         string   `json:"trusted_catalog_digest"`
	WorkerResourceManifestDigest string   `json:"worker_resource_manifest_digest"`
	WorkerOCICatalogDigest       string   `json:"worker_oci_catalog_digest"`
	WorkerBinaryDigest           string   `json:"worker_binary_digest"`
	RecipeArtifactMode           string   `json:"recipe_artifact_mode"`
	SnapshotIDs                  []string `json:"snapshot_ids"`
	CreatedAt                    string   `json:"created_at"`
}

func (manifest ImageManifest) Validate() error {
	if manifest.SchemaVersion != ImageManifestSchema || !validVersion(manifest.ArtifactVersion) || !awsIDPattern.MatchString(manifest.Region) ||
		!strings.HasPrefix(manifest.ImageID, "ami-") || manifest.ImageName == "" || !strings.HasPrefix(manifest.BaseAMIID, "ami-") ||
		len(ociSourcePattern.FindStringSubmatch(manifest.OCISource)) != 3 ||
		(manifest.RecipeArtifactMode != RecipeArtifactStatic && manifest.RecipeArtifactMode != RecipeArtifactDynamic) || len(manifest.SnapshotIDs) == 0 {
		return ErrInvalidConfig
	}
	for _, digest := range []string{manifest.ArchiveSHA256, manifest.TrustedCatalogDigest, manifest.WorkerResourceManifestDigest, manifest.WorkerOCICatalogDigest, manifest.WorkerBinaryDigest} {
		if !digestPattern.MatchString(digest) {
			return ErrInvalidConfig
		}
	}
	for _, id := range manifest.SnapshotIDs {
		if !strings.HasPrefix(id, "snap-") {
			return ErrInvalidConfig
		}
	}
	if _, err := time.Parse(time.RFC3339Nano, manifest.CreatedAt); err != nil {
		return ErrInvalidConfig
	}
	return nil
}

func ParseImageManifest(raw []byte) (ImageManifest, error) {
	var manifest ImageManifest
	if strictDecode(raw, &manifest) != nil || manifest.Validate() != nil {
		return ImageManifest{}, ErrInvalidConfig
	}
	return manifest, nil
}

type ValidatedArtifact struct {
	Catalog       TrustedCatalog
	CatalogDigest string
	ArchiveSHA256 string
	ArchiveSize   int64
	ImageDigest   string
	Path          string
}

func validVersion(value string) bool {
	return versionPattern.MatchString(value) && value != "latest" && value != "v1.0.3" && value != "1.0.3"
}

func namedSHA(raw []byte) string { return "sha256:" + hex.EncodeToString(raw) }

func digestJSON(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return namedSHA(sum[:]), nil
}

func digestCanonical(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var compatible any
	if err := decoder.Decode(&compatible); err != nil {
		return "", err
	}
	compatible, err = integersForCBOR(compatible)
	if err != nil {
		return "", err
	}
	mode, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return "", err
	}
	encoded, err := mode.Marshal(compatible)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return namedSHA(sum[:]), nil
}

func integersForCBOR(value any) (any, error) {
	switch typed := value.(type) {
	case json.Number:
		if strings.ContainsAny(string(typed), ".eE") {
			return nil, ErrInvalidArtifact
		}
		integer, err := typed.Int64()
		if err != nil {
			return nil, err
		}
		return integer, nil
	case []any:
		for i := range typed {
			converted, err := integersForCBOR(typed[i])
			if err != nil {
				return nil, err
			}
			typed[i] = converted
		}
		return typed, nil
	case map[string]any:
		for key := range typed {
			converted, err := integersForCBOR(typed[key])
			if err != nil {
				return nil, err
			}
			typed[key] = converted
		}
		return typed, nil
	default:
		return value, nil
	}
}

func strictDecode(raw []byte, target any) error {
	if len(raw) == 0 || rejectDuplicateKeys(raw) != nil {
		return ErrInvalidArtifact
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrInvalidArtifact
	}
	return nil
}

func rejectDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanJSON(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return ErrInvalidArtifact
	}
	return nil
}

func scanJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delim == '{' {
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			key, ok := keyToken.(string)
			if err != nil || !ok {
				return ErrInvalidArtifact
			}
			if _, exists := seen[key]; exists {
				return ErrInvalidArtifact
			}
			seen[key] = struct{}{}
			if scanJSON(decoder) != nil {
				return ErrInvalidArtifact
			}
		}
	} else if delim == '[' {
		for decoder.More() {
			if scanJSON(decoder) != nil {
				return ErrInvalidArtifact
			}
		}
	} else {
		return ErrInvalidArtifact
	}
	end, err := decoder.Token()
	if err != nil || (delim == '{' && end != json.Delim('}')) || (delim == '[' && end != json.Delim(']')) {
		return ErrInvalidArtifact
	}
	return nil
}

func sortedFiles(files []ArtifactFile) []ArtifactFile {
	result := append([]ArtifactFile(nil), files...)
	sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
	return result
}
