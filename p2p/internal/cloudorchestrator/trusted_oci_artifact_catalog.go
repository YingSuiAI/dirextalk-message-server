package cloudorchestrator

import (
	"bytes"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
)

const TrustedOCIArtifactCatalogV1Schema = "dirextalk.trusted-oci-artifact-catalog/v1"

const (
	TrustedOCIArtifactControllerCatalogPath = "controller-trusted-artifact-catalog.json"
	TrustedOCIArtifactCompiledRecipePath    = "compiled-recipe-artifact.json"
	TrustedOCIArtifactWorkerCatalogPath     = "worker-oci-catalog.json"
	TrustedOCIArtifactWorkerManifestPath    = "worker-resource-manifest.json"
	TrustedOCIArtifactWorkerBinaryPath      = "cloud-worker"
)

var immutableArtifactVersionPattern = regexp.MustCompile(`^v?(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*$`)

type TrustedOCIArtifactFileV1 struct {
	Path      string `json:"path"`
	SHA256    string `json:"sha256"`
	SizeBytes uint64 `json:"size_bytes"`
	Mode      uint32 `json:"mode"`
}

// TrustedOCIArtifactCatalogV1 is one Controller-owned registration record for
// one immutable Recipe artifact. It binds the de-secreted control-plane
// artifact to the exact Worker catalog, resource manifest, and binary shipped
// in the same deterministic archive.
type TrustedOCIArtifactCatalogV1 struct {
	SchemaVersion                string                     `json:"schema_version"`
	ArtifactVersion              string                     `json:"artifact_version"`
	RecipeID                     string                     `json:"recipe_id"`
	RecipeDigest                 string                     `json:"recipe_digest"`
	RecipeRevision               uint64                     `json:"recipe_revision"`
	ImageSource                  OCIImageSourceReferenceV1  `json:"image_source"`
	CompiledRecipeArtifactDigest string                     `json:"compiled_recipe_artifact_digest"`
	WorkerResourceManifestDigest string                     `json:"worker_resource_manifest_digest"`
	WorkerOCICatalogDigest       string                     `json:"worker_oci_catalog_digest"`
	WorkerBinaryDigest           string                     `json:"worker_binary_digest"`
	RuntimeIdentity              string                     `json:"runtime_identity"`
	Files                        []TrustedOCIArtifactFileV1 `json:"files"`
}

func (catalog TrustedOCIArtifactCatalogV1) Validate() error {
	if catalog.SchemaVersion != TrustedOCIArtifactCatalogV1Schema ||
		!immutableArtifactVersionPattern.MatchString(catalog.ArtifactVersion) ||
		catalog.ArtifactVersion == "v1.0.3" || catalog.ArtifactVersion == "1.0.3" || catalog.ArtifactVersion == "latest" ||
		validateIdentifier("recipe_id", catalog.RecipeID) != nil || catalog.RecipeRevision == 0 ||
		catalog.ImageSource.Validate() != nil || catalog.RuntimeIdentity != "dirextalk-podman-v1" {
		return errors.New("trusted OCI artifact catalog identity is invalid")
	}
	for _, digest := range []string{catalog.RecipeDigest, catalog.CompiledRecipeArtifactDigest, catalog.WorkerResourceManifestDigest, catalog.WorkerOCICatalogDigest, catalog.WorkerBinaryDigest} {
		if validateDigest("digest", digest) != nil {
			return errors.New("trusted OCI artifact catalog digest is invalid")
		}
	}
	wantModes := map[string]uint32{
		TrustedOCIArtifactCompiledRecipePath: 0o644,
		TrustedOCIArtifactWorkerCatalogPath:  0o644,
		TrustedOCIArtifactWorkerManifestPath: 0o644,
		TrustedOCIArtifactWorkerBinaryPath:   0o755,
	}
	if len(catalog.Files) != len(wantModes) {
		return errors.New("trusted OCI artifact catalog files are invalid")
	}
	previous := ""
	for _, file := range catalog.Files {
		mode, ok := wantModes[file.Path]
		if !ok || file.Path <= previous || file.Mode != mode || file.SizeBytes == 0 || validateDigest("sha256", file.SHA256) != nil {
			return errors.New("trusted OCI artifact catalog file is invalid")
		}
		previous = file.Path
		delete(wantModes, file.Path)
	}
	if len(wantModes) != 0 {
		return errors.New("trusted OCI artifact catalog files are incomplete")
	}
	return nil
}

func (catalog TrustedOCIArtifactCatalogV1) Digest() (string, error) {
	normalized := catalog
	normalized.Files = append([]TrustedOCIArtifactFileV1(nil), catalog.Files...)
	sort.Slice(normalized.Files, func(i, j int) bool { return normalized.Files[i].Path < normalized.Files[j].Path })
	if err := normalized.Validate(); err != nil {
		return "", err
	}
	raw, err := canonicalCBOR(normalized)
	if err != nil {
		return "", err
	}
	return digestCanonicalCBOR(raw), nil
}

func ParseTrustedOCIArtifactCatalogV1(raw []byte) (TrustedOCIArtifactCatalogV1, error) {
	if len(raw) == 0 || len(raw) > 64*1024 {
		return TrustedOCIArtifactCatalogV1{}, errors.New("trusted OCI artifact catalog JSON is invalid")
	}
	top, err := compiledRecipeExactObject(raw, []string{"schema_version", "artifact_version", "recipe_id", "recipe_digest", "recipe_revision", "image_source", "compiled_recipe_artifact_digest", "worker_resource_manifest_digest", "worker_oci_catalog_digest", "worker_binary_digest", "runtime_identity", "files"})
	if err != nil || compiledRecipeExactArray(top["files"], []string{"path", "sha256", "size_bytes", "mode"}) != nil {
		return TrustedOCIArtifactCatalogV1{}, errors.New("trusted OCI artifact catalog JSON is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var catalog TrustedOCIArtifactCatalogV1
	if decoder.Decode(&catalog) != nil || decoder.Decode(&struct{}{}) == nil || catalog.Validate() != nil {
		return TrustedOCIArtifactCatalogV1{}, errors.New("trusted OCI artifact catalog JSON is invalid")
	}
	return catalog, nil
}
