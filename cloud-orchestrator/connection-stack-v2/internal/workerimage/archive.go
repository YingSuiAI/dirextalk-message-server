package workerimage

import (
	"archive/tar"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

const maxMetadataFile = 4 << 20

var archiveOrder = []string{controllerCatalogPath, "compiled-recipe-artifact.json", "worker-oci-catalog.json", "worker-resource-manifest.json", "cloud-worker"}

type compiledAction struct {
	Kind               string   `json:"kind"`
	ActionID           string   `json:"action_id"`
	RootRequired       bool     `json:"root_required"`
	TimeoutSeconds     uint32   `json:"timeout_seconds"`
	CheckpointSequence []string `json:"checkpoint_sequence"`
}
type requirements struct {
	MinVCPU         uint16 `json:"min_vcpu"`
	MinMemoryMiB    uint32 `json:"min_memory_mib"`
	MinDiskGiB      uint32 `json:"min_disk_gib"`
	MinGPUCount     uint16 `json:"min_gpu_count,omitempty"`
	MinGPUMemoryMiB uint32 `json:"min_gpu_memory_mib,omitempty"`
	Architecture    string `json:"architecture"`
}
type volumeSlot struct {
	SlotID   string `json:"slot_id"`
	Purpose  string `json:"purpose"`
	ReadOnly bool   `json:"read_only"`
}
type secretSlot struct {
	SlotID   string `json:"slot_id"`
	Purpose  string `json:"purpose"`
	Delivery string `json:"delivery"`
}
type compiledArtifact struct {
	SchemaVersion                 string           `json:"schema_version"`
	RecipeID                      string           `json:"recipe_id"`
	RecipeDigest                  string           `json:"recipe_digest"`
	RecipeRevision                uint64           `json:"recipe_revision"`
	ImageSource                   string           `json:"image_source"`
	OfficialSourceArtifactDigests []string         `json:"official_source_artifact_digests"`
	Architecture                  string           `json:"architecture"`
	Requirements                  requirements     `json:"requirements"`
	WorkerResourceManifestDigest  string           `json:"worker_resource_manifest_digest"`
	ArtifactDigest                string           `json:"artifact_digest"`
	MediaType                     string           `json:"media_type"`
	SizeBytes                     uint64           `json:"size_bytes"`
	Actions                       []compiledAction `json:"actions"`
	HealthContractDigest          string           `json:"health_contract_digest"`
	LifecycleContractDigest       string           `json:"lifecycle_contract_digest"`
	VolumeSlots                   []volumeSlot     `json:"volume_slots"`
	DataSlots                     []volumeSlot     `json:"data_slots"`
	SecretSlots                   []secretSlot     `json:"secret_slots"`
}
type probe struct {
	Scheme         string `json:"scheme"`
	Port           uint16 `json:"port"`
	Path           string `json:"path"`
	ExpectedStatus uint16 `json:"expected_status"`
	BodySHA256     string `json:"body_sha256"`
}
type health struct {
	Liveness  probe `json:"liveness"`
	Readiness probe `json:"readiness"`
	Semantic  probe `json:"semantic"`
}
type bundle struct {
	SchemaVersion           string           `json:"schema_version"`
	ImageSource             string           `json:"image_source"`
	ArtifactDigest          string           `json:"artifact_digest"`
	ImageDigest             string           `json:"image_digest"`
	ImageSizeBytes          uint64           `json:"image_size_bytes"`
	Architecture            string           `json:"architecture"`
	Actions                 []compiledAction `json:"actions"`
	Health                  health           `json:"health"`
	HealthContractDigest    string           `json:"health_contract_digest"`
	LifecycleContractDigest string           `json:"lifecycle_contract_digest"`
}
type secretTarget struct {
	SlotID         string `json:"slot_id"`
	FileName       string `json:"file_name"`
	EnvironmentKey string `json:"environment_key"`
}
type workerCatalogEntry struct {
	ArtifactDigest string         `json:"artifact_digest"`
	BundleDigest   string         `json:"bundle_digest"`
	ActionIDs      []string       `json:"action_ids"`
	SecretTargets  []secretTarget `json:"secret_targets"`
	Descriptor     bundle         `json:"descriptor"`
}
type workerCatalog struct {
	SchemaVersion string               `json:"schema_version"`
	Entries       []workerCatalogEntry `json:"entries"`
}
type workerManifest struct {
	SchemaVersion      string `json:"schema_version"`
	WorkerBinaryDigest string `json:"worker_binary_digest"`
	CatalogDigest      string `json:"catalog_digest"`
	RuntimeIdentity    string `json:"runtime_identity"`
}
type workerCatalogDigestEntry struct {
	ArtifactDigest string         `json:"artifact_digest"`
	BundleDigest   string         `json:"bundle_digest"`
	ActionIDs      []string       `json:"action_ids"`
	SecretTargets  []secretTarget `json:"secret_targets"`
}
type workerCatalogDigestDocument struct {
	SchemaVersion string                     `json:"schema_version"`
	Entries       []workerCatalogDigestEntry `json:"entries"`
}

func ValidateArchive(path string) (ValidatedArtifact, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Size() <= 0 {
		return ValidatedArtifact{}, ErrInvalidArtifact
	}
	file, err := os.Open(path)
	if err != nil {
		return ValidatedArtifact{}, err
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) || after.Size() != before.Size() {
		return ValidatedArtifact{}, ErrInvalidArtifact
	}
	reader := tar.NewReader(file)
	metadata := map[string][]byte{}
	var catalog TrustedCatalog
	for index, wantPath := range archiveOrder {
		header, err := reader.Next()
		if err != nil || header.Name != wantPath || header.Typeflag != tar.TypeReg || header.ModTime.Unix() != 0 || header.Uid != 0 || header.Gid != 0 || header.Uname != "" || header.Gname != "" {
			return ValidatedArtifact{}, ErrInvalidArtifact
		}
		wantMode := int64(0o644)
		if wantPath == "cloud-worker" {
			wantMode = 0o755
		}
		if header.Mode != wantMode || header.Size <= 0 {
			return ValidatedArtifact{}, ErrInvalidArtifact
		}
		hash := sha256.New()
		source := io.TeeReader(reader, hash)
		if index < 4 {
			if header.Size > maxMetadataFile {
				return ValidatedArtifact{}, ErrInvalidArtifact
			}
			raw, err := io.ReadAll(io.LimitReader(source, header.Size+1))
			if err != nil || int64(len(raw)) != header.Size {
				return ValidatedArtifact{}, ErrInvalidArtifact
			}
			metadata[wantPath] = raw
		} else {
			if written, err := io.Copy(io.Discard, source); err != nil || written != header.Size {
				return ValidatedArtifact{}, ErrInvalidArtifact
			}
		}
		if index == 0 {
			if strictDecode(metadata[wantPath], &catalog) != nil || validateTrustedCatalog(catalog) != nil {
				return ValidatedArtifact{}, ErrInvalidArtifact
			}
		} else {
			binding, ok := catalogFile(catalog, wantPath)
			if !ok || binding.Mode != uint32(wantMode) || binding.SizeBytes != uint64(header.Size) || binding.SHA256 != namedSHA(hash.Sum(nil)) {
				return ValidatedArtifact{}, ErrInvalidArtifact
			}
		}
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		return ValidatedArtifact{}, ErrInvalidArtifact
	}
	position, err := file.Seek(0, io.SeekCurrent)
	if err != nil || position != before.Size() {
		return ValidatedArtifact{}, fmt.Errorf("%w: archive length", ErrInvalidArtifact)
	}

	var artifact compiledArtifact
	if strictDecode(metadata["compiled-recipe-artifact.json"], &artifact) != nil {
		return ValidatedArtifact{}, fmt.Errorf("%w: compiled artifact shape", ErrInvalidArtifact)
	}
	normalizeArtifact(&artifact)
	artifactDigest, err := digestCanonical(artifact)
	if err != nil || artifactDigest != catalog.CompiledRecipeArtifactDigest || artifact.WorkerResourceManifestDigest != catalog.WorkerResourceManifestDigest || artifact.ArtifactDigest == "" || artifact.ImageSource != catalog.ImageSource {
		return ValidatedArtifact{}, fmt.Errorf("%w: compiled artifact binding", ErrInvalidArtifact)
	}
	var worker workerManifest
	if strictDecode(metadata["worker-resource-manifest.json"], &worker) != nil || worker.SchemaVersion != "dirextalk.worker-resource-manifest/v1" || worker.RuntimeIdentity != RuntimeIdentity {
		return ValidatedArtifact{}, fmt.Errorf("%w: worker manifest shape", ErrInvalidArtifact)
	}
	workerDigest, err := digestJSON(worker)
	if err != nil || workerDigest != catalog.WorkerResourceManifestDigest || worker.WorkerBinaryDigest != catalog.WorkerBinaryDigest || worker.CatalogDigest != catalog.WorkerOCICatalogDigest {
		return ValidatedArtifact{}, fmt.Errorf("%w: worker manifest binding", ErrInvalidArtifact)
	}
	var oci workerCatalog
	if strictDecode(metadata["worker-oci-catalog.json"], &oci) != nil || len(oci.Entries) != 1 || oci.SchemaVersion != "dirextalk.worker-oci-catalog/v1" {
		return ValidatedArtifact{}, fmt.Errorf("%w: worker catalog shape", ErrInvalidArtifact)
	}
	normalizeWorkerCatalog(&oci)
	digestDocument := workerCatalogDigestDocument{SchemaVersion: oci.SchemaVersion, Entries: make([]workerCatalogDigestEntry, len(oci.Entries))}
	for i, entry := range oci.Entries {
		secretTargets := make([]secretTarget, len(entry.SecretTargets))
		copy(secretTargets, entry.SecretTargets)
		digestDocument.Entries[i] = workerCatalogDigestEntry{entry.ArtifactDigest, entry.BundleDigest, append([]string(nil), entry.ActionIDs...), secretTargets}
	}
	ociDigest, err := digestJSON(digestDocument)
	entry := oci.Entries[0]
	descriptorDigest, descriptorErr := digestCanonical(entry.Descriptor)
	if err != nil || descriptorErr != nil || descriptorDigest != entry.BundleDigest || ociDigest != catalog.WorkerOCICatalogDigest || entry.ArtifactDigest != artifact.ArtifactDigest || entry.Descriptor.ImageDigest != artifact.ArtifactDigest || entry.Descriptor.ImageDigest != entry.Descriptor.ArtifactDigest || !validPinnedImageSource(catalog.ImageSource, artifact.ArtifactDigest) || entry.Descriptor.ImageSource != catalog.ImageSource {
		return ValidatedArtifact{}, fmt.Errorf("%w: worker catalog binding", ErrInvalidArtifact)
	}
	catalog.Files = sortedFiles(catalog.Files)
	catalogDigest, err := digestCanonical(catalog)
	if err != nil {
		return ValidatedArtifact{}, ErrInvalidArtifact
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return ValidatedArtifact{}, ErrInvalidArtifact
	}
	// S3 ChecksumSHA256 covers the complete object bytes, including tar
	// padding. Hash the exact file only after the structural validation above.
	archiveHash := sha256.New()
	if _, err := io.Copy(archiveHash, file); err != nil {
		return ValidatedArtifact{}, ErrInvalidArtifact
	}
	final, err := file.Stat()
	if err != nil || !final.Mode().IsRegular() || !os.SameFile(before, final) || final.Size() != before.Size() {
		return ValidatedArtifact{}, ErrInvalidArtifact
	}
	return ValidatedArtifact{Catalog: catalog, CatalogDigest: catalogDigest, ArchiveSHA256: namedSHA(archiveHash.Sum(nil)), ArchiveSize: before.Size(), ImageDigest: entry.Descriptor.ImageDigest, Path: path}, nil
}

func validateTrustedCatalog(catalog TrustedCatalog) error {
	if catalog.SchemaVersion != TrustedCatalogSchema || !validVersion(catalog.ArtifactVersion) || catalog.RecipeID == "" || catalog.RecipeRevision == 0 || catalog.RuntimeIdentity != RuntimeIdentity || len(catalog.Files) != 4 || !validPinnedImageSource(catalog.ImageSource, "") {
		return ErrInvalidArtifact
	}
	for _, value := range []string{catalog.RecipeDigest, catalog.CompiledRecipeArtifactDigest, catalog.WorkerResourceManifestDigest, catalog.WorkerOCICatalogDigest, catalog.WorkerBinaryDigest} {
		if !digestPattern.MatchString(value) {
			return ErrInvalidArtifact
		}
	}
	want := map[string]uint32{"cloud-worker": 0o755, "compiled-recipe-artifact.json": 0o644, "worker-oci-catalog.json": 0o644, "worker-resource-manifest.json": 0o644}
	previous := ""
	for _, file := range catalog.Files {
		mode, ok := want[file.Path]
		if !ok || file.Path <= previous || file.Mode != mode || file.SizeBytes == 0 || !digestPattern.MatchString(file.SHA256) {
			return ErrInvalidArtifact
		}
		previous = file.Path
		delete(want, file.Path)
	}
	if len(want) != 0 {
		return ErrInvalidArtifact
	}
	return nil
}

func validPinnedImageSource(source, digest string) bool {
	match := ociSourcePattern.FindStringSubmatch(source)
	return len(match) == 3 && (digest == "" || match[2] == digest) && !strings.Contains(source, "//") && !strings.Contains(source, "/../") && !strings.Contains(source, "/./") && !strings.Contains(source, "/@")
}

func catalogFile(catalog TrustedCatalog, path string) (ArtifactFile, bool) {
	for _, file := range catalog.Files {
		if file.Path == path {
			return file, true
		}
	}
	return ArtifactFile{}, false
}
func normalizeArtifact(artifact *compiledArtifact) {
	sort.Strings(artifact.OfficialSourceArtifactDigests)
	sort.Slice(artifact.Actions, func(i, j int) bool {
		if artifact.Actions[i].Kind == artifact.Actions[j].Kind {
			return artifact.Actions[i].ActionID < artifact.Actions[j].ActionID
		}
		return artifact.Actions[i].Kind < artifact.Actions[j].Kind
	})
	sort.Slice(artifact.VolumeSlots, func(i, j int) bool { return artifact.VolumeSlots[i].SlotID < artifact.VolumeSlots[j].SlotID })
	sort.Slice(artifact.DataSlots, func(i, j int) bool { return artifact.DataSlots[i].SlotID < artifact.DataSlots[j].SlotID })
	sort.Slice(artifact.SecretSlots, func(i, j int) bool { return artifact.SecretSlots[i].SlotID < artifact.SecretSlots[j].SlotID })
}
func normalizeWorkerCatalog(catalog *workerCatalog) {
	for i := range catalog.Entries {
		sort.Strings(catalog.Entries[i].ActionIDs)
		sort.Slice(catalog.Entries[i].SecretTargets, func(a, b int) bool {
			return catalog.Entries[i].SecretTargets[a].SlotID < catalog.Entries[i].SecretTargets[b].SlotID
		})
	}
	sort.Slice(catalog.Entries, func(i, j int) bool { return catalog.Entries[i].ArtifactDigest < catalog.Entries[j].ArtifactDigest })
}
