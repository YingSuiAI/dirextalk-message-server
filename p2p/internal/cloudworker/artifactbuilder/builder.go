package artifactbuilder

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/recipecompiler"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

const (
	BuildSpecV1Schema                = "dirextalk.worker-artifact-build-spec/v1"
	ControllerTrustedArtifactCatalog = cloudorchestrator.TrustedOCIArtifactControllerCatalogPath
	maxInputBytes                    = 1 << 20
)

var fileNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

var ErrInvalidBuildInput = errors.New("worker artifact build input is invalid")

type FileSecretTargetV1 struct {
	SlotID   string `json:"slot_id"`
	FileName string `json:"file_name"`
}

// BuildSpecV1 is compiler-owned input. ImageSource is limited to a public,
// allowlisted repository pinned to ImageDigest; the spec contains no mutable
// tag, credential, arbitrary URL, command, host path, secret ref, or value.
type BuildSpecV1 struct {
	SchemaVersion   string                                        `json:"schema_version"`
	ArtifactVersion string                                        `json:"artifact_version"`
	RecipeRevision  uint64                                        `json:"recipe_revision"`
	ImageSource     cloudorchestrator.OCIImageSourceReferenceV1   `json:"image_source"`
	ImageDigest     string                                        `json:"image_digest"`
	ImageSizeBytes  uint64                                        `json:"image_size_bytes"`
	Actions         []cloudorchestrator.CompiledRecipeActionV1    `json:"actions"`
	Health          cloudorchestrator.OCIServiceHealthV1          `json:"health"`
	SecretTargets   []FileSecretTargetV1                          `json:"secret_targets"`
	VolumeTargets   []cloudorchestrator.OCIServiceMountTargetV1   `json:"volume_targets,omitempty"`
	DataTargets     []cloudorchestrator.OCIServiceMountTargetV1   `json:"data_targets,omitempty"`
	RuntimeProfile  *cloudorchestrator.OCIServiceRuntimeProfileV1 `json:"runtime_profile,omitempty"`
}

type Result struct {
	Catalog       cloudorchestrator.TrustedOCIArtifactCatalogV1
	CatalogDigest string
	ArchiveSHA256 string
	ArchiveBytes  uint64
}

type payload struct {
	path string
	mode uint32
	raw  []byte
}

// BuildArchive creates the complete immutable Worker artifact without network
// access or cloud mutation. The caller owns the output destination.
func BuildArchive(recipeRaw, specRaw []byte, workerBinaryPath string, output io.Writer) (Result, error) {
	if len(recipeRaw) == 0 || len(recipeRaw) > maxInputBytes || len(specRaw) == 0 || len(specRaw) > maxInputBytes || output == nil {
		return Result{}, ErrInvalidBuildInput
	}
	var recipe cloudorchestrator.RecipeV1
	if strictDecode(recipeRaw, &recipe) != nil || recipe.Validate() != nil {
		return Result{}, ErrInvalidBuildInput
	}
	var spec BuildSpecV1
	if strictDecode(specRaw, &spec) != nil || validateBuildSpec(recipe, spec) != nil {
		return Result{}, ErrInvalidBuildInput
	}

	binary, binaryDigest, binarySize, err := openAndHashRegular(workerBinaryPath)
	if err != nil {
		return Result{}, ErrInvalidBuildInput
	}
	defer binary.Close()

	compilerConfig := recipecompiler.Config{
		RecipeRevision: spec.RecipeRevision, ImageSource: spec.ImageSource, ImageDigest: spec.ImageDigest, ImageSizeBytes: spec.ImageSizeBytes,
		Architecture: recipe.Requirements.Architecture, HealthContract: recipe.Health, LifecycleContract: recipe.Lifecycle,
		Actions: cloneActions(spec.Actions), Health: spec.Health,
		VolumeTargets:  append([]cloudorchestrator.OCIServiceMountTargetV1(nil), spec.VolumeTargets...),
		DataTargets:    append([]cloudorchestrator.OCIServiceMountTargetV1(nil), spec.DataTargets...),
		RuntimeProfile: spec.RuntimeProfile,
	}
	bundle, err := recipecompiler.CompileOCIServiceBundleDescriptor(recipe, compilerConfig)
	if err != nil {
		return Result{}, ErrInvalidBuildInput
	}
	bundleDigest, err := bundle.Digest()
	if err != nil {
		return Result{}, ErrInvalidBuildInput
	}
	secretTargets := make([]recipeexec.SecretTarget, len(spec.SecretTargets))
	for index, target := range spec.SecretTargets {
		secretTargets[index] = recipeexec.SecretTarget{SlotID: target.SlotID, FileName: target.FileName}
	}
	actionIDs := make([]string, len(bundle.Actions))
	for index, action := range bundle.Actions {
		actionIDs[index] = action.ActionID
	}
	workerCatalog := recipeexec.WorkerOCICatalogV1{SchemaVersion: recipeexec.WorkerOCICatalogV1Schema, Entries: []recipeexec.WorkerOCICatalogEntryV1{{
		ArtifactDigest: bundle.ArtifactDigest, BundleDigest: bundleDigest, ActionIDs: actionIDs, SecretTargets: secretTargets, Descriptor: bundle,
	}}}
	workerCatalogDigest, err := workerCatalog.Digest()
	if err != nil {
		return Result{}, ErrInvalidBuildInput
	}
	resourceManifest := recipeexec.WorkerResourceManifestV1{
		SchemaVersion: recipeexec.WorkerResourceManifestV1Schema, WorkerBinaryDigest: binaryDigest,
		CatalogDigest: workerCatalogDigest, RuntimeIdentity: recipeexec.WorkerRuntimeIdentityPodmanV1,
	}
	resourceManifestDigest, err := resourceManifest.Digest()
	if err != nil {
		return Result{}, ErrInvalidBuildInput
	}
	artifact, err := recipecompiler.FinalizeOCIServiceArtifact(recipe, compilerConfig, bundle, resourceManifestDigest)
	if err != nil {
		return Result{}, ErrInvalidBuildInput
	}
	artifactDigest, err := artifact.Digest()
	if err != nil {
		return Result{}, ErrInvalidBuildInput
	}

	// The strict wire parser requires arrays rather than JSON null even when a
	// Recipe has no slots of a given kind.
	if artifact.VolumeSlots == nil {
		artifact.VolumeSlots = []cloudorchestrator.RecipeVolumeSlotRequirementV1{}
	}
	if artifact.DataSlots == nil {
		artifact.DataSlots = []cloudorchestrator.RecipeDataSlotRequirementV1{}
	}
	if artifact.SecretSlots == nil {
		artifact.SecretSlots = []cloudorchestrator.RecipeSecretSlotRequirementV1{}
	}
	artifactRaw, workerCatalogRaw, resourceManifestRaw, err := marshalPayloads(artifact, workerCatalog, resourceManifest)
	if err != nil {
		return Result{}, ErrInvalidBuildInput
	}
	files := []cloudorchestrator.TrustedOCIArtifactFileV1{
		fileBinding(cloudorchestrator.TrustedOCIArtifactCompiledRecipePath, artifactRaw, 0o644),
		fileBinding(cloudorchestrator.TrustedOCIArtifactWorkerCatalogPath, workerCatalogRaw, 0o644),
		fileBinding(cloudorchestrator.TrustedOCIArtifactWorkerManifestPath, resourceManifestRaw, 0o644),
		{Path: cloudorchestrator.TrustedOCIArtifactWorkerBinaryPath, SHA256: binaryDigest, SizeBytes: binarySize, Mode: 0o755},
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	recipeDigest, _ := recipe.Digest()
	controllerCatalog := cloudorchestrator.TrustedOCIArtifactCatalogV1{
		SchemaVersion: cloudorchestrator.TrustedOCIArtifactCatalogV1Schema, ArtifactVersion: spec.ArtifactVersion,
		RecipeID: recipe.RecipeID, RecipeDigest: recipeDigest, RecipeRevision: spec.RecipeRevision,
		ImageSource:                  artifact.ImageSource,
		CompiledRecipeArtifactDigest: artifactDigest, WorkerResourceManifestDigest: resourceManifestDigest,
		WorkerOCICatalogDigest: workerCatalogDigest, WorkerBinaryDigest: binaryDigest,
		RuntimeIdentity: recipeexec.WorkerRuntimeIdentityPodmanV1, Files: files,
	}
	controllerDigest, err := controllerCatalog.Digest()
	if err != nil {
		return Result{}, ErrInvalidBuildInput
	}
	controllerRaw, err := json.Marshal(controllerCatalog)
	if err != nil {
		return Result{}, ErrInvalidBuildInput
	}

	archiveHash := sha256.New()
	counting := &countWriter{writer: io.MultiWriter(output, archiveHash)}
	tarWriter := tar.NewWriter(counting)
	archivePayloads := []payload{{ControllerTrustedArtifactCatalog, 0o644, controllerRaw},
		{cloudorchestrator.TrustedOCIArtifactCompiledRecipePath, 0o644, artifactRaw},
		{cloudorchestrator.TrustedOCIArtifactWorkerCatalogPath, 0o644, workerCatalogRaw},
		{cloudorchestrator.TrustedOCIArtifactWorkerManifestPath, 0o644, resourceManifestRaw}}
	for _, item := range archivePayloads {
		if err := writeTarBytes(tarWriter, item); err != nil {
			return Result{}, err
		}
	}
	if _, err := binary.Seek(0, io.SeekStart); err != nil || writeTarReader(tarWriter, cloudorchestrator.TrustedOCIArtifactWorkerBinaryPath, 0o755, binarySize, binary) != nil {
		return Result{}, ErrInvalidBuildInput
	}
	if err := tarWriter.Close(); err != nil {
		return Result{}, err
	}
	return Result{Catalog: controllerCatalog, CatalogDigest: controllerDigest, ArchiveSHA256: namedDigest(archiveHash.Sum(nil)), ArchiveBytes: counting.count}, nil
}

func validateBuildSpec(recipe cloudorchestrator.RecipeV1, spec BuildSpecV1) error {
	pinnedDigest, sourceErr := spec.ImageSource.PinnedDigest()
	if spec.SchemaVersion != BuildSpecV1Schema || spec.RecipeRevision == 0 || spec.ImageSizeBytes == 0 || sourceErr != nil || pinnedDigest != spec.ImageDigest {
		return ErrInvalidBuildInput
	}
	if spec.Health.Liveness.Validate() != nil || spec.Health.Readiness.Validate() != nil || spec.Health.Semantic.Validate() != nil {
		return ErrInvalidBuildInput
	}
	wantSlots := make(map[string]struct{}, len(recipe.SecretSlots))
	for _, slot := range recipe.SecretSlots {
		if slot.Delivery != cloudorchestrator.SecretDeliveryFile {
			return ErrInvalidBuildInput
		}
		wantSlots[slot.SlotID] = struct{}{}
	}
	if len(spec.SecretTargets) != len(wantSlots) {
		return ErrInvalidBuildInput
	}
	seenFiles := map[string]struct{}{}
	for _, target := range spec.SecretTargets {
		if _, ok := wantSlots[target.SlotID]; !ok || !fileNamePattern.MatchString(target.FileName) || target.FileName == "environment" || filepath.Base(target.FileName) != target.FileName {
			return ErrInvalidBuildInput
		}
		if _, exists := seenFiles[target.FileName]; exists {
			return ErrInvalidBuildInput
		}
		seenFiles[target.FileName] = struct{}{}
		delete(wantSlots, target.SlotID)
	}
	if spec.RuntimeProfile != nil {
		for _, variable := range spec.RuntimeProfile.Environment {
			if strings.HasSuffix(variable.Name, "_FILE") {
				if _, ok := seenFiles[filepath.Base(variable.Value)]; !ok || variable.Value != "/run/secrets/"+filepath.Base(variable.Value) {
					return ErrInvalidBuildInput
				}
			}
		}
	}
	return nil
}

func strictDecode(raw []byte, target any) error {
	if err := rejectDuplicateKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrInvalidBuildInput
	}
	return nil
}

func rejectDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return ErrInvalidBuildInput
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			key, ok := keyToken.(string)
			if err != nil || !ok {
				return ErrInvalidBuildInput
			}
			if _, duplicate := seen[key]; duplicate {
				return ErrInvalidBuildInput
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
	default:
		return ErrInvalidBuildInput
	}
	end, err := decoder.Token()
	if err != nil || end != matchingDelimiter(delim) {
		return ErrInvalidBuildInput
	}
	return nil
}

func matchingDelimiter(open json.Delim) json.Delim {
	if open == '{' {
		return '}'
	}
	return ']'
}

func openAndHashRegular(path string) (*os.File, string, uint64, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Size() <= 0 {
		return nil, "", 0, ErrInvalidBuildInput
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, "", 0, err
	}
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) || after.Size() != before.Size() {
		file.Close()
		return nil, "", 0, ErrInvalidBuildInput
	}
	hash := sha256.New()
	written, err := io.Copy(hash, file)
	if err != nil || written != after.Size() {
		file.Close()
		return nil, "", 0, ErrInvalidBuildInput
	}
	return file, namedDigest(hash.Sum(nil)), uint64(written), nil
}

func marshalPayloads(artifact cloudorchestrator.CompiledRecipeArtifactV1, catalog recipeexec.WorkerOCICatalogV1, manifest recipeexec.WorkerResourceManifestV1) ([]byte, []byte, []byte, error) {
	a, err := json.Marshal(artifact)
	if err != nil {
		return nil, nil, nil, err
	}
	c, err := json.Marshal(catalog)
	if err != nil {
		return nil, nil, nil, err
	}
	m, err := json.Marshal(manifest)
	return a, c, m, err
}

func fileBinding(path string, raw []byte, mode uint32) cloudorchestrator.TrustedOCIArtifactFileV1 {
	hash := sha256.Sum256(raw)
	return cloudorchestrator.TrustedOCIArtifactFileV1{Path: path, SHA256: namedDigest(hash[:]), SizeBytes: uint64(len(raw)), Mode: mode}
}

func writeTarBytes(writer *tar.Writer, item payload) error {
	return writeTarReader(writer, item.path, item.mode, uint64(len(item.raw)), bytes.NewReader(item.raw))
}

func writeTarReader(writer *tar.Writer, path string, mode uint32, size uint64, reader io.Reader) error {
	header := &tar.Header{Name: path, Mode: int64(mode), Size: int64(size), Typeflag: tar.TypeReg, ModTime: time.Unix(0, 0).UTC(), Format: tar.FormatUSTAR}
	if err := writer.WriteHeader(header); err != nil {
		return err
	}
	written, err := io.Copy(writer, io.LimitReader(reader, int64(size)+1))
	if err != nil || uint64(written) != size {
		return fmt.Errorf("write deterministic artifact: %w", ErrInvalidBuildInput)
	}
	return nil
}

func namedDigest(raw []byte) string { return "sha256:" + hex.EncodeToString(raw) }

func cloneActions(actions []cloudorchestrator.CompiledRecipeActionV1) []cloudorchestrator.CompiledRecipeActionV1 {
	cloned := append([]cloudorchestrator.CompiledRecipeActionV1(nil), actions...)
	for index := range cloned {
		cloned[index].CheckpointSequence = append([]string(nil), cloned[index].CheckpointSequence...)
	}
	return cloned
}

type countWriter struct {
	writer io.Writer
	count  uint64
}

func (writer *countWriter) Write(raw []byte) (int, error) {
	n, err := writer.writer.Write(raw)
	writer.count += uint64(n)
	return n, err
}
