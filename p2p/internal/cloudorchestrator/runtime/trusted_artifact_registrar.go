package runtime

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

const maxTrustedArtifactJSONBytes = 8 << 20

var ErrTrustedArtifactCatalogInvalid = errors.New("trusted OCI artifact catalog is invalid")

type TrustedArtifactRegistrationStore interface {
	RegisterTrustedCloudRecipeArtifact(context.Context, cloudmodule.RegisterTrustedRecipeArtifactRequest) (cloudmodule.RegisterTrustedRecipeArtifactResult, error)
}

type trustedArtifactRegistration struct {
	artifact   cloudcontracts.CompiledRecipeArtifactV1
	catalog    cloudcontracts.TrustedOCIArtifactCatalogV1
	catalogRaw []byte
}

// TrustedArtifactRegistrar validates a Controller-produced immutable bundle
// before exposing its compiled Recipe artifact to the install runner.
type TrustedArtifactRegistrar struct {
	store       TrustedArtifactRegistrationStore
	catalogFile string
	now         func() time.Time
	load        func(string) (trustedArtifactRegistration, error)
}

func NewTrustedArtifactRegistrar(store TrustedArtifactRegistrationStore, catalogFile string, now func() time.Time) *TrustedArtifactRegistrar {
	if now == nil {
		now = time.Now
	}
	return &TrustedArtifactRegistrar{store: store, catalogFile: catalogFile, now: now, load: loadTrustedArtifactRegistration}
}

// Register is a startup gate, not an iteration runner. Any failure prevents
// all control-plane loops (including install) from starting.
func (registrar *TrustedArtifactRegistrar) Register(ctx context.Context) error {
	if registrar == nil || registrar.store == nil || registrar.load == nil || registrar.now == nil || registrar.catalogFile == "" {
		return ErrTrustedArtifactCatalogInvalid
	}
	registration, err := registrar.load(registrar.catalogFile)
	if err != nil {
		return ErrTrustedArtifactCatalogInvalid
	}
	registeredAt := registrar.now().UnixMilli()
	if registeredAt <= 0 {
		return ErrTrustedArtifactCatalogInvalid
	}
	_, err = registrar.store.RegisterTrustedCloudRecipeArtifact(ctx, cloudmodule.RegisterTrustedRecipeArtifactRequest{Artifact: registration.artifact, RegisteredAt: registeredAt})
	return err
}

// RegisterArchive performs the same durable catalog registration and also
// verifies the exact deterministic Stage-S tar against both the controller
// catalog embedded in the tar and every already-extracted trusted file.
func (registrar *TrustedArtifactRegistrar) RegisterArchive(ctx context.Context, archiveFile string) (TrustedRecipeArtifactArchive, error) {
	if registrar == nil || registrar.store == nil || registrar.load == nil || registrar.now == nil || registrar.catalogFile == "" || archiveFile == "" {
		return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
	}
	registration, err := registrar.load(registrar.catalogFile)
	if err != nil {
		return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
	}
	archive, err := loadTrustedRecipeArtifactArchive(archiveFile, registration)
	if err != nil {
		return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
	}
	registeredAt := registrar.now().UnixMilli()
	if registeredAt <= 0 {
		return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
	}
	result, err := registrar.store.RegisterTrustedCloudRecipeArtifact(ctx, cloudmodule.RegisterTrustedRecipeArtifactRequest{Artifact: registration.artifact, RegisteredAt: registeredAt})
	if err != nil || !registeredArtifactMatches(result.Artifact, registration.artifact) {
		if err != nil {
			return TrustedRecipeArtifactArchive{}, err
		}
		return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
	}
	return archive, nil
}

func loadTrustedArtifactRegistration(catalogFile string) (trustedArtifactRegistration, error) {
	rawCatalog, err := readStrictRootOwnedFile(catalogFile, 64<<10)
	if err != nil {
		return trustedArtifactRegistration{}, err
	}
	catalog, err := cloudcontracts.ParseTrustedOCIArtifactCatalogV1(rawCatalog)
	if err != nil {
		return trustedArtifactRegistration{}, err
	}
	directory := filepath.Dir(catalogFile)
	files := make(map[string]cloudcontracts.TrustedOCIArtifactFileV1, len(catalog.Files))
	for _, file := range catalog.Files {
		files[file.Path] = file
	}
	compiledRaw, err := readAndVerifyTrustedArtifactFile(directory, files[cloudcontracts.TrustedOCIArtifactCompiledRecipePath], maxTrustedArtifactJSONBytes)
	if err != nil {
		return trustedArtifactRegistration{}, err
	}
	workerCatalogRaw, err := readAndVerifyTrustedArtifactFile(directory, files[cloudcontracts.TrustedOCIArtifactWorkerCatalogPath], maxTrustedArtifactJSONBytes)
	if err != nil {
		return trustedArtifactRegistration{}, err
	}
	workerManifestRaw, err := readAndVerifyTrustedArtifactFile(directory, files[cloudcontracts.TrustedOCIArtifactWorkerManifestPath], maxTrustedArtifactJSONBytes)
	if err != nil {
		return trustedArtifactRegistration{}, err
	}
	if files[cloudcontracts.TrustedOCIArtifactWorkerBinaryPath].SHA256 != catalog.WorkerBinaryDigest {
		return trustedArtifactRegistration{}, ErrTrustedArtifactCatalogInvalid
	}
	if _, err := readAndVerifyTrustedArtifactFile(directory, files[cloudcontracts.TrustedOCIArtifactWorkerBinaryPath], 0); err != nil {
		return trustedArtifactRegistration{}, err
	}

	artifact, err := cloudcontracts.ParseCompiledRecipeArtifactV1(compiledRaw)
	if err != nil {
		return trustedArtifactRegistration{}, err
	}
	artifactDigest, err := artifact.Digest()
	if err != nil || artifactDigest != catalog.CompiledRecipeArtifactDigest || artifact.RecipeID != catalog.RecipeID || artifact.RecipeDigest != catalog.RecipeDigest || artifact.ImageSource != catalog.ImageSource ||
		artifact.RecipeRevision != catalog.RecipeRevision || artifact.WorkerResourceManifestDigest != catalog.WorkerResourceManifestDigest {
		return trustedArtifactRegistration{}, ErrTrustedArtifactCatalogInvalid
	}
	workerCatalog, err := recipeexec.ParseWorkerOCICatalogV1(workerCatalogRaw)
	if err != nil {
		return trustedArtifactRegistration{}, err
	}
	workerManifest, err := recipeexec.ParseWorkerResourceManifestV1(workerManifestRaw)
	if err != nil {
		return trustedArtifactRegistration{}, err
	}
	workerCatalogDigest, catalogDigestErr := workerCatalog.Digest()
	workerManifestDigest, manifestDigestErr := workerManifest.Digest()
	if catalogDigestErr != nil || manifestDigestErr != nil || workerCatalogDigest != catalog.WorkerOCICatalogDigest || workerManifestDigest != catalog.WorkerResourceManifestDigest ||
		workerManifest.CatalogDigest != catalog.WorkerOCICatalogDigest || workerManifest.WorkerBinaryDigest != catalog.WorkerBinaryDigest || workerManifest.RuntimeIdentity != catalog.RuntimeIdentity {
		return trustedArtifactRegistration{}, ErrTrustedArtifactCatalogInvalid
	}
	resolver, err := recipeexec.NewOCICatalogResolver(workerCatalog, workerManifest, catalog.WorkerResourceManifestDigest, catalog.WorkerBinaryDigest)
	if err != nil {
		return trustedArtifactRegistration{}, err
	}
	bundle, err := resolver.Resolve(context.Background(), artifact.ArtifactDigest)
	if err != nil {
		return trustedArtifactRegistration{}, err
	}
	descriptor, err := resolver.LookupDescriptor(context.Background(), artifact.ArtifactDigest)
	if err != nil || !trustedArtifactExecutionBindingExact(artifact, bundle, descriptor) {
		return trustedArtifactRegistration{}, ErrTrustedArtifactCatalogInvalid
	}
	return trustedArtifactRegistration{artifact: artifact, catalog: catalog, catalogRaw: append([]byte(nil), rawCatalog...)}, nil
}

func registeredArtifactMatches(stored cloudmodule.TrustedRecipeArtifact, artifact cloudcontracts.CompiledRecipeArtifactV1) bool {
	descriptorDigest, err := artifact.Digest()
	return err == nil && stored.Status == "verified" && stored.ArtifactDigest == artifact.ArtifactDigest && stored.DescriptorDigest == descriptorDigest &&
		stored.RecipeID == artifact.RecipeID && stored.RecipeDigest == artifact.RecipeDigest && stored.RecipeRevision == artifact.RecipeRevision &&
		stored.WorkerResourceManifestDigest == artifact.WorkerResourceManifestDigest
}

func loadTrustedRecipeArtifactArchive(path string, registration trustedArtifactRegistration) (TrustedRecipeArtifactArchive, error) {
	before, err := os.Lstat(filepath.Clean(path))
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || !rootOwnedFile(before) || before.Mode().Perm()&0o022 != 0 ||
		before.Size() <= 0 || before.Size() > 256<<20 || registration.catalog.Validate() != nil || registration.artifact.Validate() != nil || len(registration.catalogRaw) == 0 {
		return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
	}
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || !rootOwnedFile(after) {
		return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
	}
	archiveHash := sha256.New()
	tee := io.TeeReader(io.LimitReader(file, before.Size()+1), archiveHash)
	reader := tar.NewReader(tee)
	files := make(map[string]cloudcontracts.TrustedOCIArtifactFileV1, len(registration.catalog.Files))
	for _, expected := range registration.catalog.Files {
		files[expected.Path] = expected
	}
	order := []string{
		"controller-trusted-artifact-catalog.json",
		cloudcontracts.TrustedOCIArtifactCompiledRecipePath,
		cloudcontracts.TrustedOCIArtifactWorkerCatalogPath,
		cloudcontracts.TrustedOCIArtifactWorkerManifestPath,
		cloudcontracts.TrustedOCIArtifactWorkerBinaryPath,
	}
	expectedArchiveSize := int64(1024)
	for _, name := range order {
		size := int64(len(registration.catalogRaw))
		if name != order[0] {
			expected, ok := files[name]
			if !ok {
				return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
			}
			size = int64(expected.SizeBytes)
		}
		expectedArchiveSize += 512 + (size+511)/512*512
	}
	if before.Size() != expectedArchiveSize {
		return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
	}
	for _, name := range order {
		header, err := reader.Next()
		mode, size := int64(0o644), int64(len(registration.catalogRaw))
		if name == order[0] {
			if err != nil || !trustedArtifactTarHeaderExact(header, name, mode, size) {
				return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
			}
			raw, err := io.ReadAll(io.LimitReader(reader, (64<<10)+1))
			if err != nil || !bytes.Equal(raw, registration.catalogRaw) {
				return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
			}
			continue
		}
		expected, ok := files[name]
		mode, size = int64(expected.Mode), int64(expected.SizeBytes)
		if err != nil || !ok || !trustedArtifactTarHeaderExact(header, name, mode, size) {
			return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
		}
		hash := sha256.New()
		var capture bytes.Buffer
		writer := io.Writer(hash)
		if name == cloudcontracts.TrustedOCIArtifactCompiledRecipePath {
			writer = io.MultiWriter(hash, &capture)
		}
		written, err := io.Copy(writer, reader)
		if err != nil || written != header.Size || "sha256:"+hex.EncodeToString(hash.Sum(nil)) != expected.SHA256 {
			return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
		}
		if name == cloudcontracts.TrustedOCIArtifactCompiledRecipePath {
			artifact, err := cloudcontracts.ParseCompiledRecipeArtifactV1(capture.Bytes())
			if err != nil || !reflect.DeepEqual(artifact, registration.artifact) {
				return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
			}
		}
	}
	if header, err := reader.Next(); err != io.EOF || header != nil {
		return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
	}
	if _, err := io.Copy(io.Discard, tee); err != nil {
		return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
	}
	trailer := make([]byte, 1024)
	if read, err := file.ReadAt(trailer, before.Size()-int64(len(trailer))); err != nil || read != len(trailer) || !bytes.Equal(trailer, make([]byte, len(trailer))) {
		return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
	}
	final, err := file.Stat()
	if err != nil || !os.SameFile(before, final) || final.Size() != before.Size() || !rootOwnedFile(final) {
		return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
	}
	catalogDigest, err := registration.catalog.Digest()
	if err != nil {
		return TrustedRecipeArtifactArchive{}, ErrTrustedArtifactCatalogInvalid
	}
	return TrustedRecipeArtifactArchive{
		Path: path, ArchiveSHA256: hex.EncodeToString(archiveHash.Sum(nil)), SizeBytes: before.Size(), ControllerCatalogDigest: catalogDigest,
		RecipeDigest: registration.artifact.RecipeDigest, ArtifactDigest: registration.artifact.ArtifactDigest,
		WorkerResourceManifestDigest: registration.artifact.WorkerResourceManifestDigest,
	}, nil
}

func trustedArtifactTarHeaderExact(header *tar.Header, name string, mode, size int64) bool {
	return header != nil && header.Name == name && header.Linkname == "" && header.Typeflag == tar.TypeReg && header.Mode == mode && header.Size == size &&
		header.Uid == 0 && header.Gid == 0 && header.Uname == "" && header.Gname == "" && header.Devmajor == 0 && header.Devminor == 0 &&
		header.ModTime.Equal(time.Unix(0, 0).UTC()) && header.AccessTime.IsZero() && header.ChangeTime.IsZero() && header.Format == tar.FormatUSTAR &&
		len(header.PAXRecords) == 0 && len(header.Xattrs) == 0
}

func trustedArtifactExecutionBindingExact(artifact cloudcontracts.CompiledRecipeArtifactV1, bundle recipeexec.Bundle, descriptor cloudcontracts.OCIServiceBundleV1) bool {
	actionIDs := make([]string, len(artifact.Actions))
	for index, action := range artifact.Actions {
		actionIDs[index] = action.ActionID
	}
	sort.Strings(actionIDs)
	targetSlots := make([]string, len(bundle.SecretTargets))
	for index, target := range bundle.SecretTargets {
		targetSlots[index] = target.SlotID
	}
	sort.Strings(targetSlots)
	artifactSlots := make([]string, len(artifact.SecretSlots))
	for index, slot := range artifact.SecretSlots {
		artifactSlots[index] = slot.SlotID
	}
	sort.Strings(artifactSlots)
	return artifact.ArtifactDigest == descriptor.ArtifactDigest && artifact.ArtifactDigest == descriptor.ImageDigest && artifact.ImageSource == descriptor.ImageSource && artifact.SizeBytes == descriptor.ImageSizeBytes &&
		artifact.Architecture == descriptor.Architecture && artifact.HealthContractDigest == descriptor.HealthContractDigest && artifact.LifecycleContractDigest == descriptor.LifecycleContractDigest &&
		artifact.SemanticReadiness == descriptor.Health.Semantic && reflect.DeepEqual(artifact.Actions, descriptor.Actions) && reflect.DeepEqual(actionIDs, bundle.ActionIDs) && reflect.DeepEqual(artifactSlots, targetSlots) &&
		volumeTargetsBindArtifactRequirements(artifact.VolumeSlots, descriptor.VolumeTargets) && dataTargetsBindArtifactRequirements(artifact.DataSlots, descriptor.DataTargets)
}

func volumeTargetsBindArtifactRequirements(requirements []cloudcontracts.RecipeVolumeSlotRequirementV1, targets []cloudcontracts.OCIServiceStorageTargetV1) bool {
	if len(requirements) != len(targets) {
		return false
	}
	bySlot := make(map[string]cloudcontracts.OCIServiceStorageTargetV1, len(targets))
	for _, target := range targets {
		bySlot[target.SlotID] = target
	}
	for _, requirement := range requirements {
		target, exists := bySlot[requirement.SlotID]
		if !exists || target.ReadOnly != requirement.ReadOnly {
			return false
		}
	}
	return true
}

func dataTargetsBindArtifactRequirements(requirements []cloudcontracts.RecipeDataSlotRequirementV1, targets []cloudcontracts.OCIServiceStorageTargetV1) bool {
	if len(requirements) != len(targets) {
		return false
	}
	bySlot := make(map[string]cloudcontracts.OCIServiceStorageTargetV1, len(targets))
	for _, target := range targets {
		bySlot[target.SlotID] = target
	}
	for _, requirement := range requirements {
		target, exists := bySlot[requirement.SlotID]
		if !exists || target.ReadOnly != requirement.ReadOnly {
			return false
		}
	}
	return true
}

func readStrictRootOwnedFile(path string, maxBytes int64) ([]byte, error) {
	path = filepath.Clean(path)
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || !rootOwnedFile(before) || before.Mode().Perm()&0o022 != 0 || before.Size() <= 0 || maxBytes <= 0 || before.Size() > maxBytes {
		return nil, ErrTrustedArtifactCatalogInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrTrustedArtifactCatalogInvalid
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || !rootOwnedFile(after) {
		return nil, ErrTrustedArtifactCatalogInvalid
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || int64(len(raw)) != before.Size() {
		return nil, ErrTrustedArtifactCatalogInvalid
	}
	return raw, nil
}

func readAndVerifyTrustedArtifactFile(directory string, expected cloudcontracts.TrustedOCIArtifactFileV1, maxCaptureBytes int64) ([]byte, error) {
	if expected.Path == "" || filepath.Base(expected.Path) != expected.Path {
		return nil, ErrTrustedArtifactCatalogInvalid
	}
	path := filepath.Join(directory, expected.Path)
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || !rootOwnedFile(before) || before.Size() != int64(expected.SizeBytes) || uint32(before.Mode().Perm()) != expected.Mode {
		return nil, ErrTrustedArtifactCatalogInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrTrustedArtifactCatalogInvalid
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || !rootOwnedFile(after) {
		return nil, ErrTrustedArtifactCatalogInvalid
	}
	hash := sha256.New()
	writer := io.Writer(hash)
	if maxCaptureBytes > 0 {
		if before.Size() > maxCaptureBytes {
			return nil, ErrTrustedArtifactCatalogInvalid
		}
		buffer := &limitedCapture{remaining: maxCaptureBytes}
		writer = io.MultiWriter(hash, buffer)
		written, copyErr := io.Copy(writer, file)
		if copyErr != nil || written != before.Size() || "sha256:"+hex.EncodeToString(hash.Sum(nil)) != expected.SHA256 {
			return nil, ErrTrustedArtifactCatalogInvalid
		}
		return append([]byte(nil), buffer.bytes...), nil
	}
	written, err := io.Copy(hash, file)
	if err != nil || written != before.Size() || "sha256:"+hex.EncodeToString(hash.Sum(nil)) != expected.SHA256 {
		return nil, ErrTrustedArtifactCatalogInvalid
	}
	return nil, nil
}

type limitedCapture struct {
	bytes     []byte
	remaining int64
}

func (capture *limitedCapture) Write(value []byte) (int, error) {
	if int64(len(value)) > capture.remaining {
		return 0, fmt.Errorf("trusted artifact exceeds capture limit")
	}
	capture.bytes = append(capture.bytes, value...)
	capture.remaining -= int64(len(value))
	return len(value), nil
}
