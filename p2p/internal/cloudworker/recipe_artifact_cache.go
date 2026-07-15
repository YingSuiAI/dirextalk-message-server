package cloudworker

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

const (
	maxRecipeArtifactControllerBytes = 64 << 10
	recipeArtifactHTTPTimeout        = 10 * time.Minute
)

var ErrRecipeArtifactUnavailable = errors.New("recipe artifact is unavailable")

var recipeArtifactFiles = map[string]os.FileMode{
	cloudorchestrator.TrustedOCIArtifactControllerCatalogPath: 0o644,
	cloudorchestrator.TrustedOCIArtifactCompiledRecipePath:    0o644,
	cloudorchestrator.TrustedOCIArtifactWorkerCatalogPath:     0o644,
	cloudorchestrator.TrustedOCIArtifactWorkerManifestPath:    0o644,
	cloudorchestrator.TrustedOCIArtifactWorkerBinaryPath:      0o755,
}

// RecipeArtifactCache owns only a deployment-private, content-addressed
// cache. Temporary grant URLs stay in the current call and are never written
// to disk or included in returned errors.
type RecipeArtifactCache struct {
	directory           string
	deploymentID        string
	client              *http.Client
	resolver            *recipeexec.OCICatalogResolver
	runningWorkerDigest string
	now                 func() time.Time
	mu                  sync.Mutex
}

func (client *SessionClient) NewRecipeArtifactCache(root, deploymentID string, resolver *recipeexec.OCICatalogResolver, runningWorkerDigest string) (*RecipeArtifactCache, error) {
	return NewRecipeArtifactCache(client, root, deploymentID, resolver, runningWorkerDigest)
}

func NewRecipeArtifactCache(session *SessionClient, root, deploymentID string, resolver *recipeexec.OCICatalogResolver, runningWorkerDigest string) (*RecipeArtifactCache, error) {
	if session == nil || session.client == nil || resolver == nil || !validIdentifier(deploymentID) || !validNamedSHA256(runningWorkerDigest) || root == "" {
		return nil, ErrRecipeArtifactUnavailable
	}
	client, err := secureHTTPClient(session.client)
	if err != nil {
		return nil, ErrRecipeArtifactUnavailable
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		return nil, ErrRecipeArtifactUnavailable
	}
	transport.Proxy = nil
	transport.ProxyConnectHeader = nil
	transport.DisableCompression = true
	transport.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	transport.DialTLSContext = nil
	transport.DialTLS = nil
	client.Timeout = recipeArtifactHTTPTimeout
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, ErrRecipeArtifactUnavailable
	}
	absolute = filepath.Clean(absolute)
	directory := filepath.Join(absolute, deploymentID)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, ErrRecipeArtifactUnavailable
	}
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil || filepath.Clean(resolved) != directory {
		return nil, ErrRecipeArtifactUnavailable
	}
	for _, candidate := range []string{absolute, directory} {
		info, statErr := os.Lstat(candidate)
		if statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !trustedRecipeCacheOwner(info) || os.Chmod(candidate, 0o700) != nil {
			return nil, ErrRecipeArtifactUnavailable
		}
	}
	now := session.now
	if now == nil {
		now = time.Now
	}
	return &RecipeArtifactCache{directory: directory, deploymentID: deploymentID, client: client, resolver: resolver, runningWorkerDigest: runningWorkerDigest, now: now}, nil
}

// Prepare validates or downloads one exact archive, atomically caches it by
// OCI artifact digest, and registers its descriptor before Executor resolves
// the task. It never persists ArtifactAccessV1.
func (cache *RecipeArtifactCache) Prepare(ctx context.Context, claimed ClaimedRecipeTask) error {
	if ctx == nil || cache == nil || cache.client == nil || cache.resolver == nil || claimed.Manifest.Validate() != nil ||
		claimed.Task.ValidateForManifest(claimed.Manifest) != nil || claimed.Manifest.DeploymentID != cache.deploymentID {
		return ErrRecipeArtifactUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()

	artifactDirectory := filepath.Join(cache.directory, strings.TrimPrefix(claimed.Manifest.ArtifactDigest, "sha256:"))
	if info, statErr := os.Lstat(artifactDirectory); statErr == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !trustedRecipeCacheOwner(info) {
			return ErrRecipeArtifactUnavailable
		}
		return cache.validateAndRegister(artifactDirectory, claimed.Manifest)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return ErrRecipeArtifactUnavailable
	}
	if claimed.ArtifactAccess == nil || claimed.ArtifactAccess.Validate() != nil {
		return ErrRecipeArtifactUnavailable
	}
	expiresAt, err := time.Parse("2006-01-02T15:04:05.000Z", claimed.ArtifactAccess.ExpiresAt)
	if err != nil || !cache.now().UTC().Before(expiresAt) {
		return ErrRecipeArtifactUnavailable
	}

	archiveFile, err := cache.download(ctx, *claimed.ArtifactAccess)
	if err != nil {
		return err
	}
	defer os.Remove(archiveFile)
	temporaryDirectory, err := cache.extract(archiveFile, claimed.Manifest.ArtifactDigest)
	if err != nil {
		return err
	}
	defer os.RemoveAll(temporaryDirectory)
	if _, err := cache.validateDirectory(temporaryDirectory, claimed.Manifest); err != nil {
		return err
	}
	if err := os.Rename(temporaryDirectory, artifactDirectory); err != nil {
		if info, statErr := os.Lstat(artifactDirectory); statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return ErrRecipeArtifactUnavailable
		}
	}
	return cache.validateAndRegister(artifactDirectory, claimed.Manifest)
}

func (cache *RecipeArtifactCache) download(ctx context.Context, access recipeexec.ArtifactAccessV1) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, access.URL, nil)
	if err != nil {
		return "", ErrRecipeArtifactUnavailable
	}
	request.Header.Set("Accept-Encoding", "identity")
	response, err := cache.client.Do(request)
	if err != nil {
		return "", ErrRecipeArtifactUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK ||
		response.Header.Get("Content-Type") != recipeexec.RecipeArtifactMediaTypeV1 ||
		response.Header.Get("Content-Encoding") != "" ||
		(response.ContentLength >= 0 && response.ContentLength != access.SizeBytes) {
		return "", ErrRecipeArtifactUnavailable
	}
	file, err := os.CreateTemp(cache.directory, ".download-*.tar")
	if err != nil {
		return "", ErrRecipeArtifactUnavailable
	}
	path := file.Name()
	keep := false
	defer func() {
		_ = file.Close()
		if !keep {
			_ = os.Remove(path)
		}
	}()
	if os.Chmod(path, 0o600) != nil {
		return "", ErrRecipeArtifactUnavailable
	}
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, access.SizeBytes+1))
	if err != nil || written != access.SizeBytes || hex.EncodeToString(hash.Sum(nil)) != access.ArchiveSHA256 || file.Sync() != nil || file.Close() != nil {
		return "", ErrRecipeArtifactUnavailable
	}
	keep = true
	return path, nil
}

func (cache *RecipeArtifactCache) extract(archiveFile, artifactDigest string) (string, error) {
	file, err := os.Open(archiveFile)
	if err != nil {
		return "", ErrRecipeArtifactUnavailable
	}
	defer file.Close()
	temporary, err := os.MkdirTemp(cache.directory, ".artifact-"+strings.TrimPrefix(artifactDigest, "sha256:")[:12]+"-*")
	if err != nil || os.Chmod(temporary, 0o700) != nil {
		return "", ErrRecipeArtifactUnavailable
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(temporary)
		}
	}()
	reader := tar.NewReader(file)
	seen := make(map[string]struct{}, len(recipeArtifactFiles))
	for {
		header, nextErr := reader.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		mode, allowed := recipeArtifactFiles[headerName(header)]
		if nextErr != nil || !allowed || header == nil || (header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA) ||
			header.Name != filepath.Base(header.Name) || strings.ContainsAny(header.Name, "/\\\x00") || header.Linkname != "" ||
			header.Mode != int64(mode.Perm()) || header.Size <= 0 || header.Size > recipeexec.MaxRecipeArtifactBytesV1 || len(header.PAXRecords) != 0 || len(header.Xattrs) != 0 {
			return "", ErrRecipeArtifactUnavailable
		}
		if _, duplicate := seen[header.Name]; duplicate {
			return "", ErrRecipeArtifactUnavailable
		}
		seen[header.Name] = struct{}{}
		target := filepath.Join(temporary, header.Name)
		output, openErr := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
		if openErr != nil {
			return "", ErrRecipeArtifactUnavailable
		}
		written, copyErr := io.CopyN(output, reader, header.Size)
		closeErr := output.Close()
		if copyErr != nil || closeErr != nil || written != header.Size || os.Chmod(target, mode) != nil {
			return "", ErrRecipeArtifactUnavailable
		}
	}
	if len(seen) != len(recipeArtifactFiles) {
		return "", ErrRecipeArtifactUnavailable
	}
	for name := range recipeArtifactFiles {
		if _, found := seen[name]; !found {
			return "", ErrRecipeArtifactUnavailable
		}
	}
	keep = true
	return temporary, nil
}

func headerName(header *tar.Header) string {
	if header == nil {
		return ""
	}
	return header.Name
}

func (cache *RecipeArtifactCache) validateAndRegister(directory string, manifest cloudorchestrator.RecipeExecutionManifestV1) error {
	validated, err := cache.validateDirectory(directory, manifest)
	if err != nil {
		return err
	}
	if err := cache.resolver.RegisterTrustedCatalog(validated.workerCatalog, validated.workerManifest, manifest.ArtifactDigest); err != nil {
		return ErrRecipeArtifactUnavailable
	}
	return nil
}

type validatedRecipeArtifact struct {
	workerCatalog  recipeexec.WorkerOCICatalogV1
	workerManifest recipeexec.WorkerResourceManifestV1
}

func (cache *RecipeArtifactCache) validateDirectory(directory string, manifest cloudorchestrator.RecipeExecutionManifestV1) (validatedRecipeArtifact, error) {
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != len(recipeArtifactFiles) {
		return validatedRecipeArtifact{}, ErrRecipeArtifactUnavailable
	}
	for _, entry := range entries {
		if _, allowed := recipeArtifactFiles[entry.Name()]; !allowed || entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return validatedRecipeArtifact{}, ErrRecipeArtifactUnavailable
		}
	}
	controllerRaw, err := readRecipeArtifactFile(directory, cloudorchestrator.TrustedOCIArtifactControllerCatalogPath, maxRecipeArtifactControllerBytes, 0o644)
	if err != nil {
		return validatedRecipeArtifact{}, err
	}
	controller, err := cloudorchestrator.ParseTrustedOCIArtifactCatalogV1(controllerRaw)
	if err != nil || controller.RecipeDigest != manifest.RecipeDigest || controller.WorkerResourceManifestDigest != manifest.WorkerResourceManifestDigest || controller.WorkerBinaryDigest != cache.runningWorkerDigest {
		return validatedRecipeArtifact{}, ErrRecipeArtifactUnavailable
	}
	for _, binding := range controller.Files {
		digest, size, verifyErr := hashRecipeArtifactFile(directory, binding.Path, os.FileMode(binding.Mode))
		if verifyErr != nil || digest != binding.SHA256 || uint64(size) != binding.SizeBytes {
			return validatedRecipeArtifact{}, ErrRecipeArtifactUnavailable
		}
	}
	compiledRaw, err := readRecipeArtifactFile(directory, cloudorchestrator.TrustedOCIArtifactCompiledRecipePath, 8<<20, 0o644)
	if err != nil {
		return validatedRecipeArtifact{}, err
	}
	compiled, err := cloudorchestrator.ParseCompiledRecipeArtifactV1(compiledRaw)
	compiledDigest, digestErr := compiled.Digest()
	if err != nil || digestErr != nil || compiledDigest != controller.CompiledRecipeArtifactDigest || compiled.RecipeID != controller.RecipeID || compiled.RecipeDigest != controller.RecipeDigest || compiled.RecipeRevision != controller.RecipeRevision ||
		compiled.WorkerResourceManifestDigest != controller.WorkerResourceManifestDigest || compiled.ArtifactDigest != manifest.ArtifactDigest || compiled.ImageSource != controller.ImageSource ||
		!compiledArtifactMatchesExecutionManifest(compiled, manifest) {
		return validatedRecipeArtifact{}, ErrRecipeArtifactUnavailable
	}
	workerCatalogRaw, err := readRecipeArtifactFile(directory, cloudorchestrator.TrustedOCIArtifactWorkerCatalogPath, 1<<20, 0o644)
	if err != nil {
		return validatedRecipeArtifact{}, err
	}
	workerCatalog, err := recipeexec.ParseWorkerOCICatalogV1(workerCatalogRaw)
	workerCatalogDigest, catalogDigestErr := workerCatalog.Digest()
	if err != nil || catalogDigestErr != nil || workerCatalogDigest != controller.WorkerOCICatalogDigest || len(workerCatalog.Entries) != 1 || !compiledArtifactMatchesWorkerEntry(compiled, workerCatalog.Entries[0]) {
		return validatedRecipeArtifact{}, ErrRecipeArtifactUnavailable
	}
	workerManifestRaw, err := readRecipeArtifactFile(directory, cloudorchestrator.TrustedOCIArtifactWorkerManifestPath, 16<<10, 0o644)
	if err != nil {
		return validatedRecipeArtifact{}, err
	}
	workerManifest, err := recipeexec.ParseWorkerResourceManifestV1(workerManifestRaw)
	workerManifestDigest, manifestDigestErr := workerManifest.Digest()
	if err != nil || manifestDigestErr != nil || workerManifestDigest != controller.WorkerResourceManifestDigest || workerManifestDigest != manifest.WorkerResourceManifestDigest || workerManifest.CatalogDigest != workerCatalogDigest || workerManifest.WorkerBinaryDigest != cache.runningWorkerDigest {
		return validatedRecipeArtifact{}, ErrRecipeArtifactUnavailable
	}
	binaryDigest, _, err := hashRecipeArtifactFile(directory, cloudorchestrator.TrustedOCIArtifactWorkerBinaryPath, 0o755)
	if err != nil || binaryDigest != cache.runningWorkerDigest {
		return validatedRecipeArtifact{}, ErrRecipeArtifactUnavailable
	}
	return validatedRecipeArtifact{workerCatalog: workerCatalog, workerManifest: workerManifest}, nil
}

func compiledArtifactMatchesExecutionManifest(artifact cloudorchestrator.CompiledRecipeArtifactV1, manifest cloudorchestrator.RecipeExecutionManifestV1) bool {
	matchedAction := false
	for _, action := range artifact.Actions {
		if action.ActionID == manifest.ActionID && action.Kind == cloudorchestrator.CompiledRecipeActionInstall && action.RootRequired == manifest.RootRequired &&
			action.TimeoutSeconds == manifest.TimeoutSeconds && reflect.DeepEqual(action.CheckpointSequence, manifest.CheckpointSequence) {
			matchedAction = true
		}
	}
	if !matchedAction || manifest.SemanticReadiness != artifact.SemanticReadiness || len(manifest.VolumeSlots) != len(artifact.VolumeSlots) || len(manifest.DataSlots) != len(artifact.DataSlots) || len(manifest.SecretSlots) != len(artifact.SecretSlots) {
		return false
	}
	volumes := make(map[string]cloudorchestrator.CompiledVolumeSlotSchemaV1, len(artifact.VolumeSlots))
	for _, slot := range artifact.VolumeSlots {
		volumes[slot.SlotID] = slot
	}
	for _, slot := range manifest.VolumeSlots {
		schema, found := volumes[slot.SlotID]
		if !found || schema.ReadOnly != slot.ReadOnly {
			return false
		}
	}
	dataSlots := make(map[string]cloudorchestrator.CompiledDataSlotSchemaV1, len(artifact.DataSlots))
	for _, slot := range artifact.DataSlots {
		dataSlots[slot.SlotID] = slot
	}
	for _, slot := range manifest.DataSlots {
		schema, found := dataSlots[slot.SlotID]
		if !found || schema.ReadOnly != slot.ReadOnly {
			return false
		}
	}
	secretSlots := make(map[string]struct{}, len(artifact.SecretSlots))
	for _, slot := range artifact.SecretSlots {
		secretSlots[slot.SlotID] = struct{}{}
	}
	for _, slot := range manifest.SecretSlots {
		if _, found := secretSlots[slot.SlotID]; !found {
			return false
		}
	}
	return true
}

func compiledArtifactMatchesWorkerEntry(artifact cloudorchestrator.CompiledRecipeArtifactV1, entry recipeexec.WorkerOCICatalogEntryV1) bool {
	descriptor := entry.Descriptor
	actionIDs := make([]string, len(artifact.Actions))
	for index, action := range artifact.Actions {
		actionIDs[index] = action.ActionID
	}
	sort.Strings(actionIDs)
	targetSlots := make([]string, len(entry.SecretTargets))
	for index, target := range entry.SecretTargets {
		targetSlots[index] = target.SlotID
	}
	sort.Strings(targetSlots)
	artifactSlots := make([]string, len(artifact.SecretSlots))
	for index, slot := range artifact.SecretSlots {
		artifactSlots[index] = slot.SlotID
	}
	sort.Strings(artifactSlots)
	return artifact.ArtifactDigest == entry.ArtifactDigest && artifact.ArtifactDigest == descriptor.ArtifactDigest && artifact.ArtifactDigest == descriptor.ImageDigest && artifact.ImageSource == descriptor.ImageSource &&
		artifact.SizeBytes == descriptor.ImageSizeBytes && artifact.Architecture == descriptor.Architecture && artifact.HealthContractDigest == descriptor.HealthContractDigest && artifact.LifecycleContractDigest == descriptor.LifecycleContractDigest &&
		artifact.SemanticReadiness == descriptor.Health.Semantic && reflect.DeepEqual(artifact.Actions, descriptor.Actions) && reflect.DeepEqual(actionIDs, entry.ActionIDs) && reflect.DeepEqual(artifactSlots, targetSlots) &&
		storageTargetsMatchVolumeSlots(descriptor.VolumeTargets, artifact.VolumeSlots) && storageTargetsMatchDataSlots(descriptor.DataTargets, artifact.DataSlots)
}

func storageTargetsMatchVolumeSlots(targets []cloudorchestrator.OCIServiceStorageTargetV1, slots []cloudorchestrator.CompiledVolumeSlotSchemaV1) bool {
	if len(targets) != len(slots) {
		return false
	}
	want := make(map[string]bool, len(slots))
	for _, slot := range slots {
		want[slot.SlotID] = slot.ReadOnly
	}
	for _, target := range targets {
		readOnly, found := want[target.SlotID]
		if !found || readOnly != target.ReadOnly {
			return false
		}
	}
	return true
}

func storageTargetsMatchDataSlots(targets []cloudorchestrator.OCIServiceStorageTargetV1, slots []cloudorchestrator.CompiledDataSlotSchemaV1) bool {
	if len(targets) != len(slots) {
		return false
	}
	want := make(map[string]bool, len(slots))
	for _, slot := range slots {
		want[slot.SlotID] = slot.ReadOnly
	}
	for _, target := range targets {
		readOnly, found := want[target.SlotID]
		if !found || readOnly != target.ReadOnly {
			return false
		}
	}
	return true
}

func readRecipeArtifactFile(directory, name string, maximum int64, mode os.FileMode) ([]byte, error) {
	path := filepath.Join(directory, name)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maximum || !trustedRecipeArtifactMode(info, mode) || !trustedRecipeCacheOwner(info) {
		return nil, ErrRecipeArtifactUnavailable
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrRecipeArtifactUnavailable
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	final, statErr := file.Stat()
	if err != nil || statErr != nil || int64(len(raw)) != info.Size() || !os.SameFile(info, final) {
		return nil, ErrRecipeArtifactUnavailable
	}
	return raw, nil
}

func hashRecipeArtifactFile(directory, name string, mode os.FileMode) (string, int64, error) {
	path := filepath.Join(directory, name)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > recipeexec.MaxRecipeArtifactBytesV1 || !trustedRecipeArtifactMode(info, mode) || !trustedRecipeCacheOwner(info) {
		return "", 0, ErrRecipeArtifactUnavailable
	}
	file, err := os.Open(path)
	if err != nil {
		return "", 0, ErrRecipeArtifactUnavailable
	}
	defer file.Close()
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(file, recipeexec.MaxRecipeArtifactBytesV1+1))
	final, statErr := file.Stat()
	if err != nil || statErr != nil || written != info.Size() || !os.SameFile(info, final) {
		return "", 0, ErrRecipeArtifactUnavailable
	}
	return fmt.Sprintf("sha256:%x", hash.Sum(nil)), written, nil
}
