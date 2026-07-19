package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/ociservice"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

const maxOCIConfigFileBytes = 4 * 1024 * 1024

type secretMaterializerProvider interface {
	NewSecretMaterializer() (recipeexec.SecretMaterializer, error)
}

type recipeArtifactCacheProvider interface {
	NewRecipeArtifactCache(string, string, *recipeexec.OCICatalogResolver, string) (*cloudworker.RecipeArtifactCache, error)
}

func newOCIRecipeProcessor(client workerSessionClient, config commandConfig, bootstrap cloudworker.BootstrapManifest) (recipeTaskProcessor, error) {
	if runtime.GOOS != "linux" || client == nil {
		return nil, recipeexec.ErrExecutorConfiguration
	}
	approvedManifestDigest, err := approvedWorkerResourceManifestDigest(bootstrap, config.dynamicRecipeArtifact)
	if err != nil {
		return nil, err
	}
	executablePath, err := os.Executable()
	if err != nil {
		return nil, recipeexec.ErrExecutorConfiguration
	}
	executableDigest, err := streamRegularFileSHA256(executablePath)
	if err != nil {
		return nil, recipeexec.ErrExecutorConfiguration
	}
	var resolver *recipeexec.OCICatalogResolver
	if config.dynamicRecipeArtifact {
		resolver, err = recipeexec.NewDynamicOCICatalogResolver(approvedManifestDigest, executableDigest)
		if err != nil {
			return nil, err
		}
	} else {
		catalogRaw, readErr := readStrictRegularFile(config.ociCatalogFile, maxOCIConfigFileBytes)
		if readErr != nil {
			return nil, recipeexec.ErrExecutorConfiguration
		}
		catalog, parseErr := recipeexec.ParseWorkerOCICatalogV1(catalogRaw)
		if parseErr != nil {
			return nil, parseErr
		}
		resourceRaw, readErr := readStrictRegularFile(config.workerResourceFile, maxOCIConfigFileBytes)
		if readErr != nil {
			return nil, recipeexec.ErrExecutorConfiguration
		}
		resourceManifest, parseErr := recipeexec.ParseWorkerResourceManifestV1(resourceRaw)
		if parseErr != nil {
			return nil, parseErr
		}
		resolver, err = recipeexec.NewOCICatalogResolver(catalog, resourceManifest, approvedManifestDigest, executableDigest)
		if err != nil {
			return nil, err
		}
	}
	checkpointStore, err := recipeexec.NewFileCheckpointStore(config.recipeCheckpointDir)
	if err != nil {
		return nil, err
	}
	driver, err := ociservice.NewProductionDriver(resolver)
	if err != nil {
		return nil, err
	}
	materializerProvider, ok := client.(secretMaterializerProvider)
	if !ok {
		return nil, recipeexec.ErrExecutorConfiguration
	}
	materializer, err := materializerProvider.NewSecretMaterializer()
	if err != nil {
		return nil, err
	}
	secretStager, err := recipeexec.NewFileSecretStager(ociservice.SecretStagingRoot, recipeexec.VerifyTmpfsRoot)
	if err != nil {
		return nil, err
	}
	transportProvider, ok := client.(recipeTaskClientProvider)
	if !ok {
		return nil, recipeexec.ErrExecutorConfiguration
	}
	transport, err := transportProvider.NewRecipeTaskClient()
	if err != nil {
		return nil, err
	}
	executor := recipeexec.Executor{
		Resolver: resolver, Store: checkpointStore, Driver: driver,
		RequireSecretMaterialization: true, Materializer: materializer, SecretStager: secretStager, SecretRetryDelay: time.Second,
	}
	if config.dynamicRecipeArtifact {
		provider, ok := client.(recipeArtifactCacheProvider)
		if !ok {
			return nil, recipeexec.ErrExecutorConfiguration
		}
		cache, cacheErr := provider.NewRecipeArtifactCache(filepath.Join(config.recipeCheckpointDir, "artifact-cache"), bootstrap.DeploymentID, resolver, executableDigest)
		if cacheErr != nil {
			return nil, cacheErr
		}
		return cloudworker.NewRecipeTaskLoopWithArtifactPreparer(transport, executor, cache)
	}
	return cloudworker.NewRecipeTaskLoopWithExecutor(transport, executor)
}

// validateInitialOCIReadiness retains a focused test seam for catalog safety.
// Mutable hosts, URLs, commands, and unpinned bodies are rejected by the
// descriptor validator; individual typed probes are now bound end to end.
func validateInitialOCIReadiness(catalog recipeexec.WorkerOCICatalogV1) error {
	if len(catalog.Entries) == 0 {
		return recipeexec.ErrExecutorConfiguration
	}
	for _, entry := range catalog.Entries {
		if entry.Descriptor.Health.Semantic.Validate() != nil {
			return recipeexec.ErrExecutorConfiguration
		}
	}
	return nil
}

func approvedWorkerResourceManifestDigest(bootstrap cloudworker.BootstrapManifest, dynamicArtifact bool) (string, error) {
	if bootstrap.WorkerImageDigest == "" || bootstrap.ArtifactManifestDigest == "" {
		return "", recipeexec.ErrExecutorConfiguration
	}
	if dynamicArtifact {
		return bootstrap.ArtifactManifestDigest, nil
	}
	if bootstrap.WorkerImageDigest != bootstrap.ArtifactManifestDigest {
		return "", recipeexec.ErrExecutorConfiguration
	}
	return bootstrap.WorkerImageDigest, nil
}

func readStrictRegularFile(path string, maximum int64) ([]byte, error) {
	if path == "" || maximum <= 0 {
		return nil, errConfigInvalid
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Size() <= 0 || before.Size() > maximum {
		return nil, errConfigInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errConfigInvalid
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) || after.Size() != before.Size() {
		return nil, errConfigInvalid
	}
	content, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(content)) != after.Size() || int64(len(content)) > maximum {
		return nil, errConfigInvalid
	}
	return content, nil
}

func streamRegularFileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", errConfigInvalid
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return "", errConfigInvalid
	}
	hash := sha256.New()
	written, err := io.Copy(hash, file)
	if err != nil || written != info.Size() {
		return "", errConfigInvalid
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}
