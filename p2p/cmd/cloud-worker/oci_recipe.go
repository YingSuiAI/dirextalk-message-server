package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"runtime"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/ociservice"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

const maxOCIConfigFileBytes = 4 * 1024 * 1024

type secretMaterializerProvider interface {
	NewSecretMaterializer() (recipeexec.SecretMaterializer, error)
}

func newOCIRecipeProcessor(client workerSessionClient, config commandConfig, bootstrap cloudworker.BootstrapManifest) (recipeTaskProcessor, error) {
	if runtime.GOOS != "linux" || client == nil {
		return nil, recipeexec.ErrExecutorConfiguration
	}
	approvedManifestDigest, err := approvedWorkerResourceManifestDigest(bootstrap)
	if err != nil {
		return nil, err
	}
	catalogRaw, err := readStrictRegularFile(config.ociCatalogFile, maxOCIConfigFileBytes)
	if err != nil {
		return nil, recipeexec.ErrExecutorConfiguration
	}
	catalog, err := recipeexec.ParseWorkerOCICatalogV1(catalogRaw)
	if err != nil {
		return nil, err
	}
	if err := validateInitialOCIReadiness(catalog); err != nil {
		return nil, err
	}
	resourceRaw, err := readStrictRegularFile(config.workerResourceFile, maxOCIConfigFileBytes)
	if err != nil {
		return nil, recipeexec.ErrExecutorConfiguration
	}
	resourceManifest, err := recipeexec.ParseWorkerResourceManifestV1(resourceRaw)
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
	resolver, err := recipeexec.NewOCICatalogResolver(catalog, resourceManifest, approvedManifestDigest, executableDigest)
	if err != nil {
		return nil, err
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
	return cloudworker.NewRecipeTaskLoopWithExecutor(transport, executor)
}

// validateInitialOCIReadiness is the deliberately narrow first-validation
// gate. Generic catalog-backed readiness is deferred until a separately
// versioned readiness protocol can bind arbitrary typed probes end to end.
func validateInitialOCIReadiness(catalog recipeexec.WorkerOCICatalogV1) error {
	want := cloudorchestrator.OCIServiceLoopbackProbeV1{
		Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 18080, Path: "/ready",
		ExpectedStatus: 200, BodySHA256: cloudworker.FixedReadinessEvidenceDigest(),
	}
	if len(catalog.Entries) == 0 {
		return recipeexec.ErrExecutorConfiguration
	}
	for _, entry := range catalog.Entries {
		health := entry.Descriptor.Health
		if health.Liveness != want || health.Readiness != want || health.Semantic != want {
			return recipeexec.ErrExecutorConfiguration
		}
	}
	return nil
}

func approvedWorkerResourceManifestDigest(bootstrap cloudworker.BootstrapManifest) (string, error) {
	if bootstrap.WorkerImageDigest == "" || bootstrap.WorkerImageDigest != bootstrap.ArtifactManifestDigest {
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
