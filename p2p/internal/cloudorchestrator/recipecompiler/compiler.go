package recipecompiler

import (
	"errors"
	"reflect"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const OCIImageManifestMediaType = "application/vnd.oci.image.manifest.v1+json"

// Config is owned by the trusted compiler, not by Recipe research output.
// It contains only fixed, typed capabilities and content-addressed inputs.
type Config struct {
	RecipeRevision               uint64
	ImageDigest                  string
	ImageSizeBytes               uint64
	Architecture                 cloudorchestrator.Architecture
	WorkerResourceManifestDigest string
	HealthContract               cloudorchestrator.HealthContractV1
	LifecycleContract            cloudorchestrator.LifecycleContractV1
	Actions                      []cloudorchestrator.CompiledRecipeActionV1
	Health                       cloudorchestrator.OCIServiceHealthV1
}

func CompileOCIServiceBundle(recipe cloudorchestrator.RecipeV1, config Config) (cloudorchestrator.CompiledRecipeArtifactV1, cloudorchestrator.OCIServiceBundleV1, error) {
	prepared, bundle, err := compileOCIServiceBundleDescriptor(recipe, config)
	if err != nil {
		return cloudorchestrator.CompiledRecipeArtifactV1{}, cloudorchestrator.OCIServiceBundleV1{}, err
	}
	artifact, err := finalizeOCIServiceArtifact(recipe, config, prepared, bundle, bundle, config.WorkerResourceManifestDigest)
	if err != nil {
		return cloudorchestrator.CompiledRecipeArtifactV1{}, cloudorchestrator.OCIServiceBundleV1{}, err
	}
	return artifact, bundle, nil
}

// CompileOCIServiceBundleDescriptor is the pre-manifest compilation phase. It
// validates every executable OCI capability but deliberately does not require
// WorkerResourceManifestDigest, allowing the returned bundle to be placed in
// the immutable Worker catalog before that manifest's digest exists.
func CompileOCIServiceBundleDescriptor(recipe cloudorchestrator.RecipeV1, config Config) (cloudorchestrator.OCIServiceBundleV1, error) {
	_, bundle, err := compileOCIServiceBundleDescriptor(recipe, config)
	return bundle, err
}

// FinalizeOCIServiceArtifact binds a previously compiled, unchanged bundle to
// the final Worker resource manifest. The bundle is recompiled from the same
// verified inputs and compared exactly before the artifact can be emitted.
func FinalizeOCIServiceArtifact(recipe cloudorchestrator.RecipeV1, config Config, bundle cloudorchestrator.OCIServiceBundleV1, workerResourceManifestDigest string) (cloudorchestrator.CompiledRecipeArtifactV1, error) {
	prepared, expectedBundle, err := compileOCIServiceBundleDescriptor(recipe, config)
	if err != nil {
		return cloudorchestrator.CompiledRecipeArtifactV1{}, err
	}
	return finalizeOCIServiceArtifact(recipe, config, prepared, expectedBundle, bundle, workerResourceManifestDigest)
}

type preparedOCIServiceCompilation struct {
	recipeDigest          string
	healthDigest          string
	lifecycleDigest       string
	officialSourceDigests []string
}

func compileOCIServiceBundleDescriptor(recipe cloudorchestrator.RecipeV1, config Config) (preparedOCIServiceCompilation, cloudorchestrator.OCIServiceBundleV1, error) {
	if err := recipe.Validate(); err != nil {
		return preparedOCIServiceCompilation{}, cloudorchestrator.OCIServiceBundleV1{}, errors.New("verified recipe is invalid")
	}
	if config.RecipeRevision == 0 || config.Architecture != recipe.Requirements.Architecture || !reflect.DeepEqual(recipe.Health, config.HealthContract) || !reflect.DeepEqual(recipe.Lifecycle, config.LifecycleContract) {
		return preparedOCIServiceCompilation{}, cloudorchestrator.OCIServiceBundleV1{}, errors.New("compiler-owned contract does not match verified recipe")
	}
	recipeDigest, err := recipe.Digest()
	if err != nil {
		return preparedOCIServiceCompilation{}, cloudorchestrator.OCIServiceBundleV1{}, err
	}
	healthDigest, err := cloudorchestrator.HealthContractDigestV1(config.HealthContract)
	if err != nil {
		return preparedOCIServiceCompilation{}, cloudorchestrator.OCIServiceBundleV1{}, errors.New("compiler health contract is invalid")
	}
	lifecycleDigest, err := cloudorchestrator.LifecycleContractDigestV1(config.LifecycleContract)
	if err != nil {
		return preparedOCIServiceCompilation{}, cloudorchestrator.OCIServiceBundleV1{}, errors.New("compiler lifecycle contract is invalid")
	}
	sourceDigests := make([]string, 0, len(recipe.Sources))
	for _, source := range recipe.Sources {
		if source.Official {
			sourceDigests = append(sourceDigests, source.ArtifactDigest)
		}
	}
	if len(sourceDigests) == 0 || !actionsMatchRecipe(recipe, config.Actions) {
		return preparedOCIServiceCompilation{}, cloudorchestrator.OCIServiceBundleV1{}, errors.New("compiler inputs do not bind verified recipe")
	}
	bundle := cloudorchestrator.OCIServiceBundleV1{
		SchemaVersion: cloudorchestrator.OCIServiceBundleV1Schema, ArtifactDigest: config.ImageDigest, ImageDigest: config.ImageDigest, ImageSizeBytes: config.ImageSizeBytes,
		Architecture: config.Architecture, Actions: cloneActions(config.Actions), Health: config.Health, HealthContractDigest: healthDigest, LifecycleContractDigest: lifecycleDigest,
	}
	if bundle.Validate() != nil {
		return preparedOCIServiceCompilation{}, cloudorchestrator.OCIServiceBundleV1{}, errors.New("compiled OCI service bundle is invalid")
	}
	return preparedOCIServiceCompilation{
		recipeDigest: recipeDigest, healthDigest: healthDigest, lifecycleDigest: lifecycleDigest,
		officialSourceDigests: append([]string(nil), sourceDigests...),
	}, bundle, nil
}

func finalizeOCIServiceArtifact(recipe cloudorchestrator.RecipeV1, config Config, prepared preparedOCIServiceCompilation, expectedBundle, bundle cloudorchestrator.OCIServiceBundleV1, workerResourceManifestDigest string) (cloudorchestrator.CompiledRecipeArtifactV1, error) {
	if config.WorkerResourceManifestDigest != "" && config.WorkerResourceManifestDigest != workerResourceManifestDigest ||
		bundle.Validate() != nil || !reflect.DeepEqual(expectedBundle, bundle) {
		return cloudorchestrator.CompiledRecipeArtifactV1{}, errors.New("compiled OCI service artifact is invalid")
	}
	artifact := cloudorchestrator.CompiledRecipeArtifactV1{
		SchemaVersion: cloudorchestrator.CompiledRecipeArtifactV1Schema, RecipeID: recipe.RecipeID, RecipeDigest: prepared.recipeDigest, RecipeRevision: config.RecipeRevision,
		OfficialSourceArtifactDigests: append([]string(nil), prepared.officialSourceDigests...), Architecture: config.Architecture, Requirements: recipe.Requirements,
		WorkerResourceManifestDigest: workerResourceManifestDigest, ArtifactDigest: bundle.ArtifactDigest, MediaType: OCIImageManifestMediaType, SizeBytes: bundle.ImageSizeBytes,
		Actions: cloneActions(bundle.Actions), HealthContractDigest: prepared.healthDigest, LifecycleContractDigest: prepared.lifecycleDigest,
		VolumeSlots: append([]cloudorchestrator.RecipeVolumeSlotRequirementV1(nil), recipe.VolumeSlots...), DataSlots: append([]cloudorchestrator.RecipeDataSlotRequirementV1(nil), recipe.DataSlots...), SecretSlots: append([]cloudorchestrator.RecipeSecretSlotRequirementV1(nil), recipe.SecretSlots...),
	}
	if artifact.Validate() != nil || !reflect.DeepEqual(artifact.Actions, bundle.Actions) || artifact.HealthContractDigest != bundle.HealthContractDigest || artifact.LifecycleContractDigest != bundle.LifecycleContractDigest || artifact.ArtifactDigest != bundle.ImageDigest || artifact.SizeBytes != bundle.ImageSizeBytes || artifact.Architecture != bundle.Architecture {
		return cloudorchestrator.CompiledRecipeArtifactV1{}, errors.New("compiled OCI service artifact is invalid")
	}
	return artifact, nil
}

func actionsMatchRecipe(recipe cloudorchestrator.RecipeV1, actions []cloudorchestrator.CompiledRecipeActionV1) bool {
	if len(actions) == 0 {
		return false
	}
	lifecycle := map[cloudorchestrator.CompiledRecipeActionKind]string{
		cloudorchestrator.CompiledRecipeActionStart: recipe.Lifecycle.Start, cloudorchestrator.CompiledRecipeActionStop: recipe.Lifecycle.Stop,
		cloudorchestrator.CompiledRecipeActionRestart: recipe.Lifecycle.Restart, cloudorchestrator.CompiledRecipeActionUpgrade: recipe.Lifecycle.Upgrade,
		cloudorchestrator.CompiledRecipeActionRollback: recipe.Lifecycle.Rollback, cloudorchestrator.CompiledRecipeActionBackup: recipe.Lifecycle.Backup,
		cloudorchestrator.CompiledRecipeActionRestore: recipe.Lifecycle.Restore, cloudorchestrator.CompiledRecipeActionDestroy: recipe.Lifecycle.Destroy,
	}
	hasInstall := false
	installCheckpoints := cloudorchestrator.OCIServiceInstallCheckpointSequenceV1()
	for _, action := range actions {
		if action.Kind == cloudorchestrator.CompiledRecipeActionInstall {
			hasInstall = action.RootRequired == recipe.Install.RootRequired && action.TimeoutSeconds == recipe.Install.TimeoutSeconds &&
				reflect.DeepEqual(action.CheckpointSequence, installCheckpoints) && reflect.DeepEqual(recipe.Install.CheckpointNames, installCheckpoints)
			continue
		}
		if expected, found := lifecycle[action.Kind]; !found || action.ActionID != expected {
			return false
		}
	}
	return hasInstall
}

func cloneActions(actions []cloudorchestrator.CompiledRecipeActionV1) []cloudorchestrator.CompiledRecipeActionV1 {
	cloned := append([]cloudorchestrator.CompiledRecipeActionV1(nil), actions...)
	for index := range cloned {
		cloned[index].CheckpointSequence = append([]string(nil), cloned[index].CheckpointSequence...)
	}
	return cloned
}
