package recipecompiler

import (
	"errors"
	"reflect"
	"sort"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const OCIImageManifestMediaType = "application/vnd.oci.image.manifest.v1+json"

// Config is owned by the trusted compiler, not by Recipe research output.
// It contains only fixed, typed capabilities and content-addressed inputs.
type Config struct {
	RecipeRevision               uint64
	ImageSource                  cloudorchestrator.OCIImageSourceReferenceV1
	ImageDigest                  string
	ImageSizeBytes               uint64
	Architecture                 cloudorchestrator.Architecture
	WorkerResourceManifestDigest string
	HealthContract               cloudorchestrator.HealthContractV1
	LifecycleContract            cloudorchestrator.LifecycleContractV1
	Actions                      []cloudorchestrator.CompiledRecipeActionV1
	Health                       cloudorchestrator.OCIServiceHealthV1
	VolumeTargets                []cloudorchestrator.OCIServiceMountTargetV1
	DataTargets                  []cloudorchestrator.OCIServiceMountTargetV1
	RuntimeProfile               *cloudorchestrator.OCIServiceRuntimeProfileV1
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
	volumeTargets, dataTargets, err := compileStorageTargets(recipe, config.VolumeTargets, config.DataTargets)
	if err != nil {
		return preparedOCIServiceCompilation{}, cloudorchestrator.OCIServiceBundleV1{}, err
	}
	runtimeProfile, err := cloudorchestrator.NormalizeOCIServiceRuntimeProfileV1(config.RuntimeProfile)
	if err != nil || !runtimeProfileBindsRecipeSecrets(runtimeProfile, recipe.SecretSlots) {
		return preparedOCIServiceCompilation{}, cloudorchestrator.OCIServiceBundleV1{}, errors.New("compiler runtime profile does not bind verified recipe")
	}
	bundle := cloudorchestrator.OCIServiceBundleV1{
		SchemaVersion: cloudorchestrator.OCIServiceBundleV1Schema, ArtifactDigest: config.ImageDigest, ImageSource: config.ImageSource, ImageDigest: config.ImageDigest, ImageSizeBytes: config.ImageSizeBytes,
		Architecture: config.Architecture, Actions: cloneActions(config.Actions), Health: config.Health, HealthContractDigest: healthDigest, LifecycleContractDigest: lifecycleDigest,
		VolumeTargets: volumeTargets, DataTargets: dataTargets, RuntimeProfile: runtimeProfile,
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
		WorkerResourceManifestDigest: workerResourceManifestDigest, ArtifactDigest: bundle.ArtifactDigest, ImageSource: bundle.ImageSource, MediaType: OCIImageManifestMediaType, SizeBytes: bundle.ImageSizeBytes,
		Actions: cloneActions(bundle.Actions), SemanticReadiness: bundle.Health.Semantic, HealthContractDigest: prepared.healthDigest, LifecycleContractDigest: prepared.lifecycleDigest,
		VolumeSlots: append([]cloudorchestrator.RecipeVolumeSlotRequirementV1(nil), recipe.VolumeSlots...), DataSlots: append([]cloudorchestrator.RecipeDataSlotRequirementV1(nil), recipe.DataSlots...), SecretSlots: append([]cloudorchestrator.RecipeSecretSlotRequirementV1(nil), recipe.SecretSlots...),
		RuntimeProfile: cloudorchestrator.CloneOCIServiceRuntimeProfileV1(bundle.RuntimeProfile),
	}
	if artifact.Validate() != nil || !reflect.DeepEqual(artifact.Actions, bundle.Actions) || artifact.SemanticReadiness != bundle.Health.Semantic || artifact.HealthContractDigest != bundle.HealthContractDigest || artifact.LifecycleContractDigest != bundle.LifecycleContractDigest || artifact.ImageSource != bundle.ImageSource || artifact.ArtifactDigest != bundle.ImageDigest || artifact.SizeBytes != bundle.ImageSizeBytes || artifact.Architecture != bundle.Architecture ||
		!storageTargetsBindRecipe(recipe, bundle.VolumeTargets, bundle.DataTargets) || !reflect.DeepEqual(artifact.RuntimeProfile, bundle.RuntimeProfile) {
		return cloudorchestrator.CompiledRecipeArtifactV1{}, errors.New("compiled OCI service artifact is invalid")
	}
	return artifact, nil
}

func compileStorageTargets(recipe cloudorchestrator.RecipeV1, volumeInputs, dataInputs []cloudorchestrator.OCIServiceMountTargetV1) ([]cloudorchestrator.OCIServiceStorageTargetV1, []cloudorchestrator.OCIServiceStorageTargetV1, error) {
	volumeRequirements := make(map[string]bool, len(recipe.VolumeSlots))
	for _, requirement := range recipe.VolumeSlots {
		volumeRequirements[requirement.SlotID] = requirement.ReadOnly
	}
	dataRequirements := make(map[string]bool, len(recipe.DataSlots))
	for _, requirement := range recipe.DataSlots {
		dataRequirements[requirement.SlotID] = requirement.ReadOnly
	}
	if len(volumeInputs) != len(volumeRequirements) || len(dataInputs) != len(dataRequirements) {
		return nil, nil, errors.New("compiler storage targets do not exactly match verified recipe")
	}
	seenSlots := make(map[string]struct{}, len(volumeInputs)+len(dataInputs))
	seenTargets := make(map[string]struct{}, len(volumeInputs)+len(dataInputs))
	bind := func(inputs []cloudorchestrator.OCIServiceMountTargetV1, requirements map[string]bool) ([]cloudorchestrator.OCIServiceStorageTargetV1, error) {
		if len(inputs) == 0 {
			return nil, nil
		}
		result := make([]cloudorchestrator.OCIServiceStorageTargetV1, 0, len(inputs))
		for _, input := range inputs {
			readOnly, exists := requirements[input.SlotID]
			if !exists || cloudorchestrator.ValidateOCIServiceContainerTarget(input.ContainerTarget) != nil {
				return nil, errors.New("compiler storage target is invalid")
			}
			if _, duplicate := seenSlots[input.SlotID]; duplicate {
				return nil, errors.New("compiler storage target slot is duplicated")
			}
			if _, duplicate := seenTargets[input.ContainerTarget]; duplicate {
				return nil, errors.New("compiler storage container target is duplicated")
			}
			seenSlots[input.SlotID], seenTargets[input.ContainerTarget] = struct{}{}, struct{}{}
			if input.OwnerUID > 65535 || input.OwnerGID > 65535 {
				return nil, errors.New("compiler storage target ownership is invalid")
			}
			if _, err := cloudorchestrator.NormalizeOCIServiceStorageDirectoryMode(input.DirectoryMode); err != nil {
				return nil, errors.New("compiler storage target mode is invalid")
			}
			result = append(result, cloudorchestrator.OCIServiceStorageTargetV1{SlotID: input.SlotID, ContainerTarget: input.ContainerTarget, ReadOnly: readOnly, OwnerUID: input.OwnerUID, OwnerGID: input.OwnerGID, DirectoryMode: input.DirectoryMode})
		}
		sort.Slice(result, func(i, j int) bool { return result[i].SlotID < result[j].SlotID })
		return result, nil
	}
	volumes, err := bind(volumeInputs, volumeRequirements)
	if err != nil {
		return nil, nil, err
	}
	data, err := bind(dataInputs, dataRequirements)
	if err != nil {
		return nil, nil, err
	}
	return volumes, data, nil
}

func storageTargetsBindRecipe(recipe cloudorchestrator.RecipeV1, volumes, data []cloudorchestrator.OCIServiceStorageTargetV1) bool {
	volumeInputs := make([]cloudorchestrator.OCIServiceMountTargetV1, len(volumes))
	for index, target := range volumes {
		volumeInputs[index] = cloudorchestrator.OCIServiceMountTargetV1{SlotID: target.SlotID, ContainerTarget: target.ContainerTarget, OwnerUID: target.OwnerUID, OwnerGID: target.OwnerGID, DirectoryMode: target.DirectoryMode}
	}
	dataInputs := make([]cloudorchestrator.OCIServiceMountTargetV1, len(data))
	for index, target := range data {
		dataInputs[index] = cloudorchestrator.OCIServiceMountTargetV1{SlotID: target.SlotID, ContainerTarget: target.ContainerTarget, OwnerUID: target.OwnerUID, OwnerGID: target.OwnerGID, DirectoryMode: target.DirectoryMode}
	}
	wantVolumes, wantData, err := compileStorageTargets(recipe, volumeInputs, dataInputs)
	return err == nil && reflect.DeepEqual(wantVolumes, volumes) && reflect.DeepEqual(wantData, data)
}

func runtimeProfileBindsRecipeSecrets(profile *cloudorchestrator.OCIServiceRuntimeProfileV1, requirements []cloudorchestrator.RecipeSecretSlotRequirementV1) bool {
	if profile == nil || len(profile.SecretEnvironment) == 0 {
		return true
	}
	bySlot := make(map[string]cloudorchestrator.RecipeSecretSlotRequirementV1, len(requirements))
	for _, requirement := range requirements {
		bySlot[requirement.SlotID] = requirement
	}
	for _, binding := range profile.SecretEnvironment {
		requirement, ok := bySlot[binding.SlotID]
		if !ok || requirement.Delivery != cloudorchestrator.SecretDeliveryFile {
			return false
		}
	}
	return true
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
	for _, action := range actions {
		expectedCheckpoints, supported := cloudorchestrator.OCIServiceActionCheckpointSequenceV1(action.Kind)
		if !supported || !action.RootRequired || !reflect.DeepEqual(action.CheckpointSequence, expectedCheckpoints) {
			return false
		}
		if action.Kind == cloudorchestrator.CompiledRecipeActionInstall {
			hasInstall = action.RootRequired == recipe.Install.RootRequired && action.TimeoutSeconds == recipe.Install.TimeoutSeconds &&
				reflect.DeepEqual(recipe.Install.CheckpointNames, expectedCheckpoints)
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
