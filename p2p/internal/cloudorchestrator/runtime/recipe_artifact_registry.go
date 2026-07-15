package runtime

import (
	"context"
	"errors"
)

// RecipeArtifactTransferRegistry selects the immutable Controller archive by
// the artifact digest already bound into the approved execution manifest.
// It lets one Orchestrator serve multiple private Recipes without making a
// mutable path or caller-supplied archive part of the execution request.
type RecipeArtifactTransferRegistry struct {
	byArtifactDigest map[string]*RecipeArtifactTransferManager
}

func NewRecipeArtifactTransferRegistry(managers ...*RecipeArtifactTransferManager) (*RecipeArtifactTransferRegistry, error) {
	if len(managers) == 0 || len(managers) > 64 {
		return nil, errors.New("recipe artifact transfer registry configuration is invalid")
	}
	registry := &RecipeArtifactTransferRegistry{byArtifactDigest: make(map[string]*RecipeArtifactTransferManager, len(managers))}
	for _, manager := range managers {
		if manager == nil || manager.archive.Validate() != nil {
			return nil, errors.New("recipe artifact transfer registry configuration is invalid")
		}
		digest := manager.archive.ArtifactDigest
		if _, exists := registry.byArtifactDigest[digest]; exists {
			return nil, errors.New("recipe artifact transfer registry contains a duplicate artifact")
		}
		registry.byArtifactDigest[digest] = manager
	}
	return registry, nil
}

func (registry *RecipeArtifactTransferRegistry) Ensure(ctx context.Context, claim RecipeInstallClaim) error {
	if registry == nil || ctx == nil || ValidateRecipeInstallClaim(claim) != nil {
		return errors.New("recipe artifact transfer registry request is invalid")
	}
	manager := registry.byArtifactDigest[claim.Manifest.ArtifactDigest]
	if manager == nil {
		return errors.New("approved recipe artifact is not registered")
	}
	return manager.Ensure(ctx, claim)
}
