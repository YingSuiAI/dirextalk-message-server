package runtime

import "context"

// RecipeManifestRegistrationStore is implemented only by the independent
// Cloud Orchestrator repository. It derives and atomically registers one
// execution manifest from already-approved durable facts; callers cannot
// supply a manifest, artifact, action, slot reference, or execution ID.
type RecipeManifestRegistrationStore interface {
	RegisterNextTrustedRecipeExecutionManifest(context.Context) (bool, error)
}

// RecipeManifestRegistrationRunner is a deliberately transport-free bridge
// between verified Worker observation and the separate execution-approval
// boundary. Registering a manifest does not start a Worker task.
type RecipeManifestRegistrationRunner struct {
	store RecipeManifestRegistrationStore
}

func NewRecipeManifestRegistrationRunner(store RecipeManifestRegistrationStore) *RecipeManifestRegistrationRunner {
	return &RecipeManifestRegistrationRunner{store: store}
}

func (runner *RecipeManifestRegistrationRunner) RunOnce(ctx context.Context) (bool, error) {
	if runner == nil || runner.store == nil {
		return false, nil
	}
	return runner.store.RegisterNextTrustedRecipeExecutionManifest(ctx)
}
