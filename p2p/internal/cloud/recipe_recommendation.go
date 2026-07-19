package cloud

import (
	"context"
	"errors"
	"strings"
)

// RecipeRecommendationStore is a narrow owner-scoped read boundary used only
// by the server-side Native Agent adapter. Public ProductCore and MCP actions
// do not expose it.
type RecipeRecommendationStore interface {
	ListCloudRecipeRecommendations(context.Context, string) ([]RecipeRecommendation, error)
}

type RecipeResourceSummary struct {
	MinVCPU         uint16 `json:"min_vcpu"`
	MinMemoryMiB    uint32 `json:"min_memory_mib"`
	MinGPUMemoryMiB uint32 `json:"min_gpu_memory_mib"`
	MinDiskGiB      uint32 `json:"min_disk_gib"`
	Architecture    string `json:"architecture"`
}

// RecipeRecommendation intentionally excludes provenance, digests, artifact
// identity, refs, ownership, and Cloud Connection data.
type RecipeRecommendation struct {
	RecipeID  string                `json:"recipe_id"`
	Name      string                `json:"name"`
	Version   string                `json:"version"`
	Maturity  string                `json:"maturity"`
	Revision  int64                 `json:"revision"`
	Resources RecipeResourceSummary `json:"resources"`
}

func (m *Module) ReadCloudRecipeRecommendations(ctx context.Context) ([]RecipeRecommendation, error) {
	if m == nil || m.store == nil {
		return nil, errors.New("cloud recipe recommendations are not configured")
	}
	owner := m.ownerMXID()
	if strings.TrimSpace(owner) == "" {
		return nil, errors.New("cloud recipe owner is unavailable")
	}
	store, ok := m.store.(RecipeRecommendationStore)
	if !ok {
		return nil, errors.New("cloud recipe recommendations are not configured")
	}
	return store.ListCloudRecipeRecommendations(ctx, owner)
}
