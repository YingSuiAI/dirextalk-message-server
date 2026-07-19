package p2p

import (
	"context"
	"fmt"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/nativeagent"
)

// serviceNativeCloudPlannerPort is the deliberately narrow bridge from the
// server-side Eino Native Agent to the Cloud ProductCore façade. It cannot
// expose the Service, AWS credentials, or any deployment lifecycle operation
// to an Agent tool.
type serviceNativeCloudPlannerPort struct{ service *Service }

func (p serviceNativeCloudPlannerPort) CreateResearchGoal(ctx context.Context, goal, connectionID, idempotencyKey string) (map[string]any, error) {
	if p.service == nil || p.service.cloudModule == nil {
		return nil, fmt.Errorf("cloud orchestrator is not configured")
	}
	return p.service.cloudModule.CreateResearchGoal(ctx, goal, connectionID, idempotencyKey)
}

func (p serviceNativeCloudPlannerPort) CreateResearchGoalWithRecipe(ctx context.Context, goal, connectionID, recipeID string, recipeRevision int64, idempotencyKey string) (map[string]any, error) {
	if p.service == nil || p.service.cloudModule == nil {
		return nil, fmt.Errorf("cloud orchestrator is not configured")
	}
	return p.service.cloudModule.CreateResearchGoalWithRecipe(ctx, goal, connectionID, recipeID, recipeRevision, idempotencyKey)
}

func (p serviceNativeCloudPlannerPort) ReadCloudStatus(ctx context.Context) (map[string]any, error) {
	if p.service == nil || p.service.cloudModule == nil {
		return nil, fmt.Errorf("cloud status is not configured")
	}
	return p.service.cloudModule.ReadCloudStatus(ctx)
}

func (p serviceNativeCloudPlannerPort) ReadCloudRecipes(ctx context.Context) ([]nativeagent.CloudRecipeRecommendation, error) {
	if p.service == nil || p.service.cloudModule == nil {
		return nil, fmt.Errorf("cloud recipe recommendations are not configured")
	}
	items, err := p.service.cloudModule.ReadCloudRecipeRecommendations(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]nativeagent.CloudRecipeRecommendation, 0, len(items))
	for _, item := range items {
		result = append(result, nativeagent.CloudRecipeRecommendation{
			RecipeID: item.RecipeID, Name: item.Name, Version: item.Version, Maturity: item.Maturity, Revision: item.Revision,
			Resources: nativeagent.CloudRecipeResourceSummary{
				MinVCPU: item.Resources.MinVCPU, MinMemoryMiB: item.Resources.MinMemoryMiB,
				MinGPUMemoryMiB: item.Resources.MinGPUMemoryMiB, MinDiskGiB: item.Resources.MinDiskGiB,
				Architecture: item.Resources.Architecture,
			},
		})
	}
	return result, nil
}
