package p2p

import (
	"context"
	"fmt"
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
