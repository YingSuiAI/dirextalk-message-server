package mcp

import (
	"context"
	"net/http"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

func (m *Module) cloudWorkloadsList(ctx context.Context, params map[string]any) (any, *dirextalkmcp.Error) {
	cloud, err := m.requireCloudReader()
	if err != nil {
		return nil, err
	}
	kind := cloudWorkloadKind(params["kind"], true)
	if kind == "" {
		return nil, dirextalkmcp.BadRequest("kind must be plan, deployment, service, or all")
	}
	limit := dirextalkmcp.Limit(params)
	switch kind {
	case "plan":
		items, storageErr := cloud.ListCloudPlans(ctx)
		if storageErr != nil {
			return nil, internalError(storageErr)
		}
		return map[string]any{"kind": kind, "items": limitCloudPlans(items, limit)}, nil
	case "deployment":
		items, storageErr := cloud.ListCloudDeployments(ctx)
		if storageErr != nil {
			return nil, internalError(storageErr)
		}
		return map[string]any{"kind": kind, "items": limitCloudDeployments(items, limit)}, nil
	case "service":
		items, storageErr := cloud.ListCloudServices(ctx)
		if storageErr != nil {
			return nil, internalError(storageErr)
		}
		return map[string]any{"kind": kind, "items": limitCloudServices(items, limit)}, nil
	default:
		plans, plansErr := cloud.ListCloudPlans(ctx)
		if plansErr != nil {
			return nil, internalError(plansErr)
		}
		deployments, deploymentsErr := cloud.ListCloudDeployments(ctx)
		if deploymentsErr != nil {
			return nil, internalError(deploymentsErr)
		}
		services, servicesErr := cloud.ListCloudServices(ctx)
		if servicesErr != nil {
			return nil, internalError(servicesErr)
		}
		return map[string]any{
			"kind":        "all",
			"plans":       limitCloudPlans(plans, limit),
			"deployments": limitCloudDeployments(deployments, limit),
			"services":    limitCloudServices(services, limit),
		}, nil
	}
}

func (m *Module) cloudWorkloadsGet(ctx context.Context, params map[string]any) (any, *dirextalkmcp.Error) {
	cloud, err := m.requireCloudReader()
	if err != nil {
		return nil, err
	}
	kind := cloudWorkloadKind(params["kind"], false)
	id := strings.TrimSpace(dirextalkmcp.TrimString(params["id"]))
	if kind == "" || id == "" {
		return nil, dirextalkmcp.BadRequest("kind and id are required")
	}
	switch kind {
	case "plan":
		item, found, storageErr := cloud.GetCloudPlan(ctx, id)
		if storageErr != nil {
			return nil, internalError(storageErr)
		}
		if !found {
			return nil, dirextalkmcp.StatusError(http.StatusNotFound, "cloud workload was not found")
		}
		return map[string]any{"kind": kind, "item": cloudPlanSummary(item)}, nil
	case "deployment":
		item, found, storageErr := cloud.GetCloudDeployment(ctx, id)
		if storageErr != nil {
			return nil, internalError(storageErr)
		}
		if !found {
			return nil, dirextalkmcp.StatusError(http.StatusNotFound, "cloud workload was not found")
		}
		return map[string]any{"kind": kind, "item": cloudDeploymentSummary(item)}, nil
	case "service":
		item, found, storageErr := cloud.GetCloudService(ctx, id)
		if storageErr != nil {
			return nil, internalError(storageErr)
		}
		if !found {
			return nil, dirextalkmcp.StatusError(http.StatusNotFound, "cloud workload was not found")
		}
		return map[string]any{"kind": kind, "item": cloudServiceSummary(item)}, nil
	default:
		return nil, dirextalkmcp.BadRequest("kind must be plan, deployment, or service")
	}
}

func (m *Module) cloudStatus(ctx context.Context, _ map[string]any) (any, *dirextalkmcp.Error) {
	cloud, err := m.requireCloudReader()
	if err != nil {
		return nil, err
	}
	plans, plansErr := cloud.ListCloudPlans(ctx)
	if plansErr != nil {
		return nil, internalError(plansErr)
	}
	deployments, deploymentsErr := cloud.ListCloudDeployments(ctx)
	if deploymentsErr != nil {
		return nil, internalError(deploymentsErr)
	}
	services, servicesErr := cloud.ListCloudServices(ctx)
	if servicesErr != nil {
		return nil, internalError(servicesErr)
	}
	alerts, alertsErr := cloud.ListCloudAlerts(ctx)
	if alertsErr != nil {
		return nil, internalError(alertsErr)
	}
	return map[string]any{
		"plans":       cloudPlanStatusCounts(plans),
		"deployments": cloudDeploymentStatusCounts(deployments),
		"services":    cloudServiceStatusCounts(services),
		"alerts":      cloudAlertSummaries(alerts),
	}, nil
}

func (m *Module) requireCloudReader() (CloudReader, *dirextalkmcp.Error) {
	if m == nil || m.cloud == nil {
		return nil, dirextalkmcp.StatusError(http.StatusServiceUnavailable, "Cloud status is not configured")
	}
	return m.cloud, nil
}

func cloudWorkloadKind(value any, allowAll bool) string {
	kind := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(dirextalkmcp.TrimString(value))), "s")
	if kind == "" && allowAll {
		return "all"
	}
	if kind == "all" && allowAll {
		return kind
	}
	if kind == "plan" || kind == "deployment" || kind == "service" {
		return kind
	}
	return ""
}

func limitCloudPlans(items []cloudmodule.Plan, limit int) []map[string]any {
	if len(items) > limit {
		items = items[:limit]
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		result = append(result, cloudPlanSummary(item))
	}
	return result
}

func limitCloudDeployments(items []cloudmodule.Deployment, limit int) []map[string]any {
	if len(items) > limit {
		items = items[:limit]
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		result = append(result, cloudDeploymentSummary(item))
	}
	return result
}

func limitCloudServices(items []cloudmodule.Service, limit int) []map[string]any {
	if len(items) > limit {
		items = items[:limit]
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		result = append(result, cloudServiceSummary(item))
	}
	return result
}

func cloudPlanSummary(item cloudmodule.Plan) map[string]any {
	return map[string]any{
		"plan_id": item.PlanID, "goal_id": item.GoalID, "status": item.Status,
		"title": item.Title, "recipe_digest": item.RecipeDigest,
		"quote_id": item.QuoteID, "revision": item.Revision, "created_at": item.CreatedAt, "updated_at": item.UpdatedAt,
	}
}

func cloudDeploymentSummary(item cloudmodule.Deployment) map[string]any {
	return map[string]any{
		"deployment_id": item.DeploymentID, "plan_id": item.PlanID,
		"execution_status": item.Execution, "outcome_status": item.Outcome, "resource_status": item.Resource,
		"revision": item.Revision, "created_at": item.CreatedAt, "updated_at": item.UpdatedAt,
	}
}

func cloudServiceSummary(item cloudmodule.Service) map[string]any {
	return map[string]any{
		"service_id": item.ServiceID, "deployment_id": item.DeploymentID, "recipe_id": item.RecipeID,
		"name": item.Name, "service_status": item.Status, "integration_status": item.Integration,
		"revision": item.Revision, "created_at": item.CreatedAt, "updated_at": item.UpdatedAt,
	}
}

func cloudPlanStatusCounts(items []cloudmodule.Plan) map[string]any {
	statuses := make([]string, 0, len(items))
	for _, item := range items {
		statuses = append(statuses, item.Status)
	}
	return cloudStatusCounts(statuses)
}

func cloudDeploymentStatusCounts(items []cloudmodule.Deployment) map[string]any {
	statuses := make([]string, 0, len(items))
	for _, item := range items {
		statuses = append(statuses, item.Execution)
	}
	return cloudStatusCounts(statuses)
}

func cloudServiceStatusCounts(items []cloudmodule.Service) map[string]any {
	statuses := make([]string, 0, len(items))
	for _, item := range items {
		statuses = append(statuses, item.Status)
	}
	return cloudStatusCounts(statuses)
}

func cloudStatusCounts(statuses []string) map[string]any {
	counts := map[string]any{"total": len(statuses)}
	for _, status := range statuses {
		status = strings.TrimSpace(status)
		if status == "" {
			status = "unknown"
		}
		current, _ := counts[status].(int)
		counts[status] = current + 1
	}
	return counts
}

func cloudAlertSummaries(items []cloudmodule.Alert) []map[string]any {
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		result = append(result, map[string]any{
			"alert_id": item.AlertID, "severity": item.Severity, "code": item.Code,
			"acknowledged": item.Acknowledged, "revision": item.Revision,
			"created_at": item.CreatedAt, "updated_at": item.UpdatedAt,
		})
	}
	return result
}
