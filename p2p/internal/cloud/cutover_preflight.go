package cloud

import (
	"context"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const (
	cutoverPreflightActiveResourcesReason = "legacy_cloud_active_resources_present"
	cutoverPreflightDataReason            = "legacy_cloud_data_present"
	cutoverPreflightReadFailedReason      = "legacy_cloud_read_failed"
)

// cutoverPreflight is deliberately a read-only compatibility gate. Direct
// cutover must not drop a legacy local Cloud fact merely because a read was
// incomplete, so an unavailable store or any individual read failure returns
// a redacted blocked result rather than a transport error.
func (m *Module) cutoverPreflight(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params); err != nil {
		return nil, err
	}
	if m == nil || m.store == nil {
		return blockedCutoverPreflight(cutoverPreflightReadFailedReason, 0), nil
	}

	goals, err := m.store.ListCloudGoals(ctx)
	if err != nil {
		return blockedCutoverPreflight(cutoverPreflightReadFailedReason, 0), nil
	}
	plans, err := m.store.ListCloudPlans(ctx)
	if err != nil {
		return blockedCutoverPreflight(cutoverPreflightReadFailedReason, 0), nil
	}
	jobs, err := m.store.ListCloudJobs(ctx)
	if err != nil {
		return blockedCutoverPreflight(cutoverPreflightReadFailedReason, 0), nil
	}
	connections, err := m.store.ListCloudConnections(ctx)
	if err != nil {
		return blockedCutoverPreflight(cutoverPreflightReadFailedReason, 0), nil
	}
	deployments, err := m.store.ListCloudDeployments(ctx)
	if err != nil {
		return blockedCutoverPreflight(cutoverPreflightReadFailedReason, 0), nil
	}
	services, err := m.store.ListCloudServices(ctx)
	if err != nil {
		return blockedCutoverPreflight(cutoverPreflightReadFailedReason, 0), nil
	}
	recipes, err := m.store.ListCloudRecipes(ctx)
	if err != nil {
		return blockedCutoverPreflight(cutoverPreflightReadFailedReason, 0), nil
	}
	alerts, err := m.store.ListCloudAlerts(ctx)
	if err != nil {
		return blockedCutoverPreflight(cutoverPreflightReadFailedReason, 0), nil
	}
	// One event is sufficient to prove that legacy Cloud event data remains.
	// The count is therefore a conservative lower bound, which is all this
	// fail-closed gate needs; it never needs an unbounded event-history read.
	events, err := m.store.ListCloudEvents(ctx, 1)
	if err != nil {
		return blockedCutoverPreflight(cutoverPreflightReadFailedReason, 0), nil
	}
	privateFootprintReader, ok := m.store.(LegacyCloudPrivateFootprintReader)
	if !ok {
		return blockedCutoverPreflight(cutoverPreflightReadFailedReason, 0), nil
	}
	privateFootprint, err := privateFootprintReader.HasLegacyCloudPrivateFootprint(ctx)
	if err != nil {
		return blockedCutoverPreflight(cutoverPreflightReadFailedReason, 0), nil
	}

	count := len(goals) + len(plans) + len(jobs) + len(connections) +
		len(deployments) + len(services) + len(recipes) + len(alerts) + len(events)
	if privateFootprint {
		// The private reader deliberately returns only existence, never an ID,
		// status, event body, command, approval, or provider detail. It is a
		// lower bound alongside the public projection counts above.
		count++
	}
	if count == 0 {
		return map[string]any{"ready": true, "blocked": false, "reason": "", "count": 0}, nil
	}
	if len(connections)+len(deployments)+len(services) > 0 {
		return blockedCutoverPreflight(cutoverPreflightActiveResourcesReason, count), nil
	}
	return blockedCutoverPreflight(cutoverPreflightDataReason, count), nil
}

func blockedCutoverPreflight(reason string, count int) map[string]any {
	return map[string]any{"ready": false, "blocked": true, "reason": reason, "count": count}
}
