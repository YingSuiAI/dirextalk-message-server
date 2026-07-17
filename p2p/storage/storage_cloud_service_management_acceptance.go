package storage

import (
	"context"
	"database/sql"
	"errors"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

// GetCloudManagedAcceptanceCompatibility resolves only the legacy public
// Service-to-Deployment mapping and the owner's already registered active
// device key. Agent remains authoritative for every management-acceptance
// fact and revalidates both values before creating a challenge.
func (s *DatabaseStore) GetCloudManagedAcceptanceCompatibility(ctx context.Context, ownerMXID, serviceID string) (cloudmodule.ManagedAcceptanceCompatibility, bool, error) {
	if s == nil || s.db == nil {
		return cloudmodule.ManagedAcceptanceCompatibility{}, false, errors.New("cloud storage is unavailable")
	}
	var result cloudmodule.ManagedAcceptanceCompatibility
	err := s.db.QueryRowContext(ctx, `
		SELECT service.deployment_id, deployment.revision, bootstrap.device_approval_key_id
		FROM p2p_cloud_services service
		JOIN p2p_cloud_deployments deployment ON deployment.deployment_id=service.deployment_id
		JOIN p2p_cloud_plans plan ON plan.plan_id=deployment.plan_id
		JOIN p2p_cloud_goals goal ON goal.goal_id=plan.goal_id
		JOIN p2p_cloud_connection_bootstraps bootstrap
		  ON bootstrap.cloud_connection_id=deployment.cloud_connection_id
		 AND bootstrap.owner_mxid=goal.owner_mxid
		 AND bootstrap.status='active'
		WHERE service.service_id=$1 AND goal.owner_mxid=$2
	`, serviceID, ownerMXID).Scan(&result.DeploymentID, &result.DeploymentRevision, &result.SignerKeyID)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.ManagedAcceptanceCompatibility{}, false, nil
	}
	if err != nil {
		return cloudmodule.ManagedAcceptanceCompatibility{}, false, err
	}
	return result, true, nil
}
