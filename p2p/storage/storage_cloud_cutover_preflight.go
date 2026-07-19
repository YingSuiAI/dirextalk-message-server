package storage

import (
	"context"
	"errors"
)

// legacyCloudPrivateFootprintQuery covers every non-projection durable table
// owned by the compatibility ProductCore Cloud facade. Public projections and event
// presence are read separately by the façade; this query closes the gap for
// private bootstrap, approval, command, lifecycle, and operational facts.
//
// Keep this list synchronized with p2p/storage/storage_migrations.go. A schema
// mismatch is intentionally an error, so the caller fails closed rather than
// declaring an unknown legacy state safe to cut over.
const legacyCloudPrivateFootprintQuery = `
SELECT EXISTS (
	SELECT 1 FROM p2p_cloud_outbox
	UNION ALL SELECT 1 FROM p2p_cloud_plan_versions
	UNION ALL SELECT 1 FROM p2p_cloud_recipe_versions
	UNION ALL SELECT 1 FROM p2p_cloud_quotes
	UNION ALL SELECT 1 FROM p2p_cloud_job_steps
	UNION ALL SELECT 1 FROM p2p_cloud_projection_outbox
	UNION ALL SELECT 1 FROM p2p_cloud_connection_brokers
	UNION ALL SELECT 1 FROM p2p_cloud_broker_commands
	UNION ALL SELECT 1 FROM p2p_cloud_connection_bootstraps
	UNION ALL SELECT 1 FROM p2p_cloud_connection_registration_commands
	UNION ALL SELECT 1 FROM p2p_cloud_plan_approvals
	UNION ALL SELECT 1 FROM p2p_cloud_deployment_commands
	UNION ALL SELECT 1 FROM p2p_cloud_deployment_resources
	UNION ALL SELECT 1 FROM p2p_cloud_worker_bootstrap_observations
	UNION ALL SELECT 1 FROM p2p_cloud_deployment_observation_commands
	UNION ALL SELECT 1 FROM p2p_cloud_execution_probe_tasks
	UNION ALL SELECT 1 FROM p2p_cloud_execution_probe_commands
	UNION ALL SELECT 1 FROM p2p_cloud_recipe_execution_manifests
	UNION ALL SELECT 1 FROM p2p_cloud_recipe_execution_approvals
	UNION ALL SELECT 1 FROM p2p_cloud_recipe_install_tasks
	UNION ALL SELECT 1 FROM p2p_cloud_recipe_install_commands
	UNION ALL SELECT 1 FROM p2p_cloud_service_readiness_tasks
	UNION ALL SELECT 1 FROM p2p_cloud_service_readiness_commands
	UNION ALL SELECT 1 FROM p2p_cloud_service_destroy_approvals
	UNION ALL SELECT 1 FROM p2p_cloud_service_destroy_commands
	UNION ALL SELECT 1 FROM p2p_cloud_service_operation_approvals
	UNION ALL SELECT 1 FROM p2p_cloud_service_operation_tasks
	UNION ALL SELECT 1 FROM p2p_cloud_service_backup_approvals
	UNION ALL SELECT 1 FROM p2p_cloud_service_backups
	UNION ALL SELECT 1 FROM p2p_cloud_service_backup_commands
	UNION ALL SELECT 1 FROM p2p_cloud_service_restore_plans
	UNION ALL SELECT 1 FROM p2p_cloud_service_restore_plan_commands
	UNION ALL SELECT 1 FROM p2p_cloud_service_restore_approvals
	UNION ALL SELECT 1 FROM p2p_cloud_service_restores
	UNION ALL SELECT 1 FROM p2p_cloud_service_restore_commands
	UNION ALL SELECT 1 FROM p2p_cloud_service_management_acceptances
	UNION ALL SELECT 1 FROM p2p_cloud_recipe_artifacts
	UNION ALL SELECT 1 FROM p2p_cloud_service_secret_bootstrap_approvals
	UNION ALL SELECT 1 FROM p2p_cloud_service_secret_observe_commands
	UNION ALL SELECT 1 FROM p2p_cloud_recipe_artifact_transfers
	UNION ALL SELECT 1 FROM p2p_cloud_recipe_artifact_commands
	UNION ALL SELECT 1 FROM p2p_cloud_pairing_resume_approvals
	UNION ALL SELECT 1 FROM p2p_cloud_service_monitors
	UNION ALL SELECT 1 FROM p2p_cloud_job_cancel_approvals
	UNION ALL SELECT 1 FROM p2p_cloud_deployment_destroy_approvals
)
`

// HasLegacyCloudPrivateFootprint is a redacted existence-only read. It never
// returns the private fact itself, and it never changes legacy data.
func (s *DatabaseStore) HasLegacyCloudPrivateFootprint(ctx context.Context) (bool, error) {
	if s == nil || s.db == nil {
		return false, errors.New("cloud storage is unavailable")
	}
	var hasFootprint bool
	if err := s.db.QueryRowContext(ctx, legacyCloudPrivateFootprintQuery).Scan(&hasFootprint); err != nil {
		return false, err
	}
	return hasFootprint, nil
}
