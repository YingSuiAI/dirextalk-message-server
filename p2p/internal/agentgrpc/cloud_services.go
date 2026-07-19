package agentgrpc

import (
	"context"
	"errors"
	"strings"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	cloudManagedServicePageSize = int32(100)
	maxCloudManagedServicePages = 16
	maxCloudManagedServices     = 1000
)

// ListCloudServices adapts the Agent's owner-scoped, paginated managed-service
// reader into ProductCore's legacy complete-array compatibility response. The
// returned projection is deliberately de-secreted: it excludes provider image,
// snapshot, and volume identifiers even though the legacy nested shape can
// represent them.
func (runner *Runner) ListCloudServices(ctx context.Context) ([]cloudmodule.Service, error) {
	if runner == nil || runner.cloud == nil {
		return nil, errors.New("agent service client is unavailable")
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()

	items := make([]cloudmodule.Service, 0)
	seenTokens := make(map[string]struct{})
	seenServices := make(map[string]struct{})
	pageToken := ""
	for page := 0; page < maxCloudManagedServicePages; page++ {
		response, err := runner.cloud.ListCloudManagedServices(callContext, &agentv1.ListCloudManagedServicesRequest{
			OwnerId: runner.ownerID, PageSize: cloudManagedServicePageSize, PageToken: pageToken,
		})
		if err != nil {
			return nil, sanitizeRPCError(callContext, err)
		}
		if response == nil || len(items)+len(response.GetServices()) > maxCloudManagedServices {
			return nil, errors.New("agent service returned an invalid cloud managed service response")
		}
		for _, remote := range response.GetServices() {
			item, mapErr := mapCloudManagedServiceRead(remote)
			if mapErr != nil {
				return nil, mapErr
			}
			if _, duplicate := seenServices[item.ServiceID]; duplicate {
				return nil, errors.New("agent service returned an invalid cloud managed service response")
			}
			seenServices[item.ServiceID] = struct{}{}
			items = append(items, item)
		}
		next := strings.TrimSpace(response.GetNextPageToken())
		if next == "" {
			return items, nil
		}
		if next == pageToken {
			return nil, errors.New("agent service returned an invalid cloud managed service cursor")
		}
		if _, duplicate := seenTokens[next]; duplicate {
			return nil, errors.New("agent service returned an invalid cloud managed service cursor")
		}
		seenTokens[next] = struct{}{}
		pageToken = next
	}
	return nil, errors.New("agent service returned too many cloud managed service pages")
}

// GetCloudService reads exactly one canonical Agent-owned managed service. A
// not-found response intentionally stays a Store-style false so the façade can
// preserve its existing ProductCore 404 envelope without local fallback.
func (runner *Runner) GetCloudService(ctx context.Context, serviceID string) (cloudmodule.Service, bool, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.Service{}, false, errors.New("agent service client is unavailable")
	}
	serviceID = strings.TrimSpace(serviceID)
	if !validManagedServiceUUID(serviceID) {
		return cloudmodule.Service{}, false, errors.New("cloud managed service id is invalid")
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.GetCloudManagedService(callContext, &agentv1.GetCloudManagedServiceRequest{
		OwnerId: runner.ownerID, ServiceId: serviceID,
	})
	if err != nil {
		if callContext.Err() == nil && status.Code(err) == codes.NotFound {
			return cloudmodule.Service{}, false, nil
		}
		return cloudmodule.Service{}, false, sanitizeRPCError(callContext, err)
	}
	if response == nil || response.GetService() == nil || response.GetService().GetServiceId() != serviceID {
		return cloudmodule.Service{}, false, errors.New("agent service returned an invalid cloud managed service response")
	}
	item, err := mapCloudManagedServiceRead(response.GetService())
	if err != nil {
		return cloudmodule.Service{}, false, err
	}
	return item, true, nil
}

func mapCloudManagedServiceRead(value *agentv1.CloudManagedCompatibilityService) (cloudmodule.Service, error) {
	invalid := func() (cloudmodule.Service, error) {
		return cloudmodule.Service{}, errors.New("agent service returned an invalid cloud managed service response")
	}
	if value == nil || !validManagedServiceUUID(value.GetServiceId()) || !validManagedServiceUUID(value.GetDeploymentId()) ||
		!validManagedServiceReadText(value.GetRecipeId()) || !validManagedServiceReadText(value.GetName()) ||
		!validManagedServiceReadText(value.GetIntegrationStatus()) || !validManagedServiceStatus(value.GetServiceStatus()) ||
		value.GetRevision() <= 0 || value.GetCreatedAtUnixMs() <= 0 || value.GetUpdatedAtUnixMs() < value.GetCreatedAtUnixMs() ||
		len(value.GetBackups()) != 1 || len(value.GetRestores()) != 1 {
		return invalid()
	}
	service := cloudmodule.Service{ServiceID: value.GetServiceId(), DeploymentID: value.GetDeploymentId(), RecipeID: value.GetRecipeId(),
		Name: value.GetName(), Status: value.GetServiceStatus(), Integration: value.GetIntegrationStatus(), Revision: value.GetRevision(),
		CreatedAt: value.GetCreatedAtUnixMs(), UpdatedAt: value.GetUpdatedAtUnixMs(),
		Backups: make([]cloudmodule.ServiceBackup, 0, len(value.GetBackups())), Restores: make([]cloudmodule.ServiceRestore, 0, len(value.GetRestores()))}
	for _, remote := range value.GetBackups() {
		backup, err := mapCloudManagedServiceReadBackup(remote, service.ServiceID, service.DeploymentID, service.UpdatedAt)
		if err != nil {
			return invalid()
		}
		service.Backups = append(service.Backups, backup)
	}
	for _, remote := range value.GetRestores() {
		restore, err := mapCloudManagedServiceReadRestore(remote, service.ServiceID, service.DeploymentID, service.UpdatedAt)
		if err != nil || restore.BackupID != service.Backups[0].BackupID {
			return invalid()
		}
		service.Restores = append(service.Restores, restore)
	}
	return service, nil
}

func mapCloudManagedServiceReadBackup(value *agentv1.CloudManagedCompatibilityBackup, serviceID, deploymentID string, serviceUpdatedAt int64) (cloudmodule.ServiceBackup, error) {
	if value == nil || !validManagedServiceUUID(value.GetBackupId()) || value.GetServiceId() != serviceID || value.GetDeploymentId() != deploymentID ||
		value.GetStatus() != "available" || value.GetRetentionPolicy() != "manual" || value.GetImageId() != "" || len(value.GetSnapshotIds()) != 0 ||
		value.GetRevision() <= 0 || value.GetCreatedAtUnixMs() <= 0 || value.GetUpdatedAtUnixMs() < value.GetCreatedAtUnixMs() || value.GetUpdatedAtUnixMs() > serviceUpdatedAt {
		return cloudmodule.ServiceBackup{}, errors.New("invalid de-secreted cloud managed service backup")
	}
	return cloudmodule.ServiceBackup{BackupID: value.GetBackupId(), ServiceID: serviceID, DeploymentID: deploymentID, Status: value.GetStatus(),
		RetentionPolicy: value.GetRetentionPolicy(), Revision: value.GetRevision(), CreatedAt: value.GetCreatedAtUnixMs(), UpdatedAt: value.GetUpdatedAtUnixMs()}, nil
}

func mapCloudManagedServiceReadRestore(value *agentv1.CloudManagedCompatibilityRestore, serviceID, deploymentID string, serviceUpdatedAt int64) (cloudmodule.ServiceRestore, error) {
	if value == nil || !validManagedServiceUUID(value.GetRestoreId()) || !validManagedServiceUUID(value.GetRestorePlanId()) || !validManagedServiceUUID(value.GetBackupId()) ||
		value.GetServiceId() != serviceID || value.GetDeploymentId() != deploymentID || value.GetStatus() != "succeeded" ||
		len(value.GetOriginalVolumeIds()) != 0 || len(value.GetReplacementVolumeIds()) != 0 || value.GetRevision() <= 0 ||
		value.GetCreatedAtUnixMs() <= 0 || value.GetUpdatedAtUnixMs() < value.GetCreatedAtUnixMs() || value.GetUpdatedAtUnixMs() > serviceUpdatedAt {
		return cloudmodule.ServiceRestore{}, errors.New("invalid de-secreted cloud managed service restore")
	}
	return cloudmodule.ServiceRestore{RestoreID: value.GetRestoreId(), RestorePlanID: value.GetRestorePlanId(), ServiceID: serviceID,
		DeploymentID: deploymentID, BackupID: value.GetBackupId(), Status: value.GetStatus(), Revision: value.GetRevision(),
		CreatedAt: value.GetCreatedAtUnixMs(), UpdatedAt: value.GetUpdatedAtUnixMs()}, nil
}

func validManagedServiceStatus(value string) bool {
	switch value {
	case "active", "degraded", "stopped":
		return true
	default:
		return false
	}
}

func validManagedServiceReadText(value string) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= 255 && !cloudmodule.ContainsSensitiveGoalMaterial(value)
}

func validManagedServiceUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}
