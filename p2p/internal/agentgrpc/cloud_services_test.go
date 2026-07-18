package agentgrpc

import (
	"strings"
	"testing"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testManagedServiceID1 = "99999999-9999-4999-8999-999999999991"
	testManagedServiceID2 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2"
	testManagedDeployment = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	testManagedBackup     = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	testManagedRestore    = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	testManagedPlan       = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
)

func TestCloudManagedServiceReaderTraversesPagesWithBoundOwnerAndDeSecretedProjection(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	first := cloudManagedService(testManagedServiceID1, 7)
	second := cloudManagedService(testManagedServiceID2, 8)
	server.cloud.listManagedServices = func(request *agentv1.ListCloudManagedServicesRequest) (*agentv1.ListCloudManagedServicesResponse, error) {
		if request.GetOwnerId() != "owner-from-config" || request.GetPageSize() != cloudManagedServicePageSize {
			t.Fatalf("managed list request = %#v", request)
		}
		switch request.GetPageToken() {
		case "":
			return &agentv1.ListCloudManagedServicesResponse{Services: []*agentv1.CloudManagedCompatibilityService{first}, NextPageToken: "page-2"}, nil
		case "page-2":
			return &agentv1.ListCloudManagedServicesResponse{Services: []*agentv1.CloudManagedCompatibilityService{second}}, nil
		default:
			return nil, status.Error(codes.InvalidArgument, "unexpected cursor")
		}
	}
	server.cloud.getManagedService = func(request *agentv1.GetCloudManagedServiceRequest) (*agentv1.GetCloudManagedServiceResponse, error) {
		if request.GetOwnerId() != "owner-from-config" || request.GetServiceId() != second.GetServiceId() {
			t.Fatalf("managed get request = %#v", request)
		}
		return &agentv1.GetCloudManagedServiceResponse{Service: second}, nil
	}

	items, err := runner.ListCloudServices(t.Context())
	if err != nil || len(items) != 2 || items[0].ServiceID != first.GetServiceId() || items[1].ServiceID != second.GetServiceId() {
		t.Fatalf("managed service list=%#v err=%v", items, err)
	}
	if len(items[0].Backups) != 1 || items[0].Backups[0].ImageID != "" || len(items[0].Backups[0].SnapshotIDs) != 0 ||
		len(items[0].Restores) != 1 || len(items[0].Restores[0].OriginalVolumeIDs) != 0 || len(items[0].Restores[0].ReplacementVolumeIDs) != 0 {
		t.Fatalf("managed service leaked provider fields: %#v", items[0])
	}
	got, found, err := runner.GetCloudService(t.Context(), second.GetServiceId())
	if err != nil || !found || got.ServiceID != second.GetServiceId() || got.Revision != second.GetRevision() {
		t.Fatalf("managed get=%#v found=%v err=%v", got, found, err)
	}
	if len(server.cloud.listManagedRequests) != 2 || len(server.cloud.getManagedRequests) != 1 || len(server.cloud.auth) != 3 {
		t.Fatalf("managed requests list/get/auth=%d/%d/%d", len(server.cloud.listManagedRequests), len(server.cloud.getManagedRequests), len(server.cloud.auth))
	}
	for _, authorization := range server.cloud.auth {
		if authorization != authorizationScheme+" "+testServiceKey {
			t.Fatalf("authorization=%q", authorization)
		}
	}
}

func TestCloudManagedServiceReaderRejectsProviderFactsAndNeverLeaksRemoteErrors(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	invalid := cloudManagedService(testManagedServiceID1, 7)
	invalid.Backups[0].ImageId = "ami-0123456789abcdef0"
	server.cloud.listManagedServices = func(*agentv1.ListCloudManagedServicesRequest) (*agentv1.ListCloudManagedServicesResponse, error) {
		return &agentv1.ListCloudManagedServicesResponse{Services: []*agentv1.CloudManagedCompatibilityService{invalid}}, nil
	}
	if _, err := runner.ListCloudServices(t.Context()); err == nil || !strings.Contains(err.Error(), "invalid cloud managed service response") || strings.Contains(err.Error(), "ami-") {
		t.Fatalf("provider fact list error=%v", err)
	}
	server.cloud.getManagedService = func(*agentv1.GetCloudManagedServiceRequest) (*agentv1.GetCloudManagedServiceResponse, error) {
		return nil, status.Error(codes.Internal, "secret-canary")
	}
	if _, _, err := runner.GetCloudService(t.Context(), testManagedServiceID1); err == nil || err.Error() != "agent service request failed (internal)" || strings.Contains(err.Error(), "secret-canary") {
		t.Fatalf("remote error=%v", err)
	}
}

func TestCloudManagedServiceReaderRejectsRestoreOutsideServiceBackup(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	invalid := cloudManagedService(testManagedServiceID1, 7)
	invalid.Restores[0].BackupId = "ffffffff-ffff-4fff-8fff-ffffffffffff"
	server.cloud.listManagedServices = func(*agentv1.ListCloudManagedServicesRequest) (*agentv1.ListCloudManagedServicesResponse, error) {
		return &agentv1.ListCloudManagedServicesResponse{Services: []*agentv1.CloudManagedCompatibilityService{invalid}}, nil
	}
	if _, err := runner.ListCloudServices(t.Context()); err == nil || err.Error() != "agent service returned an invalid cloud managed service response" {
		t.Fatalf("disconnected restore backup error=%v", err)
	}
}

func TestCloudManagedServiceReaderRejectsCursorCyclesAndBounds(t *testing.T) {
	t.Run("self cursor cycle", func(t *testing.T) {
		server := startRuntimeServer(t)
		runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
		server.cloud.listManagedServices = func(*agentv1.ListCloudManagedServicesRequest) (*agentv1.ListCloudManagedServicesResponse, error) {
			return &agentv1.ListCloudManagedServicesResponse{NextPageToken: "repeat"}, nil
		}
		if _, err := runner.ListCloudServices(t.Context()); err == nil || err.Error() != "agent service returned an invalid cloud managed service cursor" {
			t.Fatalf("self cursor cycle error=%v", err)
		}
	})

	t.Run("non-adjacent cursor cycle", func(t *testing.T) {
		server := startRuntimeServer(t)
		runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
		server.cloud.listManagedServices = func(request *agentv1.ListCloudManagedServicesRequest) (*agentv1.ListCloudManagedServicesResponse, error) {
			switch request.GetPageToken() {
			case "":
				return &agentv1.ListCloudManagedServicesResponse{NextPageToken: "first"}, nil
			case "first":
				return &agentv1.ListCloudManagedServicesResponse{NextPageToken: "second"}, nil
			case "second":
				return &agentv1.ListCloudManagedServicesResponse{NextPageToken: "first"}, nil
			default:
				return nil, status.Error(codes.InvalidArgument, "unexpected cursor")
			}
		}
		if _, err := runner.ListCloudServices(t.Context()); err == nil || err.Error() != "agent service returned an invalid cloud managed service cursor" {
			t.Fatalf("cursor cycle error=%v", err)
		}
	})

	t.Run("page and item limits", func(t *testing.T) {
		server := startRuntimeServer(t)
		runner := newTestRunner(t, server, Config{UnaryTimeout: 2 * time.Second})
		server.cloud.listManagedServices = func(request *agentv1.ListCloudManagedServicesRequest) (*agentv1.ListCloudManagedServicesResponse, error) {
			return &agentv1.ListCloudManagedServicesResponse{NextPageToken: "page-" + request.GetPageToken()}, nil
		}
		if _, err := runner.ListCloudServices(t.Context()); err == nil || err.Error() != "agent service returned too many cloud managed service pages" {
			t.Fatalf("page limit error=%v", err)
		}

		tooMany := make([]*agentv1.CloudManagedCompatibilityService, maxCloudManagedServices+1)
		server.cloud.listManagedServices = func(*agentv1.ListCloudManagedServicesRequest) (*agentv1.ListCloudManagedServicesResponse, error) {
			return &agentv1.ListCloudManagedServicesResponse{Services: tooMany}, nil
		}
		if _, err := runner.ListCloudServices(t.Context()); err == nil || err.Error() != "agent service returned an invalid cloud managed service response" {
			t.Fatalf("item limit error=%v", err)
		}
	})
}

func cloudManagedService(serviceID string, revision int64) *agentv1.CloudManagedCompatibilityService {
	now := time.Date(2026, 7, 18, 8, 0, 0, 0, time.UTC).UnixMilli()
	return &agentv1.CloudManagedCompatibilityService{ServiceId: serviceID, DeploymentId: testManagedDeployment, RecipeId: "managed-recipe-v1",
		Name: "Managed service", ServiceStatus: "active", IntegrationStatus: "not_requested", Revision: revision, CreatedAtUnixMs: now, UpdatedAtUnixMs: now + 1,
		Backups: []*agentv1.CloudManagedCompatibilityBackup{{BackupId: testManagedBackup, ServiceId: serviceID, DeploymentId: testManagedDeployment,
			Status: "available", RetentionPolicy: "manual", Revision: 3, CreatedAtUnixMs: now, UpdatedAtUnixMs: now}},
		Restores: []*agentv1.CloudManagedCompatibilityRestore{{RestoreId: testManagedRestore, RestorePlanId: testManagedPlan, ServiceId: serviceID,
			DeploymentId: testManagedDeployment, BackupId: testManagedBackup, Status: "succeeded", Revision: 4, CreatedAtUnixMs: now, UpdatedAtUnixMs: now}},
	}
}
