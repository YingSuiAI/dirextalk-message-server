package cloud

import (
	"context"
	"testing"
	"time"
)

func TestModuleQueuesRestorePlanWithoutCallerAWSParameters(t *testing.T) {
	now := time.Date(2026, 7, 15, 20, 0, 0, 0, time.UTC)
	store := &serviceRestorePlanModuleStore{result: CreateServiceRestorePlanResult{Plan: ServiceRestorePlan{RestorePlanID: "restore-plan-module-0001", ServiceID: "service-restore-module-0001", DeploymentID: "deployment-restore-module-0001", BackupID: "backup-restore-module-0001", Status: "planning", Revision: 1}, Job: Job{JobID: "job-restore-module-0001", PlanID: "plan-module-0001", DeploymentID: "deployment-restore-module-0001", Kind: "restore_plan", Execution: "queued", Outcome: "pending", Revision: 1}, Created: true}}
	published := 0
	m := New(store, Config{OwnerMXID: func() string { return "@owner:example.com" }, Now: func() time.Time { return now }, NewID: func(kind string) string { return kind + "-generated-0001" }, Publish: func(context.Context, string, string, map[string]any) error { published++; return nil }})
	result, apiErr := m.Handlers()[actionServicesRestorePlan](t.Context(), map[string]any{"service_id": "service-restore-module-0001", "backup_id": "backup-restore-module-0001", "expected_revision": float64(3), "idempotency_key": "33333333-3333-4333-8333-333333333333"})
	if apiErr != nil || result == nil || store.request.ServiceID != "service-restore-module-0001" || store.request.BackupID != "backup-restore-module-0001" || published != 1 {
		t.Fatalf("result=%#v request=%#v published=%d error=%v", result, store.request, published, apiErr)
	}
	_, apiErr = m.Handlers()[actionServicesRestorePlan](t.Context(), map[string]any{"service_id": "service-restore-module-0001", "backup_id": "backup-restore-module-0001", "expected_revision": float64(3), "region": "us-east-1", "idempotency_key": "44444444-4444-4444-8444-444444444444"})
	if apiErr == nil {
		t.Fatal("caller-selected AWS parameters must be rejected")
	}
}

type serviceRestorePlanModuleStore struct {
	Store
	request CreateServiceRestorePlanRequest
	result  CreateServiceRestorePlanResult
}

func (s *serviceRestorePlanModuleStore) CreateCloudServiceRestorePlan(_ context.Context, r CreateServiceRestorePlanRequest) (CreateServiceRestorePlanResult, error) {
	s.request = r
	return s.result, nil
}
