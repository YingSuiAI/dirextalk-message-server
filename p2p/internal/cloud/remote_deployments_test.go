package cloud

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"
)

type deploymentQueryStore struct {
	Store
	items     []Deployment
	listCalls int
	getCalls  int
}

func (store *deploymentQueryStore) ListCloudDeployments(context.Context) ([]Deployment, error) {
	store.listCalls++
	return append([]Deployment(nil), store.items...), nil
}

func (store *deploymentQueryStore) GetCloudDeployment(_ context.Context, id string) (Deployment, bool, error) {
	store.getCalls++
	for _, item := range store.items {
		if item.DeploymentID == id {
			return item, true, nil
		}
	}
	return Deployment{}, false, nil
}

type recordingDeploymentReader struct {
	items     []Deployment
	err       error
	listCalls int
	getCalls  int
}

func (reader *recordingDeploymentReader) ListCloudDeployments(context.Context) ([]Deployment, error) {
	reader.listCalls++
	return append([]Deployment(nil), reader.items...), reader.err
}

func (reader *recordingDeploymentReader) GetCloudDeployment(_ context.Context, id string) (Deployment, bool, error) {
	reader.getCalls++
	if reader.err != nil {
		return Deployment{}, false, reader.err
	}
	for _, item := range reader.items {
		if item.DeploymentID == id {
			return item, true, nil
		}
	}
	return Deployment{}, false, nil
}

func TestDeploymentQueriesUseOnlyConfiguredRemoteReader(t *testing.T) {
	local := Deployment{DeploymentID: "local-deployment", PlanID: "local-plan", ConnectionID: "local-connection"}
	remote := Deployment{
		DeploymentID: "remote-deployment", PlanID: "remote-plan", ConnectionID: "remote-connection",
		Execution: "running", Outcome: "pending", Resource: "active", Revision: 7, CreatedAt: 100, UpdatedAt: 200,
	}
	store := &deploymentQueryStore{items: []Deployment{local}}
	reader := &recordingDeploymentReader{items: []Deployment{remote}}
	module := New(store, Config{DeploymentReader: reader})

	listed, actionErr := module.Handlers()[actionDeploymentsList](t.Context(), map[string]any{})
	if actionErr != nil {
		t.Fatal(actionErr)
	}
	if got := listed.(map[string]any)["deployments"]; !reflect.DeepEqual(got, []Deployment{remote}) {
		t.Fatalf("remote deployment list = %#v", got)
	}
	got, actionErr := module.Handlers()[actionDeploymentsGet](t.Context(), map[string]any{"deployment_id": remote.DeploymentID})
	if actionErr != nil || !reflect.DeepEqual(got, remote) {
		t.Fatalf("remote deployment get = %#v, err=%#v", got, actionErr)
	}
	if reader.listCalls != 1 || reader.getCalls != 1 || store.listCalls != 0 || store.getCalls != 0 {
		t.Fatalf("reader calls list/get=%d/%d, local=%d/%d", reader.listCalls, reader.getCalls, store.listCalls, store.getCalls)
	}

	// The remote seam is read-only. A lifecycle mutation must still require the
	// local mutation store and must never invoke the remote reader.
	_, actionErr = module.Handlers()[actionDeploymentsDestroyPlan](t.Context(), map[string]any{
		"deployment_id": remote.DeploymentID, "expected_revision": float64(7),
		"idempotency_key": "11111111-1111-4111-8111-111111111111",
	})
	if actionErr == nil || actionErr.Status != http.StatusServiceUnavailable || reader.listCalls != 1 || reader.getCalls != 1 {
		t.Fatalf("deployment mutation crossed remote reader: err=%#v calls=%d/%d", actionErr, reader.listCalls, reader.getCalls)
	}
}

func TestDeploymentQueriesFallBackLocallyOnlyWithoutRemoteReader(t *testing.T) {
	local := Deployment{DeploymentID: "local-deployment", PlanID: "local-plan", ConnectionID: "local-connection"}
	store := &deploymentQueryStore{items: []Deployment{local}}
	module := New(store, Config{})
	listed, actionErr := module.Handlers()[actionDeploymentsList](t.Context(), map[string]any{})
	if actionErr != nil || !reflect.DeepEqual(listed.(map[string]any)["deployments"], []Deployment{local}) {
		t.Fatalf("local deployment list = %#v, err=%#v", listed, actionErr)
	}
	got, actionErr := module.Handlers()[actionDeploymentsGet](t.Context(), map[string]any{"deployment_id": local.DeploymentID})
	if actionErr != nil || !reflect.DeepEqual(got, local) || store.listCalls != 1 || store.getCalls != 1 {
		t.Fatalf("local deployment get = %#v, err=%#v calls=%d/%d", got, actionErr, store.listCalls, store.getCalls)
	}
}

func TestRemoteDeploymentQueryErrorsPreserveProductCoreEnvelope(t *testing.T) {
	reader := &recordingDeploymentReader{err: errors.New("agent service request failed (internal)")}
	module := New(nil, Config{DeploymentReader: reader})
	if _, actionErr := module.Handlers()[actionDeploymentsList](t.Context(), map[string]any{}); actionErr == nil ||
		actionErr.Status != http.StatusInternalServerError || actionErr.Error != "internal error: agent service request failed (internal)" {
		t.Fatalf("remote list error = %#v", actionErr)
	}
	if _, actionErr := module.Handlers()[actionDeploymentsGet](t.Context(), map[string]any{"deployment_id": "missing"}); actionErr == nil ||
		actionErr.Status != http.StatusInternalServerError || actionErr.Error != "internal error: agent service request failed (internal)" {
		t.Fatalf("remote get error = %#v", actionErr)
	}

	reader.err = nil
	if _, actionErr := module.Handlers()[actionDeploymentsGet](t.Context(), map[string]any{"deployment_id": "missing"}); actionErr == nil ||
		actionErr.Status != http.StatusNotFound || actionErr.Code != "cloud_deployment_not_found" || actionErr.Error != "cloud deployment was not found" {
		t.Fatalf("remote not-found error = %#v", actionErr)
	}
}
