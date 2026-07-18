package cloud

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"
)

const remoteManagedServiceID = "11111111-1111-4111-8111-111111111111"

type serviceQueryStore struct {
	Store
	items     []Service
	err       error
	listCalls int
	getCalls  int
}

func (store *serviceQueryStore) ListCloudServices(context.Context) ([]Service, error) {
	store.listCalls++
	return append([]Service(nil), store.items...), store.err
}

func (store *serviceQueryStore) GetCloudService(_ context.Context, id string) (Service, bool, error) {
	store.getCalls++
	if store.err != nil {
		return Service{}, false, store.err
	}
	for _, item := range store.items {
		if item.ServiceID == id {
			return item, true, nil
		}
	}
	return Service{}, false, nil
}

type recordingServiceReader struct {
	items     []Service
	err       error
	listCalls int
	getCalls  int
}

func (reader *recordingServiceReader) ListCloudServices(context.Context) ([]Service, error) {
	reader.listCalls++
	return append([]Service(nil), reader.items...), reader.err
}

func (reader *recordingServiceReader) GetCloudService(_ context.Context, id string) (Service, bool, error) {
	reader.getCalls++
	if reader.err != nil {
		return Service{}, false, reader.err
	}
	for _, item := range reader.items {
		if item.ServiceID == id {
			return item, true, nil
		}
	}
	return Service{}, false, nil
}

func TestServiceQueriesUseRemoteReaderForCanonicalIDsAndKeepLegacyCompatibility(t *testing.T) {
	remote := Service{ServiceID: remoteManagedServiceID, DeploymentID: "22222222-2222-4222-8222-222222222222", RecipeID: "recipe-managed",
		Name: "Agent managed", Status: "running", Integration: "integrated", Revision: 7, CreatedAt: 100, UpdatedAt: 200}
	staleLocal := remote
	staleLocal.Name = "stale ProductCore copy"
	legacy := Service{ServiceID: "legacy-service", DeploymentID: "legacy-deployment", Name: "Legacy", Status: "running", Integration: "integrated", Revision: 2, CreatedAt: 10, UpdatedAt: 20}
	zeroUUIDLegacy := Service{ServiceID: "00000000-0000-0000-0000-000000000000", DeploymentID: "legacy-deployment", Name: "Zero UUID legacy", Status: "running", Integration: "integrated", Revision: 3, CreatedAt: 11, UpdatedAt: 21}
	store := &serviceQueryStore{items: []Service{staleLocal, legacy, zeroUUIDLegacy}}
	reader := &recordingServiceReader{items: []Service{remote}}
	module := New(store, Config{ServiceReader: reader})

	listed, actionErr := module.Handlers()[actionServicesList](t.Context(), map[string]any{})
	if actionErr != nil {
		t.Fatal(actionErr)
	}
	if got := listed.(map[string]any)["services"]; !reflect.DeepEqual(got, []Service{remote, legacy, zeroUUIDLegacy}) {
		t.Fatalf("service list = %#v", got)
	}
	got, actionErr := module.Handlers()[actionServicesGet](t.Context(), map[string]any{"service_id": remote.ServiceID})
	if actionErr != nil || !reflect.DeepEqual(got, remote) {
		t.Fatalf("remote service get = %#v, err=%#v", got, actionErr)
	}
	got, actionErr = module.Handlers()[actionServicesGet](t.Context(), map[string]any{"service_id": legacy.ServiceID})
	if actionErr != nil || !reflect.DeepEqual(got, legacy) {
		t.Fatalf("legacy service get = %#v, err=%#v", got, actionErr)
	}
	if reader.listCalls != 1 || reader.getCalls != 1 || store.listCalls != 1 || store.getCalls != 1 {
		t.Fatalf("reader list/get=%d/%d local list/get=%d/%d", reader.listCalls, reader.getCalls, store.listCalls, store.getCalls)
	}
}

func TestCloudBootstrapSharesAgentManagedServiceProjection(t *testing.T) {
	remote := Service{ServiceID: remoteManagedServiceID, DeploymentID: "22222222-2222-4222-8222-222222222222", RecipeID: "recipe-managed",
		Name: "Agent managed", Status: "active", Integration: "not_requested", Revision: 7, CreatedAt: 100, UpdatedAt: 200}
	staleLocal := remote
	staleLocal.Name = "stale ProductCore copy"
	legacy := Service{ServiceID: "legacy-service", DeploymentID: "legacy-deployment", Name: "Legacy", Status: "running", Integration: "integrated", Revision: 2, CreatedAt: 10, UpdatedAt: 20}
	reader := &recordingServiceReader{items: []Service{remote}}
	module := New(dialogueStatusStore{services: []Service{staleLocal, legacy}}, Config{ServiceReader: reader})

	result, actionErr := module.bootstrap(t.Context(), map[string]any{})
	if actionErr != nil {
		t.Fatalf("cloud.bootstrap: %#v", actionErr)
	}
	services, ok := result.(map[string]any)["services"].([]Service)
	if !ok || !reflect.DeepEqual(services, []Service{remote, legacy}) {
		t.Fatalf("bootstrap services = %#v", result)
	}
	if reader.listCalls != 1 {
		t.Fatalf("bootstrap Agent service reads=%d", reader.listCalls)
	}
}

func TestRemoteCanonicalServiceNeverFallsBackToLocalState(t *testing.T) {
	local := Service{ServiceID: remoteManagedServiceID, DeploymentID: "22222222-2222-4222-8222-222222222222", Name: "stale", Status: "running", Integration: "integrated", Revision: 1, CreatedAt: 1, UpdatedAt: 1}
	store := &serviceQueryStore{items: []Service{local}}
	reader := &recordingServiceReader{}
	module := New(store, Config{ServiceReader: reader})

	if _, actionErr := module.Handlers()[actionServicesGet](t.Context(), map[string]any{"service_id": remoteManagedServiceID}); actionErr == nil ||
		actionErr.Status != http.StatusNotFound || actionErr.Code != "cloud_service_not_found" || store.getCalls != 0 || reader.getCalls != 1 {
		t.Fatalf("canonical fallback = err=%#v local=%d remote=%d", actionErr, store.getCalls, reader.getCalls)
	}
	reader.err = errors.New("agent service request failed (internal)")
	if _, actionErr := module.Handlers()[actionServicesList](t.Context(), map[string]any{}); actionErr == nil ||
		actionErr.Status != http.StatusInternalServerError || actionErr.Error != "internal error: agent service request failed (internal)" || store.listCalls != 0 {
		t.Fatalf("remote list failure = %#v local=%d", actionErr, store.listCalls)
	}
}

func TestServiceQueriesFallBackFullyLocallyWithoutRemoteReader(t *testing.T) {
	local := Service{ServiceID: remoteManagedServiceID, DeploymentID: "22222222-2222-4222-8222-222222222222", Name: "local", Status: "running", Integration: "integrated", Revision: 1, CreatedAt: 1, UpdatedAt: 1}
	store := &serviceQueryStore{items: []Service{local}}
	module := New(store, Config{})
	listed, actionErr := module.Handlers()[actionServicesList](t.Context(), map[string]any{})
	if actionErr != nil || !reflect.DeepEqual(listed.(map[string]any)["services"], []Service{local}) {
		t.Fatalf("local service list = %#v, err=%#v", listed, actionErr)
	}
	got, actionErr := module.Handlers()[actionServicesGet](t.Context(), map[string]any{"service_id": local.ServiceID})
	if actionErr != nil || !reflect.DeepEqual(got, local) || store.listCalls != 1 || store.getCalls != 1 {
		t.Fatalf("local service get = %#v, err=%#v calls=%d/%d", got, actionErr, store.listCalls, store.getCalls)
	}
}
