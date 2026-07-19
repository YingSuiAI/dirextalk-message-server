package p2p

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type productCloudServiceReader struct {
	items     []CloudService
	listCalls int
	getCalls  int
}

func (reader *productCloudServiceReader) ListCloudServices(context.Context) ([]CloudService, error) {
	reader.listCalls++
	return append([]CloudService(nil), reader.items...), nil
}

func (reader *productCloudServiceReader) GetCloudService(_ context.Context, id string) (CloudService, bool, error) {
	reader.getCalls++
	for _, item := range reader.items {
		if item.ServiceID == id {
			return item, true, nil
		}
	}
	return CloudService{}, false, nil
}

func TestProductCoreCloudServiceQueriesPreserveAgentProjectionAndOwnerAuth(t *testing.T) {
	item := CloudService{
		ServiceID: "11111111-1111-4111-8111-111111111111", DeploymentID: "22222222-2222-4222-8222-222222222222", RecipeID: "recipe-managed",
		Name: "Agent managed", Status: "running", Integration: "integrated", Revision: 7, CreatedAt: 1000, UpdatedAt: 2000,
	}
	reader := &productCloudServiceReader{items: []CloudService{item}}
	service := NewService(Config{ServerName: "example.com", CloudServiceReader: reader})
	router := newP2PTestRouter(service)

	unauthorized := jsonRequest(t, "/_p2p/query", map[string]any{"action": "cloud.services.list", "params": map[string]any{}})
	unauthorizedRecorder := httptest.NewRecorder()
	router.ServeHTTP(unauthorizedRecorder, unauthorized)
	if unauthorizedRecorder.Code != http.StatusUnauthorized || reader.listCalls != 0 {
		t.Fatalf("unauthorized remote service query status=%d calls=%d", unauthorizedRecorder.Code, reader.listCalls)
	}

	list := jsonRequest(t, "/_p2p/query", map[string]any{"action": "cloud.services.list", "params": map[string]any{}})
	list.Header.Set("Authorization", "Bearer "+service.AccessToken())
	listRecorder := httptest.NewRecorder()
	router.ServeHTTP(listRecorder, list)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("cloud.services.list = %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	listResult := decodeJSONMap(t, listRecorder.Body.String())
	items, ok := listResult["services"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("service list response = %#v", listResult)
	}
	assertProductCloudService(t, items[0].(map[string]any), item)

	get := jsonRequest(t, "/_p2p/query", map[string]any{"action": "cloud.services.get", "params": map[string]any{"service_id": item.ServiceID}})
	get.Header.Set("Authorization", "Bearer "+service.AccessToken())
	getRecorder := httptest.NewRecorder()
	router.ServeHTTP(getRecorder, get)
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("cloud.services.get = %d body=%s", getRecorder.Code, getRecorder.Body.String())
	}
	for _, forbidden := range []string{"image_id", "snapshot_id", "snapshot_ids", "original_volume_id", "replacement_volume_id", "secret_ref"} {
		if strings.Contains(getRecorder.Body.String(), forbidden) {
			t.Fatalf("managed service exposed forbidden field %q: %s", forbidden, getRecorder.Body.String())
		}
	}
	assertProductCloudService(t, decodeJSONMap(t, getRecorder.Body.String()), item)
	if reader.listCalls != 1 || reader.getCalls != 1 {
		t.Fatalf("remote reader calls list/get=%d/%d", reader.listCalls, reader.getCalls)
	}
}

func TestProductCoreCloudBootstrapUsesAgentManagedServiceProjection(t *testing.T) {
	item := CloudService{
		ServiceID: "11111111-1111-4111-8111-111111111111", DeploymentID: "22222222-2222-4222-8222-222222222222", RecipeID: "recipe-managed",
		Name: "Agent managed", Status: "active", Integration: "not_requested", Revision: 7, CreatedAt: 1000, UpdatedAt: 2000,
	}
	reader := &productCloudServiceReader{items: []CloudService{item}}
	service := NewService(Config{ServerName: "example.com", CloudServiceReader: reader})
	router := newP2PTestRouter(service)

	request := jsonRequest(t, "/_p2p/query", map[string]any{"action": "cloud.bootstrap", "params": map[string]any{}})
	request.Header.Set("Authorization", "Bearer "+service.AccessToken())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("cloud.bootstrap = %d body=%s", recorder.Code, recorder.Body.String())
	}
	bootstrap := decodeJSONMap(t, recorder.Body.String())
	items, ok := bootstrap["services"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("bootstrap services = %#v", bootstrap["services"])
	}
	assertProductCloudService(t, items[0].(map[string]any), item)
	if reader.listCalls != 1 {
		t.Fatalf("bootstrap Agent service reads=%d", reader.listCalls)
	}
}

func assertProductCloudService(t *testing.T, got map[string]any, want CloudService) {
	t.Helper()
	if got["service_id"] != want.ServiceID || got["deployment_id"] != want.DeploymentID || got["recipe_id"] != want.RecipeID ||
		got["name"] != want.Name || got["service_status"] != want.Status || got["integration_status"] != want.Integration ||
		got["revision"] != float64(want.Revision) || got["created_at"] != float64(want.CreatedAt) || got["updated_at"] != float64(want.UpdatedAt) || len(got) != 9 {
		t.Fatalf("service response = %#v", got)
	}
}
