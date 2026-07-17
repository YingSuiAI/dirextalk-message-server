package p2p

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type productCloudDeploymentReader struct {
	items     []CloudDeployment
	listCalls int
	getCalls  int
}

func (reader *productCloudDeploymentReader) ListCloudDeployments(context.Context) ([]CloudDeployment, error) {
	reader.listCalls++
	return append([]CloudDeployment(nil), reader.items...), nil
}

func (reader *productCloudDeploymentReader) GetCloudDeployment(_ context.Context, id string) (CloudDeployment, bool, error) {
	reader.getCalls++
	for _, item := range reader.items {
		if item.DeploymentID == id {
			return item, true, nil
		}
	}
	return CloudDeployment{}, false, nil
}

func TestProductCoreCloudDeploymentQueriesPreserveRemoteResponseAndOwnerAuth(t *testing.T) {
	item := CloudDeployment{
		DeploymentID: "deployment-remote-1", PlanID: "plan-remote-1", ConnectionID: "connection-remote-1",
		Execution: "running", Outcome: "pending", Resource: "active", Revision: 7, CreatedAt: 1000, UpdatedAt: 2000,
		Health: &CloudDeploymentHealthSummary{
			Status: "degraded", Revision: 4, ObservedAt: 1500, NextDueAt: 2500, ProbeCount: 2,
			ProbeCounts: []CloudDeploymentHealthProbeCount{{Kind: "liveness", Count: 1}, {Kind: "readiness", Count: 1}},
			ExternalEvidenceDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			EvidenceType:           "independent_external",
		},
	}
	reader := &productCloudDeploymentReader{items: []CloudDeployment{item}}
	service := NewService(Config{ServerName: "example.com", CloudDeploymentReader: reader})
	router := newP2PTestRouter(service)

	unauthorized := jsonRequest(t, "/_p2p/query", map[string]any{"action": "cloud.deployments.list", "params": map[string]any{}})
	unauthorizedRecorder := httptest.NewRecorder()
	router.ServeHTTP(unauthorizedRecorder, unauthorized)
	if unauthorizedRecorder.Code != http.StatusUnauthorized || reader.listCalls != 0 {
		t.Fatalf("unauthorized remote deployment query status=%d calls=%d", unauthorizedRecorder.Code, reader.listCalls)
	}

	list := jsonRequest(t, "/_p2p/query", map[string]any{"action": "cloud.deployments.list", "params": map[string]any{}})
	list.Header.Set("Authorization", "Bearer "+service.AccessToken())
	listRecorder := httptest.NewRecorder()
	router.ServeHTTP(listRecorder, list)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("cloud.deployments.list = %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	listResult := decodeJSONMap(t, listRecorder.Body.String())
	items, ok := listResult["deployments"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("deployment list response = %#v", listResult)
	}
	assertProductCloudDeployment(t, items[0].(map[string]any), item)

	get := jsonRequest(t, "/_p2p/query", map[string]any{"action": "cloud.deployments.get", "params": map[string]any{"deployment_id": item.DeploymentID}})
	get.Header.Set("Authorization", "Bearer "+service.AccessToken())
	getRecorder := httptest.NewRecorder()
	router.ServeHTTP(getRecorder, get)
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("cloud.deployments.get = %d body=%s", getRecorder.Code, getRecorder.Body.String())
	}
	for _, forbidden := range []string{"probe_url", "response_body", "headers", "pairing", "secret_ref"} {
		if strings.Contains(getRecorder.Body.String(), forbidden) {
			t.Fatalf("cloud deployment health exposed forbidden field %q: %s", forbidden, getRecorder.Body.String())
		}
	}
	assertProductCloudDeployment(t, decodeJSONMap(t, getRecorder.Body.String()), item)
	if reader.listCalls != 1 || reader.getCalls != 1 {
		t.Fatalf("remote reader calls list/get=%d/%d", reader.listCalls, reader.getCalls)
	}
}

func assertProductCloudDeployment(t *testing.T, got map[string]any, want CloudDeployment) {
	t.Helper()
	if got["deployment_id"] != want.DeploymentID || got["plan_id"] != want.PlanID ||
		got["cloud_connection_id"] != want.ConnectionID || got["execution_status"] != want.Execution ||
		got["outcome_status"] != want.Outcome || got["resource_status"] != want.Resource ||
		got["revision"] != float64(want.Revision) || got["created_at"] != float64(want.CreatedAt) ||
		got["updated_at"] != float64(want.UpdatedAt) || len(got) != 10 {
		t.Fatalf("deployment response = %#v", got)
	}
	health, ok := got["health"].(map[string]any)
	if !ok || want.Health == nil || health["status"] != want.Health.Status || health["revision"] != float64(want.Health.Revision) ||
		health["observed_at"] != float64(want.Health.ObservedAt) || health["next_due_at"] != float64(want.Health.NextDueAt) ||
		health["probe_count"] != float64(want.Health.ProbeCount) || health["external_evidence_digest"] != want.Health.ExternalEvidenceDigest ||
		health["evidence_type"] != want.Health.EvidenceType || len(health["probe_counts"].([]any)) != len(want.Health.ProbeCounts) || len(health) != 8 {
		t.Fatalf("deployment health response = %#v", health)
	}
}
