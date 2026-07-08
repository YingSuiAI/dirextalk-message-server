package dirextalkplugin

import (
	"encoding/json"
	"testing"
)

func TestPluginRecordJSONContracts(t *testing.T) {
	raw, err := json.Marshal(struct {
		Catalog CatalogEntry `json:"catalog"`
		Plugin  Instance     `json:"plugin"`
		Job     Job          `json:"job"`
		Secret  Secret       `json:"secret"`
	}{
		Catalog: CatalogEntry{
			ID:             "io.dirextalk.ops",
			Name:           "Ops",
			Version:        "1.0.0",
			Description:    "operations",
			Image:          "ops:latest",
			Digest:         "sha256:abc",
			MinBaseVersion: "0.1.0",
			Permissions:    []string{"docker"},
			Events:         []string{"ops.event"},
			Actions:        []string{"ops.status.get"},
		},
		Plugin: Instance{
			ID:        "io.dirextalk.ops",
			Name:      "Ops",
			Version:   "1.0.0",
			Image:     "ops:latest",
			Digest:    "sha256:abc",
			Status:    "enabled",
			Enabled:   true,
			Config:    map[string]any{"mode": "safe"},
			LastJobID: "job_1",
			CreatedAt: 123,
			UpdatedAt: 456,
		},
		Job: Job{
			JobID:     "job_1",
			PluginID:  "io.dirextalk.ops",
			Action:    "enable",
			Status:    "done",
			CreatedAt: 123,
			UpdatedAt: 456,
		},
		Secret: Secret{
			PluginID:  "io.dirextalk.ops",
			Name:      "api_key",
			Value:     "secret",
			UpdatedAt: 456,
		},
	})
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var got map[string]map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if got["catalog"]["min_base_version"] != "0.1.0" {
		t.Fatalf("expected catalog JSON contract, got %#v", got["catalog"])
	}
	if got["plugin"]["last_job_id"] != "job_1" || got["plugin"]["enabled"] != true {
		t.Fatalf("expected plugin JSON contract, got %#v", got["plugin"])
	}
	if got["job"]["job_id"] != "job_1" {
		t.Fatalf("expected job JSON contract, got %#v", got["job"])
	}
	if got["secret"]["plugin_id"] != "io.dirextalk.ops" || got["secret"]["name"] != "api_key" {
		t.Fatalf("expected secret public fields, got %#v", got["secret"])
	}
	if _, ok := got["secret"]["Value"]; ok {
		t.Fatalf("secret Value must not be serialized, got %#v", got["secret"])
	}
	if _, ok := got["secret"]["value"]; ok {
		t.Fatalf("secret value must not be serialized, got %#v", got["secret"])
	}
}
