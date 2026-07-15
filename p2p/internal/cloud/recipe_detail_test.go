package cloud

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestRecipesGetReturnsOwnerScopedDesecretedDetail(t *testing.T) {
	retrievedAt := time.Date(2026, 7, 15, 8, 0, 0, 0, time.UTC)
	store := &recipeDetailModuleStore{detail: RecipeDetail{
		RecipeID: "recipe-detail-1", Name: "Knowledge node", Version: "v2", Maturity: "managed", Revision: 4,
		Digest:          "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Requirements:    RecipeDetailRequirements{MinVCPU: 4, MinMemoryMiB: 8192, MinDiskGiB: 80, Architecture: cloudcontracts.ArchitectureAMD64},
		OfficialSources: []RecipeOfficialSource{{Version: "v2.0.0", Commit: "0123456789abcdef0123456789abcdef01234567", ArtifactDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", License: "Apache-2.0", RetrievedAt: retrievedAt}},
		Health:          RecipeDetailHealth{Liveness: RecipeDetailProbe{Kind: cloudcontracts.ProbeHTTP, Target: "/healthz"}, Readiness: RecipeDetailProbe{Kind: cloudcontracts.ProbeHTTP, Target: "/readyz"}, Semantic: RecipeDetailProbe{Kind: cloudcontracts.ProbeCommand, Target: "verify-index"}},
		Lifecycle:       RecipeDetailLifecycle{Start: "start", Stop: "stop", Restart: "restart", Upgrade: "upgrade", Rollback: "rollback", Backup: "backup", Restore: "restore", Destroy: "destroy"},
		VolumeSlots:     []RecipeDetailVolumeSlot{{SlotID: "knowledge_volume", Purpose: "persistent index", ReadOnly: false}},
		DataSlots:       []RecipeDetailDataSlot{{SlotID: "documents", Purpose: "source documents", ReadOnly: true}},
		SecretSlots:     []RecipeDetailSecretSlot{{SlotID: "model_token", Purpose: "model access", Delivery: cloudcontracts.SecretDeliveryEnvironment}},
	}, found: true}
	module := New(store, Config{OwnerMXID: func() string { return "@owner:example.com" }})

	result, apiErr := module.Handlers()[actionRecipesGet](t.Context(), map[string]any{"recipe_id": "recipe-detail-1"})
	if apiErr != nil || store.owner != "@owner:example.com" || store.recipeID != "recipe-detail-1" {
		t.Fatalf("result=%#v owner=%q recipe=%q err=%v", result, store.owner, store.recipeID, apiErr)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	payload := string(encoded)
	for _, required := range []string{"\"requirements\"", "\"official_sources\"", "\"health\"", "\"lifecycle\"", "\"volume_slots\"", "\"data_slots\"", "\"secret_slots\""} {
		if !strings.Contains(payload, required) {
			t.Fatalf("recipe detail omitted %s: %s", required, payload)
		}
	}
	for _, forbidden := range []string{"secret_ref", "provider_object", "object_key", "owner_mxid", "cloud_connection_id", "artifact_body", "command_text", "source_url"} {
		if strings.Contains(payload, forbidden) {
			t.Fatalf("recipe detail leaked %q: %s", forbidden, payload)
		}
	}
}

type recipeDetailModuleStore struct {
	Store
	detail   RecipeDetail
	found    bool
	owner    string
	recipeID string
}

func (s *recipeDetailModuleStore) GetCloudRecipeDetail(_ context.Context, owner, recipeID string) (RecipeDetail, bool, error) {
	s.owner, s.recipeID = owner, recipeID
	return s.detail, s.found, nil
}
