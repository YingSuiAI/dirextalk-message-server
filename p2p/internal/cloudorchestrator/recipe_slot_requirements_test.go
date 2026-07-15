package cloudorchestrator_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestRecipeV1LegacyDigestRemainsStableWithoutSlotRequirements(t *testing.T) {
	recipe := slotTestRecipe(time.Date(2026, time.July, 14, 10, 0, 0, 0, time.UTC))
	if recipe.VolumeSlots != nil || recipe.DataSlots != nil || recipe.SecretSlots != nil {
		t.Fatal("legacy Recipe fixture unexpectedly has slot requirements")
	}
	digest, err := recipe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	const want = "sha256:d8961b9df00ebefe4c5cd708d3136ea8fbe75c6b66e2dee3cfab2379ece97104"
	if digest != want {
		t.Fatalf("legacy Recipe digest=%s", digest)
	}
	raw, err := json.Marshal(recipe)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"volume_slots", "data_slots", "secret_slots"} {
		if strings.Contains(string(raw), field) {
			t.Fatalf("legacy Recipe JSON unexpectedly contains %s: %s", field, raw)
		}
	}
}

func TestRecipeV1SlotRequirementsAreStrictAndDigestBound(t *testing.T) {
	recipe := slotTestRecipe(time.Date(2026, time.July, 14, 10, 0, 0, 0, time.UTC))
	recipe.VolumeSlots = []cloudorchestrator.RecipeVolumeSlotRequirementV1{{SlotID: "models", Purpose: "local model data", ReadOnly: true}, {SlotID: "logs", Purpose: "durable logs", ReadOnly: false}}
	recipe.DataSlots = []cloudorchestrator.RecipeDataSlotRequirementV1{{SlotID: "knowledge", Purpose: "knowledge corpus", ReadOnly: true}}
	recipe.SecretSlots = []cloudorchestrator.RecipeSecretSlotRequirementV1{{SlotID: "model_token", Purpose: "model provider access", Delivery: cloudorchestrator.SecretDeliveryFile}}
	base, err := recipe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	reordered := recipe
	reordered.VolumeSlots = append([]cloudorchestrator.RecipeVolumeSlotRequirementV1(nil), recipe.VolumeSlots...)
	reordered.VolumeSlots[0], reordered.VolumeSlots[1] = reordered.VolumeSlots[1], reordered.VolumeSlots[0]
	if digest, err := reordered.Digest(); err != nil || digest != base {
		t.Fatalf("reordered slot digest=%s base=%s err=%v", digest, base, err)
	}

	tamper := []func(*cloudorchestrator.RecipeV1){
		func(v *cloudorchestrator.RecipeV1) { v.VolumeSlots[0].ReadOnly = false },
		func(v *cloudorchestrator.RecipeV1) { v.DataSlots[0].Purpose = "updated knowledge corpus" },
		func(v *cloudorchestrator.RecipeV1) {
			v.SecretSlots[0].Delivery = cloudorchestrator.SecretDeliveryEnvironment
		},
	}
	for _, mutate := range tamper {
		changed := recipe
		changed.VolumeSlots = append([]cloudorchestrator.RecipeVolumeSlotRequirementV1(nil), recipe.VolumeSlots...)
		changed.DataSlots = append([]cloudorchestrator.RecipeDataSlotRequirementV1(nil), recipe.DataSlots...)
		changed.SecretSlots = append([]cloudorchestrator.RecipeSecretSlotRequirementV1(nil), recipe.SecretSlots...)
		mutate(&changed)
		got, err := changed.Digest()
		if err != nil || got == base {
			t.Fatalf("slot tamper digest=%s base=%s err=%v", got, base, err)
		}
	}

	invalid := []struct {
		name   string
		mutate func(*cloudorchestrator.RecipeV1)
	}{
		{"cross-kind duplicate", func(v *cloudorchestrator.RecipeV1) { v.SecretSlots[0].SlotID = v.DataSlots[0].SlotID }},
		{"reference", func(v *cloudorchestrator.RecipeV1) { v.DataSlots[0].Purpose = "data_ref:private" }},
		{"path", func(v *cloudorchestrator.RecipeV1) { v.VolumeSlots[0].Purpose = "/srv/models" }},
		{"environment", func(v *cloudorchestrator.RecipeV1) { v.SecretSlots[0].Purpose = "MODEL_TOKEN" }},
		{"value", func(v *cloudorchestrator.RecipeV1) { v.SecretSlots[0].Purpose = "token=value" }},
		{"command", func(v *cloudorchestrator.RecipeV1) { v.VolumeSlots[0].Purpose = "curl payload" }},
		{"url", func(v *cloudorchestrator.RecipeV1) { v.DataSlots[0].Purpose = "https://example.invalid/data" }},
		{"delivery", func(v *cloudorchestrator.RecipeV1) { v.SecretSlots[0].Delivery = "prompt" }},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			changed := recipe
			changed.VolumeSlots = append([]cloudorchestrator.RecipeVolumeSlotRequirementV1(nil), recipe.VolumeSlots...)
			changed.DataSlots = append([]cloudorchestrator.RecipeDataSlotRequirementV1(nil), recipe.DataSlots...)
			changed.SecretSlots = append([]cloudorchestrator.RecipeSecretSlotRequirementV1(nil), recipe.SecretSlots...)
			test.mutate(&changed)
			if err := changed.Validate(); err == nil || !strings.Contains(err.Error(), "slot") {
				t.Fatalf("Validate()=%v", err)
			}
		})
	}
}

func slotTestRecipe(now time.Time) cloudorchestrator.RecipeV1 {
	return cloudorchestrator.RecipeV1{
		SchemaVersion: cloudorchestrator.SchemaVersionV1, RecipeID: "recipe-slot-contract-1", Name: "Private knowledge node", Maturity: cloudorchestrator.RecipeExperimental,
		Sources:      []cloudorchestrator.RecipeSourceV1{{URL: "https://github.com/example/knowledge-node", Version: "v1.2.3", Commit: "0123456789abcdef0123456789abcdef01234567", ArtifactDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", License: "Apache-2.0", RetrievedAt: now, Official: true}},
		Requirements: cloudorchestrator.ResourceRequirementsV1{MinVCPU: 4, MinMemoryMiB: 8192, MinDiskGiB: 80, Architecture: cloudorchestrator.ArchitectureAMD64},
		Install:      cloudorchestrator.InstallContractV1{RootRequired: true, TimeoutSeconds: 1800, CheckpointNames: []string{"image_pulled", "service_started"}, Steps: []cloudorchestrator.InstallStepV1{{ID: "install-service", Summary: "Install the official image", TimeoutSeconds: 900}}},
		Health:       cloudorchestrator.HealthContractV1{Liveness: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/healthz"}, Readiness: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/readyz"}, Semantic: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeCommand, Target: "verify-index"}},
		Lifecycle:    cloudorchestrator.LifecycleContractV1{Start: "start-service", Stop: "stop-service", Restart: "restart-service", Upgrade: "upgrade-service", Rollback: "rollback-service", Backup: "backup-data", Restore: "restore-data", Destroy: "destroy-service"},
	}
}
