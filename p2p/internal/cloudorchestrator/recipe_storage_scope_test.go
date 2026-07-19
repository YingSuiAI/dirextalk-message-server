package cloudorchestrator_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestRecipeStorageReferencesAreDeterministicStableAndDistinct(t *testing.T) {
	planID := "plan-" + strings.Repeat("a", 100)
	volumes := []cloudorchestrator.RecipeVolumeSlotRequirementV1{
		{SlotID: "models", Purpose: "model-cache", ReadOnly: true},
		{SlotID: "state", Purpose: "service-state", ReadOnly: false},
	}
	data := []cloudorchestrator.RecipeDataSlotRequirementV1{
		{SlotID: "dataset", Purpose: "knowledge-dataset", ReadOnly: true},
		{SlotID: "index", Purpose: "knowledge-index", ReadOnly: false},
	}
	firstVolumes, err := cloudorchestrator.VolumeSlotsForRecipe(planID, volumes)
	if err != nil {
		t.Fatal(err)
	}
	firstData, err := cloudorchestrator.DataSlotsForRecipe(planID, data)
	if err != nil {
		t.Fatal(err)
	}
	reorderedVolumes, err := cloudorchestrator.VolumeSlotsForRecipe(planID, []cloudorchestrator.RecipeVolumeSlotRequirementV1{volumes[1], volumes[0]})
	if err != nil {
		t.Fatal(err)
	}
	reorderedData, err := cloudorchestrator.DataSlotsForRecipe(planID, []cloudorchestrator.RecipeDataSlotRequirementV1{data[1], data[0]})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(firstVolumes, reorderedVolumes) || !reflect.DeepEqual(firstData, reorderedData) {
		t.Fatal("reordering Recipe requirements changed deterministic storage bindings")
	}
	if !firstVolumes[0].ReadOnly || firstVolumes[1].ReadOnly || !firstData[0].ReadOnly || firstData[1].ReadOnly {
		t.Fatalf("read_only was not preserved: volumes=%#v data=%#v", firstVolumes, firstData)
	}
	seen := map[string]struct{}{}
	for _, ref := range []string{firstVolumes[0].VolumeRef, firstVolumes[1].VolumeRef, firstData[0].DataRef, firstData[1].DataRef} {
		if _, duplicate := seen[ref]; duplicate {
			t.Fatalf("storage refs are not unique across slots: %q", ref)
		}
		seen[ref] = struct{}{}
	}
}

func TestRecipeExecutionManifestStorageBindingsFailClosed(t *testing.T) {
	now := time.Date(2026, time.July, 15, 10, 0, 0, 0, time.UTC)
	recipe := validRecipe(t, now)
	recipe.VolumeSlots = []cloudorchestrator.RecipeVolumeSlotRequirementV1{
		{SlotID: "state", Purpose: "service-state", ReadOnly: false},
		{SlotID: "models", Purpose: "model-cache", ReadOnly: true},
	}
	recipe.DataSlots = []cloudorchestrator.RecipeDataSlotRequirementV1{{SlotID: "dataset", Purpose: "knowledge-dataset", ReadOnly: true}}
	plan := validPlan(t, now)
	recipeDigest, err := recipe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	plan.Recipe.Digest = recipeDigest
	planHash, err := plan.Hash()
	if err != nil {
		t.Fatal(err)
	}
	manifest := validRecipeExecutionManifest(t)
	manifest.PlanID, manifest.PlanRevision, manifest.PlanHash, manifest.RecipeDigest = plan.PlanID, plan.Revision, planHash, recipeDigest
	manifest.VolumeSlots, err = cloudorchestrator.VolumeSlotsForRecipe(plan.PlanID, recipe.VolumeSlots)
	if err != nil {
		t.Fatal(err)
	}
	manifest.DataSlots, err = cloudorchestrator.DataSlotsForRecipe(plan.PlanID, recipe.DataSlots)
	if err != nil {
		t.Fatal(err)
	}
	if err := manifest.ValidateForPlanAndRecipe(plan, recipe); err != nil {
		t.Fatalf("ValidateForPlanAndRecipe() error = %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*cloudorchestrator.RecipeExecutionManifestV1)
	}{
		{name: "volume ref", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) {
			value.VolumeSlots[0].VolumeRef = "volume_ref:plan/tampered"
		}},
		{name: "volume readonly", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) {
			value.VolumeSlots[0].ReadOnly = !value.VolumeSlots[0].ReadOnly
		}},
		{name: "missing data", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) { value.DataSlots = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := cloneRecipeExecutionManifest(manifest)
			test.mutate(&changed)
			if err := changed.ValidateForPlanAndRecipe(plan, recipe); err == nil {
				t.Fatal("tampered storage binding was accepted")
			}
		})
	}
}
