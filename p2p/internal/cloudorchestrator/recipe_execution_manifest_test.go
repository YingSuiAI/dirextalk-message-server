package cloudorchestrator_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestRecipeExecutionManifestBindsSealedExecutionScope(t *testing.T) {
	manifest := validRecipeExecutionManifest(t)
	baseline, err := manifest.Digest()
	if err != nil {
		t.Fatalf("Digest() error = %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*cloudorchestrator.RecipeExecutionManifestV1)
	}{
		{name: "execution", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) { value.ExecutionID = "execution-recipe-2" }},
		{name: "deployment", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) { value.DeploymentID = "deployment-recipe-2" }},
		{name: "plan", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) { value.PlanID = "plan-2" }},
		{name: "plan hash", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) { value.PlanHash = artifactDigest('b') }},
		{name: "plan revision", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) { value.PlanRevision++ }},
		{name: "recipe", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) { value.RecipeDigest = artifactDigest('c') }},
		{name: "worker resource manifest", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) {
			value.WorkerResourceManifestDigest = artifactDigest('d')
		}},
		{name: "compiled artifact", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) { value.ArtifactDigest = artifactDigest('e') }},
		{name: "action", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) { value.ActionID = "restart-service" }},
		{name: "root boundary", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) { value.RootRequired = !value.RootRequired }},
		{name: "timeout", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) { value.TimeoutSeconds++ }},
		{name: "checkpoint sequence", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) {
			value.CheckpointSequence = []string{"artifact_verified", "install_complete", "service_ready"}
		}},
		{name: "volume slots", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) {
			value.VolumeSlots[0].VolumeRef = "volume_ref:data-b"
		}},
		{name: "data slots", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) {
			value.DataSlots[0].DataRef = "data_ref:dataset-b"
		}},
		{name: "secret slots", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) {
			value.SecretSlots[0].SecretRef = "secret_ref:model-token-b"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := cloneRecipeExecutionManifest(manifest)
			test.mutate(&changed)
			digest, err := changed.Digest()
			if err != nil {
				t.Fatalf("changed Digest() error = %v", err)
			}
			if digest == baseline {
				t.Fatalf("%s did not change the execution manifest digest", test.name)
			}
		})
	}

	first, err := manifest.CanonicalRecipeExecutionManifestCBOR()
	if err != nil {
		t.Fatalf("CanonicalRecipeExecutionManifestCBOR() error = %v", err)
	}
	second, err := manifest.CanonicalRecipeExecutionManifestCBOR()
	if err != nil {
		t.Fatalf("second CanonicalRecipeExecutionManifestCBOR() error = %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("same execution manifest did not produce stable deterministic CBOR")
	}
	if err := manifest.VerifyDigest(baseline); err != nil {
		t.Fatalf("VerifyDigest(own digest) error = %v", err)
	}
	if err := manifest.VerifyDigest(artifactDigest('f')); err == nil {
		t.Fatal("VerifyDigest() accepted a different artifact digest")
	}
}

func TestRecipeExecutionManifestRejectsUnsafeMaterialAndUnboundPlan(t *testing.T) {
	manifest := validRecipeExecutionManifest(t)
	for _, test := range []struct {
		name   string
		mutate func(*cloudorchestrator.RecipeExecutionManifestV1)
	}{
		{name: "schema", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) {
			value.SchemaVersion = cloudorchestrator.SchemaVersionV1
		}},
		{name: "command action", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) {
			value.ActionID = "curl https://worker.invalid"
		}},
		{name: "zero timeout", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) { value.TimeoutSeconds = 0 }},
		{name: "oversized timeout", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) { value.TimeoutSeconds = 24*60*60 + 1 }},
		{name: "empty checkpoints", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) { value.CheckpointSequence = nil }},
		{name: "duplicate checkpoints", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) {
			value.CheckpointSequence = []string{"artifact_verified", "artifact_verified"}
		}},
		{name: "unsafe checkpoint code", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) {
			value.CheckpointSequence = []string{"artifact-verified", "install_complete"}
		}},
		{name: "URL-shaped volume reference", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) {
			value.VolumeSlots[0].VolumeRef = "https://worker.invalid/volume"
		}},
		{name: "credential-shaped data reference", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) {
			value.DataSlots[0].DataRef = "data_ref:AKIAIOSFODNN7EXAMPLE"
		}},
		{name: "raw secret", mutate: func(value *cloudorchestrator.RecipeExecutionManifestV1) {
			value.SecretSlots[0].SecretRef = "sk-0123456789abcdef0123456789abcdef"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := cloneRecipeExecutionManifest(manifest)
			test.mutate(&changed)
			if err := changed.Validate(); err == nil {
				t.Fatal("Validate() accepted unsafe or malformed recipe execution material")
			}
		})
	}

	plan := validPlan(t, time.Date(2026, time.July, 15, 10, 0, 0, 0, time.UTC))
	expectedSecret, err := cloudorchestrator.SecretReferenceForRecipeSlot(plan.PlanID, cloudorchestrator.RecipeSecretSlotRequirementV1{SlotID: "model-token", Purpose: "model-access", Delivery: cloudorchestrator.SecretDeliveryFile})
	if err != nil {
		t.Fatal(err)
	}
	plan.SecretScope = []cloudorchestrator.SecretReferenceV1{expectedSecret}
	planHash, err := plan.Hash()
	if err != nil {
		t.Fatalf("PlanV1.Hash() error = %v", err)
	}
	manifest.PlanID = plan.PlanID
	manifest.PlanHash = planHash
	manifest.PlanRevision = plan.Revision
	manifest.RecipeDigest = plan.Recipe.Digest
	manifest.SecretSlots = []cloudorchestrator.SecretSlotV1{{SlotID: "model-token", SecretRef: expectedSecret.SecretRef}}
	if err := manifest.ValidateForPlan(plan); err != nil {
		t.Fatalf("ValidateForPlan() error = %v", err)
	}
	manifest.SecretSlots[0].SecretRef = "secret_ref:registry-token"
	if err := manifest.ValidateForPlan(plan); err == nil {
		t.Fatal("ValidateForPlan() accepted a secret outside the reviewed plan")
	}
	manifest.SecretSlots = nil
	manifest.PlanRevision++
	if err := manifest.ValidateForPlan(plan); err == nil {
		t.Fatal("ValidateForPlan() accepted a different plan revision")
	}

	assertJSONFields(t, validRecipeExecutionManifest(t), []string{
		"schema_version", "execution_id", "deployment_id", "plan_id", "plan_hash", "plan_revision", "recipe_digest",
		"worker_resource_manifest_digest", "artifact_digest", "action_id", "root_required", "timeout_seconds", "checkpoint_sequence",
		"volume_slots", "data_slots", "secret_slots",
	})
}

func TestRecipeExecutionManifestCanonicalizesUnorderedSlotsButNotCheckpointOrder(t *testing.T) {
	manifest := validRecipeExecutionManifest(t)
	baseline, err := manifest.Digest()
	if err != nil {
		t.Fatalf("Digest() error = %v", err)
	}

	reorderedSlots := manifest
	reorderedSlots.VolumeSlots = []cloudorchestrator.VolumeSlotV1{
		{SlotID: "models", VolumeRef: "volume_ref:models-a", ReadOnly: true},
		{SlotID: "data", VolumeRef: "volume_ref:data-a", ReadOnly: false},
	}
	reorderedSlots.DataSlots = []cloudorchestrator.DataSlotV1{
		{SlotID: "dataset", DataRef: "data_ref:dataset-a", ReadOnly: true},
		{SlotID: "bootstrap", DataRef: "data_ref:bootstrap-a", ReadOnly: true},
	}
	reorderedSlots.SecretSlots = []cloudorchestrator.SecretSlotV1{
		manifest.SecretSlots[1],
		manifest.SecretSlots[0],
	}
	digest, err := reorderedSlots.Digest()
	if err != nil {
		t.Fatalf("reordered slots Digest() error = %v", err)
	}
	if digest != baseline {
		t.Fatal("unordered slot declarations changed the manifest digest")
	}

	reorderedCheckpoints := manifest
	reorderedCheckpoints.CheckpointSequence = []string{"install_complete", "artifact_verified", "health_verified"}
	digest, err = reorderedCheckpoints.Digest()
	if err != nil {
		t.Fatalf("reordered checkpoint Digest() error = %v", err)
	}
	if digest == baseline {
		t.Fatal("checkpoint order did not change the manifest digest")
	}
}

func validRecipeExecutionManifest(t *testing.T) cloudorchestrator.RecipeExecutionManifestV1 {
	t.Helper()
	plan := validPlan(t, time.Date(2026, time.July, 15, 10, 0, 0, 0, time.UTC))
	planHash, err := plan.Hash()
	if err != nil {
		t.Fatalf("PlanV1.Hash() error = %v", err)
	}
	secretScope, err := cloudorchestrator.SecretScopeForRecipe(plan.PlanID, []cloudorchestrator.RecipeSecretSlotRequirementV1{
		{SlotID: "registry-token", Purpose: "source-access", Delivery: cloudorchestrator.SecretDeliveryFile},
		{SlotID: "model-token", Purpose: "model-access", Delivery: cloudorchestrator.SecretDeliveryFile},
	})
	if err != nil {
		t.Fatal(err)
	}
	return cloudorchestrator.RecipeExecutionManifestV1{
		SchemaVersion:                cloudorchestrator.RecipeExecutionManifestV1Schema,
		ExecutionID:                  "execution-recipe-1",
		DeploymentID:                 "deployment-recipe-1",
		PlanID:                       plan.PlanID,
		PlanHash:                     planHash,
		PlanRevision:                 plan.Revision,
		RecipeDigest:                 plan.Recipe.Digest,
		WorkerResourceManifestDigest: artifactDigest('a'),
		ArtifactDigest:               artifactDigest('b'),
		ActionID:                     "install-service",
		RootRequired:                 true,
		TimeoutSeconds:               1800,
		CheckpointSequence:           []string{"artifact_verified", "install_complete", "health_verified"},
		VolumeSlots: []cloudorchestrator.VolumeSlotV1{
			{SlotID: "data", VolumeRef: "volume_ref:data-a", ReadOnly: false},
			{SlotID: "models", VolumeRef: "volume_ref:models-a", ReadOnly: true},
		},
		DataSlots: []cloudorchestrator.DataSlotV1{
			{SlotID: "bootstrap", DataRef: "data_ref:bootstrap-a", ReadOnly: true},
			{SlotID: "dataset", DataRef: "data_ref:dataset-a", ReadOnly: true},
		},
		SecretSlots: []cloudorchestrator.SecretSlotV1{
			{SlotID: "registry-token", SecretRef: secretScope[0].SecretRef},
			{SlotID: "model-token", SecretRef: secretScope[1].SecretRef},
		},
	}
}

func cloneRecipeExecutionManifest(manifest cloudorchestrator.RecipeExecutionManifestV1) cloudorchestrator.RecipeExecutionManifestV1 {
	clone := manifest
	clone.CheckpointSequence = append([]string(nil), manifest.CheckpointSequence...)
	clone.VolumeSlots = append([]cloudorchestrator.VolumeSlotV1(nil), manifest.VolumeSlots...)
	clone.DataSlots = append([]cloudorchestrator.DataSlotV1(nil), manifest.DataSlots...)
	clone.SecretSlots = append([]cloudorchestrator.SecretSlotV1(nil), manifest.SecretSlots...)
	return clone
}

func TestRecipeExecutionManifestDoesNotTreatPlainArtifactDigestAsPlanHash(t *testing.T) {
	manifest := validRecipeExecutionManifest(t)
	manifest.PlanHash = artifactDigest('a')
	if err := manifest.Validate(); err != nil {
		t.Fatalf("Validate() rejected a syntactically valid binding hash: %v", err)
	}
	if err := manifest.VerifyDigest(manifest.PlanHash); err == nil || !strings.Contains(err.Error(), "canonical artifact") {
		t.Fatalf("VerifyDigest(plan hash) error = %v, want canonical artifact mismatch", err)
	}
}
