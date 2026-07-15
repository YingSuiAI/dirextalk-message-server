package cloudorchestrator_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestCompiledRecipeArtifactIsStrictAndOrderIndependent(t *testing.T) {
	artifact := compiledRecipeArtifact()
	raw, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := cloudorchestrator.ParseCompiledRecipeArtifactV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	first, err := parsed.Digest()
	if err != nil {
		t.Fatal(err)
	}
	reordered := artifact
	reordered.OfficialSourceArtifactDigests[0], reordered.OfficialSourceArtifactDigests[1] = reordered.OfficialSourceArtifactDigests[1], reordered.OfficialSourceArtifactDigests[0]
	reordered.Actions[0], reordered.Actions[1] = reordered.Actions[1], reordered.Actions[0]
	reordered.VolumeSlots[0], reordered.VolumeSlots[1] = reordered.VolumeSlots[1], reordered.VolumeSlots[0]
	second, err := reordered.Digest()
	if err != nil || first != second {
		t.Fatalf("order-independent digest first=%s second=%s err=%v", first, second, err)
	}

	unknown := append([]byte(nil), raw[:len(raw)-1]...)
	unknown = append(unknown, []byte(`,"url":"https://attacker.invalid"}`)...)
	if _, err := cloudorchestrator.ParseCompiledRecipeArtifactV1(unknown); err == nil {
		t.Fatal("unknown URL field was accepted")
	}
	duplicate := strings.Replace(string(raw), `"recipe_id":`, `"recipe_id":"other","recipe_id":`, 1)
	if _, err := cloudorchestrator.ParseCompiledRecipeArtifactV1([]byte(duplicate)); err == nil {
		t.Fatal("duplicate top-level field was accepted")
	}
	duplicateAction := strings.Replace(string(raw), `"action_id":`, `"action_id":"other","action_id":`, 1)
	if _, err := cloudorchestrator.ParseCompiledRecipeArtifactV1([]byte(duplicateAction)); err == nil {
		t.Fatal("duplicate nested field was accepted")
	}
}

func TestCompiledRecipeArtifactRejectsExecutableAndAmbiguousCapabilities(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*cloudorchestrator.CompiledRecipeArtifactV1)
	}{
		{"secret material", func(v *cloudorchestrator.CompiledRecipeArtifactV1) { v.SecretSlots[0].Purpose = "AKIAIOSFODNN7EXAMPLE" }},
		{"path", func(v *cloudorchestrator.CompiledRecipeArtifactV1) { v.Actions[0].ActionID = "/usr/bin/install" }},
		{"url", func(v *cloudorchestrator.CompiledRecipeArtifactV1) {
			v.DataSlots[0].Purpose = "https://example.invalid/data"
		}},
		{"command", func(v *cloudorchestrator.CompiledRecipeArtifactV1) { v.VolumeSlots[0].Purpose = "prepare; rm -rf" }},
		{"reference", func(v *cloudorchestrator.CompiledRecipeArtifactV1) { v.SecretSlots[0].Purpose = "secret_ref:model" }},
		{"duplicate action", func(v *cloudorchestrator.CompiledRecipeArtifactV1) { v.Actions[1].ActionID = v.Actions[0].ActionID }},
		{"duplicate action kind", func(v *cloudorchestrator.CompiledRecipeArtifactV1) { v.Actions[1].Kind = v.Actions[0].Kind }},
		{"duplicate slot", func(v *cloudorchestrator.CompiledRecipeArtifactV1) { v.SecretSlots[0].SlotID = v.VolumeSlots[0].SlotID }},
		{"duplicate source", func(v *cloudorchestrator.CompiledRecipeArtifactV1) {
			v.OfficialSourceArtifactDigests[1] = v.OfficialSourceArtifactDigests[0]
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifact := compiledRecipeArtifact()
			test.mutate(&artifact)
			if err := artifact.Validate(); err == nil {
				t.Fatal("invalid compiled artifact was accepted")
			}
		})
	}
}

func TestCompiledRecipeArtifactDigestBindsEveryCapabilityBoundary(t *testing.T) {
	base := compiledRecipeArtifact()
	want, err := base.Digest()
	if err != nil {
		t.Fatal(err)
	}
	mutations := []func(*cloudorchestrator.CompiledRecipeArtifactV1){
		func(v *cloudorchestrator.CompiledRecipeArtifactV1) { v.RecipeRevision++ },
		func(v *cloudorchestrator.CompiledRecipeArtifactV1) { v.Requirements.MinMemoryMiB++ },
		func(v *cloudorchestrator.CompiledRecipeArtifactV1) { v.ArtifactDigest = compiledDigest("f") },
		func(v *cloudorchestrator.CompiledRecipeArtifactV1) {
			v.Actions[0].RootRequired = !v.Actions[0].RootRequired
		},
		func(v *cloudorchestrator.CompiledRecipeArtifactV1) {
			v.Actions[0].CheckpointSequence[0] = "different_checkpoint"
		},
		func(v *cloudorchestrator.CompiledRecipeArtifactV1) {
			v.VolumeSlots[0].ReadOnly = !v.VolumeSlots[0].ReadOnly
		},
		func(v *cloudorchestrator.CompiledRecipeArtifactV1) {
			v.SecretSlots[0].Delivery = cloudorchestrator.SecretDeliveryEnvironment
		},
	}
	for _, mutate := range mutations {
		changed := compiledRecipeArtifact()
		mutate(&changed)
		got, err := changed.Digest()
		if err != nil || got == want {
			t.Fatalf("changed boundary digest=%s original=%s err=%v", got, want, err)
		}
	}
}

func TestCompiledRecipeArtifactGolden(t *testing.T) {
	canonical, err := compiledRecipeArtifact().CanonicalCompiledRecipeArtifactCBOR()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canonical)
	const want = "6e37b290ea9f5af060eaa35c9b0def92f8f008f69d1b841433bb10a36e180dba"
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("compiled artifact payload digest=%s", got)
	}
}

func compiledRecipeArtifact() cloudorchestrator.CompiledRecipeArtifactV1 {
	return cloudorchestrator.CompiledRecipeArtifactV1{
		SchemaVersion: cloudorchestrator.CompiledRecipeArtifactV1Schema, RecipeID: "recipe-private-0001", RecipeDigest: compiledDigest("a"), RecipeRevision: 4,
		OfficialSourceArtifactDigests: []string{compiledDigest("c"), compiledDigest("b")}, Architecture: cloudorchestrator.ArchitectureAMD64,
		Requirements:                 cloudorchestrator.ResourceRequirementsV1{MinVCPU: 4, MinMemoryMiB: 8192, MinDiskGiB: 80, Architecture: cloudorchestrator.ArchitectureAMD64},
		WorkerResourceManifestDigest: compiledDigest("d"), ArtifactDigest: compiledDigest("e"), MediaType: "application/vnd.dirextalk.recipe", SizeBytes: 1048576,
		Actions: []cloudorchestrator.CompiledRecipeActionV1{
			{Kind: cloudorchestrator.CompiledRecipeActionRestart, ActionID: "service_restart_v1", RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: []string{"service_restarted", "health_verified"}},
			{Kind: cloudorchestrator.CompiledRecipeActionInstall, ActionID: "service_install_v1", RootRequired: true, TimeoutSeconds: 1800, CheckpointSequence: []string{"artifact_verified", "service_installed", "health_verified"}},
		},
		HealthContractDigest: compiledDigest("6"), LifecycleContractDigest: compiledDigest("7"),
		VolumeSlots: []cloudorchestrator.RecipeVolumeSlotRequirementV1{{SlotID: "logs", Purpose: "durable logs", ReadOnly: false}, {SlotID: "model", Purpose: "local model data", ReadOnly: true}},
		DataSlots:   []cloudorchestrator.RecipeDataSlotRequirementV1{{SlotID: "knowledge", Purpose: "knowledge corpus", ReadOnly: true}},
		SecretSlots: []cloudorchestrator.RecipeSecretSlotRequirementV1{{SlotID: "model_token", Purpose: "model provider access", Delivery: cloudorchestrator.SecretDeliveryFile}},
	}
}

func compiledDigest(character string) string { return "sha256:" + strings.Repeat(character, 64) }
