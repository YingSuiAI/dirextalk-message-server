package cloudorchestrator_test

import (
	"testing"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestManagedCapabilityIsDerivedFromTheTrustedCompiledArtifact(t *testing.T) {
	artifact := compiledRecipeArtifact()
	artifact.Actions = append(artifact.Actions,
		cloudorchestrator.CompiledRecipeActionV1{Kind: cloudorchestrator.CompiledRecipeActionStart, ActionID: "service_start_v1", RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: []string{"service_started", "health_verified"}},
		cloudorchestrator.CompiledRecipeActionV1{Kind: cloudorchestrator.CompiledRecipeActionStop, ActionID: "service_stop_v1", RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: []string{"service_stopped"}},
	)
	installedDigest := compiledDigest("9")
	capability, err := cloudorchestrator.ManagedCapabilityFromCompiledArtifact(artifact, installedDigest)
	if err != nil {
		t.Fatal(err)
	}
	if capability.ArtifactDigest != artifact.ArtifactDigest || capability.InstalledManifestDigest != installedDigest || capability.Start.ActionID != "service_start_v1" || capability.Stop.ActionID != "service_stop_v1" || capability.Restart.ActionID != "service_restart_v1" {
		t.Fatalf("capability = %#v", capability)
	}

	withoutStop := artifact
	withoutStop.Actions = append([]cloudorchestrator.CompiledRecipeActionV1(nil), artifact.Actions[:len(artifact.Actions)-1]...)
	if _, err := cloudorchestrator.ManagedCapabilityFromCompiledArtifact(withoutStop, installedDigest); err == nil {
		t.Fatal("managed capability accepted an artifact without the stop action")
	}
}
