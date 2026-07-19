package storepg

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestRecipeArtifactTransferReplaysSignedPhaseCommandsAcrossLeaseRecovery(t *testing.T) {
	ctx := context.Background()
	now, database, store, bootstrap := prepareExecutionProbeTask(t)
	manifest, _, _ := seedApprovedRecipeInstall(t, ctx, database, bootstrap, now)

	clock := now.Add(3 * time.Minute)
	lease := 0
	store.cfg.Now = func() time.Time { return clock }
	store.cfg.NewLeaseToken = func() string {
		lease++
		return "recipe-artifact-lease-" + string(rune('0'+lease))
	}
	claim, found, err := store.ClaimRecipeInstall(ctx, "recipe-artifact-worker-1", time.Minute)
	if err != nil || !found || claim.Phase != runtime.RecipeInstallPhaseIssue {
		t.Fatalf("claim=%#v found=%v err=%v", claim, found, err)
	}
	if err := store.PersistRecipeInstallCommand(ctx, claim, signedRecipeInstallCommand(t, claim, clock)); err != nil {
		t.Fatal(err)
	}
	binding := runtime.RecipeArtifactTransferBinding{
		DeploymentID: claim.DeploymentID, TaskID: claim.TaskID, ExecutionID: claim.ExecutionID,
		RecipeDigest: manifest.RecipeDigest, ArtifactDigest: manifest.ArtifactDigest, ManifestDigest: claim.ManifestDigest,
		ArchiveSHA256: strings.Repeat("e", 64), SizeBytes: 4096, MediaType: runtime.RecipeArtifactTarMediaType,
	}
	prepare, err := store.LoadOrCreateRecipeArtifactTransfer(ctx, claim, binding)
	if err != nil || prepare.Phase != runtime.RecipeArtifactTransferPhasePrepare {
		t.Fatalf("prepare=%#v err=%v", prepare, err)
	}
	prepareSigned := testSignedArtifactCommand("prepare", clock)
	if err := store.PersistRecipeArtifactTransferCommand(ctx, claim, prepare, prepareSigned); err != nil {
		t.Fatal(err)
	}

	firstRetry := clock.Add(2 * time.Minute)
	if err := store.DeferRecipeInstall(ctx, claim, "artifact_prepare_retry", firstRetry); err != nil {
		t.Fatal(err)
	}
	clock = firstRetry
	claim, found, err = store.ClaimRecipeInstall(ctx, "recipe-artifact-worker-2", time.Minute)
	if err != nil || !found {
		t.Fatalf("reclaim found=%v err=%v", found, err)
	}
	replayedPrepare, err := store.LoadOrCreateRecipeArtifactTransfer(ctx, claim, binding)
	if err != nil || replayedPrepare.Command.NodeCounter != prepare.Command.NodeCounter || replayedPrepare.Command.SignedEnvelope != prepareSigned.EnvelopeJSON {
		t.Fatalf("prepare replay=%#v err=%v", replayedPrepare.Command, err)
	}
	versionID := "3Lg_example_version-id"
	if err := store.RecordRecipeArtifactVersion(ctx, claim, replayedPrepare, versionID); err != nil {
		t.Fatal(err)
	}
	complete, err := store.LoadOrCreateRecipeArtifactTransfer(ctx, claim, binding)
	if err != nil || complete.Phase != runtime.RecipeArtifactTransferPhaseComplete || complete.VersionID != versionID || complete.Command.NodeCounter <= prepare.Command.NodeCounter || complete.Command.RequestDigest == prepare.Command.RequestDigest {
		t.Fatalf("complete=%#v err=%v", complete, err)
	}
	completeSigned := testSignedArtifactCommand("complete", clock)
	if err := store.PersistRecipeArtifactTransferCommand(ctx, claim, complete, completeSigned); err != nil {
		t.Fatal(err)
	}

	secondRetry := clock.Add(2 * time.Minute)
	if err := store.DeferRecipeInstall(ctx, claim, "artifact_complete_retry", secondRetry); err != nil {
		t.Fatal(err)
	}
	clock = secondRetry
	claim, found, err = store.ClaimRecipeInstall(ctx, "recipe-artifact-worker-3", time.Minute)
	if err != nil || !found {
		t.Fatalf("second reclaim found=%v err=%v", found, err)
	}
	replayedComplete, err := store.LoadOrCreateRecipeArtifactTransfer(ctx, claim, binding)
	if err != nil || replayedComplete.Command.NodeCounter != complete.Command.NodeCounter || replayedComplete.Command.SignedEnvelope != completeSigned.EnvelopeJSON {
		t.Fatalf("complete replay=%#v err=%v", replayedComplete.Command, err)
	}
	if err := store.CommitRecipeArtifactTransfer(ctx, claim, replayedComplete); err != nil {
		t.Fatal(err)
	}
	verified, err := store.LoadOrCreateRecipeArtifactTransfer(ctx, claim, binding)
	if err != nil || verified.Phase != runtime.RecipeArtifactTransferPhaseVerified || verified.VersionID != versionID {
		t.Fatalf("verified=%#v err=%v", verified, err)
	}
	var state, storedVersion string
	var commandCount int
	if err := database.DB().QueryRowContext(ctx, `SELECT state,version_id FROM p2p_cloud_recipe_artifact_transfers WHERE execution_id=$1`, claim.ExecutionID).Scan(&state, &storedVersion); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_recipe_artifact_commands WHERE execution_id=$1`, claim.ExecutionID).Scan(&commandCount); err != nil {
		t.Fatal(err)
	}
	if state != "verified" || storedVersion != versionID || commandCount != 2 {
		t.Fatalf("state=%q version=%q commands=%d", state, storedVersion, commandCount)
	}
}

func testSignedArtifactCommand(phase string, now time.Time) runtime.SignedRecipeArtifactTransferCommand {
	return runtime.SignedRecipeArtifactTransferCommand{
		EnvelopeJSON: `{"phase":"` + phase + `"}`, PayloadJSON: `{"phase":"` + phase + `"}`,
		PayloadSHA256: strings.Repeat("a", 64), RequestSHA256: strings.Repeat("b", 64),
		IssuedAt: now.UTC(), ExpiresAt: now.Add(time.Minute).UTC(),
	}
}
