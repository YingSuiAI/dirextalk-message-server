package storepg

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestPairingResumeApprovalRequeuesExistingDeploymentExactlyOnce(t *testing.T) {
	now, database, store, observationClaim := prepareWorkerBootstrapObservationClaim(t)
	signedObservation := signedWorkerBootstrapObservationCommand(t, observationClaim, now)
	if err := store.MarkWorkerBootstrapObservationStarted(context.Background(), observationClaim); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistWorkerBootstrapObservationCommand(context.Background(), observationClaim, signedObservation); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitWorkerBootstrapObservation(context.Background(), observationClaim, validWorkerBootstrapObservation(observationClaim, now, 2)); err != nil {
		t.Fatal(err)
	}
	var recipeJSON string
	if err := database.DB().QueryRow(`SELECT display_json FROM p2p_cloud_recipe_versions ORDER BY created_at DESC LIMIT 1`).Scan(&recipeJSON); err != nil {
		t.Fatal(err)
	}
	var recipe cloudcontracts.RecipeV1
	if err := json.Unmarshal([]byte(recipeJSON), &recipe); err != nil {
		t.Fatal(err)
	}
	recipeDigest, _ := recipe.Digest()
	artifact := manifestRegistrationArtifact(t, recipe, recipeDigest)
	if result, err := store.RegisterTrustedCloudRecipeArtifact(context.Background(), cloudmodule.RegisterTrustedRecipeArtifactRequest{Artifact: artifact, RegisteredAt: now.UnixMilli()}); err != nil || !result.Created {
		t.Fatalf("register artifact created=%v err=%v", result.Created, err)
	}
	if created, err := store.RegisterNextTrustedRecipeExecutionManifest(context.Background()); err != nil || !created {
		t.Fatalf("register manifest created=%v err=%v", created, err)
	}
	privateKey, publicSPKI := provisionDeviceKey(t)
	if _, err := database.DB().Exec(`UPDATE p2p_cloud_connection_bootstraps SET device_approval_public_key_spki_base64=$1 WHERE cloud_connection_id=$2`, publicSPKI, observationClaim.ConnectionID); err != nil {
		t.Fatal(err)
	}
	var planID string
	var previousRevision int64
	if err := database.DB().QueryRow(`SELECT plan_id,revision FROM p2p_cloud_deployments WHERE deployment_id=$1`, observationClaim.DeploymentID).Scan(&planID, &previousRevision); err != nil {
		t.Fatal(err)
	}
	resumeAt := now.Add(time.Minute).UnixMilli()
	if _, err := database.DB().Exec(`UPDATE p2p_cloud_deployments SET execution_status='waiting_user_pairing',revision=revision+1,updated_at=$1 WHERE deployment_id=$2`, resumeAt, observationClaim.DeploymentID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB().Exec(`UPDATE p2p_cloud_recipe_execution_manifests SET status='approved',revision=revision+1,updated_at=$1 WHERE deployment_id=$2`, resumeAt, observationClaim.DeploymentID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB().Exec(`INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at)VALUES('job-pairing-resume',$1,$2,'install','waiting_user','pending','waiting_user_pairing','',3,$3,$3)`, planID, observationClaim.DeploymentID, resumeAt); err != nil {
		t.Fatal(err)
	}
	prepare := cloudmodule.PreparePairingResumeRequest{OwnerMXID: "@owner:example.com", DeploymentID: observationClaim.DeploymentID, ExpectedRevision: previousRevision + 1, IdempotencyHash: "pairing-prepare-idem", RequestDigest: "pairing-prepare-request", ApprovalID: "approval-pairing-resume", ChallengeID: "challenge-pairing-resume", CreatedAt: resumeAt, ExpiresAt: resumeAt + int64((5 * time.Minute).Milliseconds())}
	prepared, err := database.PrepareCloudPairingResume(context.Background(), prepare)
	if err != nil || !prepared.Created || prepared.Confirmation.Deployment.Execution != "waiting_user_pairing" {
		t.Fatalf("prepare=%#v err=%v", prepared, err)
	}
	replayedPrepare, err := database.PrepareCloudPairingResume(context.Background(), prepare)
	if err != nil || replayedPrepare.Created || replayedPrepare.Confirmation.Approval.ChallengeID != prepared.Confirmation.Approval.ChallengeID {
		t.Fatalf("prepare replay=%#v err=%v", replayedPrepare, err)
	}
	conflictingPrepare := prepare
	conflictingPrepare.ExpectedRevision++
	if _, err := database.PrepareCloudPairingResume(context.Background(), conflictingPrepare); !errors.Is(err, cloudmodule.ErrIdempotencyConflict) {
		t.Fatalf("prepare conflict err=%v", err)
	}
	signed, err := prepared.Confirmation.Approval.Sign(privateKey, time.UnixMilli(resumeAt).Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	approveAt := resumeAt + int64(time.Minute.Milliseconds())
	approve := cloudmodule.ApprovePairingResumeRequest{OwnerMXID: prepare.OwnerMXID, DeploymentID: prepare.DeploymentID, ExpectedRevision: prepare.ExpectedRevision, IdempotencyHash: "pairing-approve-idem", RequestDigest: "pairing-approve-request", Approval: signed, OutboxID: "outbox-pairing-resume", DeploymentEventID: "event-pairing-deployment", JobEventID: "event-pairing-job", CreatedAt: approveAt}
	approved, err := database.ApproveCloudPairingResume(context.Background(), approve)
	if err != nil || !approved.Created || approved.Deployment.Execution != "queued" || approved.Job.Execution != "queued" || approved.Job.Checkpoint != "pairing_resume_queued" {
		t.Fatalf("approve=%#v err=%v", approved, err)
	}
	replayed, err := database.ApproveCloudPairingResume(context.Background(), approve)
	if err != nil || replayed.Created || replayed.Deployment.DeploymentID != approved.Deployment.DeploymentID || replayed.Job.JobID != approved.Job.JobID {
		t.Fatalf("approve replay=%#v err=%v", replayed, err)
	}
	conflictingApprove := approve
	conflictingApprove.ExpectedRevision++
	if _, err := database.ApproveCloudPairingResume(context.Background(), conflictingApprove); !errors.Is(err, cloudmodule.ErrIdempotencyConflict) {
		t.Fatalf("approve conflict err=%v", err)
	}
	var deployments, intents int
	var payload string
	if err := database.DB().QueryRow(`SELECT COUNT(*) FROM p2p_cloud_deployments WHERE plan_id=$1`, planID).Scan(&deployments); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT COUNT(*),MAX(payload_json) FROM p2p_cloud_outbox WHERE kind=$1`, cloudmodule.OutboxKindDeploymentPairingResumeRequested).Scan(&intents, &payload); err != nil {
		t.Fatal(err)
	}
	if deployments != 1 || intents != 1 || strings.Contains(payload, "http") || strings.Contains(payload, "device_code") || strings.Contains(payload, "qr") {
		t.Fatalf("deployments=%d intents=%d payload=%q", deployments, intents, payload)
	}
	notWaiting := prepare
	notWaiting.IdempotencyHash, notWaiting.RequestDigest, notWaiting.ApprovalID, notWaiting.ChallengeID = "pairing-after-resume-idem", "pairing-after-resume-request", "approval-pairing-after-resume", "challenge-pairing-after-resume"
	notWaiting.ExpectedRevision = approved.Deployment.Revision
	if _, err := database.PrepareCloudPairingResume(context.Background(), notWaiting); !errors.Is(err, cloudmodule.ErrPairingResumeConflict) {
		t.Fatalf("non-waiting deployment pairing resume err=%v", err)
	}
}

func TestStoreRegistersRecipeExecutionManifestFromApprovedObservedFactsOnce(t *testing.T) {
	now, database, store, observationClaim := prepareWorkerBootstrapObservationClaim(t)
	signedObservation := signedWorkerBootstrapObservationCommand(t, observationClaim, now)
	if err := store.MarkWorkerBootstrapObservationStarted(context.Background(), observationClaim); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistWorkerBootstrapObservationCommand(context.Background(), observationClaim, signedObservation); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitWorkerBootstrapObservation(context.Background(), observationClaim, validWorkerBootstrapObservation(observationClaim, now, 2)); err != nil {
		t.Fatal(err)
	}

	var recipeJSON string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT version.display_json FROM p2p_cloud_recipe_versions version
		JOIN p2p_cloud_recipes recipe ON recipe.recipe_id=version.recipe_id AND recipe.revision=version.revision
		LIMIT 1
	`).Scan(&recipeJSON); err != nil {
		t.Fatal(err)
	}
	var recipe cloudcontracts.RecipeV1
	if err := json.Unmarshal([]byte(recipeJSON), &recipe); err != nil {
		t.Fatal(err)
	}
	recipeDigest, err := recipe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	artifact := manifestRegistrationArtifact(t, recipe, recipeDigest)
	registered, err := store.RegisterTrustedCloudRecipeArtifact(context.Background(), cloudmodule.RegisterTrustedRecipeArtifactRequest{
		Artifact: artifact, RegisteredAt: now.UnixMilli(),
	})
	if err != nil || !registered.Created {
		t.Fatalf("artifact registration created=%v err=%v", registered.Created, err)
	}

	// An expired Worker lease is not a manifest source even when every other
	// durable binding is still valid.
	if _, err := database.DB().ExecContext(context.Background(), `UPDATE p2p_cloud_worker_bootstrap_observations SET worker_lease_expires_at=$1 WHERE deployment_id=$2`, now.UnixMilli(), observationClaim.DeploymentID); err != nil {
		t.Fatal(err)
	}
	if created, err := store.RegisterNextTrustedRecipeExecutionManifest(context.Background()); err != nil || created {
		t.Fatalf("expired Worker created=%v err=%v", created, err)
	}
	if _, err := database.DB().ExecContext(context.Background(), `UPDATE p2p_cloud_worker_bootstrap_observations SET worker_lease_expires_at=$1 WHERE deployment_id=$2`, now.Add(4*time.Minute).UnixMilli(), observationClaim.DeploymentID); err != nil {
		t.Fatal(err)
	}

	created, err := store.RegisterNextTrustedRecipeExecutionManifest(context.Background())
	if err != nil || !created {
		t.Fatalf("manifest registration created=%v err=%v", created, err)
	}
	var executionID, planID, planHash, connectionID, manifestDigest, manifestJSON, status string
	var planRevision, revision int64
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT execution_id,plan_id,plan_revision,plan_hash,cloud_connection_id,manifest_digest,manifest_json,status,revision
		FROM p2p_cloud_recipe_execution_manifests WHERE deployment_id=$1
	`, observationClaim.DeploymentID).Scan(&executionID, &planID, &planRevision, &planHash, &connectionID, &manifestDigest, &manifestJSON, &status, &revision); err != nil {
		t.Fatal(err)
	}
	var manifest cloudcontracts.RecipeExecutionManifestV1
	if err := json.Unmarshal([]byte(manifestJSON), &manifest); err != nil {
		t.Fatal(err)
	}
	computedDigest, err := manifest.Digest()
	if err != nil || manifest.ExecutionID != executionID || manifest.PlanID != planID || int64(manifest.PlanRevision) != planRevision ||
		manifest.PlanHash != planHash || connectionID != observationClaim.ConnectionID || computedDigest != manifestDigest ||
		manifest.DeploymentID != observationClaim.DeploymentID || manifest.ArtifactDigest != artifact.ArtifactDigest ||
		manifest.WorkerResourceManifestDigest != artifact.WorkerResourceManifestDigest || manifest.ActionID != artifact.Actions[0].ActionID ||
		status != "registered" || revision != 1 {
		t.Fatalf("registered manifest=%#v connection=%q digest=%q status=%q revision=%d err=%v", manifest, connectionID, manifestDigest, status, revision, err)
	}
	if replayed, err := store.RegisterNextTrustedRecipeExecutionManifest(context.Background()); err != nil || replayed {
		t.Fatalf("registration replay created=%v err=%v", replayed, err)
	}
}

func TestRecipeManifestRegistrationDerivesExactLogicalStorageBindings(t *testing.T) {
	now, database, _, observationClaim := prepareWorkerBootstrapObservationClaim(t)
	var planJSON string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT display_json FROM p2p_cloud_plan_versions
		WHERE plan_id=$1 ORDER BY revision DESC LIMIT 1
	`, observationClaim.PlanID).Scan(&planJSON); err != nil {
		t.Fatal(err)
	}
	var plan cloudcontracts.PlanV1
	if err := json.Unmarshal([]byte(planJSON), &plan); err != nil {
		t.Fatal(err)
	}
	recipe := testResearchOutput(t, now).Recipe
	recipe.VolumeSlots = []cloudcontracts.RecipeVolumeSlotRequirementV1{
		{SlotID: "state", Purpose: "service-state", ReadOnly: false},
		{SlotID: "models", Purpose: "model-cache", ReadOnly: true},
	}
	recipe.DataSlots = []cloudcontracts.RecipeDataSlotRequirementV1{{SlotID: "dataset", Purpose: "knowledge-dataset", ReadOnly: true}}
	recipeDigest, err := recipe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	plan.Status = cloudcontracts.PlanApproved
	plan.Revision++
	plan.Recipe = cloudcontracts.RecipeBindingV1{RecipeID: recipe.RecipeID, Digest: recipeDigest, Maturity: recipe.Maturity}
	approvedHash, err := plan.Hash()
	if err != nil {
		t.Fatal(err)
	}
	artifact := manifestRegistrationArtifact(t, recipe, recipeDigest)
	artifact.VolumeSlots = append([]cloudcontracts.RecipeVolumeSlotRequirementV1(nil), recipe.VolumeSlots...)
	artifact.DataSlots = append([]cloudcontracts.RecipeDataSlotRequirementV1(nil), recipe.DataSlots...)
	artifact.SecretSlots = append([]cloudcontracts.RecipeSecretSlotRequirementV1(nil), recipe.SecretSlots...)
	candidate := recipeManifestRegistrationCandidate{
		DeploymentID: observationClaim.DeploymentID,
		PlanID:       plan.PlanID,
		PlanRevision: plan.Revision,
	}
	manifest, err := deriveRecipeExecutionManifest(candidate, plan, recipe, approvedHash, artifact, trustedStoreDigest("6"))
	if err != nil {
		t.Fatalf("deriveRecipeExecutionManifest() error = %v", err)
	}
	wantVolumes, _ := cloudcontracts.VolumeSlotsForRecipe(plan.PlanID, recipe.VolumeSlots)
	wantData, _ := cloudcontracts.DataSlotsForRecipe(plan.PlanID, recipe.DataSlots)
	if len(manifest.VolumeSlots) != 2 || len(manifest.DataSlots) != 1 ||
		manifest.VolumeSlots[0] != wantVolumes[0] || manifest.VolumeSlots[1] != wantVolumes[1] || manifest.DataSlots[0] != wantData[0] {
		t.Fatalf("logical storage bindings = volumes:%#v data:%#v", manifest.VolumeSlots, manifest.DataSlots)
	}

	tampered := artifact
	tampered.VolumeSlots = append([]cloudcontracts.RecipeVolumeSlotRequirementV1(nil), artifact.VolumeSlots...)
	tampered.VolumeSlots[0].ReadOnly = !tampered.VolumeSlots[0].ReadOnly
	if _, err := deriveRecipeExecutionManifest(candidate, plan, recipe, approvedHash, tampered, trustedStoreDigest("6")); err == nil {
		t.Fatal("artifact storage requirement drift was accepted")
	}
}

func manifestRegistrationArtifact(t *testing.T, recipe cloudcontracts.RecipeV1, recipeDigest string) cloudcontracts.CompiledRecipeArtifactV1 {
	t.Helper()
	healthDigest, err := cloudcontracts.HealthContractDigestV1(recipe.Health)
	if err != nil {
		t.Fatal(err)
	}
	lifecycleDigest, err := cloudcontracts.LifecycleContractDigestV1(recipe.Lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	artifact := cloudcontracts.CompiledRecipeArtifactV1{
		SchemaVersion: cloudcontracts.CompiledRecipeArtifactV1Schema, RecipeID: recipe.RecipeID, RecipeDigest: recipeDigest, RecipeRevision: 1,
		OfficialSourceArtifactDigests: []string{recipe.Sources[0].ArtifactDigest}, Architecture: recipe.Requirements.Architecture, Requirements: recipe.Requirements,
		WorkerResourceManifestDigest: trustedStoreDigest("c"), ArtifactDigest: trustedStoreDigest("2"), ImageSource: cloudcontracts.OCIImageSourceReferenceV1("ghcr.io/dirextalk/manifest-service@" + trustedStoreDigest("2")), MediaType: "application/vnd.oci.image.manifest.v1+json", SizeBytes: 1048576,
		Actions:              []cloudcontracts.CompiledRecipeActionV1{{Kind: cloudcontracts.CompiledRecipeActionInstall, ActionID: "service_install_v1", RootRequired: recipe.Install.RootRequired, TimeoutSeconds: recipe.Install.TimeoutSeconds, CheckpointSequence: cloudcontracts.OCIServiceInstallCheckpointSequenceV1()}},
		SemanticReadiness:    cloudcontracts.OCIServiceLoopbackProbeV1{Scheme: cloudcontracts.OCIServiceProbeHTTP, Port: 8080, Path: "/semantic", ExpectedStatus: 200, BodySHA256: trustedStoreDigest("5")},
		HealthContractDigest: healthDigest, LifecycleContractDigest: lifecycleDigest,
		VolumeSlots: []cloudcontracts.RecipeVolumeSlotRequirementV1{}, DataSlots: []cloudcontracts.RecipeDataSlotRequirementV1{}, SecretSlots: []cloudcontracts.RecipeSecretSlotRequirementV1{},
	}
	if err := artifact.Validate(); err != nil {
		t.Fatalf("manifest artifact fixture: %v", err)
	}
	return artifact
}
