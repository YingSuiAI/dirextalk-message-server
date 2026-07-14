package cloudorchestrator_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestRecipeExecutionApprovalBindsTheReviewedExecutionScope(t *testing.T) {
	now := time.Date(2026, time.July, 15, 10, 0, 0, 0, time.UTC)
	plan, manifest := approvedExecutionScope(t, now)
	target := cloudorchestrator.RecipeExecutionTargetV1{
		DeploymentID:       manifest.DeploymentID,
		DeploymentRevision: 3,
	}
	approval, err := cloudorchestrator.NewRecipeExecutionApprovalV1(
		plan,
		manifest,
		target,
		"recipe-execution-approval-1",
		"recipe-execution-challenge-1",
		"owner-device-1",
		now,
		now.Add(5*time.Minute),
	)
	if err != nil {
		t.Fatalf("NewRecipeExecutionApprovalV1() error = %v", err)
	}
	if err := approval.ValidateAgainst(plan, manifest, target, now); err != nil {
		t.Fatalf("ValidateAgainst() error = %v", err)
	}
	first, err := approval.SigningPayload()
	if err != nil {
		t.Fatalf("SigningPayload() error = %v", err)
	}
	second, err := approval.SigningPayload()
	if err != nil {
		t.Fatalf("second SigningPayload() error = %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("same recipe execution approval did not produce deterministic CBOR")
	}

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	signed, err := approval.Sign(privateKey, now)
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}
	if err := signed.Verify(publicKey, now); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}

	for _, test := range []struct {
		name    string
		mutate  func(*cloudorchestrator.PlanV1, *cloudorchestrator.RecipeExecutionManifestV1, *cloudorchestrator.RecipeExecutionTargetV1)
		wantErr error
	}{
		{
			name: "plan scope",
			mutate: func(plan *cloudorchestrator.PlanV1, _ *cloudorchestrator.RecipeExecutionManifestV1, _ *cloudorchestrator.RecipeExecutionTargetV1) {
				plan.ResourceScope.DiskGiB++
			},
			wantErr: cloudorchestrator.ErrRecipeExecutionApprovalBinding,
		},
		{
			name: "deployment identity",
			mutate: func(_ *cloudorchestrator.PlanV1, _ *cloudorchestrator.RecipeExecutionManifestV1, target *cloudorchestrator.RecipeExecutionTargetV1) {
				target.DeploymentID = "other-deployment-1"
			},
			wantErr: cloudorchestrator.ErrRecipeExecutionApprovalBinding,
		},
		{
			name: "deployment revision",
			mutate: func(_ *cloudorchestrator.PlanV1, _ *cloudorchestrator.RecipeExecutionManifestV1, target *cloudorchestrator.RecipeExecutionTargetV1) {
				target.DeploymentRevision++
			},
			wantErr: cloudorchestrator.ErrRecipeExecutionApprovalBinding,
		},
		{
			name: "compiled artifact",
			mutate: func(_ *cloudorchestrator.PlanV1, manifest *cloudorchestrator.RecipeExecutionManifestV1, _ *cloudorchestrator.RecipeExecutionTargetV1) {
				manifest.ArtifactDigest = artifactDigest('f')
			},
			wantErr: cloudorchestrator.ErrRecipeExecutionApprovalBinding,
		},
		{
			name: "root boundary",
			mutate: func(_ *cloudorchestrator.PlanV1, manifest *cloudorchestrator.RecipeExecutionManifestV1, _ *cloudorchestrator.RecipeExecutionTargetV1) {
				manifest.RootRequired = !manifest.RootRequired
			},
			wantErr: cloudorchestrator.ErrRecipeExecutionApprovalBinding,
		},
		{
			name: "checkpoint order",
			mutate: func(_ *cloudorchestrator.PlanV1, manifest *cloudorchestrator.RecipeExecutionManifestV1, _ *cloudorchestrator.RecipeExecutionTargetV1) {
				manifest.CheckpointSequence[0], manifest.CheckpointSequence[1] = manifest.CheckpointSequence[1], manifest.CheckpointSequence[0]
			},
			wantErr: cloudorchestrator.ErrRecipeExecutionApprovalBinding,
		},
		{
			name: "volume slot",
			mutate: func(_ *cloudorchestrator.PlanV1, manifest *cloudorchestrator.RecipeExecutionManifestV1, _ *cloudorchestrator.RecipeExecutionTargetV1) {
				manifest.VolumeSlots[0].VolumeRef = "volume_ref:other-data"
			},
			wantErr: cloudorchestrator.ErrRecipeExecutionApprovalBinding,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			changedPlan := plan
			changedManifest := cloneRecipeExecutionManifest(manifest)
			changedTarget := target
			test.mutate(&changedPlan, &changedManifest, &changedTarget)
			if err := approval.ValidateAgainst(changedPlan, changedManifest, changedTarget, now); !errors.Is(err, test.wantErr) {
				t.Fatalf("ValidateAgainst() error = %v, want %v", err, test.wantErr)
			}
		})
	}

	signed.ArtifactDigest = artifactDigest('f')
	if err := signed.Verify(publicKey, now); err == nil {
		t.Fatal("Verify() accepted a signature after the compiled artifact changed")
	}
}

func TestRecipeExecutionApprovalRequiresAnApprovedPlanAndBoundedExpiry(t *testing.T) {
	now := time.Date(2026, time.July, 15, 10, 0, 0, 0, time.UTC)
	plan, manifest := approvedExecutionScope(t, now)
	plan.Status = cloudorchestrator.PlanReadyForConfirmation
	target := cloudorchestrator.RecipeExecutionTargetV1{DeploymentID: manifest.DeploymentID, DeploymentRevision: 1}
	wrongTarget := target
	wrongTarget.DeploymentID = "other-deployment-1"
	if _, err := cloudorchestrator.NewRecipeExecutionApprovalV1(approvedPlan(plan), manifest, wrongTarget, "recipe-execution-approval-1", "recipe-execution-challenge-1", "owner-device-1", now, now.Add(5*time.Minute)); err == nil {
		t.Fatal("NewRecipeExecutionApprovalV1() accepted a target outside the manifest deployment")
	}
	for _, test := range []struct {
		name      string
		plan      cloudorchestrator.PlanV1
		expiresAt time.Time
	}{
		{name: "not approved", plan: plan, expiresAt: now.Add(5 * time.Minute)},
		{name: "expired", plan: approvedPlan(plan), expiresAt: now},
		{name: "too long", plan: approvedPlan(plan), expiresAt: now.Add(5*time.Minute + time.Nanosecond)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := cloudorchestrator.NewRecipeExecutionApprovalV1(test.plan, manifest, target, "recipe-execution-approval-1", "recipe-execution-challenge-1", "owner-device-1", now, test.expiresAt); err == nil {
				t.Fatal("NewRecipeExecutionApprovalV1() accepted an unsafe approval boundary")
			}
		})
	}
}

func approvedPlan(plan cloudorchestrator.PlanV1) cloudorchestrator.PlanV1 {
	plan.Status = cloudorchestrator.PlanApproved
	return plan
}

func approvedExecutionScope(t *testing.T, now time.Time) (cloudorchestrator.PlanV1, cloudorchestrator.RecipeExecutionManifestV1) {
	t.Helper()
	plan := approvedPlan(validPlan(t, now))
	manifest := validRecipeExecutionManifest(t)
	planHash, err := plan.Hash()
	if err != nil {
		t.Fatalf("plan.Hash() error = %v", err)
	}
	manifest.PlanID = plan.PlanID
	manifest.PlanHash = planHash
	manifest.PlanRevision = plan.Revision
	manifest.RecipeDigest = plan.Recipe.Digest
	manifest.SecretSlots = []cloudorchestrator.SecretSlotV1{{SlotID: "model-token", SecretRef: "secret_ref:model-token"}}
	if err := manifest.ValidateForPlan(plan); err != nil {
		t.Fatalf("manifest.ValidateForPlan() error = %v", err)
	}
	return plan, manifest
}
