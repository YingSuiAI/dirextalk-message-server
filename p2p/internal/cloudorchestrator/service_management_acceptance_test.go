package cloudorchestrator_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestServiceManagementAcceptanceBindsVerifiedEvidence(t *testing.T) {
	now := time.Date(2026, 7, 16, 1, 0, 0, 0, time.UTC)
	target := managementAcceptanceTarget()
	approval, err := cloudorchestrator.NewServiceManagementAcceptanceApprovalV1(target, "approval-management-0001", "challenge-management-0001", "device-management-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	signed, err := approval.Sign(private, now.Add(time.Minute))
	if err != nil || signed.Verify(public, now.Add(time.Minute)) != nil || signed.ValidateAgainst(target, now.Add(time.Minute)) != nil {
		t.Fatalf("signed acceptance invalid: %v", err)
	}
	payload, _ := approval.SigningPayload()
	sum := sha256.Sum256(payload)
	if got := hex.EncodeToString(sum[:]); got != "852681d306761d041df9380a53989afc98ae9bfac110da3a9e5b5c47c52e02b5" {
		t.Fatalf("management acceptance signing payload digest=%s", got)
	}
	for name, mutate := range map[string]func(*cloudorchestrator.ServiceManagementAcceptanceTargetV1){
		"manifest": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) {
			v.InstalledManifestDigest = digest("f")
		},
		"readiness evidence": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) {
			v.ReadinessStackObservationDigest = digest("f")
		},
		"recipe maturity": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) {
			v.RecipeMaturity = cloudorchestrator.RecipeManaged
		},
		"restart": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) {
			v.RestartOperationID = "operation-other"
		},
		"restore": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) { v.RestoreRevision++ },
		"secret": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) {
			v.SecretSlots[0].SecretRef = "secret_ref:other"
		},
		"destroy": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) {
			v.DestroyVolumeIDs[0] = "vol-0cccccccccccccccc"
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := managementAcceptanceTarget()
			mutate(&changed)
			if signed.ValidateAgainst(changed, now.Add(time.Minute)) == nil {
				t.Fatal("tampered evidence unexpectedly accepted")
			}
		})
	}
}

func managementAcceptanceTarget() cloudorchestrator.ServiceManagementAcceptanceTargetV1 {
	return cloudorchestrator.ServiceManagementAcceptanceTargetV1{
		AcceptanceID: "acceptance-management-0001", ServiceID: "service-management-0001", ServiceRevision: 8,
		DeploymentID: "deployment-management-0001", DeploymentRevision: 11, CloudConnectionID: "connection-management-0001",
		RecipeID: "recipe-management-0001", RecipeDigest: digest("a"), RecipeRevision: 2, RecipeMaturity: cloudorchestrator.RecipeAwaitingManagementAccept,
		InstalledManifestDigest: digest("b"), ArtifactDigest: digest("c"), RestartOperationID: "operation-management-restart-0001", RestartOperationRevision: 3,
		ReadinessSemanticEvidenceDigest: cloudorchestrator.FixedReadinessEvidenceDigestV1, ReadinessStackObservationDigest: digest("e"),
		BackupID: "backup-management-0001", BackupRevision: 2, RestoreID: "restore-management-0001", RestoreRevision: 4,
		SourceArtifactDigests: []string{digest("d")},
		Health:                cloudorchestrator.HealthContractV1{Liveness: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/live"}, Readiness: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/ready"}, Semantic: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeCommand, Target: "probe-semantic"}},
		Lifecycle:             cloudorchestrator.LifecycleContractV1{Start: "start", Stop: "stop", Restart: "restart", Upgrade: "upgrade", Rollback: "rollback", Backup: "backup", Restore: "restore", Destroy: "destroy"},
		VolumeSlots:           []cloudorchestrator.VolumeSlotV1{{SlotID: "knowledge", VolumeRef: "volume_ref:knowledge", ReadOnly: true}}, DataSlots: []cloudorchestrator.DataSlotV1{{SlotID: "corpus", DataRef: "data_ref:corpus", ReadOnly: true}}, SecretSlots: []cloudorchestrator.SecretSlotV1{{SlotID: "model", SecretRef: "secret_ref:model"}},
		DestroyInstanceID: "i-0123456789abcdef0", DestroyVolumeIDs: []string{"vol-0bbbbbbbbbbbbbbbb", "vol-0aaaaaaaaaaaaaaaa"}, DestroyNetworkInterfaceIDs: []string{"eni-0123456789abcdef0"}, AcceptancePolicy: cloudorchestrator.ServiceManagementAcceptancePolicy,
	}
}

func digest(fill string) string {
	return "sha256:" + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill
}
