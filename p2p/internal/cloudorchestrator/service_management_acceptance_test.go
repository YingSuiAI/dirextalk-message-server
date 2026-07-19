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
	if got := hex.EncodeToString(sum[:]); got != "f85968ddfb155d602cd0b2e50de45c30cf1f8145f9fa059c860acd327d80206e" {
		t.Fatalf("management acceptance signing payload digest=%s", got)
	}
	for name, mutate := range map[string]func(*cloudorchestrator.ServiceManagementAcceptanceTargetV1){
		"owner": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) {
			v.OwnerID = "@other:example.com"
		},
		"agent instance": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) {
			v.AgentInstanceID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		},
		"plan": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) {
			v.PlanHash = digest("0")
		},
		"connection revision": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) {
			v.ConnectionRevision++
		},
		"resource": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) {
			v.Resources[0].Revision++
		},
		"health": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) {
			v.HealthEvidenceDigest = digest("0")
		},
		"cost": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) {
			v.CostAlertAmountMinor++
		},
		"maintenance": func(v *cloudorchestrator.ServiceManagementAcceptanceTargetV1) {
			v.Lifecycle.Maintenance = "maintenance-v2"
		},
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
			changedApproval, err := cloudorchestrator.NewServiceManagementAcceptanceApprovalV1(
				changed, approval.ApprovalID, approval.ChallengeID, approval.SignerKeyID, approval.IssuedAt, approval.ExpiresAt,
			)
			if err != nil {
				t.Fatal(err)
			}
			changedPayload, err := changedApproval.SigningPayload()
			if err != nil {
				t.Fatal(err)
			}
			if string(changedPayload) == string(payload) {
				t.Fatal("tampered evidence did not change signing payload")
			}
		})
	}
}

func managementAcceptanceTarget() cloudorchestrator.ServiceManagementAcceptanceTargetV1 {
	return cloudorchestrator.ServiceManagementAcceptanceTargetV1{
		AgentInstanceID: "11111111-1111-4111-8111-111111111111", OwnerID: "@owner:example.com",
		AcceptanceID: "acceptance-management-0001", ServiceID: "service-management-0001", ServiceRevision: 8,
		DeploymentID: "deployment-management-0001", DeploymentRevision: 11, CloudConnectionID: "connection-management-0001", ConnectionRevision: 6,
		PlanID: "plan-management-0001", PlanRevision: 7, PlanHash: digest("9"),
		RecipeID: "recipe-management-0001", RecipeDigest: digest("a"), RecipeRevision: 2, RecipeMaturity: cloudorchestrator.RecipeAwaitingManagementAccept,
		InstalledManifestDigest: digest("b"), ArtifactDigest: digest("c"), RestartOperationID: "operation-management-restart-0001", RestartOperationRevision: 3,
		ReadinessSemanticEvidenceDigest: cloudorchestrator.FixedReadinessEvidenceDigestV1, ReadinessStackObservationDigest: digest("e"),
		BackupID: "backup-management-0001", BackupRevision: 2, RestoreID: "restore-management-0001", RestoreRevision: 4,
		SourceArtifactDigests: []string{digest("d")},
		HealthRevision:        5, HealthMonitorKind: "service", HealthStatus: "healthy", HealthEvidenceType: "independent_external",
		HealthEvidenceDigest: digest("8"), HealthObservedAt: time.Date(2026, 7, 16, 0, 59, 0, 0, time.UTC),
		Currency: "USD", CostAlertAmountMinor: 2500,
		Health:      cloudorchestrator.HealthContractV1{Liveness: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/live"}, Readiness: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/ready"}, Semantic: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeCommand, Target: "probe-semantic"}},
		Lifecycle:   cloudorchestrator.ServiceManagementAcceptanceLifecycleV2{Start: "start", Stop: "stop", Maintenance: "maintenance", Restart: "restart", Upgrade: "upgrade", Rollback: "rollback", Backup: "backup", Restore: "restore", Destroy: "destroy"},
		VolumeSlots: []cloudorchestrator.VolumeSlotV1{{SlotID: "knowledge", VolumeRef: "volume_ref:knowledge", ReadOnly: true}}, DataSlots: []cloudorchestrator.DataSlotV1{{SlotID: "corpus", DataRef: "data_ref:corpus", ReadOnly: true}}, SecretSlots: []cloudorchestrator.SecretSlotV1{{SlotID: "model", SecretRef: "secret_ref:model"}},
		Resources: []cloudorchestrator.ServiceManagementAcceptanceResourceV2{
			{ResourceID: "22222222-2222-4222-8222-222222222222", Type: "ebs", Revision: 2, ProviderID: "vol-0aaaaaaaaaaaaaaaa", TagDigest: digest("6")},
			{ResourceID: "33333333-3333-4333-8333-333333333333", Type: "ebs", Revision: 3, ProviderID: "vol-0bbbbbbbbbbbbbbbb", TagDigest: digest("7")},
			{ResourceID: "44444444-4444-4444-8444-444444444444", Type: "ec2", Revision: 4, ProviderID: "i-0123456789abcdef0", TagDigest: digest("5")},
			{ResourceID: "55555555-5555-4555-8555-555555555555", Type: "eni", Revision: 5, ProviderID: "eni-0123456789abcdef0", TagDigest: digest("4")},
		},
		DestroyInstanceID: "i-0123456789abcdef0", DestroyVolumeIDs: []string{"vol-0bbbbbbbbbbbbbbbb", "vol-0aaaaaaaaaaaaaaaa"}, DestroyNetworkInterfaceIDs: []string{"eni-0123456789abcdef0"}, AcceptancePolicy: cloudorchestrator.ServiceManagementAcceptancePolicy,
	}
}

func digest(fill string) string {
	return "sha256:" + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill + fill
}
