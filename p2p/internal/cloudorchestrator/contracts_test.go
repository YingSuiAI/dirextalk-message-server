package cloudorchestrator_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestPlanV1HashCanonicalizesSetScopesAndBindsApproval(t *testing.T) {
	now := time.Date(2026, time.July, 14, 10, 0, 0, 0, time.UTC)
	plan := validPlan(t, now)

	firstHash, err := plan.Hash()
	if err != nil {
		t.Fatalf("Hash() error = %v", err)
	}

	reordered := plan
	reordered.SecretScope[0], reordered.SecretScope[1] = reordered.SecretScope[1], reordered.SecretScope[0]
	reordered.IntegrationScope[0], reordered.IntegrationScope[1] = reordered.IntegrationScope[1], reordered.IntegrationScope[0]
	secondHash, err := reordered.Hash()
	if err != nil {
		t.Fatalf("reordered Hash() error = %v", err)
	}
	if firstHash != secondHash {
		t.Fatalf("set ordering changed plan hash: %q != %q", firstHash, secondHash)
	}

	approval, err := cloudorchestrator.NewApprovalV1(plan, "approval-1", "challenge-1", "owner-device-1", now.Add(10*time.Minute))
	if err != nil {
		t.Fatalf("NewApprovalV1() error = %v", err)
	}
	if approval.PlanHash != firstHash || approval.PlanRevision != plan.Revision || approval.QuoteID != plan.Quote.QuoteID || approval.CloudConnectionID != plan.CloudConnectionID || approval.RecipeDigest != plan.Recipe.Digest {
		t.Fatalf("approval did not bind the plan identity: %#v", approval)
	}
	if err := approval.ValidateAgainstPlan(plan, now); err != nil {
		t.Fatalf("ValidateAgainstPlan() error = %v", err)
	}

	approval.NetworkScope.PublicIngress = !approval.NetworkScope.PublicIngress
	if err := approval.ValidateAgainstPlan(plan, now); err == nil {
		t.Fatal("ValidateAgainstPlan() accepted a changed network scope")
	}
}

func TestApprovalV1SignatureBindsPayloadAndExpiry(t *testing.T) {
	now := time.Date(2026, time.July, 14, 10, 0, 0, 0, time.UTC)
	plan := validPlan(t, now)
	approval, err := cloudorchestrator.NewApprovalV1(plan, "approval-1", "challenge-1", "owner-device-1", now.Add(10*time.Minute))
	if err != nil {
		t.Fatalf("NewApprovalV1() error = %v", err)
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

	signed.ExpiresAt = signed.ExpiresAt.Add(time.Minute)
	if err := signed.Verify(publicKey, now); err == nil {
		t.Fatal("Verify() accepted a signature after expiry changed")
	}
}

func TestContractsRejectSecretMaterialAndUnapprovedIngress(t *testing.T) {
	now := time.Date(2026, time.July, 14, 10, 0, 0, 0, time.UTC)
	plan := validPlan(t, now)
	plan.SecretScope[0].SecretRef = "AKIAIOSFODNN7EXAMPLE"
	if err := plan.Validate(); err == nil {
		t.Fatal("Validate() accepted AWS access-key-shaped material in secret_ref")
	}

	plan = validPlan(t, now)
	plan.NetworkScope = cloudorchestrator.NetworkScopeV1{
		PublicIngress:          true,
		EntryPoint:             cloudorchestrator.EntryPointALB,
		TLSRequired:            false,
		AuthenticationRequired: true,
		Ingress: []cloudorchestrator.IngressRuleV1{{
			Protocol: "https",
			Port:     443,
			Purpose:  "service-ui",
		}},
	}
	if err := plan.Validate(); err == nil {
		t.Fatal("Validate() accepted public ingress without TLS")
	}
}

func validPlan(t *testing.T, now time.Time) cloudorchestrator.PlanV1 {
	t.Helper()
	recipe := cloudorchestrator.RecipeV1{
		SchemaVersion: cloudorchestrator.SchemaVersionV1,
		RecipeID:      "recipe-knowledge-node-1",
		Name:          "Knowledge node",
		Maturity:      cloudorchestrator.RecipeExperimental,
		Sources: []cloudorchestrator.RecipeSourceV1{{
			URL:            "https://github.com/example/knowledge-node",
			Version:        "v1.2.3",
			Commit:         "0123456789abcdef0123456789abcdef01234567",
			ArtifactDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			License:        "Apache-2.0",
			RetrievedAt:    now,
			Official:       true,
		}},
		Requirements: cloudorchestrator.ResourceRequirementsV1{
			MinVCPU:      4,
			MinMemoryMiB: 8192,
			MinDiskGiB:   80,
			Architecture: cloudorchestrator.ArchitectureAMD64,
		},
		Install: cloudorchestrator.InstallContractV1{
			RootRequired:    true,
			TimeoutSeconds:  1800,
			CheckpointNames: []string{"image-pulled", "service-started"},
			Steps: []cloudorchestrator.InstallStepV1{{
				ID: "install-service", Summary: "Install the official image", TimeoutSeconds: 900,
			}},
		},
		Health: cloudorchestrator.HealthContractV1{
			Liveness:  cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/healthz"},
			Readiness: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/readyz"},
			Semantic:  cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeCommand, Target: "verify-index"},
		},
		Lifecycle: cloudorchestrator.LifecycleContractV1{
			Start: "start-service", Stop: "stop-service", Restart: "restart-service", Upgrade: "upgrade-service", Rollback: "rollback-service", Backup: "backup-data", Restore: "restore-data", Destroy: "destroy-service",
		},
	}
	recipeDigest, err := recipe.Digest()
	if err != nil {
		t.Fatalf("recipe.Digest() error = %v", err)
	}

	quote := cloudorchestrator.QuoteV1{
		SchemaVersion:     cloudorchestrator.SchemaVersionV1,
		QuoteID:           "quote-1",
		CloudConnectionID: "connection-1",
		Region:            "us-east-1",
		Currency:          "USD",
		QuotedAt:          now,
		ValidUntil:        now.Add(15 * time.Minute),
		Candidates: []cloudorchestrator.QuoteCandidateV1{{
			CandidateID:       "recommended",
			Tier:              cloudorchestrator.QuoteTierRecommended,
			InstanceType:      "m7i.xlarge",
			PurchaseOption:    cloudorchestrator.PurchaseOnDemand,
			Architecture:      cloudorchestrator.ArchitectureAMD64,
			VCPU:              4,
			MemoryMiB:         16384,
			GPUCount:          0,
			GPUMemoryMiB:      0,
			HourlyMinor:       2016,
			ThirtyDayMinor:    1451520,
			StartupUpperMinor: 0,
			EstimatedDiskGiB:  80,
			AvailabilityZones: []string{"us-east-1a", "us-east-1b"},
		}},
	}
	quoteDigest, err := quote.Digest()
	if err != nil {
		t.Fatalf("quote.Digest() error = %v", err)
	}

	plan := cloudorchestrator.PlanV1{
		SchemaVersion:     cloudorchestrator.SchemaVersionV1,
		PlanID:            "plan-1",
		Revision:          7,
		Status:            cloudorchestrator.PlanReadyForConfirmation,
		CloudConnectionID: "connection-1",
		Recipe: cloudorchestrator.RecipeBindingV1{
			RecipeID: recipe.RecipeID,
			Digest:   recipeDigest,
			Maturity: recipe.Maturity,
		},
		Quote: cloudorchestrator.QuoteBindingV1{
			QuoteID:     quote.QuoteID,
			Digest:      quoteDigest,
			ValidUntil:  quote.ValidUntil,
			CandidateID: "recommended",
		},
		ResourceScope: cloudorchestrator.ResourceScopeV1{
			Region:            "us-east-1",
			AvailabilityZones: []string{"us-east-1b", "us-east-1a"},
			InstanceType:      "m7i.xlarge",
			Architecture:      cloudorchestrator.ArchitectureAMD64,
			VCPU:              4,
			MemoryMiB:         16384,
			DiskGiB:           80,
			PurchaseOption:    cloudorchestrator.PurchaseOnDemand,
		},
		NetworkScope: cloudorchestrator.NetworkScopeV1{
			PublicIngress:          false,
			EntryPoint:             cloudorchestrator.EntryPointNone,
			TLSRequired:            false,
			AuthenticationRequired: false,
		},
		SecretScope: []cloudorchestrator.SecretReferenceV1{
			{SecretRef: "secret_ref:model-token", Purpose: "model-access", Delivery: cloudorchestrator.SecretDeliveryFile},
			{SecretRef: "secret_ref:github-app", Purpose: "source-access", Delivery: cloudorchestrator.SecretDeliveryFile},
		},
		IntegrationScope: []cloudorchestrator.IntegrationScopeV1{
			{Kind: cloudorchestrator.IntegrationMCP, Name: "mcp"},
			{Kind: cloudorchestrator.IntegrationWeb, Name: "web-ui"},
		},
	}
	if err := plan.Validate(); err != nil {
		t.Fatalf("plan.Validate() error = %v", err)
	}
	return plan
}
