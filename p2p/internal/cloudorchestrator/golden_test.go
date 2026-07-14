package cloudorchestrator_test

import (
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestV1CanonicalPlanAndApprovalGoldenVectors(t *testing.T) {
	now := time.Date(2026, time.July, 14, 10, 0, 0, 0, time.UTC)
	plan := validPlan(t, now)
	canonicalPlan, err := plan.CanonicalPlanJSON()
	if err != nil {
		t.Fatalf("CanonicalPlanJSON() error = %v", err)
	}
	planHash, err := plan.Hash()
	if err != nil {
		t.Fatalf("Hash() error = %v", err)
	}
	approval, err := cloudorchestrator.NewApprovalV1(plan, "approval-1", "challenge-1", "owner-device-1", now.Add(10*time.Minute))
	if err != nil {
		t.Fatalf("NewApprovalV1() error = %v", err)
	}
	payload, err := approval.SigningPayload()
	if err != nil {
		t.Fatalf("SigningPayload() error = %v", err)
	}

	const wantPlanCanonical = `{"cloud_connection_id":"connection-1","hash_algorithm":"canonical-json-sha256","integration_scope":[{"kind":"mcp","name":"mcp"},{"kind":"web","name":"web-ui"}],"network_scope":{"authentication_required":false,"entry_point":"none","public_ingress":false,"tls_required":false},"plan_id":"plan-1","quote":{"candidate_id":"recommended","digest":"sha256:f5b25116b89b7f6728974726f31883af4afa1afed8a6bb8fc2ee09159dbee238","quote_id":"quote-1","valid_until":"2026-07-14T10:15:00Z"},"recipe":{"digest":"sha256:ef1abbaba7ed4b6522353f5e151d82b89d61ad59eec63a79d59e0da2ebf65008","maturity":"experimental","recipe_id":"recipe-knowledge-node-1"},"resource_scope":{"architecture":"amd64","availability_zones":["us-east-1a","us-east-1b"],"disk_gib":80,"instance_type":"m7i.xlarge","memory_mib":16384,"purchase_option":"on_demand","region":"us-east-1","vcpu":4},"revision":7,"schema_version":"cloud-orchestrator/v1","secret_scope":[{"delivery":"file","purpose":"source-access","secret_ref":"secret_ref:github-app"},{"delivery":"file","purpose":"model-access","secret_ref":"secret_ref:model-token"}]}`
	const wantPlanHash = "sha256:cea50ec3d30ce086a885e41aabedd10301a397e6da7bb0dc9f9faec196bff18a"
	const wantApprovalPayload = `{"approval_id":"approval-1","challenge_id":"challenge-1","cloud_connection_id":"connection-1","expires_at":"2026-07-14T10:10:00Z","hash_algorithm":"canonical-json-sha256","integration_scope":[{"kind":"mcp","name":"mcp"},{"kind":"web","name":"web-ui"}],"network_scope":{"authentication_required":false,"entry_point":"none","public_ingress":false,"tls_required":false},"payload_version":"approval-signing-payload/v1","plan_hash":"sha256:cea50ec3d30ce086a885e41aabedd10301a397e6da7bb0dc9f9faec196bff18a","plan_id":"plan-1","plan_revision":7,"quote_digest":"sha256:f5b25116b89b7f6728974726f31883af4afa1afed8a6bb8fc2ee09159dbee238","quote_id":"quote-1","quote_valid_until":"2026-07-14T10:15:00Z","recipe_digest":"sha256:ef1abbaba7ed4b6522353f5e151d82b89d61ad59eec63a79d59e0da2ebf65008","resource_scope":{"architecture":"amd64","availability_zones":["us-east-1a","us-east-1b"],"disk_gib":80,"instance_type":"m7i.xlarge","memory_mib":16384,"purchase_option":"on_demand","region":"us-east-1","vcpu":4},"schema_version":"cloud-orchestrator/v1","secret_scope":[{"delivery":"file","purpose":"source-access","secret_ref":"secret_ref:github-app"},{"delivery":"file","purpose":"model-access","secret_ref":"secret_ref:model-token"}],"signer_key_id":"owner-device-1"}`
	if string(canonicalPlan) != wantPlanCanonical || planHash != wantPlanHash || string(payload) != wantApprovalPayload {
		t.Fatalf("update V1 golden vectors:\nplan canonical: %s\nplan hash: %s\napproval payload: %s", canonicalPlan, planHash, payload)
	}
}
