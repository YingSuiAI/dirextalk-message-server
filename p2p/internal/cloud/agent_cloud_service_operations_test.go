package cloud

import (
	"encoding/base64"
	"testing"
	"time"
)

func TestAgentV2ServiceOperationsBindApprovalAndQuoteUsage(t *testing.T) {
	now := time.Date(2026, time.July, 18, 8, 0, 0, 0, time.UTC)
	plan := readyAgentPlan(now)
	plan.SchemaVersion = AgentCloudPlanSchemaV2
	volumeDigest, err := agentCloudVolumeScopeDigest(plan.Resource.VolumeScopes[0])
	if err != nil {
		t.Fatal(err)
	}
	plan.ServiceOperations = AgentCloudServiceOperationScope{
		PrivateEndpoints: []AgentCloudPrivateEndpointOperation{{
			OperationKey: "endpoint-s3", Service: "s3", SecurityGroupSource: "worker_dedicated",
			PrivateDNSEnabled: true, MonthlyHours: 720, DataMiBPerMonth: 96,
		}},
		Snapshots: []AgentCloudSnapshotOperation{{
			OperationKey: "snapshot-knowledge", SourceVolumeSlotID: "knowledge", SourceVolumeSpecDigest: volumeDigest,
			Disposition: "delete_with_deployment", MaxRetentionSeconds: plan.Retention.MaxLifetimeSeconds,
		}},
	}
	if err := validateReadableAgentCloudPlan(plan); err != nil {
		t.Fatalf("valid v2 plan rejected: %v", err)
	}

	challenge := challengeForAgentPlan(plan, now)
	approval := agentApprovalFromChallenge(plan, challenge)
	if approval.SchemaVersion != AgentCloudApprovalSchemaV2 || approval.ServiceOperations == nil ||
		len(approval.ServiceOperations.PrivateEndpoints) != 1 || len(approval.ServiceOperations.Snapshots) != 1 {
		t.Fatalf("v2 approval lost service operations: %#v", approval)
	}
	approval.Signature = base64.RawURLEncoding.EncodeToString(make([]byte, 64))
	decoded, _, err := decodeAgentCloudApproval(approvalMapForAgentTest(t, approval), now)
	if err != nil || decoded.ServiceOperations == nil || len(decoded.ServiceOperations.Snapshots) != 1 {
		t.Fatalf("valid v2 approval was rejected: %#v err=%v", decoded, err)
	}
	approvalDrift := approvalMapForAgentTest(t, approval)
	approvalDrift["service_operations"].(map[string]any)["snapshots"].([]any)[0].(map[string]any)["source_volume_spec_digest"] = "sha256:" + repeatAgentHex("e")
	if _, _, err := decodeAgentCloudApproval(approvalDrift, now); err == nil {
		t.Fatal("approval with snapshot scope drift was accepted")
	}

	planView := agentCloudPlanView(plan)
	if _, leakedSchema := planView["schema_version"]; leakedSchema {
		t.Fatalf("owner plan view exposed internal schema marker: %#v", planView)
	}
	if operations, ok := planView["service_operations"].(agentCloudServiceOperationScopeV1); !ok || len(operations.PrivateEndpoints) != 1 || len(operations.Snapshots) != 1 {
		t.Fatalf("owner plan view lost v2 operations: %#v", planView)
	}

	quote := quoteForAgentPlan(plan, now)
	quote.Usage = AgentCloudUsageEstimate{
		RuntimeHoursPerMonth: 730, PrivateEndpointHours: 720, PrivateEndpointDataMiB: 96, SnapshotGiBMonths: 3,
	}
	quoteView, ok := agentCloudQuoteView(quote)
	if !ok || quoteView.Usage == nil || quoteView.Usage.PrivateEndpointHours != 720 || quoteView.Usage.PrivateEndpointDataMiB != 96 || quoteView.Usage.SnapshotGiBMonths != 3 {
		t.Fatalf("v2 quote usage was not projected: %#v ok=%v", quoteView, ok)
	}
	if _, ok := agentCloudPlanViewWithQuote(plan, quote); !ok {
		t.Fatal("v2 plan and quote binding was rejected")
	}

	quote.Usage.PrivateEndpointHours++
	if _, ok := agentCloudQuoteView(quote); ok {
		t.Fatal("quote with endpoint-usage drift was accepted")
	}
	quote.Usage.PrivateEndpointHours--
	plan.ServiceOperations.Snapshots[0].SourceVolumeSpecDigest = "sha256:" + repeatAgentHex("f")
	if err := validateReadableAgentCloudPlan(plan); err == nil {
		t.Fatal("plan with an unbound snapshot source digest was accepted")
	}

	legacy := readyAgentPlan(now)
	if _, present := agentCloudPlanView(legacy)["service_operations"]; present {
		t.Fatal("v1 owner plan view exposed service operations")
	}
	legacyQuote, legacyQuoteOK := agentCloudQuoteView(quoteForAgentPlan(legacy, now))
	if !legacyQuoteOK || legacyQuote.Usage != nil {
		t.Fatalf("v1 projection changed shape: %#v ok=%v", legacyQuote, legacyQuoteOK)
	}
	legacy.ServiceOperations = AgentCloudServiceOperationScope{PrivateEndpoints: []AgentCloudPrivateEndpointOperation{{OperationKey: "endpoint-s3"}}}
	if err := validateReadableAgentCloudPlan(legacy); err == nil {
		t.Fatal("v1 plan with service operations was accepted")
	}
}
