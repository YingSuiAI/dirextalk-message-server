package agentgrpc

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestManagedPreparationAdapterMatchesAgentGoldenAndRejectsPayloadDrift(t *testing.T) {
	issuedAt := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	operationID := "11111111-1111-4111-8111-111111111111"
	sourceID := "88888888-8888-4888-8888-888888888888"
	snapshotID := uuid.NewSHA1(uuid.MustParse(operationID), []byte("snapshot:"+sourceID+":knowledge")).String()
	replacementID := uuid.NewSHA1(uuid.MustParse(operationID), []byte("replacement:"+sourceID+":knowledge")).String()
	source := &agentv1.CloudManagedPreparationResourceFact{
		ResourceId: sourceID, ProviderId: "vol-0123456789abcdef0", Revision: 8,
		SpecDigest: preparationDigest("5"), TagDigest: preparationDigest("6"),
	}
	scopeProto := &agentv1.CloudManagedPreparationScope{
		SchemaVersion: "dirextalk.agent.cloud.service-operation-scope/v1", Intent: "MANAGED_PREPARATION",
		PreparationOperationId: operationID, OwnerId: "owner-golden", AgentInstanceId: "22222222-2222-4222-8222-222222222222",
		DeploymentId: "33333333-3333-4333-8333-333333333333", DeploymentRevision: 7,
		ConnectionId: "55555555-5555-4555-8555-555555555555", ConnectionRevision: 3,
		PlanId: "66666666-6666-4666-8666-666666666666", PlanRevision: 4, PlanHash: preparationDigest("1"),
		RecipeId: "postgresql", RecipeDigest: preparationDigest("2"), RecipeRevision: 5,
		Ec2: &agentv1.CloudManagedPreparationResourceFact{
			ResourceId: "77777777-7777-4777-8777-777777777777", ProviderId: "i-0123456789abcdef0",
			Revision: 6, SpecDigest: preparationDigest("3"), TagDigest: preparationDigest("4"),
		},
		SourceVolumes: []*agentv1.CloudManagedPreparationResourceFact{source},
		Restart: &agentv1.CloudManagedPreparationRestart{
			OperationId:             uuid.NewSHA1(uuid.MustParse(operationID), []byte("restart")).String(),
			ExpectedInitialRevision: 1, Action: "restart", LifecycleRestartRef: "restart-service",
			ExecutionBundleDigest: preparationDigest("7"),
		},
		Volumes: []*agentv1.CloudManagedPreparationVolume{{
			SlotId: "knowledge", SourceVolume: source, SnapshotResourceId: snapshotID, ReplacementVolumeResourceId: replacementID,
			AvailabilityZone: "us-east-1a", SizeGib: 80, VolumeType: "gp3", Iops: 3000, ThroughputMibps: 125,
			KmsKeyId: "alias/dtx-agent-golden", DeviceName: "/dev/sdf", MountPath: "/srv/knowledge",
			Persistent: true, Disposition: "retain_with_managed_service",
		}},
		ServiceMonitorRevision: 9, ServiceMonitorSuiteDigest: preparationDigest("8"), Currency: "USD",
		CostAlertAmountMinor: 25_000, ExpectedInstalledManifestDigest: preparationDigest("9"),
	}
	setPreparationVolumeSourceDigest(t, source, scopeProto.GetVolumes()[0])
	scope, err := mapManagedPreparationScope(scopeProto)
	if err != nil {
		t.Fatal(err)
	}
	if volume := scope.Volumes[0]; volume.VolumeType != "gp3" || volume.IOPS != 3000 ||
		volume.ThroughputMiBPS != 125 || volume.MountPath != "/srv/knowledge" ||
		volume.ReadOnly || !volume.Persistent || volume.Disposition != "retain_with_managed_service" {
		t.Fatalf("mapped volume=%#v", volume)
	}
	payload, err := canonicalManagedPreparationPayload(managedPreparationSigningPayload{
		SchemaVersion:  "dirextalk.agent.cloud.service-operation-challenge/v1",
		PayloadVersion: "dirextalk.agent.cloud.service-operation-signing-payload/v1", Intent: "MANAGED_PREPARATION",
		ChallengeID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", OperationID: operationID, SignerKeyID: "device-golden",
		Scope: scope, IssuedAt: issuedAt, ExpiresAt: issuedAt.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	const golden = "1c12fffde17b8bc5b7e975a9270e0e791d55071dc33323519b96f9616eec2f51"
	if got := hex.EncodeToString(sum[:]); got != golden {
		t.Fatalf("payload digest=%s want=%s", got, golden)
	}
	challenge := &agentv1.CloudManagedPreparationChallenge{
		SchemaVersion: "dirextalk.agent.cloud.service-operation-challenge/v1",
		ChallengeId:   "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", OperationId: operationID, SignerKeyId: "device-golden",
		Scope: scopeProto, ScopeDigest: "sha256:" + golden, IssuedAt: timestamppb.New(issuedAt),
		ExpiresAt: timestamppb.New(issuedAt.Add(5 * time.Minute)), SigningPayloadCbor: payload,
	}
	mapped, err := mapManagedPreparationChallenge(challenge)
	if err != nil || mapped.Scope.EC2.ProviderID != scopeProto.GetEc2().GetProviderId() {
		t.Fatalf("mapped=%#v err=%v", mapped, err)
	}
	challenge.SigningPayloadCbor = append(append([]byte(nil), payload...), 0)
	if _, err = mapManagedPreparationChallenge(challenge); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("tampered payload error=%v", err)
	}
	challenge.SigningPayloadCbor = payload
	scopeProto.GetVolumes()[0].MountPath = "/run/secrets/data"
	if _, err = mapManagedPreparationScope(scopeProto); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("reserved mount error=%v", err)
	}
	scopeProto.GetVolumes()[0].MountPath = "/srv/knowledge"
	source.SpecDigest = preparationDigest("5")
	if _, err = mapManagedPreparationScope(scopeProto); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("source spec drift error=%v", err)
	}
}

func TestManagedPreparationAdapterMapsV2BoundedSnapshotTerms(t *testing.T) {
	now := time.Date(2026, 7, 18, 8, 0, 0, 0, time.UTC)
	challenge := preparationOperationChallengeProto(t, now)
	volume := challenge.GetScope().GetVolumes()[0]

	// Frozen V1 scopes cannot silently gain a V2-only field.
	volume.SnapshotOperationKey = "snapshot-data"
	if _, err := mapManagedPreparationScope(challenge.GetScope()); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("V1 scope with V2 term error=%v", err)
	}
	volume.SnapshotOperationKey = ""

	challenge.SchemaVersion = cloudmodule.AgentCloudManagedPreparationChallengeSchemaV2
	challenge.Scope.SchemaVersion = cloudmodule.AgentCloudManagedPreparationScopeSchemaV2
	volume.SnapshotOperationKey = "snapshot-data"
	volume.SnapshotSourceVolumeScopeDigest = preparationDigest("f")
	volume.SnapshotMaxRetentionSeconds = 3600
	scope, err := mapManagedPreparationScope(challenge.GetScope())
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicalManagedPreparationPayload(managedPreparationSigningPayload{
		SchemaVersion: challenge.GetSchemaVersion(), PayloadVersion: cloudmodule.AgentCloudManagedPreparationSigningPayloadV2,
		Intent: "MANAGED_PREPARATION", ChallengeID: challenge.GetChallengeId(), OperationID: challenge.GetOperationId(),
		SignerKeyID: challenge.GetSignerKeyId(), Scope: scope, IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	const v2Golden = "f06f840eb29ec23a6485acca0872dbcb763c30058794ddcbed667ea929a4c2e4"
	if got := hex.EncodeToString(sum[:]); got != v2Golden {
		t.Fatalf("V2 payload digest=%s want=%s", got, v2Golden)
	}
	challenge.ScopeDigest = "sha256:" + hex.EncodeToString(sum[:])
	challenge.SigningPayloadCbor = payload
	mapped, err := mapManagedPreparationChallenge(challenge)
	if err != nil || mapped.SchemaVersion != cloudmodule.AgentCloudManagedPreparationChallengeSchemaV2 ||
		mapped.Scope.SchemaVersion != cloudmodule.AgentCloudManagedPreparationScopeSchemaV2 ||
		mapped.Scope.Volumes[0].SnapshotOperationKey != "snapshot-data" ||
		mapped.Scope.Volumes[0].SnapshotSourceVolumeScopeDigest != preparationDigest("f") ||
		mapped.Scope.Volumes[0].SnapshotMaxRetentionSeconds != 3600 {
		t.Fatalf("mapped=%#v err=%v", mapped, err)
	}

	volume.SnapshotMaxRetentionSeconds = 0
	if _, err = mapManagedPreparationScope(challenge.GetScope()); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("V2 scope without retention error=%v", err)
	}
}

func TestManagedPreparationAdapterRequiresClosedStepsAndSucceededResult(t *testing.T) {
	now := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	challenge := preparationOperationChallengeProto(t, now)
	operation := &agentv1.CloudManagedPreparationOperation{
		OperationId: challenge.GetOperationId(), Challenge: challenge,
		Status:       agentv1.CloudManagedPreparationStatus_CLOUD_MANAGED_PREPARATION_STATUS_SUCCEEDED,
		CurrentPhase: "finalize", Revision: 14, CreatedAt: timestamppb.New(now), UpdatedAt: timestamppb.New(now.Add(time.Minute)),
		Result: &agentv1.CloudManagedPreparationResult{
			PreparationId: uuid.NewString(), PreparationDigest: preparationDigest("a"),
			FreshHealthDigest: preparationDigest("b"), FreshHealthRevision: 3, FreshHealthObservedAt: timestamppb.New(now),
			CostDigest: preparationDigest("c"), CostPolicyRevision: 4, CostObservedAt: timestamppb.New(now),
			StackDigest: preparationDigest("d"), StackRevision: 5, StackObservedAt: timestamppb.New(now),
		},
	}
	for index, phase := range []string{"restart", "backup", "restore_create", "restore_swap", "semantic_health", "finalize"} {
		startedAt, completedAt := now.Add(time.Duration(index)*time.Second), now.Add(time.Duration(index+1)*time.Second)
		operation.Steps = append(operation.Steps, &agentv1.CloudManagedPreparationStep{
			Phase: phase, Ordinal: int32(index + 1),
			Status:   agentv1.CloudManagedPreparationStepStatus_CLOUD_MANAGED_PREPARATION_STEP_STATUS_SUCCEEDED,
			Revision: 2, IntentDigest: preparationDigest("e"),
			StartedAt: timestamppb.New(startedAt), CompletedAt: timestamppb.New(completedAt),
		})
	}
	if mapped, err := mapManagedPreparationOperation(operation); err != nil || mapped.Result == nil || len(mapped.Steps) != 6 {
		t.Fatalf("mapped=%#v err=%v", mapped, err)
	}
	operation.Steps[3].Phase = "restore_delete"
	if _, err := mapManagedPreparationOperation(operation); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("open phase error=%v", err)
	}
	operation.Steps[3].Phase = "restore_swap"
	operation.Status = agentv1.CloudManagedPreparationStatus_CLOUD_MANAGED_PREPARATION_STATUS_RUNNING
	if _, err := mapManagedPreparationOperation(operation); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("running result error=%v", err)
	}
}

func preparationOperationChallengeProto(t *testing.T, now time.Time) *agentv1.CloudManagedPreparationChallenge {
	t.Helper()
	operationID, sourceID := "11111111-1111-4111-8111-111111111111", "88888888-8888-4888-8888-888888888888"
	source := &agentv1.CloudManagedPreparationResourceFact{
		ResourceId: sourceID, ProviderId: "vol-0123456789abcdef0", Revision: 2,
		SpecDigest: preparationDigest("1"), TagDigest: preparationDigest("2"),
	}
	scopeProto := &agentv1.CloudManagedPreparationScope{
		SchemaVersion: "dirextalk.agent.cloud.service-operation-scope/v1", Intent: "MANAGED_PREPARATION",
		PreparationOperationId: operationID, OwnerId: "owner-test", AgentInstanceId: "22222222-2222-4222-8222-222222222222",
		DeploymentId: "33333333-3333-4333-8333-333333333333", DeploymentRevision: 7,
		ConnectionId: "55555555-5555-4555-8555-555555555555", ConnectionRevision: 3,
		PlanId: "66666666-6666-4666-8666-666666666666", PlanRevision: 4, PlanHash: preparationDigest("3"), RecipeId: "postgresql",
		RecipeDigest: preparationDigest("4"), RecipeRevision: 5,
		Ec2: &agentv1.CloudManagedPreparationResourceFact{
			ResourceId: "77777777-7777-4777-8777-777777777777", ProviderId: "i-0123456789abcdef0", Revision: 6,
			SpecDigest: preparationDigest("5"), TagDigest: preparationDigest("6"),
		},
		SourceVolumes: []*agentv1.CloudManagedPreparationResourceFact{source},
		Restart: &agentv1.CloudManagedPreparationRestart{
			OperationId:             uuid.NewSHA1(uuid.MustParse(operationID), []byte("restart")).String(),
			ExpectedInitialRevision: 1, Action: "restart", LifecycleRestartRef: "restart-service", ExecutionBundleDigest: preparationDigest("7"),
		},
		Volumes: []*agentv1.CloudManagedPreparationVolume{{
			SlotId: "data", SourceVolume: source,
			SnapshotResourceId:          uuid.NewSHA1(uuid.MustParse(operationID), []byte("snapshot:"+sourceID+":data")).String(),
			ReplacementVolumeResourceId: uuid.NewSHA1(uuid.MustParse(operationID), []byte("replacement:"+sourceID+":data")).String(),
			AvailabilityZone:            "us-east-1a", SizeGib: 80, VolumeType: "gp3", Iops: 3000, ThroughputMibps: 125,
			KmsKeyId: "alias/dtx-agent-test", DeviceName: "/dev/sdf", MountPath: "/srv/data",
			Persistent: true, Disposition: "retain_with_managed_service",
		}},
		ServiceMonitorRevision: 9, ServiceMonitorSuiteDigest: preparationDigest("8"), Currency: "USD",
		CostAlertAmountMinor: 2500, ExpectedInstalledManifestDigest: preparationDigest("9"),
	}
	setPreparationVolumeSourceDigest(t, source, scopeProto.GetVolumes()[0])
	scope, err := mapManagedPreparationScope(scopeProto)
	if err != nil {
		t.Fatal(err)
	}
	challengeID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	payload, err := canonicalManagedPreparationPayload(managedPreparationSigningPayload{
		SchemaVersion:  "dirextalk.agent.cloud.service-operation-challenge/v1",
		PayloadVersion: "dirextalk.agent.cloud.service-operation-signing-payload/v1", Intent: "MANAGED_PREPARATION",
		ChallengeID: challengeID, OperationID: operationID, SignerKeyID: "device-test", Scope: scope,
		IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	return &agentv1.CloudManagedPreparationChallenge{
		SchemaVersion: "dirextalk.agent.cloud.service-operation-challenge/v1", ChallengeId: challengeID,
		OperationId: operationID, SignerKeyId: "device-test", Scope: scopeProto,
		ScopeDigest: "sha256:" + hex.EncodeToString(sum[:]), IssuedAt: timestamppb.New(now),
		ExpiresAt: timestamppb.New(now.Add(5 * time.Minute)), SigningPayloadCbor: payload,
	}
}

func setPreparationVolumeSourceDigest(t *testing.T, source *agentv1.CloudManagedPreparationResourceFact, volume *agentv1.CloudManagedPreparationVolume) {
	t.Helper()
	digest, err := cloudmodule.ManagedPreparationVolumeSourceSpecDigest(cloudmodule.AgentCloudManagedPreparationVolume{
		SlotID: volume.GetSlotId(), AvailabilityZone: volume.GetAvailabilityZone(), SizeGiB: volume.GetSizeGib(),
		VolumeType: volume.GetVolumeType(), IOPS: volume.GetIops(), ThroughputMiBPS: volume.GetThroughputMibps(),
		KMSKeyID: volume.GetKmsKeyId(), DeviceName: volume.GetDeviceName(), MountPath: volume.GetMountPath(),
		ReadOnly: volume.GetReadOnly(), Persistent: volume.GetPersistent(), Disposition: volume.GetDisposition(),
	})
	if err != nil {
		t.Fatal(err)
	}
	source.SpecDigest = digest
}

func preparationDigest(fill string) string { return "sha256:" + strings.Repeat(fill, 64) }
