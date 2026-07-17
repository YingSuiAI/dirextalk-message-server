package cloud

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type managedPreparationModuleClient struct {
	*agentControlModuleClient
	challenge      AgentCloudManagedPreparationChallenge
	createRequest  AgentCloudManagedPreparationCreateRequest
	createErr      error
	approveRequest AgentCloudManagedPreparationApproveRequest
	approveResult  AgentCloudManagedPreparationOperation
	approveErr     error
	getResult      AgentCloudManagedPreparationOperation
	getFound       bool
	getFoundAfter  int
	getErr         error
	createCalls    int
	approveCalls   int
	getCalls       int
}

func (client *managedPreparationModuleClient) CreateCloudManagedPreparation(_ context.Context, request AgentCloudManagedPreparationCreateRequest) (AgentCloudManagedPreparationChallenge, error) {
	client.createCalls++
	client.createRequest = request
	return client.challenge, client.createErr
}

func (client *managedPreparationModuleClient) ApproveCloudManagedPreparation(_ context.Context, request AgentCloudManagedPreparationApproveRequest) (AgentCloudManagedPreparationOperation, error) {
	client.approveCalls++
	client.approveRequest = request
	return client.approveResult, client.approveErr
}

func (client *managedPreparationModuleClient) GetCloudManagedPreparation(_ context.Context, request AgentCloudManagedPreparationGetRequest) (AgentCloudManagedPreparationOperation, bool, error) {
	client.getCalls++
	if request.OperationID != client.getResult.OperationID {
		return AgentCloudManagedPreparationOperation{}, false, ErrAgentCloudControlInvalid
	}
	return client.getResult, client.getFound && client.getCalls > client.getFoundAfter, client.getErr
}

func TestManagedPreparationFacadeSignsFullScopeAndRecoversWithoutResending(t *testing.T) {
	now := time.Date(2026, 7, 17, 13, 0, 0, 0, time.UTC)
	serviceID, deploymentID, operationID := "service-managed-preparation", uuid.NewString(), uuid.NewString()
	challenge := managedPreparationTestChallenge(t, now, deploymentID, operationID)
	operation := managedPreparationTestOperation(challenge, now)
	client := &managedPreparationModuleClient{
		agentControlModuleClient: &agentControlModuleClient{}, challenge: challenge,
		approveErr: ErrAgentCloudControlUnavailable, getResult: operation, getFound: true, getFoundAfter: 1,
	}
	store := &managedAcceptanceProjectionStore{compatibility: ManagedAcceptanceCompatibility{
		DeploymentID: deploymentID, DeploymentRevision: 7, SignerKeyID: challenge.SignerKeyID,
	}, found: true}
	published := 0
	module := New(store, Config{
		OwnerMXID: func() string { return challenge.Scope.OwnerID }, Now: func() time.Time { return now },
		Publish: func(context.Context, string, string, map[string]any) error {
			published++
			return nil
		},
		AgentCloudControlClient: client,
	})

	prepared, apiErr := module.Handlers()[actionServicesManagedPreparationPrepare](t.Context(), map[string]any{
		"service_id": serviceID, "expected_revision": int64(7), "cost_alert_amount_minor": int64(2500),
		"idempotency_key": uuid.NewString(),
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	confirmation := prepared.(map[string]any)["confirmation"].(map[string]any)
	approval := confirmation["approval"].(managedPreparationApprovalV1)
	if approval.Scope.EC2.ProviderID != challenge.Scope.EC2.ProviderID ||
		confirmation["signing_payload_cbor"] != base64.RawURLEncoding.EncodeToString(challenge.SigningPayloadCBOR) ||
		confirmation["signing_payload_digest"] != challenge.ScopeDigest ||
		client.createRequest.DeploymentID != deploymentID || client.createRequest.ExpectedDeploymentRevision != 7 ||
		client.createRequest.CostAlertAmountMinor != 2500 || published != 0 {
		t.Fatalf("prepare=%#v request=%#v published=%d", prepared, client.createRequest, published)
	}

	approval.Signature = base64.RawURLEncoding.EncodeToString(make([]byte, 64))
	approved, apiErr := module.Handlers()[actionServicesManagedPreparationApprove](t.Context(), map[string]any{
		"service_id": serviceID, "expected_revision": int64(7), "approval": approval, "idempotency_key": uuid.NewString(),
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	view := approved.(map[string]any)["operation"].(map[string]any)
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	if client.approveCalls != 1 || client.getCalls != 2 || client.approveRequest.OperationID != operationID ||
		client.approveRequest.ScopeDigest != challenge.ScopeDigest || client.approveRequest.Approval.OperationID != operationID ||
		published != 0 || strings.Contains(string(encoded), challenge.Scope.EC2.ProviderID) ||
		view["status"] != "succeeded" || view["result"].(map[string]any)["health"].(map[string]any)["revision"] != int64(9) {
		t.Fatalf("approve=%#v request=%#v calls=%d/%d published=%d", approved, client.approveRequest, client.approveCalls, client.getCalls, published)
	}

	// An exact replay is read before a signature can be sent again.
	replayed, apiErr := module.Handlers()[actionServicesManagedPreparationApprove](t.Context(), map[string]any{
		"service_id": serviceID, "expected_revision": int64(7), "approval": approval, "idempotency_key": uuid.NewString(),
	})
	if apiErr != nil || !reflect.DeepEqual(replayed, approved) || client.approveCalls != 1 || client.getCalls != 3 {
		t.Fatalf("replay=%#v err=%v calls=%d/%d", replayed, apiErr, client.approveCalls, client.getCalls)
	}

	// A revision gap is a replacement boundary, not an opportunity to reuse
	// the old signed operation.
	store.compatibility.DeploymentRevision = 8
	if result, gapErr := module.Handlers()[actionServicesManagedPreparationGet](t.Context(), map[string]any{
		"service_id": serviceID, "expected_revision": int64(7), "operation_id": operationID,
	}); gapErr == nil || result != nil || client.getCalls != 3 {
		t.Fatalf("revision gap result=%#v err=%v calls=%d", result, gapErr, client.getCalls)
	}
}

func TestManagedPreparationExactGetErrorRemainsUnavailable(t *testing.T) {
	now := time.Date(2026, 7, 17, 13, 0, 0, 0, time.UTC)
	serviceID, deploymentID, operationID := "service-managed-preparation", uuid.NewString(), uuid.NewString()
	challenge := managedPreparationTestChallenge(t, now, deploymentID, operationID)
	operation := managedPreparationTestOperation(challenge, now)
	client := &managedPreparationModuleClient{
		agentControlModuleClient: &agentControlModuleClient{}, getResult: operation, getErr: ErrAgentCloudControlUnavailable,
	}
	store := &managedAcceptanceProjectionStore{compatibility: ManagedAcceptanceCompatibility{
		DeploymentID: deploymentID, DeploymentRevision: 7, SignerKeyID: challenge.SignerKeyID,
	}, found: true}
	module := New(store, Config{
		OwnerMXID: func() string { return challenge.Scope.OwnerID }, Now: func() time.Time { return now }, AgentCloudControlClient: client,
	})
	approval := managedPreparationApprovalV1{
		SchemaVersion: managedPreparationChallengeSchema, ChallengeID: challenge.ChallengeID, OperationID: challenge.OperationID,
		SignerKeyID: challenge.SignerKeyID, Scope: challenge.Scope, ScopeDigest: challenge.ScopeDigest,
		IssuedAt: challenge.IssuedAt, ExpiresAt: challenge.ExpiresAt, OperationRevision: 1,
		Signature: base64.RawURLEncoding.EncodeToString(make([]byte, 64)),
	}
	result, apiErr := module.Handlers()[actionServicesManagedPreparationApprove](t.Context(), map[string]any{
		"service_id": serviceID, "expected_revision": int64(7), "approval": approval, "idempotency_key": uuid.NewString(),
	})
	if apiErr == nil || result != nil || apiErr.Status != 503 || client.approveCalls != 0 {
		t.Fatalf("approve exact-Get error was rewritten or signature sent: result=%#v err=%#v calls=%d", result, apiErr, client.approveCalls)
	}
	result, apiErr = module.Handlers()[actionServicesManagedPreparationGet](t.Context(), map[string]any{
		"service_id": serviceID, "expected_revision": int64(7), "operation_id": operationID,
	})
	if apiErr == nil || result != nil || apiErr.Status != 503 {
		t.Fatalf("get error was rewritten: result=%#v err=%#v", result, apiErr)
	}
}

func TestManagedPreparationFacadeDispatchesV2BoundedSnapshotPayload(t *testing.T) {
	now := time.Date(2026, 7, 18, 9, 0, 0, 0, time.UTC)
	serviceID, deploymentID, operationID := "service-managed-preparation", uuid.NewString(), uuid.NewString()
	challenge := managedPreparationTestChallengeV2(t, now, deploymentID, operationID)
	client := &managedPreparationModuleClient{
		agentControlModuleClient: &agentControlModuleClient{}, challenge: challenge,
	}
	store := &managedAcceptanceProjectionStore{compatibility: ManagedAcceptanceCompatibility{
		DeploymentID: deploymentID, DeploymentRevision: 7, SignerKeyID: challenge.SignerKeyID,
	}, found: true}
	module := New(store, Config{
		OwnerMXID: func() string { return challenge.Scope.OwnerID }, Now: func() time.Time { return now },
		AgentCloudControlClient: client,
	})

	prepared, apiErr := module.Handlers()[actionServicesManagedPreparationPrepare](t.Context(), map[string]any{
		"service_id": serviceID, "expected_revision": int64(7), "cost_alert_amount_minor": int64(2500),
		"idempotency_key": uuid.NewString(),
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	approval := prepared.(map[string]any)["confirmation"].(map[string]any)["approval"].(managedPreparationApprovalV1)
	volume := approval.Scope.Volumes[0]
	if approval.SchemaVersion != managedPreparationChallengeSchemaV2 || approval.Scope.SchemaVersion != managedPreparationScopeSchemaV2 ||
		volume.SnapshotOperationKey != "snapshot-data" || volume.SnapshotSourceVolumeScopeDigest != managementDigest("f") ||
		volume.SnapshotMaxRetentionSeconds != 3600 {
		t.Fatalf("v2 approval=%#v", approval)
	}
	approval.Signature = base64.RawURLEncoding.EncodeToString(make([]byte, 64))
	decoded, _, err := decodeManagedPreparationApproval(approval)
	if err != nil || decoded.SchemaVersion != managedPreparationChallengeSchemaV2 {
		t.Fatalf("decoded=%#v err=%v", decoded, err)
	}

	// The V2-only terms are mandatory and cannot be smuggled into a frozen V1
	// approval payload.
	v1 := managedPreparationTestChallenge(t, now, deploymentID, uuid.NewString())
	v1.Scope.Volumes[0].SnapshotOperationKey = "snapshot-data"
	if validManagedPreparationScopeSchema(v1.Scope) {
		t.Fatal("V1 scope accepted V2-only snapshot terms")
	}
	if _, err := managedPreparationApprovalSigningPayload(managedPreparationApprovalV1{
		SchemaVersion: v1.SchemaVersion, Scope: v1.Scope,
	}); err == nil {
		t.Fatal("V1 signing payload accepted V2-only snapshot terms")
	}
	frozen := managedPreparationTestChallenge(t, now, deploymentID, uuid.NewString())
	v1Approval := managedPreparationApprovalV1{
		SchemaVersion: frozen.SchemaVersion, ChallengeID: frozen.ChallengeID, OperationID: frozen.OperationID,
		SignerKeyID: frozen.SignerKeyID, Scope: frozen.Scope, ScopeDigest: frozen.ScopeDigest,
		IssuedAt: frozen.IssuedAt, ExpiresAt: frozen.ExpiresAt, OperationRevision: frozen.Revision,
		Signature: base64.RawURLEncoding.EncodeToString(make([]byte, 64)),
	}
	encoded, err := json.Marshal(v1Approval)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err = json.Unmarshal(encoded, &raw); err != nil {
		t.Fatal(err)
	}
	raw["scope"].(map[string]any)["volumes"].([]any)[0].(map[string]any)["snapshot_operation_key"] = ""
	if _, _, err = decodeManagedPreparationApproval(raw); err == nil {
		t.Fatal("V1 approval accepted an explicitly present V2 key")
	}
}

func managedPreparationTestChallenge(t *testing.T, now time.Time, deploymentID, operationID string) AgentCloudManagedPreparationChallenge {
	t.Helper()
	scope := AgentCloudManagedPreparationScope{
		SchemaVersion: managedPreparationScopeSchema, Intent: managedPreparationIntent, PreparationOperationID: operationID,
		OwnerID: "@owner:example.com", AgentInstanceID: uuid.NewString(), DeploymentID: deploymentID, DeploymentRevision: 7,
		ConnectionID: uuid.NewString(), ConnectionRevision: 4, PlanID: uuid.NewString(), PlanRevision: 3,
		PlanHash: managementDigest("1"), RecipeID: "recipe-managed-preparation", RecipeDigest: managementDigest("2"), RecipeRevision: 5,
		EC2: AgentCloudManagedPreparationResourceFact{ResourceID: uuid.NewString(), ProviderID: "i-0123456789abcdef0", Revision: 2,
			SpecDigest: managementDigest("3"), TagDigest: managementDigest("4")},
		SourceVolumes: []AgentCloudManagedPreparationResourceFact{{ResourceID: uuid.NewString(), ProviderID: "vol-0123456789abcdef0",
			Revision: 3, SpecDigest: managementDigest("5"), TagDigest: managementDigest("6")}},
		Restart: AgentCloudManagedPreparationRestart{OperationID: uuid.NewSHA1(uuid.MustParse(operationID), []byte("restart")).String(),
			ExpectedInitialRevision: 1, Action: "restart", LifecycleRestartRef: "restart", ExecutionBundleDigest: managementDigest("7")},
		ServiceMonitorRevision: 6, ServiceMonitorSuiteDigest: managementDigest("8"), Currency: "USD",
		CostAlertAmountMinor: 2500, ExpectedInstalledManifestDigest: managementDigest("9"),
	}
	scope.Volumes = []AgentCloudManagedPreparationVolume{{
		SlotID: "data", SourceVolume: scope.SourceVolumes[0], SnapshotResourceID: uuid.NewString(),
		ReplacementVolumeResourceID: uuid.NewString(), AvailabilityZone: "us-east-1a", SizeGiB: 20,
		VolumeType: "gp3", IOPS: 3000, ThroughputMiBPS: 125,
		KMSKeyID: "alias/dtx-agent-test", DeviceName: "/dev/sdf", MountPath: "/srv/data",
		Persistent: true, Disposition: "retain_with_managed_service",
	}}
	sourceSpecDigest, err := ManagedPreparationVolumeSourceSpecDigest(scope.Volumes[0])
	if err != nil {
		t.Fatal(err)
	}
	scope.SourceVolumes[0].SpecDigest = sourceSpecDigest
	scope.Volumes[0].SourceVolume.SpecDigest = sourceSpecDigest
	challengeID, signerKeyID := uuid.NewString(), "device-managed-preparation"
	payload, err := managedPreparationApprovalSigningPayload(managedPreparationApprovalV1{
		SchemaVersion: managedPreparationChallengeSchema, ChallengeID: challengeID, OperationID: operationID,
		SignerKeyID: signerKeyID, Scope: scope, IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute), OperationRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	return AgentCloudManagedPreparationChallenge{
		SchemaVersion: managedPreparationChallengeSchema, ChallengeID: challengeID, OperationID: operationID, SignerKeyID: signerKeyID,
		ScopeDigest: "sha256:" + hexString(sum[:]), Scope: scope, IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute),
		SigningPayloadCBOR: payload, Revision: 1,
	}
}

func managedPreparationTestChallengeV2(t *testing.T, now time.Time, deploymentID, operationID string) AgentCloudManagedPreparationChallenge {
	t.Helper()
	challenge := managedPreparationTestChallenge(t, now, deploymentID, operationID)
	challenge.SchemaVersion = managedPreparationChallengeSchemaV2
	challenge.Scope.SchemaVersion = managedPreparationScopeSchemaV2
	challenge.Scope.Volumes[0].SnapshotOperationKey = "snapshot-data"
	challenge.Scope.Volumes[0].SnapshotSourceVolumeScopeDigest = managementDigest("f")
	challenge.Scope.Volumes[0].SnapshotMaxRetentionSeconds = 3600
	payload, err := managedPreparationApprovalSigningPayload(managedPreparationApprovalV1{
		SchemaVersion: challenge.SchemaVersion, ChallengeID: challenge.ChallengeID, OperationID: challenge.OperationID,
		SignerKeyID: challenge.SignerKeyID, Scope: challenge.Scope, IssuedAt: challenge.IssuedAt,
		ExpiresAt: challenge.ExpiresAt, OperationRevision: challenge.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	challenge.ScopeDigest = "sha256:" + hexString(sum[:])
	challenge.SigningPayloadCBOR = payload
	return challenge
}

func managedPreparationTestOperation(challenge AgentCloudManagedPreparationChallenge, now time.Time) AgentCloudManagedPreparationOperation {
	steps := make([]AgentCloudManagedPreparationStep, 0, len(managedPreparationPhases))
	for index, phase := range managedPreparationPhases {
		started, completed := now.Add(time.Duration(index+1)*time.Minute), now.Add(time.Duration(index+1)*time.Minute+time.Second)
		steps = append(steps, AgentCloudManagedPreparationStep{
			Phase: phase, Ordinal: int32(index + 1), Status: "succeeded", Revision: 2, IntentDigest: managementDigest("a"),
			StartedAt: &started, CompletedAt: &completed,
		})
	}
	return AgentCloudManagedPreparationOperation{
		OperationID: challenge.OperationID, Challenge: challenge, Status: "succeeded", CurrentPhase: "finalize", Revision: 14,
		Steps: steps, CreatedAt: now, UpdatedAt: now.Add(10 * time.Minute),
		Result: &AgentCloudManagedPreparationResult{
			PreparationID: uuid.NewString(), PreparationDigest: managementDigest("b"),
			FreshHealthDigest: managementDigest("c"), FreshHealthRevision: 9, FreshHealthObservedAt: now.Add(7 * time.Minute),
			CostDigest: managementDigest("d"), CostPolicyRevision: 4, CostObservedAt: now.Add(8 * time.Minute),
			StackDigest: managementDigest("e"), StackRevision: 11, StackObservedAt: now.Add(9 * time.Minute),
		},
	}
}

func hexString(value []byte) string {
	const digits = "0123456789abcdef"
	result := make([]byte, len(value)*2)
	for index, item := range value {
		result[index*2], result[index*2+1] = digits[item>>4], digits[item&0x0f]
	}
	return string(result)
}
