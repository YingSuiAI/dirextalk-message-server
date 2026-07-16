package cloud

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

type agentControlModuleClient struct {
	plan                AgentCloudPlan
	listedPlans         []AgentCloudPlan
	planFound           bool
	getPlanErr          error
	challenge           AgentCloudChallenge
	challengeErr        error
	challengeRequest    AgentCloudChallengeRequest
	approveRequest      AgentCloudApproveRequest
	approveErr          error
	approveToApproved   bool
	establishRequest    AgentCloudEstablishRequest
	establishConnection AgentCloudConnection
	establishErr        error
	recoveredConnection AgentCloudConnection
	listedConnections   []AgentCloudConnection
	recoveredFound      bool
	recoveredErr        error
	getPlanCalls        int
	challengeCalls      int
	approveCalls        int
	establishCalls      int
	getConnectionCalls  int
	listConnectionCalls int
}

func (client *agentControlModuleClient) ListAgentCloudPlans(context.Context) ([]AgentCloudPlan, error) {
	return append([]AgentCloudPlan(nil), client.listedPlans...), client.getPlanErr
}

func (client *agentControlModuleClient) ListAgentCloudConnections(context.Context) ([]AgentCloudConnection, error) {
	client.listConnectionCalls++
	return append([]AgentCloudConnection(nil), client.listedConnections...), client.recoveredErr
}

func (client *agentControlModuleClient) GetAgentCloudPlan(_ context.Context, request AgentCloudPlanRequest) (AgentCloudPlan, bool, error) {
	client.getPlanCalls++
	if request.PlanID != client.plan.PlanID {
		return AgentCloudPlan{}, false, ErrAgentCloudControlInvalid
	}
	return client.plan, client.planFound, client.getPlanErr
}

func (client *agentControlModuleClient) CreateAgentCloudApprovalChallenge(_ context.Context, request AgentCloudChallengeRequest) (AgentCloudChallenge, error) {
	client.challengeCalls++
	client.challengeRequest = request
	return client.challenge, client.challengeErr
}

func (client *agentControlModuleClient) ApproveAgentCloudPlan(_ context.Context, request AgentCloudApproveRequest) (AgentCloudPlan, error) {
	client.approveCalls++
	client.approveRequest = request
	if client.approveToApproved {
		client.plan = approvedAgentPlan(client.plan)
	}
	return client.plan, client.approveErr
}

func (client *agentControlModuleClient) EstablishAgentAWSConnection(_ context.Context, request AgentCloudEstablishRequest) (AgentCloudConnection, error) {
	client.establishCalls++
	client.establishRequest = request
	return client.establishConnection, client.establishErr
}

func (client *agentControlModuleClient) GetAgentCloudConnection(_ context.Context, request AgentCloudConnectionRequest) (AgentCloudConnection, bool, error) {
	client.getConnectionCalls++
	if request.ConnectionID != client.plan.ConnectionID {
		return AgentCloudConnection{}, false, ErrAgentCloudControlInvalid
	}
	return client.recoveredConnection, client.recoveredFound, client.recoveredErr
}

type agentProvenanceStore struct {
	Store
	localPlan          Plan
	localQuote         QuoteView
	localConnection    Connection
	prepareCalls       int
	approveCalls       int
	registrationCalls  int
	getPlanCalls       int
	getConnectionCalls int
}

func (store *agentProvenanceStore) ListCloudPlans(context.Context) ([]Plan, error) {
	return []Plan{store.localPlan}, nil
}

func (store *agentProvenanceStore) GetCloudPlan(_ context.Context, id string) (Plan, bool, error) {
	store.getPlanCalls++
	return store.localPlan, id == store.localPlan.PlanID, nil
}

func (store *agentProvenanceStore) GetCloudQuote(_ context.Context, id string) (QuoteView, bool, error) {
	return store.localQuote, id == store.localQuote.QuoteID, nil
}

func (store *agentProvenanceStore) ListCloudConnections(context.Context) ([]Connection, error) {
	return []Connection{store.localConnection}, nil
}

func (store *agentProvenanceStore) GetCloudConnection(_ context.Context, id string) (Connection, bool, error) {
	store.getConnectionCalls++
	return store.localConnection, id == store.localConnection.ConnectionID, nil
}

func (store *agentProvenanceStore) PrepareCloudPlanConfirmation(_ context.Context, request PreparePlanConfirmationRequest) (PreparePlanConfirmationResult, error) {
	store.prepareCalls++
	return PreparePlanConfirmationResult{Confirmation: PlanConfirmation{Plan: store.localPlan}}, nil
}

func (store *agentProvenanceStore) ApproveCloudPlan(_ context.Context, request ApproveCloudPlanRequest) (ApproveCloudPlanResult, error) {
	store.approveCalls++
	return ApproveCloudPlanResult{Plan: store.localPlan, Deployment: request.Deployment, Job: request.Job}, nil
}

func (store *agentProvenanceStore) CompleteCloudConnectionBootstrap(_ context.Context, request CompleteConnectionBootstrapRequest) (CompleteConnectionBootstrapResult, error) {
	store.registrationCalls++
	return CompleteConnectionBootstrapResult{Bootstrap: ConnectionBootstrap{
		BootstrapID: request.BootstrapID, ConnectionID: store.localConnection.ConnectionID,
		Status: ConnectionBootstrapVerificationQueued, Revision: request.ExpectedRevision + 1, JobID: request.Job.JobID,
	}}, nil
}

func TestAgentPlanConfirmationReturnsExactUnsignedDescriptorAndRejectsClientScope(t *testing.T) {
	now := time.Date(2026, time.July, 16, 8, 0, 0, 0, time.UTC)
	plan := readyAgentPlan(now)
	client := &agentControlModuleClient{plan: plan, planFound: true}
	client.challenge = challengeForAgentPlan(plan, now)
	module := New(nil, Config{Now: func() time.Time { return now }, AgentCloudControlClient: client})
	params := map[string]any{
		"plan_id": plan.PlanID, "expected_revision": float64(plan.Revision), "quote_id": plan.QuoteID,
		"candidate_tier": "economy", "signer_key_id": client.challenge.SignerKeyID,
		"idempotency_key": "019f6a80-1234-7abc-8def-0123456789ab",
	}
	result, apiErr := module.Handlers()[actionPlansConfirmationPrepare](t.Context(), params)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	confirmation := result.(map[string]any)["confirmation"].(map[string]any)
	approval := confirmation["approval"].(agentCloudApprovalV1)
	if approval.SchemaVersion != agentCloudApprovalSchema || approval.ConnectionID != plan.ConnectionID || approval.QuoteCandidateID != "economic" ||
		approval.ResourceScope.Region != plan.Resource.Region || approval.RetentionScope.Class != "ephemeral" || approval.Signature != "" {
		t.Fatalf("unsigned Agent approval descriptor=%#v", approval)
	}
	encoded, err := json.Marshal(approval)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err = json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if _, present := wire["signature"]; present {
		t.Fatalf("unsigned descriptor exposed a signature field: %s", encoded)
	}
	resource := wire["resource_scope"].(map[string]any)
	retention := wire["retention_scope"].(map[string]any)
	if _, present := resource["candidate_profile"]; present || retention["class"] != "ephemeral" || wire["connection_id"] != plan.ConnectionID {
		t.Fatalf("descriptor diverged from Agent ApprovalV1 tags: %s", encoded)
	}
	if client.challengeCalls != 1 || !reflect.DeepEqual(client.challengeRequest.ExpectedPlan, plan) || client.challengeRequest.SignerKeyID != client.challenge.SignerKeyID ||
		confirmation["signing_payload_cbor"] != base64.RawURLEncoding.EncodeToString(client.challenge.SigningPayloadCBOR) {
		t.Fatalf("challenge request=%#v confirmation=%#v", client.challengeRequest, confirmation)
	}

	injected := cloneAgentParams(params)
	injected["connection_id"] = "019f6a80-1234-7abc-8def-0123456789ac"
	if _, apiErr = module.Handlers()[actionPlansConfirmationPrepare](t.Context(), injected); apiErr == nil || client.challengeCalls != 1 {
		t.Fatalf("client connection scope was accepted: err=%#v calls=%d", apiErr, client.challengeCalls)
	}
	injected = cloneAgentParams(params)
	injected["owner_id"] = "another-owner"
	if _, apiErr = module.Handlers()[actionPlansConfirmationPrepare](t.Context(), injected); apiErr == nil || client.challengeCalls != 1 {
		t.Fatalf("client owner scope was accepted: err=%#v calls=%d", apiErr, client.challengeCalls)
	}
}

func TestAgentPlanApprovalRecoversDurableApprovedPlanWithoutFabricatedWork(t *testing.T) {
	now := time.Date(2026, time.July, 16, 8, 0, 0, 0, time.UTC)
	ready := readyAgentPlan(now)
	challenge := challengeForAgentPlan(ready, now)
	client := &agentControlModuleClient{
		plan: ready, planFound: true, challenge: challenge,
		approveErr: ErrAgentCloudControlConflict, approveToApproved: true,
	}
	clock := now
	module := New(nil, Config{Now: func() time.Time { return clock }, AgentCloudControlClient: client})
	approval := agentApprovalFromChallenge(ready, challenge)
	approval.Signature = base64.RawURLEncoding.EncodeToString(make([]byte, 64))
	params := map[string]any{
		"plan_id": ready.PlanID, "expected_revision": float64(ready.Revision), "approval": approval,
		"idempotency_key": "019f6a80-1234-7abc-8def-0123456789ad",
	}
	result, apiErr := module.Handlers()[actionPlansApprove](t.Context(), params)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	response := result.(map[string]any)
	if response["submission_status"] != "waiting_connection" || response["plan"] == nil || response["deployment"] != nil || response["job"] != nil ||
		client.approveCalls != 1 || client.getPlanCalls != 2 {
		t.Fatalf("approval recovery response=%#v client=%#v", response, client)
	}
	if client.approveRequest.IdempotencyKey != "019f6a80-1234-7abc-8def-0123456789ad" ||
		client.approveRequest.Approval.SignerKeyID != challenge.SignerKeyID || len(client.approveRequest.Approval.Signature) != 64 {
		t.Fatalf("approval RPC request=%#v", client.approveRequest)
	}

	clock = challenge.ExpiresAt.Add(time.Second)
	client.getPlanCalls, client.approveCalls = 0, 0
	replayed, replayErr := module.Handlers()[actionPlansApprove](t.Context(), params)
	if replayErr != nil || replayed.(map[string]any)["submission_status"] != "waiting_connection" || client.getPlanCalls != 1 || client.approveCalls != 0 {
		t.Fatalf("durable approval replay=%#v err=%#v client=%#v", replayed, replayErr, client)
	}

	invalid := approvalMapForAgentTest(t, approval)
	invalid["owner_mxid"] = "@owner:example.com"
	clock = now
	client.plan = ready
	client.getPlanCalls, client.approveCalls = 0, 0
	if _, apiErr = module.Handlers()[actionPlansApprove](t.Context(), map[string]any{
		"plan_id": ready.PlanID, "expected_revision": float64(ready.Revision), "approval": invalid,
		"idempotency_key": "019f6a80-1234-7abc-8def-0123456789ae",
	}); apiErr == nil || client.getPlanCalls != 0 || client.approveCalls != 0 {
		t.Fatalf("approval with unknown field reached Agent: err=%#v get=%d approve=%d", apiErr, client.getPlanCalls, client.approveCalls)
	}
}

func TestAgentConnectionEstablishBindsRolePlanAndReconcilesUnknownResult(t *testing.T) {
	now := time.Date(2026, time.July, 16, 8, 0, 0, 0, time.UTC)
	ready := readyAgentPlan(now)
	approved := approvedAgentPlan(ready)
	challenge := challengeForAgentPlan(ready, now)
	approval := agentApprovalFromChallenge(ready, challenge)
	approval.Signature = base64.RawURLEncoding.EncodeToString(make([]byte, 64))
	rolePlan := ConnectionRolePlan{
		BootstrapID: "bootstrap-agent-control-0001", CloudConnectionID: ready.ConnectionID, Provider: "aws", Region: ready.Resource.Region,
		Status: ConnectionBootstrapAwaitingStack, Revision: 1, ExpiresAt: now.Add(15 * time.Minute).UnixMilli(), AllowRootCredentialBootstrap: true,
		CloudFormationParams: map[string]string{"DeviceApprovalKeyId": challenge.SignerKeyID},
	}
	store := &credentialBootstrapModuleStore{plan: rolePlan}
	client := &agentControlModuleClient{plan: approved, planFound: true, establishErr: ErrAgentCloudControlUnavailable}
	clock := now
	module := New(store, Config{
		OwnerMXID: func() string { return "@owner:example.com" }, Now: func() time.Time { return clock }, AgentCloudControlClient: client,
	})
	params := map[string]any{
		"bootstrap_id": rolePlan.BootstrapID, "expected_revision": float64(rolePlan.Revision),
		"session_id": "019f6a80-1234-7abc-8def-0123456789af", "expected_session_revision": float64(2),
		"plan_id": approved.PlanID, "expected_plan_revision": float64(approved.Revision), "approval": approval,
		"idempotency_key": "019f6a80-1234-7abc-8def-0123456789b0",
	}
	clock = challenge.ExpiresAt.Add(time.Second)
	result, apiErr := module.Handlers()[actionConnectionsRegistrationComplete](t.Context(), params)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	connection := result.(map[string]any)["connection"].(map[string]any)
	if connection["status"] != agentCloudPendingReconciliation || connection["cloud_connection_id"] != ready.ConnectionID ||
		client.establishCalls != 1 || client.getConnectionCalls != 1 || store.calls != 2 {
		t.Fatalf("unknown establish result=%#v client=%#v role-plan reads=%d", result, client, store.calls)
	}
	if len(store.loadRequests) != 2 || store.loadRequests[0].Now != store.loadRequests[1].Now {
		t.Fatalf("role-plan read-back changed its authorization instant: %#v", store.loadRequests)
	}
	if client.establishRequest.ExpectedConnectionID != ready.ConnectionID || client.establishRequest.ExpectedRegion != ready.Resource.Region ||
		client.establishRequest.Approval.SignerKeyID != challenge.SignerKeyID || client.establishRequest.IdempotencyKey != params["idempotency_key"] {
		t.Fatalf("establish request was not server-bound: %#v", client.establishRequest)
	}

	client.recoveredConnection = activeAgentConnection(approved, now)
	client.recoveredConnection.Status = "degraded"
	client.recoveredFound = true
	store.calls, store.loadRequests, client.establishCalls, client.getConnectionCalls = 0, nil, 0, 0
	result, apiErr = module.Handlers()[actionConnectionsRegistrationComplete](t.Context(), params)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	connection = result.(map[string]any)["connection"].(map[string]any)
	if connection["status"] != agentCloudPendingReconciliation || client.getConnectionCalls != 1 {
		t.Fatalf("non-active read-back was exposed as established: %#v", result)
	}

	client.recoveredConnection = activeAgentConnection(approved, now)
	client.recoveredFound = true
	store.calls, store.loadRequests, client.establishCalls, client.getConnectionCalls = 0, nil, 0, 0
	result, apiErr = module.Handlers()[actionConnectionsRegistrationComplete](t.Context(), params)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	connection = result.(map[string]any)["connection"].(map[string]any)
	if connection["status"] != "active" || connection["cloud_connection_id"] != ready.ConnectionID || client.establishCalls != 1 || client.getConnectionCalls != 1 || store.calls != 2 {
		t.Fatalf("establish read-back response=%#v", result)
	}

	wrongSigner := approval
	wrongSigner.SignerKeyID = "cloud-device-ffffffffffffffffffffffff"
	store.calls, client.establishCalls = 0, 0
	bad := cloneAgentParams(params)
	bad["approval"] = wrongSigner
	if _, apiErr = module.Handlers()[actionConnectionsRegistrationComplete](t.Context(), bad); apiErr == nil || client.establishCalls != 0 || store.calls != 1 {
		t.Fatalf("role-plan signer substitution reached Agent: err=%#v calls=%d reads=%d", apiErr, client.establishCalls, store.calls)
	}
}

func TestAgentConnectionGetProvidesOwnerBoundUnknownResultReadBack(t *testing.T) {
	now := time.Date(2026, time.July, 16, 8, 0, 0, 0, time.UTC)
	plan := approvedAgentPlan(readyAgentPlan(now))
	connection := activeAgentConnection(plan, now)
	connection.Status = "establishing"
	client := &agentControlModuleClient{plan: plan, recoveredConnection: connection, recoveredFound: true}
	module := New(nil, Config{AgentCloudControlClient: client})

	result, apiErr := module.Handlers()[actionConnectionsGet](t.Context(), map[string]any{"cloud_connection_id": connection.ConnectionID})
	if apiErr != nil || result.(map[string]any)["status"] != "establishing" || client.getConnectionCalls != 1 {
		t.Fatalf("Agent connection read-back=%#v err=%#v calls=%d", result, apiErr, client.getConnectionCalls)
	}

	client.recoveredConnection.Status = "unknown"
	if _, apiErr = module.Handlers()[actionConnectionsGet](t.Context(), map[string]any{"cloud_connection_id": connection.ConnectionID}); apiErr == nil || apiErr.Status != http.StatusBadGateway {
		t.Fatalf("invalid Agent connection status was exposed: %#v", apiErr)
	}
}

func TestAgentCloudControlRoutesByEntityProvenanceAndMergesConnectionList(t *testing.T) {
	now := time.Date(2026, time.July, 16, 8, 0, 0, 0, time.UTC)
	agentPlan := readyAgentPlan(now)
	agentConnection := activeAgentConnection(agentPlan, now)
	localPlan := Plan{
		PlanID: "plan-local-0001", ConnectionID: "connection-local-0001", QuoteID: "quote-local-0001",
		Status: "ready_for_confirmation", PlanHash: "sha256:" + strings.Repeat("a", 64), Revision: 3,
		CreatedAt: now.UnixMilli(), UpdatedAt: now.UnixMilli(),
	}
	localConnection := Connection{
		ConnectionID: localPlan.ConnectionID, Provider: "aws", AccountID: "210987654321", Region: "us-east-1",
		Mode: "connection_stack_v2", Status: "active", Revision: 2, CreatedAt: now.UnixMilli(), UpdatedAt: now.UnixMilli(),
	}
	store := &agentProvenanceStore{
		localPlan: localPlan, localQuote: QuoteView{QuoteID: localPlan.QuoteID, ConnectionID: localPlan.ConnectionID, ValidUntil: now.Add(10 * time.Minute)},
		localConnection: localConnection,
	}
	client := &agentControlModuleClient{
		plan: agentPlan, planFound: true, recoveredConnection: agentConnection, recoveredFound: true,
		listedPlans: []AgentCloudPlan{agentPlan}, listedConnections: []AgentCloudConnection{agentConnection},
	}
	module := New(store, Config{
		OwnerMXID: func() string { return "@owner:example.com" }, Now: func() time.Time { return now }, AgentCloudControlClient: client,
	})

	listed, apiErr := module.Handlers()[actionConnectionsList](t.Context(), map[string]any{})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	connections := listed.(map[string]any)["connections"].([]Connection)
	if len(connections) != 2 || connections[0].ConnectionID != localConnection.ConnectionID || connections[1].ConnectionID != agentConnection.ConnectionID ||
		connections[1].Mode != agentCloudConnectionMode || client.listConnectionCalls != 1 {
		t.Fatalf("merged connections=%#v client=%#v", connections, client)
	}

	localRead, apiErr := module.Handlers()[actionConnectionsGet](t.Context(), map[string]any{"cloud_connection_id": localConnection.ConnectionID})
	if apiErr != nil || localRead.(Connection).ConnectionID != localConnection.ConnectionID || store.getConnectionCalls != 1 || client.getConnectionCalls != 0 {
		t.Fatalf("local connection read=%#v err=%#v store=%d agent=%d", localRead, apiErr, store.getConnectionCalls, client.getConnectionCalls)
	}
	agentRead, apiErr := module.Handlers()[actionConnectionsGet](t.Context(), map[string]any{"cloud_connection_id": agentConnection.ConnectionID})
	if apiErr != nil || agentRead.(map[string]any)["cloud_connection_id"] != agentConnection.ConnectionID || client.getConnectionCalls != 1 {
		t.Fatalf("Agent connection read=%#v err=%#v calls=%d", agentRead, apiErr, client.getConnectionCalls)
	}

	localPlanRead, apiErr := module.Handlers()[actionPlansGet](t.Context(), map[string]any{"plan_id": localPlan.PlanID})
	if apiErr != nil || localPlanRead.(Plan).PlanID != localPlan.PlanID || store.getPlanCalls != 1 {
		t.Fatalf("local plan read=%#v err=%#v store=%d", localPlanRead, apiErr, store.getPlanCalls)
	}
	agentPlanRead, apiErr := module.Handlers()[actionPlansGet](t.Context(), map[string]any{"plan_id": agentPlan.PlanID})
	if apiErr != nil || agentPlanRead.(map[string]any)["plan_id"] != agentPlan.PlanID || client.getPlanCalls != 1 {
		t.Fatalf("Agent plan read=%#v err=%#v calls=%d", agentPlanRead, apiErr, client.getPlanCalls)
	}
	listedPlans, apiErr := module.Handlers()[actionPlansList](t.Context(), map[string]any{})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	plans := listedPlans.(map[string]any)["plans"].([]Plan)
	if len(plans) != 2 || plans[0].PlanID != localPlan.PlanID || plans[1].PlanID != agentPlan.PlanID || plans[1].ConnectionID != agentPlan.ConnectionID || plans[1].Revision != agentPlan.Revision {
		t.Fatalf("merged plans=%#v", plans)
	}

	client.getPlanCalls = 0
	prepared, apiErr := module.Handlers()[actionPlansConfirmationPrepare](t.Context(), map[string]any{
		"plan_id": localPlan.PlanID, "expected_revision": float64(localPlan.Revision), "quote_id": localPlan.QuoteID,
		"candidate_tier": "economy", "idempotency_key": "019f6a80-1234-7abc-8def-0123456789d1",
	})
	if apiErr != nil || prepared == nil || store.prepareCalls != 1 || client.getPlanCalls != 0 {
		t.Fatalf("local prepare=%#v err=%#v store=%d agent=%d", prepared, apiErr, store.prepareCalls, client.getPlanCalls)
	}

	approval := cloudcontracts.ApprovalV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1, ApprovalID: "approval-local-0001", ChallengeID: "challenge-local-0001", SignerKeyID: "device-key-local",
		PlanID: localPlan.PlanID, PlanHash: localPlan.PlanHash, PlanRevision: uint64(localPlan.Revision), QuoteID: localPlan.QuoteID,
		QuoteDigest: "sha256:" + strings.Repeat("b", 64), QuoteValidUntil: now.Add(10 * time.Minute), CloudConnectionID: localConnection.ConnectionID,
		RecipeDigest: "sha256:" + strings.Repeat("c", 64),
		ResourceScope: cloudcontracts.ResourceScopeV1{
			Region: localConnection.Region, AvailabilityZones: []string{"us-east-1a"}, InstanceType: "t3.large", Architecture: cloudcontracts.ArchitectureAMD64,
			VCPU: 2, MemoryMiB: 8192, DiskGiB: 40, PurchaseOption: cloudcontracts.PurchaseOnDemand,
		},
		NetworkScope: cloudcontracts.NetworkScopeV1{EntryPoint: cloudcontracts.EntryPointNone}, ExpiresAt: now.Add(5 * time.Minute),
		Signature: base64.RawURLEncoding.EncodeToString(make([]byte, 64)),
	}
	approved, apiErr := module.Handlers()[actionPlansApprove](t.Context(), map[string]any{
		"plan_id": localPlan.PlanID, "expected_revision": float64(localPlan.Revision), "approval": approval,
		"idempotency_key": "019f6a80-1234-7abc-8def-0123456789d2",
	})
	if apiErr != nil || approved == nil || store.approveCalls != 1 || client.approveCalls != 0 {
		t.Fatalf("local approve=%#v err=%#v store=%d agent=%d", approved, apiErr, store.approveCalls, client.approveCalls)
	}

	registered, apiErr := module.Handlers()[actionConnectionsRegistrationComplete](t.Context(), map[string]any{
		"bootstrap_id": "bootstrap-local-0001", "expected_revision": float64(1),
		"idempotency_key":    "019f6a80-1234-7abc-8def-0123456789d3",
		"broker_command_url": "https://broker.example.com/command", "stack_arn": "arn:aws:cloudformation:us-east-1:210987654321:stack/local/00000000-0000-0000-0000-000000000001",
	})
	if apiErr != nil || registered == nil || store.registrationCalls != 1 || client.establishCalls != 0 {
		t.Fatalf("local registration=%#v err=%#v store=%d agent=%d", registered, apiErr, store.registrationCalls, client.establishCalls)
	}

	localPlanReads, localConnectionReads := store.getPlanCalls, store.getConnectionCalls
	agentless := New(store, Config{OwnerMXID: func() string { return "@owner:example.com" }, Now: func() time.Time { return now }})
	for name, request := range map[string]struct {
		action string
		params map[string]any
	}{
		"plan get":                {actionPlansGet, map[string]any{"plan_id": agentPlan.PlanID}},
		"connection get":          {actionConnectionsGet, map[string]any{"cloud_connection_id": agentConnection.ConnectionID}},
		"plan prepare":            {actionPlansConfirmationPrepare, map[string]any{"plan_id": agentPlan.PlanID}},
		"plan approve":            {actionPlansApprove, map[string]any{"plan_id": agentPlan.PlanID}},
		"connection registration": {actionConnectionsRegistrationComplete, map[string]any{"plan_id": agentPlan.PlanID}},
	} {
		t.Run("agentless "+name, func(t *testing.T) {
			if _, err := agentless.Handlers()[request.action](t.Context(), request.params); err == nil || err.Status != http.StatusServiceUnavailable {
				t.Fatalf("canonical Agent entity fell back to local source: %#v", err)
			}
		})
	}
	if store.getPlanCalls != localPlanReads || store.getConnectionCalls != localConnectionReads ||
		store.prepareCalls != 1 || store.approveCalls != 1 || store.registrationCalls != 1 {
		t.Fatalf("canonical Agent requests mutated local source: %#v", store)
	}
}

func readyAgentPlan(now time.Time) AgentCloudPlan {
	return AgentCloudPlan{
		PlanID: "019f6a80-1234-7abc-8def-012345678901", OwnerID: "dirextalk-project:example.com",
		ConnectionID: "019f6a80-1234-7abc-8def-012345678902",
		Recipe:       AgentCloudRecipeBinding{RecipeID: "recipe-openclaw-0001", Digest: "sha256:" + repeatAgentHex("1"), Maturity: "experimental"},
		QuoteID:      "019f6a80-1234-7abc-8def-012345678903", QuoteDigest: "sha256:" + repeatAgentHex("2"),
		QuoteScopeDigest: "sha256:" + repeatAgentHex("3"), CandidateProfile: "economic", QuoteValidUntil: now.Add(10 * time.Minute),
		Resource: AgentCloudResourceScope{
			Region: "ap-northeast-1", AvailabilityZones: []string{"ap-northeast-1a"}, InstanceType: "t3.large", InstanceCount: 1,
			Architecture: "amd64", VCPU: 2, MemoryMiB: 8192, DiskGiB: 40, VolumeType: "gp3", VolumeEncrypted: true,
			PurchaseOption: "on_demand", WorkerImageID: "ami-0123456789abcdef0", WorkerImageDigest: "sha256:" + repeatAgentHex("4"),
		},
		Network:          AgentCloudNetworkScope{EntryPoint: "none", TLSRequired: true, AuthenticationRequired: true},
		SecretScope:      []AgentCloudSecretScope{{SecretRef: "secret-ref-openclaw", Purpose: "service_token", Delivery: "worker_runtime"}},
		IntegrationScope: []AgentCloudIntegrationScope{{Kind: "grpc", Name: "worker-control", Scopes: []string{"checkpoint"}}},
		Retention:        AgentCloudRetentionScope{Class: "ephemeral", AutoDestroy: true, GracePeriodSeconds: 1800, MaxLifetimeSeconds: 86400},
		Status:           agentCloudReadyForConfirmation, PlanHash: "sha256:" + repeatAgentHex("5"), Revision: 7,
	}
}

func challengeForAgentPlan(plan AgentCloudPlan, now time.Time) AgentCloudChallenge {
	return AgentCloudChallenge{
		ApprovalID: "019f6a80-1234-7abc-8def-012345678904", ChallengeID: "challenge_agent_control_0001",
		SignerKeyID: "cloud-device-0123456789abcdef01234567", AgentInstanceID: "agent-instance-0001", OwnerID: plan.OwnerID,
		PlanID: plan.PlanID, PlanRevision: plan.Revision, PlanHash: plan.PlanHash, ConnectionID: plan.ConnectionID,
		RecipeDigest: plan.Recipe.Digest, QuoteID: plan.QuoteID, QuoteDigest: plan.QuoteDigest,
		QuoteScopeDigest: plan.QuoteScopeDigest, QuoteCandidateID: plan.CandidateProfile,
		ExpiresAt: now.Add(5 * time.Minute), Revision: 1, SigningPayloadCBOR: []byte{0xa1, 0x01, 0x02},
	}
}

func approvedAgentPlan(value AgentCloudPlan) AgentCloudPlan {
	value.Status = AgentCloudPlanStatusApproved
	value.Revision++
	value.PlanHash = "sha256:" + repeatAgentHex("6")
	return value
}

func activeAgentConnection(plan AgentCloudPlan, now time.Time) AgentCloudConnection {
	return AgentCloudConnection{
		ConnectionID: plan.ConnectionID, OwnerID: plan.OwnerID, AccountID: "123456789012", Region: plan.Resource.Region,
		ControlRoleARN: "arn:aws:iam::123456789012:role/dirextalk-agent-control", FoundationStackID: "arn:aws:cloudformation:ap-northeast-1:123456789012:stack/dirextalk-agent/stack-id",
		Status: "active", Revision: 1, CredentialGeneration: 1, CreatedAt: now, UpdatedAt: now,
	}
}

func approvalMapForAgentTest(t *testing.T, value agentCloudApprovalV1) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err = json.Unmarshal(encoded, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func cloneAgentParams(values map[string]any) map[string]any {
	result := make(map[string]any, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func repeatAgentHex(value string) string {
	return value + value + value + value + value + value + value + value + value + value + value + value + value + value + value + value +
		value + value + value + value + value + value + value + value + value + value + value + value + value + value + value + value +
		value + value + value + value + value + value + value + value + value + value + value + value + value + value + value + value +
		value + value + value + value + value + value + value + value + value + value + value + value + value + value + value + value
}
