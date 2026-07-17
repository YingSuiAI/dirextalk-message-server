package cloud

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type rootHelperKeyModuleClient struct {
	*agentControlModuleClient
	prepared       AgentRootHelperKeyApproval
	approved       AgentRootHelperKeyApproval
	prepareRequest AgentRootHelperKeyPrepareRequest
	approveRequest AgentRootHelperKeyApproveRequest
	prepareErr     error
	approveErr     error
	getErr         error
	getFound       bool
	approveCalls   int
	getCalls       int
}

func (client *rootHelperKeyModuleClient) PrepareRootHelperKey(_ context.Context, request AgentRootHelperKeyPrepareRequest) (AgentRootHelperKeyApproval, error) {
	client.prepareRequest = request
	return client.prepared, client.prepareErr
}

func (client *rootHelperKeyModuleClient) ApproveRootHelperKey(_ context.Context, request AgentRootHelperKeyApproveRequest) (AgentRootHelperKeyApproval, error) {
	client.approveCalls++
	client.approveRequest = request
	return client.approved, client.approveErr
}

func (client *rootHelperKeyModuleClient) GetRootHelperKey(_ context.Context, request AgentRootHelperKeyGetRequest) (AgentRootHelperKeyApproval, bool, error) {
	client.getCalls++
	if request.DeliveryID != client.prepared.Binding.DeliveryID ||
		request.DeploymentID != client.prepared.Binding.DeploymentID {
		return AgentRootHelperKeyApproval{}, false, ErrAgentCloudControlInvalid
	}
	if !client.getFound {
		return AgentRootHelperKeyApproval{}, false, client.getErr
	}
	if client.getCalls == 1 {
		return client.prepared, true, client.getErr
	}
	return client.approved, true, client.getErr
}

func TestRootHelperKeyFacadePreservesSigningTransportAndRecoversWithoutPublishing(t *testing.T) {
	now := time.Date(2026, 7, 17, 15, 0, 0, 0, time.UTC)
	serviceID, deploymentID := "service-root-helper", uuid.NewString()
	prepared := rootHelperKeyTestApproval(t, now, deploymentID)
	approved := prepared
	approved.Status, approved.Revision, approved.DeviceSignature = "approved", 2, bytes.Repeat([]byte{0x41}, 64)
	approved.UpdatedAt = now.Add(time.Second)
	client := &rootHelperKeyModuleClient{
		agentControlModuleClient: &agentControlModuleClient{}, prepared: prepared, approved: approved,
		approveErr: ErrAgentCloudControlUnavailable, getFound: true,
	}
	store := &managedAcceptanceProjectionStore{compatibility: ManagedAcceptanceCompatibility{
		DeploymentID: deploymentID, DeploymentRevision: 7, SignerKeyID: prepared.DeviceSignerKeyID,
	}, found: true}
	published := 0
	module := New(store, Config{
		OwnerMXID: func() string { return prepared.Binding.OwnerID }, Now: func() time.Time { return now },
		Publish: func(context.Context, string, string, map[string]any) error {
			published++
			return nil
		},
		AgentCloudControlClient: client,
	})
	result, apiErr := module.Handlers()[actionServicesRootHelperKeyPrepare](t.Context(), map[string]any{
		"service_id": serviceID, "expected_revision": int64(7), "idempotency_key": uuid.NewString(),
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	confirmation := result.(map[string]any)["confirmation"].(map[string]any)
	public := confirmation["approval"].(rootHelperKeyPublicApproval)
	if public.Binding.SecretPlan.Name != prepared.Binding.SecretPlan.Name ||
		confirmation["signing_payload_cbor"] != base64.RawURLEncoding.EncodeToString(prepared.SigningPayloadCBOR) ||
		client.prepareRequest.DeploymentID != deploymentID || client.prepareRequest.ExpectedDeploymentRevision != 7 ||
		client.prepareRequest.DeviceSignerKeyID != prepared.DeviceSignerKeyID || published != 0 {
		t.Fatalf("prepare=%#v request=%#v published=%d", result, client.prepareRequest, published)
	}
	public.DeviceSignature = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x41}, 64))
	result, apiErr = module.Handlers()[actionServicesRootHelperKeyApprove](t.Context(), map[string]any{
		"service_id": serviceID, "expected_revision": int64(7), "approval": public, "idempotency_key": uuid.NewString(),
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	view := result.(map[string]any)["approval"].(map[string]any)
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	if client.getCalls != 2 || client.approveCalls != 1 || client.approveRequest.DeliveryID != prepared.Binding.DeliveryID ||
		len(client.approveRequest.DeviceSignature) != 64 || view["status"] != "approved" || published != 0 ||
		strings.Contains(string(encoded), prepared.Binding.SecretPlan.Name) ||
		strings.Contains(string(encoded), prepared.Binding.SecretPlan.KMSKeyARN) ||
		strings.Contains(string(encoded), public.PublicKey) || strings.Contains(string(encoded), public.Nonce) {
		t.Fatalf("approve=%#v request=%#v calls=%d/%d published=%d", result, client.approveRequest, client.approveCalls, client.getCalls, published)
	}
	replayed, apiErr := module.Handlers()[actionServicesRootHelperKeyApprove](t.Context(), map[string]any{
		"service_id": serviceID, "expected_revision": int64(7), "approval": public, "idempotency_key": uuid.NewString(),
	})
	if apiErr != nil || !reflect.DeepEqual(result, replayed) || client.approveCalls != 1 || client.getCalls != 3 {
		t.Fatalf("replay=%#v err=%v calls=%d/%d", replayed, apiErr, client.approveCalls, client.getCalls)
	}
	store.compatibility.DeploymentRevision = 8
	if gap, gapErr := module.Handlers()[actionServicesRootHelperKeyGet](t.Context(), map[string]any{
		"service_id": serviceID, "expected_revision": int64(7), "delivery_id": prepared.Binding.DeliveryID,
	}); gapErr == nil || gap != nil || client.getCalls != 3 {
		t.Fatalf("revision gap=%#v err=%v calls=%d", gap, gapErr, client.getCalls)
	}
}

func TestRootHelperKeyApproveGetErrorDoesNotSendSignature(t *testing.T) {
	now := time.Date(2026, 7, 17, 15, 0, 0, 0, time.UTC)
	serviceID, deploymentID := "service-root-helper", uuid.NewString()
	prepared := rootHelperKeyTestApproval(t, now, deploymentID)
	client := &rootHelperKeyModuleClient{
		agentControlModuleClient: &agentControlModuleClient{}, prepared: prepared, getErr: ErrAgentCloudControlUnavailable, getFound: true,
	}
	store := &managedAcceptanceProjectionStore{compatibility: ManagedAcceptanceCompatibility{
		DeploymentID: deploymentID, DeploymentRevision: 7, SignerKeyID: prepared.DeviceSignerKeyID,
	}, found: true}
	module := New(store, Config{
		OwnerMXID: func() string { return prepared.Binding.OwnerID }, AgentCloudControlClient: client,
	})
	public := rootHelperKeyApprovalToPublic(prepared)
	public.DeviceSignature = base64.RawURLEncoding.EncodeToString(make([]byte, 64))
	result, apiErr := module.Handlers()[actionServicesRootHelperKeyApprove](t.Context(), map[string]any{
		"service_id": serviceID, "expected_revision": int64(7), "approval": public, "idempotency_key": uuid.NewString(),
	})
	if apiErr == nil || apiErr.Status != 503 || result != nil || client.approveCalls != 0 {
		t.Fatalf("result=%#v err=%#v approve_calls=%d", result, apiErr, client.approveCalls)
	}
}

func rootHelperKeyTestApproval(t *testing.T, now time.Time, deploymentID string) AgentRootHelperKeyApproval {
	t.Helper()
	publicKey, nonce := bytes.Repeat([]byte{0x31}, 32), bytes.Repeat([]byte{0x32}, 32)
	publicDigest, nonceDigest := sha256.Sum256(publicKey), sha256.Sum256(nonce)
	agentID, deliveryID := uuid.NewString(), uuid.NewString()
	binding := AgentRootHelperKeyBinding{
		SchemaVersion: rootHelperKeySchema, AgentInstanceID: agentID, OwnerID: "@owner:example.com",
		DeliveryID: deliveryID, DeploymentID: deploymentID, BindingRevision: 7, InstanceID: "i-0123456789abcdef0",
		WorkerRoleARN:     "arn:aws:iam::123456789012:role/worker",
		WorkerPrincipalID: "AROATESTROLEIDENTIFIER:i-0123456789abcdef0", HelperID: "root-helper",
		SignerKeyID: "helper-key-7", PublicKeyDigest: "sha256:" + hex.EncodeToString(publicDigest[:]),
		SecretPlan: AgentRootHelperKeySecretPlan{
			Partition: "aws", AccountID: "123456789012", Region: "us-west-2",
			Name:      "dtx/" + agentID + "/deployments/" + deploymentID + "/__dirextalk_root_helper_key",
			VersionID: deliveryID, KMSKeyARN: "arn:aws:kms:us-west-2:123456789012:key/abcd",
			TargetPath: "/etc/dirextalk-root-helper/signing.key", FileMode: 0o400,
		},
		NonceDigest: "sha256:" + hex.EncodeToString(nonceDigest[:]),
	}
	payload, err := rootHelperKeyBindingSigningPayload(binding)
	if err != nil {
		t.Fatal(err)
	}
	return AgentRootHelperKeyApproval{
		SchemaVersion: rootHelperKeyApprovalSchema, ChallengeID: uuid.NewString(), DeviceSignerKeyID: "device-root-helper",
		Binding: binding, PublicKey: publicKey, Nonce: nonce, SigningPayloadCBOR: payload,
		SigningPayloadDigest: digestRootHelperKeyPayload(payload), Status: "awaiting_approval", Revision: 1,
		CreatedAt: now, UpdatedAt: now,
	}
}
