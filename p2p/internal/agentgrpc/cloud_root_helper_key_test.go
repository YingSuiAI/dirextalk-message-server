package agentgrpc

import (
	"bytes"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRootHelperKeyAdapterMatchesAgentCBORGoldenAndRejectsDrift(t *testing.T) {
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	binding := rootHelperGoldenBinding()
	contextValue := strings.Join([]string{
		binding.SchemaVersion, binding.AgentInstanceID, binding.OwnerID, binding.DeliveryID,
		binding.DeploymentID, binding.InstanceID, binding.HelperID, binding.SignerKeyID,
		strconv.FormatInt(binding.BindingRevision, 10),
	}, "\x00")
	root := bytes.Repeat([]byte{0x71}, 32)
	seed := rootHelperDerive(root, "root-helper-private/v1", contextValue)
	nonce := rootHelperDerive(root, "root-helper-nonce/v1", contextValue)
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := append([]byte(nil), privateKey[ed25519.SeedSize:]...)
	clear(seed)
	payload, err := cloudmodule.RootHelperKeyBindingSigningPayload(binding)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	const golden = "554a1217fa251e63451edf4984da097935a7c6361e3192c5c7a0d30abb37c363"
	if got := hex.EncodeToString(sum[:]); got != golden {
		t.Fatalf("payload digest=%s want=%s", got, golden)
	}
	approval := &agentv1.RootHelperKeyDeliveryApproval{
		SchemaVersion: "dirextalk.agent.root-helper-key-approval/v1",
		ChallengeId:   "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", DeviceSignerKeyId: "device-golden",
		Binding: rootHelperBindingProto(binding), PublicKey: publicKey, Nonce: nonce,
		SigningPayloadCbor: payload, SigningPayloadDigest: "sha256:" + golden,
		Status:   agentv1.RootHelperKeyDeliveryApprovalStatus_ROOT_HELPER_KEY_DELIVERY_APPROVAL_STATUS_AWAITING_APPROVAL,
		Revision: 1, CreatedAt: timestamppb.New(now), UpdatedAt: timestamppb.New(now),
	}
	mapped, err := mapRootHelperKeyApproval(approval)
	compatibility := cloudmodule.ManagedAcceptanceCompatibility{
		DeploymentID: binding.DeploymentID, DeploymentRevision: binding.BindingRevision, SignerKeyID: "device-golden",
	}
	if err != nil || cloudmodule.ValidateAgentRootHelperKeyApproval(mapped, binding.OwnerID, compatibility) != nil ||
		mapped.Binding.SecretPlan.Name != binding.SecretPlan.Name {
		t.Fatalf("mapped=%#v err=%v", mapped, err)
	}
	approval.SigningPayloadCbor = append(append([]byte(nil), payload...), 0)
	if _, err = mapRootHelperKeyApproval(approval); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("tampered payload error=%v", err)
	}
}

func TestRootHelperKeyAdapterRejectsSecretCoordinateDriftAndSignatureStateMismatch(t *testing.T) {
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	binding := rootHelperGoldenBinding()
	publicKey, nonce := bytes.Repeat([]byte{0x41}, 32), bytes.Repeat([]byte{0x42}, 32)
	publicDigest, nonceDigest := sha256.Sum256(publicKey), sha256.Sum256(nonce)
	binding.PublicKeyDigest = "sha256:" + hex.EncodeToString(publicDigest[:])
	binding.NonceDigest = "sha256:" + hex.EncodeToString(nonceDigest[:])
	payload, err := cloudmodule.RootHelperKeyBindingSigningPayload(binding)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	approval := cloudmodule.AgentRootHelperKeyApproval{
		SchemaVersion: "dirextalk.agent.root-helper-key-approval/v1", ChallengeID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		DeviceSignerKeyID: "device-golden", Binding: binding, PublicKey: publicKey, Nonce: nonce,
		SigningPayloadCBOR: payload, SigningPayloadDigest: "sha256:" + hex.EncodeToString(sum[:]),
		Status: "awaiting_approval", Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	compatibility := cloudmodule.ManagedAcceptanceCompatibility{
		DeploymentID: binding.DeploymentID, DeploymentRevision: binding.BindingRevision, SignerKeyID: "device-golden",
	}
	approval.Binding.Secret = cloudmodule.AgentRootHelperKeySecretCoordinate{
		ARN:  "arn:aws:secretsmanager:us-west-2:123456789012:secret:other-name-x",
		Name: "other-name", VersionID: binding.SecretPlan.VersionID, KMSKeyARN: binding.SecretPlan.KMSKeyARN,
	}
	if err := cloudmodule.ValidateAgentRootHelperKeyApproval(approval, binding.OwnerID, compatibility); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("secret coordinate drift error=%v", err)
	}
	approval.Binding.Secret = cloudmodule.AgentRootHelperKeySecretCoordinate{}
	approval.DeviceSignature = make([]byte, 64)
	if err := cloudmodule.ValidateAgentRootHelperKeyApproval(approval, binding.OwnerID, compatibility); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("awaiting signature error=%v", err)
	}
}

func rootHelperGoldenBinding() cloudmodule.AgentRootHelperKeyBinding {
	return cloudmodule.AgentRootHelperKeyBinding{
		SchemaVersion: "dirextalk.agent.root-helper-key/v1", AgentInstanceID: "11111111-1111-4111-8111-111111111111",
		OwnerID: "owner-golden", DeliveryID: "22222222-2222-4222-8222-222222222222",
		DeploymentID: "33333333-3333-4333-8333-333333333333", BindingRevision: 7,
		InstanceID: "i-0123456789abcdef0", WorkerRoleARN: "arn:aws:iam::123456789012:role/worker",
		WorkerPrincipalID: "AROATESTROLEIDENTIFIER:i-0123456789abcdef0", HelperID: "root-helper",
		SignerKeyID:     "helper-key-7",
		PublicKeyDigest: "sha256:28af48ec972053d406a63a280be41557e92173266ee133ac7c46507e1587982f",
		SecretPlan: cloudmodule.AgentRootHelperKeySecretPlan{
			Partition: "aws", AccountID: "123456789012", Region: "us-west-2",
			Name:       "dtx/11111111-1111-4111-8111-111111111111/deployments/33333333-3333-4333-8333-333333333333/__dirextalk_root_helper_key",
			VersionID:  "22222222-2222-4222-8222-222222222222",
			KMSKeyARN:  "arn:aws:kms:us-west-2:123456789012:key/abcd",
			TargetPath: "/etc/dirextalk-root-helper/signing.key", FileMode: 0o400,
		},
		NonceDigest: "sha256:11cdea1e6431dec1dcc711cfed4d521be639695023a688cdf1fa24ccb0443f30",
	}
}

func rootHelperBindingProto(value cloudmodule.AgentRootHelperKeyBinding) *agentv1.RootHelperKeyDeviceBinding {
	return &agentv1.RootHelperKeyDeviceBinding{
		SchemaVersion: value.SchemaVersion, AgentInstanceId: value.AgentInstanceID, OwnerId: value.OwnerID,
		DeliveryId: value.DeliveryID, DeploymentId: value.DeploymentID, BindingRevision: value.BindingRevision,
		InstanceId: value.InstanceID, WorkerRoleArn: value.WorkerRoleARN, WorkerPrincipalId: value.WorkerPrincipalID,
		HelperId: value.HelperID, SignerKeyId: value.SignerKeyID, PublicKeyDigest: value.PublicKeyDigest,
		NonceDigest: value.NonceDigest, SecretPlan: &agentv1.RootHelperKeySecretPlan{
			Partition: value.SecretPlan.Partition, AccountId: value.SecretPlan.AccountID, Region: value.SecretPlan.Region,
			Name: value.SecretPlan.Name, VersionId: value.SecretPlan.VersionID, KmsKeyArn: value.SecretPlan.KMSKeyARN,
			TargetPath: value.SecretPlan.TargetPath, FileMode: value.SecretPlan.FileMode,
		},
	}
}

func rootHelperDerive(root []byte, domain, value string) []byte {
	mac := hmac.New(sha256.New, root)
	_, _ = mac.Write([]byte(domain))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(value))
	return mac.Sum(nil)
}
