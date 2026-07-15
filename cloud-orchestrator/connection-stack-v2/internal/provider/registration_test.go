package provider

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

func TestRegistrationAttestorBindsStackRuntimeAndWorkerConfiguration(t *testing.T) {
	config := RegistrationConfig{
		ConnectionID: "connection-0001", ConnectionGeneration: 1, NodeKeyID: "node-key-01",
		AccountID: "123456789012", Region: "ap-northeast-1",
		StackARN:  "arn:aws:cloudformation:ap-northeast-1:123456789012:stack/dirextalk-test/12345678-1234-1234-1234-123456789012",
		URLSuffix: "amazonaws.com", StageName: "prod",
		WorkerArtifact:               contract.WorkerArtifactReference{Kind: "fixed_ami", AMIID: "ami-0123456789abcdef0"},
		WorkerNetwork:                contract.WorkerNetworkReference{VPCID: "vpc-0123456789abcdef0", SubnetID: "subnet-0123456789abcdef0", AvailabilityZone: "ap-northeast-1a"},
		WorkerResourceManifestDigest: "sha256:" + strings.Repeat("b", 64),
	}
	attestor, err := NewRegistrationAttestor(config)
	if err != nil {
		t.Fatalf("NewRegistrationAttestor(): %v", err)
	}
	command, request := registrationProviderCommand(t, config.StackARN)
	registration, err := attestor.Attest(t.Context(), api.GatewayRuntime{
		DomainName: "abcdefghij.execute-api.ap-northeast-1.amazonaws.com", Stage: "prod",
	}, command, request)
	if err != nil {
		t.Fatalf("Attest(): %v", err)
	}
	if registration.BrokerCommandURL != "https://abcdefghij.execute-api.ap-northeast-1.amazonaws.com/prod/v2/commands" || registration.WorkerArtifact != config.WorkerArtifact || registration.WorkerNetwork != config.WorkerNetwork {
		t.Fatalf("registration = %#v", registration)
	}
	if _, err := contract.MarshalCommittedRegistrationResult(command, registration); err != nil {
		t.Fatalf("registration does not satisfy public contract: %v", err)
	}

	_, err = attestor.Attest(t.Context(), api.GatewayRuntime{DomainName: "PRIVATE_INVALID_DOMAIN", Stage: "prod"}, command, request)
	if err == nil || strings.Contains(err.Error(), "PRIVATE_INVALID_DOMAIN") {
		t.Fatalf("unsafe registration error = %v", err)
	}

	config.StageName = "preview"
	if _, err := NewRegistrationAttestor(config); err == nil {
		t.Fatal("NewRegistrationAttestor() accepted a non-prod Broker stage")
	}
}

func registrationProviderCommand(t *testing.T, stackARN string) (contract.Command, contract.RegistrationRequest) {
	t.Helper()
	request := contract.RegistrationRequest{BootstrapID: "bootstrap-0001", RequestedRegion: "ap-northeast-1", StackARN: stackARN}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	command := contract.Command{
		Schema: contract.CommandSchema, ConnectionID: "connection-0001", CommandID: "command-registration-0001", NodeKeyID: "node-key-01",
		IssuedAt: "2026-07-15T01:02:03.000Z", ExpiresAt: "2026-07-15T01:07:03.000Z", ExpectedGeneration: 1,
		NodeCounter: 7, Action: contract.ActionRegistrationVerify, PayloadB64: base64.StdEncoding.EncodeToString(payload),
		PayloadSHA256: hex.EncodeToString(sum[:]), SignatureB64: base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)),
	}
	decoded, err := command.RegistrationRequest()
	if err != nil {
		t.Fatalf("RegistrationRequest(): %v", err)
	}
	return command, decoded
}
