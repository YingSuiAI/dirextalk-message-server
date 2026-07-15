package contract

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestParseAndVerifyPreservesV2SignatureBase(t *testing.T) {
	command, raw, publicKey := signedTestCommand(t, ActionQuoteRequest)
	parsed, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if err = parsed.ValidateAt(time.Date(2026, 7, 15, 1, 2, 3, 0, time.UTC)); err != nil {
		t.Fatalf("ValidateAt() error = %v", err)
	}
	if err = parsed.VerifyNodeSignature(publicKey); err != nil {
		t.Fatalf("VerifyNodeSignature() error = %v", err)
	}
	wantBase := "dirextalk.aws.command-signature/v2\n" +
		"schema=dirextalk.aws.command/v2\n" +
		"connection_id=connection-0001\n" +
		"command_id=command-0001\n" +
		"node_key_id=node-key-01\n" +
		"issued_at=2026-07-15T01:02:03.000Z\n" +
		"expires_at=2026-07-15T01:07:03.000Z\n" +
		"expected_generation=1\n" +
		"node_counter=7\n" +
		"action=quote.request\n" +
		"payload_sha256=" + command.PayloadSHA256 + "\n" +
		"approval_binding_sha256=\n" +
		"approval_challenge_id=\n" +
		"approval_signature_sha256=\n" +
		"approval_proof_payload_sha256=\n"
	gotBase, err := parsed.SignatureBase()
	if err != nil {
		t.Fatalf("SignatureBase() error = %v", err)
	}
	if got := gotBase; got != wantBase {
		t.Fatalf("SignatureBase() mismatch\nwant:\n%s\ngot:\n%s", wantBase, got)
	}
	gotRequestSHA, err := parsed.RequestSHA256()
	if err != nil {
		t.Fatalf("RequestSHA256() error = %v", err)
	}
	if got, want := gotRequestSHA, sha256Hex([]byte(wantBase)); got != want {
		t.Fatalf("RequestSHA256() = %q, want %q", got, want)
	}
}

func TestQuoteGoldenMatchesExistingOrchestratorContract(t *testing.T) {
	const payload = `{"quote_request_id":"quote-request-0001","plan_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","region":"us-east-1","candidates":[{"candidate_id":"candidate-0001","tier":"recommended","instance_type":"t3.large","purchase_option":"on_demand","estimated_disk_gib":40}]}`
	const wantPayloadSHA256 = "2ecd0701be511231c6e5ce075ed14948682900c163acec8d0a93bc06a636511c"
	const wantRequestSHA256 = "3b68d5d5be6a39f3e42fd3f323c17b4dddc36e38ab6d45335719c6b160ec7e52"
	const wantSignatureBase = "dirextalk.aws.command-signature/v2\n" +
		"schema=dirextalk.aws.command/v2\n" +
		"connection_id=connection-0001\n" +
		"command_id=command-0001\n" +
		"node_key_id=node-key-1\n" +
		"issued_at=2026-07-14T12:00:00.123Z\n" +
		"expires_at=2026-07-14T12:04:00.123Z\n" +
		"expected_generation=2\n" +
		"node_counter=7\n" +
		"action=quote.request\n" +
		"payload_sha256=" + wantPayloadSHA256 + "\n" +
		"approval_binding_sha256=\n" +
		"approval_challenge_id=\n" +
		"approval_signature_sha256=\n" +
		"approval_proof_payload_sha256=\n"

	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, ed25519.SeedSize))
	command := Command{
		Schema:             CommandSchema,
		ConnectionID:       "connection-0001",
		CommandID:          "command-0001",
		NodeKeyID:          "node-key-1",
		IssuedAt:           "2026-07-14T12:00:00.123Z",
		ExpiresAt:          "2026-07-14T12:04:00.123Z",
		ExpectedGeneration: 2,
		NodeCounter:        7,
		Action:             ActionQuoteRequest,
		PayloadB64:         base64.StdEncoding.EncodeToString([]byte(payload)),
		PayloadSHA256:      wantPayloadSHA256,
	}
	signatureBase, err := command.SignatureBase()
	if err != nil {
		t.Fatal(err)
	}
	if signatureBase != wantSignatureBase {
		t.Fatalf("SignatureBase() = %q, want %q", signatureBase, wantSignatureBase)
	}
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(signatureBase)))
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := parsed.RequestSHA256(); err != nil || got != wantRequestSHA256 {
		t.Fatalf("RequestSHA256() = %q, %v; want %q", got, err, wantRequestSHA256)
	}
	if err := parsed.VerifyNodeSignature(privateKey.Public().(ed25519.PublicKey)); err != nil {
		t.Fatalf("VerifyNodeSignature() error = %v", err)
	}
}

func TestParseRejectsAmbiguousOrTamperedEnvelope(t *testing.T) {
	_, raw, _ := signedTestCommand(t, ActionQuoteRequest)

	tests := []struct {
		name string
		raw  []byte
		code string
	}{
		{
			name: "duplicate outer key",
			raw:  []byte(strings.TrimSuffix(string(raw), "}") + `,"schema":"dirextalk.aws.command/v2"}`),
			code: "invalid_command",
		},
		{
			name: "unknown outer key",
			raw:  []byte(strings.TrimSuffix(string(raw), "}") + `,"surprise":true}`),
			code: "invalid_command",
		},
		{
			name: "noncanonical payload base64",
			raw:  marshalCommand(t, func(mutated *Command) { mutated.PayloadB64 += "\n" }),
			code: "invalid_payload",
		},
		{
			name: "payload hash mismatch",
			raw: marshalCommand(t, func(mutated *Command) {
				mutated.PayloadSHA256 = strings.Repeat("0", 64)
			}),
			code: "payload_digest_mismatch",
		},
		{
			name: "invalid UTF8 payload",
			raw: marshalCommand(t, func(mutated *Command) {
				payload := []byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}
				sum := sha256.Sum256(payload)
				mutated.PayloadB64 = base64.StdEncoding.EncodeToString(payload)
				mutated.PayloadSHA256 = hex.EncodeToString(sum[:])
			}),
			code: "invalid_payload",
		},
		{
			name: "unknown action",
			raw: marshalCommand(t, func(mutated *Command) {
				mutated.Action = "ec2.anything"
			}),
			code: "unsupported_action",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.raw)
			if got := Code(err); got != tt.code {
				t.Fatalf("Parse() code = %q, want %q (err %v)", got, tt.code, err)
			}
		})
	}
}

func TestDeploymentCreateRejectsIncompleteTypedProofBeforeSignature(t *testing.T) {
	command, _, publicKey := signedTestCommand(t, ActionDeploymentCreate)
	command.ApprovalProof = json.RawMessage(`{"approval_id":"approval-0001"}`)
	command.SignatureB64 = base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if !parsed.HasApprovalProof() {
		t.Fatal("HasApprovalProof() = false")
	}
	if got := Code(parsed.VerifyNodeSignature(publicKey)); got != "invalid_deployment_request" {
		t.Fatalf("VerifyNodeSignature() code = %q, want invalid_deployment_request", got)
	}
	if _, err := parsed.SignatureBase(); Code(err) != "invalid_deployment_request" {
		t.Fatalf("SignatureBase() code = %q, want invalid_deployment_request", Code(err))
	}
}

func TestValidateAtRejectsExpiredCommand(t *testing.T) {
	_, raw, _ := signedTestCommand(t, ActionQuoteRequest)
	parsed, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got := Code(parsed.ValidateAt(time.Date(2026, 7, 15, 1, 7, 3, 0, time.UTC))); got != "expired_command" {
		t.Fatalf("ValidateAt() code = %q, want expired_command", got)
	}
}

func TestValidateAtRejectsFutureCommandBeyondClockSkew(t *testing.T) {
	command, _, _ := signedTestCommand(t, ActionQuoteRequest)
	command.IssuedAt = "2026-07-15T01:04:04.000Z"
	command.ExpiresAt = "2026-07-15T01:05:04.000Z"
	signatureBase, err := command.SignatureBase()
	if err != nil {
		t.Fatal(err)
	}
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(testPrivateKey(), []byte(signatureBase)))
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got := Code(parsed.ValidateAt(time.Date(2026, 7, 15, 1, 2, 3, 0, time.UTC))); got != "future_command" {
		t.Fatalf("ValidateAt() code = %q, want future_command", got)
	}
}

func TestParseRejectsOversizedOuterCommand(t *testing.T) {
	_, err := Parse(make([]byte, MaxCommandBytes+1))
	if got := Code(err); got != "request_too_large" {
		t.Fatalf("Parse() code = %q, want request_too_large", got)
	}
}

func signedTestCommand(t *testing.T, action string) (Command, []byte, ed25519.PublicKey) {
	t.Helper()
	payload := []byte(`{"quote_request_id":"quote-0001","plan_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","region":"ap-northeast-1","candidates":[]}`)
	sum := sha256.Sum256(payload)
	command := Command{
		Schema:             CommandSchema,
		ConnectionID:       "connection-0001",
		CommandID:          "command-0001",
		NodeKeyID:          "node-key-01",
		IssuedAt:           "2026-07-15T01:02:03.000Z",
		ExpiresAt:          "2026-07-15T01:07:03.000Z",
		ExpectedGeneration: 1,
		NodeCounter:        7,
		Action:             action,
		PayloadB64:         base64.StdEncoding.EncodeToString(payload),
		PayloadSHA256:      hex.EncodeToString(sum[:]),
	}
	if action == ActionDeploymentCreate {
		command.ApprovalProof = json.RawMessage(`{"approval_id":"approval-0001"}`)
	}
	privateKey := testPrivateKey()
	if action == ActionDeploymentCreate {
		command.SignatureB64 = base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
	} else {
		signatureBase, err := command.SignatureBase()
		if err != nil {
			t.Fatal(err)
		}
		command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(signatureBase)))
	}
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	return command, raw, privateKey.Public().(ed25519.PublicKey)
}

func marshalCommand(t *testing.T, mutate func(*Command)) []byte {
	t.Helper()
	command, _, _ := signedTestCommand(t, ActionQuoteRequest)
	mutate(&command)
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func testPrivateKey() ed25519.PrivateKey {
	seed := sha256.Sum256([]byte("dirextalk-connection-stack-v2-contract-test"))
	return ed25519.NewKeyFromSeed(seed[:])
}

func sha256Hex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
