package contract

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestReadOnlySuccessFixturesMatchGoStackContract(t *testing.T) {
	quoteCommand := fixtureCommand(t, ActionQuoteRequest, "command-0001", "node-key-1", 2, 7,
		"2026-07-14T12:00:00.123Z", "2026-07-14T12:04:00.123Z",
		QuoteRequest{
			QuoteRequestID: "quote-request-0001",
			PlanDigest:     "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Region:         "us-east-1",
			Candidates:     []QuoteCandidate{{CandidateID: "candidate-0001", Tier: "recommended", InstanceType: "t3.large", PurchaseOption: "on_demand", EstimatedDiskGiB: 40}},
		})
	var quoteResult QuoteResult
	readFixture(t, "quote-success-v1.json", &quoteResult)
	quoteRequest, err := quoteCommand.QuoteRequest()
	if err != nil || ValidateQuoteResult(quoteCommand, quoteRequest, quoteResult) != nil {
		t.Fatalf("quote fixture is incompatible: request=%v validate=%v", err, ValidateQuoteResult(quoteCommand, quoteRequest, quoteResult))
	}

	registrationCommand := fixtureCommand(t, ActionRegistrationVerify, "command-0002", "node-key-1", 2, 8,
		"2026-07-14T12:00:00.123Z", "2026-07-14T12:04:00.123Z",
		RegistrationRequest{
			BootstrapID: "bootstrap-0001", RequestedRegion: "us-east-1",
			StackARN: "arn:aws:cloudformation:us-east-1:123456789012:stack/DirextalkConnection-0001/01234567-89ab-cdef-0123-456789abcdef",
		})
	var registrationResult RegistrationResult
	readFixture(t, "registration-success-v1.json", &registrationResult)
	registrationRequest, err := registrationCommand.RegistrationRequest()
	if err != nil || ValidateRegistrationResult(registrationCommand, registrationRequest, registrationResult) != nil {
		t.Fatalf("registration fixture is incompatible: request=%v validate=%v", err, ValidateRegistrationResult(registrationCommand, registrationRequest, registrationResult))
	}
}

func fixtureCommand(t *testing.T, action, commandID, nodeKeyID string, generation, counter int64, issuedAt, expiresAt string, request any) Command {
	t.Helper()
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	payloadSHA := sha256.Sum256(payload)
	return Command{
		Schema: CommandSchema, ConnectionID: "connection-0001", CommandID: commandID, NodeKeyID: nodeKeyID,
		IssuedAt: issuedAt, ExpiresAt: expiresAt, ExpectedGeneration: generation, NodeCounter: counter, Action: action,
		PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: hex.EncodeToString(payloadSHA[:]),
		SignatureB64: base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)),
	}
}

func readFixture(t *testing.T, name string, target any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatal(err)
	}
}
