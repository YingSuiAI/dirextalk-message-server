package brokertransport

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"reflect"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestBuildQuoteCommandBindsOnlyThePrePriceRequest(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	transport, err := New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	request := testQuoteRequest(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	logical := runtime.QuoteCommand{
		CommandID: "command-quote-0001", ConnectionID: request.CloudConnectionID, NodeKeyID: "node-key-1",
		ExpectedGeneration: 1, NodeCounter: 7, RequestDigest: digest,
	}
	signed, err := transport.BuildQuoteCommand(logical, request)
	if err != nil {
		t.Fatal(err)
	}
	if signed.RequestSHA256 == signed.PayloadSHA256 || len(signed.RequestSHA256) != 64 || signed.IssuedAt != now || signed.ExpiresAt != now.Add(commandLifetime) {
		t.Fatalf("signed command identity = %#v", signed)
	}
	parsed, err := broker.ParseQuoteCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ConnectionID != logical.ConnectionID || parsed.CommandID != logical.CommandID || parsed.NodeKeyID != logical.NodeKeyID || parsed.ExpectedGeneration != logical.ExpectedGeneration || parsed.NodeCounter != logical.NodeCounter || parsed.RequestSHA256() != signed.RequestSHA256 || parsed.PayloadSHA256 != signed.PayloadSHA256 {
		t.Fatalf("parsed command does not bind logical identity: %#v", parsed)
	}
	decoded, err := parsed.QuoteRequest()
	if err != nil || !reflect.DeepEqual(decoded, brokerRequest(request, digest)) || signed.PayloadJSON == "" {
		t.Fatalf("decoded request=%#v err=%v", decoded, err)
	}
}

func TestRequestQuoteRejectsChangedPersistedEnvelopeBeforeNetwork(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := New(privateKey, func() time.Time { return time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	request := testQuoteRequest(t)
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	logical := runtime.QuoteCommand{
		CommandID: "command-quote-0001", ConnectionID: request.CloudConnectionID, NodeKeyID: "node-key-1",
		ExpectedGeneration: 1, NodeCounter: 7, RequestDigest: digest,
	}
	signed, err := transport.BuildQuoteCommand(logical, request)
	if err != nil {
		t.Fatal(err)
	}
	logical.PayloadJSON = signed.PayloadJSON
	logical.PayloadSHA256 = signed.PayloadSHA256
	logical.RequestSHA256 = signed.RequestSHA256
	logical.SignedEnvelope = signed.EnvelopeJSON
	logical.IssuedAt = signed.IssuedAt
	logical.ExpiresAt = signed.ExpiresAt
	signed.PayloadJSON += " "
	if _, err := transport.RequestQuote(context.Background(), "https://broker.example/v2/commands", logical, signed, request); err == nil {
		t.Fatal("modified persisted payload must be rejected before any HTTP request")
	}
}

func testQuoteRequest(t *testing.T) cloudcontracts.QuoteRequestV1 {
	t.Helper()
	request := cloudcontracts.QuoteRequestV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1, QuoteRequestID: "quote-request-0001", PlanID: "plan-quote-0001", PlanRevision: 2,
		CloudConnectionID: "connection-quote-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Region: "ap-south-1",
		Candidates: []cloudcontracts.QuoteRequestCandidateV1{{
			CandidateID: "recommended-0001", Tier: cloudcontracts.QuoteTierRecommended, InstanceType: "m7i.xlarge",
			PurchaseOption: cloudcontracts.PurchaseOnDemand, EstimatedDiskGiB: 80,
		}},
	}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	return request
}
