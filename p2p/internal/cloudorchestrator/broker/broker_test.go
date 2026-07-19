package broker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNewQuoteCommandUsesConnectionStackV2CanonicalEnvelope(t *testing.T) {
	command := testCommand(t)
	const wantPayload = `{"quote_request_id":"quote-request-0001","plan_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","region":"us-east-1","candidates":[{"candidate_id":"candidate-0001","tier":"recommended","instance_type":"t3.large","purchase_option":"on_demand","estimated_disk_gib":40}]}`
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
		"payload_sha256=2ecd0701be511231c6e5ce075ed14948682900c163acec8d0a93bc06a636511c\n" +
		"approval_binding_sha256=\n" +
		"approval_challenge_id=\n" +
		"approval_signature_sha256=\n" +
		"approval_proof_payload_sha256=\n"

	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := string(payload); got != wantPayload {
		t.Fatalf("payload differs from canonical V2 payload\n got: %s\nwant: %s", got, wantPayload)
	}
	if command.PayloadSHA256 != wantPayloadSHA256 {
		t.Fatalf("payload SHA-256 = %q, want %q", command.PayloadSHA256, wantPayloadSHA256)
	}
	if got := command.SignatureBase(); got != wantSignatureBase {
		t.Fatalf("signature base differs from buildNodeSignatureBase\n got: %q\nwant: %q", got, wantSignatureBase)
	}
	if got := command.RequestSHA256(); got != wantRequestSHA256 {
		t.Fatalf("request SHA-256 = %q, want %q", got, wantRequestSHA256)
	}
	seed := bytes.Repeat([]byte{0x42}, ed25519.SeedSize)
	privateKey := ed25519.NewKeyFromSeed(seed)
	if err := command.VerifySignature(privateKey.Public().(ed25519.PublicKey)); err != nil {
		t.Fatalf("Connection Stack V2 signature does not verify: %v", err)
	}
	rawCommand, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	if _, err := ParseQuoteCommand(rawCommand); err != nil {
		t.Fatalf("ParseQuoteCommand canonical envelope: %v", err)
	}
	wrongCase := bytes.Replace(rawCommand, []byte(`"schema"`), []byte(`"Schema"`), 1)
	if _, err := ParseQuoteCommand(wrongCase); err == nil {
		t.Fatal("ParseQuoteCommand accepted a differently cased field")
	}
}

func TestQuoteCommandValidateBindingRejectsLogicalDrift(t *testing.T) {
	command := testCommand(t)
	request, err := command.QuoteRequest()
	if err != nil {
		t.Fatal(err)
	}
	issuedAt, err := time.Parse(canonicalInstantLayout, command.IssuedAt)
	if err != nil {
		t.Fatal(err)
	}
	expiresAt, err := time.Parse(canonicalInstantLayout, command.ExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	binding := QuoteCommandBinding{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		IssuedAt: issuedAt, ExpiresAt: expiresAt, Request: request,
	}
	if err := command.ValidateBinding(binding); err != nil {
		t.Fatalf("valid command binding: %v", err)
	}
	binding.NodeCounter++
	if err := command.ValidateBinding(binding); err == nil {
		t.Fatal("counter drift must be rejected")
	}
}

func TestClientSubmitQuoteAcceptsBoundReceiptAndQuote(t *testing.T) {
	command := testCommand(t)
	want := validQuoteResult(t, command)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/prod/v2/commands" || request.URL.RawQuery != "" || request.TLS == nil {
			t.Errorf("unexpected broker request: method=%s path=%s query=%q tls=%t", request.Method, request.URL.Path, request.URL.RawQuery, request.TLS != nil)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if got := request.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}
		var received QuoteCommand
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Errorf("decode command: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := received.Validate(); err != nil {
			t.Errorf("received invalid command: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		if err := json.NewEncoder(writer).Encode(want); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server, DefaultMaxResponseBytes)
	transport, ok := client.httpClient.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil || transport.TLSClientConfig == nil || transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Fatal("client transport must use direct TLS 1.2+ connections")
	}
	got, err := client.SubmitQuote(context.Background(), command)
	if err != nil {
		t.Fatalf("SubmitQuote: %v", err)
	}
	if !quotesEqual(got.Quote, want.Quote) || got.Receipt.RequestSHA256 != command.RequestSHA256() || got.Status != "quote_issued" {
		t.Fatalf("unexpected validated quote result: %#v", got)
	}
}

func TestClientRejectsUnboundQuoteAndUnsafeEndpoint(t *testing.T) {
	command := testCommand(t)
	result := validQuoteResult(t, command)
	result.Quote.RequestSHA256 = strings.Repeat("0", 64)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(result)
	}))
	defer server.Close()

	client := newTestClient(t, server, DefaultMaxResponseBytes)
	_, err := client.SubmitQuote(context.Background(), command)
	assertErrorCode(t, err, "invalid_broker_response")

	for _, endpoint := range []string{
		"http://broker.example/prod/v2/commands",
		"https://broker.example/prod/not-commands",
		"https://broker.example/prod/v2/commands?redirect=1",
		"https://node@broker.example/prod/v2/commands",
		"https://broker.example/prod%2fv2/commands",
	} {
		if _, err := NewClient(ClientOptions{Endpoint: endpoint}); err == nil {
			t.Fatalf("NewClient(%q) unexpectedly accepted unsafe endpoint", endpoint)
		}
	}
}

func TestClientSurfacesExpiredCommandV2Error(t *testing.T) {
	command := testCommand(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		writer.WriteHeader(http.StatusUnauthorized)
		_, _ = writer.Write([]byte(`{"error":{"code":"expired_command"}}`))
	}))
	defer server.Close()

	_, err := newTestClient(t, server, DefaultMaxResponseBytes).SubmitQuote(context.Background(), command)
	assertErrorCode(t, err, "expired_command")
	var brokerError *Error
	if !errors.As(err, &brokerError) || brokerError.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired command error = %#v, want HTTP 401", err)
	}
}

func TestClientKeepsMalformedV2ErrorGeneric(t *testing.T) {
	command := testCommand(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"error":{"code":"expired_command","detail":"not accepted"}}`))
	}))
	defer server.Close()

	_, err := newTestClient(t, server, DefaultMaxResponseBytes).SubmitQuote(context.Background(), command)
	assertErrorCode(t, err, "broker_http_status")
}

func TestStrictQuoteResponseRejectsDuplicateFields(t *testing.T) {
	result := validQuoteResult(t, testCommand(t))
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal quote result: %v", err)
	}
	duplicateStatus := bytes.Replace(raw, []byte(`{"status":`), []byte(`{"status":"quote_issued","status":`), 1)
	if _, err := decodeQuoteResultJSON(duplicateStatus); err == nil {
		t.Fatal("decodeQuoteResultJSON accepted duplicate response fields")
	}
}

func TestStrictQuoteResponseRequiresCapacityMetadata(t *testing.T) {
	command := testCommand(t)
	result := validQuoteResult(t, command)
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal quote result: %v", err)
	}
	missingArchitecture := bytes.ReplaceAll(raw, []byte(`,"architecture":"amd64"`), nil)
	if _, err := decodeQuoteResultJSON(missingArchitecture); err == nil {
		t.Fatal("decodeQuoteResultJSON accepted a quote without architecture metadata")
	}

	result.Quote.Candidates[0].GPUCount = 1
	result.Receipt.Quote.Candidates[0].GPUCount = 1
	if err := ValidateQuoteResult(command, result); err == nil {
		t.Fatal("ValidateQuoteResult accepted GPU count without GPU memory metadata")
	}
}

func TestClientRejectsRedirectAndOversizedResponse(t *testing.T) {
	command := testCommand(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", "/prod/v2/commands")
		writer.WriteHeader(http.StatusFound)
	}))
	defer server.Close()

	client := newTestClient(t, server, 1024)
	_, err := client.SubmitQuote(context.Background(), command)
	assertErrorCode(t, err, "broker_http_status")

	// The endpoint is fixed by Client. Serve the oversized body on a separate
	// TLS server rather than allowing a query parameter to influence a request.
	oversizedServer := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(strings.Repeat("x", 1025)))
	}))
	defer oversizedServer.Close()
	oversizedClient := newTestClient(t, oversizedServer, 1024)
	_, err = oversizedClient.SubmitQuote(context.Background(), command)
	assertErrorCode(t, err, "broker_response_too_large")
}

func testCommand(t *testing.T) QuoteCommand {
	t.Helper()
	seed := bytes.Repeat([]byte{0x42}, ed25519.SeedSize)
	command, err := NewQuoteCommand(QuoteCommandInput{
		ConnectionID:       "connection-0001",
		CommandID:          "command-0001",
		NodeKeyID:          "node-key-1",
		ExpectedGeneration: 2,
		NodeCounter:        7,
		IssuedAt:           time.Date(2026, time.July, 14, 12, 0, 0, 123456789, time.UTC),
		ExpiresAt:          time.Date(2026, time.July, 14, 12, 4, 0, 123987654, time.UTC),
		Request: QuoteRequest{
			QuoteRequestID: "quote-request-0001",
			PlanDigest:     "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Region:         "us-east-1",
			Candidates: []QuoteCandidate{{
				CandidateID:      "candidate-0001",
				Tier:             "recommended",
				InstanceType:     "t3.large",
				PurchaseOption:   "on_demand",
				EstimatedDiskGiB: 40,
			}},
		},
		PrivateKey: ed25519.NewKeyFromSeed(seed),
	})
	if err != nil {
		t.Fatalf("NewQuoteCommand: %v", err)
	}
	return command
}

func validQuoteResult(t *testing.T, command QuoteCommand) QuoteResult {
	t.Helper()
	request, err := command.QuoteRequest()
	if err != nil {
		t.Fatalf("QuoteRequest: %v", err)
	}
	quote := Quote{
		Schema:         QuoteSchema,
		QuoteID:        "quote-" + command.RequestSHA256()[:32],
		ConnectionID:   command.ConnectionID,
		CommandID:      command.CommandID,
		RequestSHA256:  command.RequestSHA256(),
		QuoteRequestID: request.QuoteRequestID,
		PlanDigest:     request.PlanDigest,
		Region:         request.Region,
		Currency:       "USD",
		QuotedAt:       command.IssuedAt,
		ValidUntil:     "2026-07-14T12:15:00.123Z",
		Candidates: []QuotedCandidate{{
			CandidateID:       request.Candidates[0].CandidateID,
			Tier:              request.Candidates[0].Tier,
			InstanceType:      request.Candidates[0].InstanceType,
			PurchaseOption:    request.Candidates[0].PurchaseOption,
			EstimatedDiskGiB:  request.Candidates[0].EstimatedDiskGiB,
			Architecture:      "amd64",
			VCPU:              2,
			MemoryMiB:         8192,
			GPUCount:          0,
			GPUMemoryMiB:      0,
			HourlyMinor:       5,
			ThirtyDayMinor:    3600,
			StartupUpperMinor: 10,
			AvailabilityZones: []string{
				"us-east-1a",
			},
		}},
		IncludedItems:   append([]string(nil), quoteIncludedItems...),
		UnincludedItems: append([]string(nil), quoteUnincludedItems...),
	}
	return QuoteResult{
		Status: "quote_issued",
		Receipt: Receipt{
			Schema:             ReceiptSchema,
			Disposition:        "committed",
			ConnectionID:       command.ConnectionID,
			ExpectedGeneration: command.ExpectedGeneration,
			NodeCounter:        command.NodeCounter,
			CommandID:          command.CommandID,
			RequestSHA256:      command.RequestSHA256(),
			Action:             QuoteAction,
			Quote:              &quote,
		},
		Quote: quote,
	}
}

func newTestClient(t *testing.T, server *httptest.Server, maximum int64) *Client {
	t.Helper()
	return newTestClientWithEndpoint(t, server.URL+"/prod/v2/commands", server.Certificate(), maximum)
}

func newTestClientWithEndpoint(t *testing.T, endpoint string, certificate *x509.Certificate, maximum int64) *Client {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	client, err := NewClient(ClientOptions{
		Endpoint:         endpoint,
		RootCAs:          roots,
		MaxResponseBytes: maximum,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func assertErrorCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error code %q, got nil", want)
	}
	var brokerError *Error
	if !errors.As(err, &brokerError) || brokerError.Code != want {
		t.Fatalf("error code = %#v, want %q", err, want)
	}
}
