package broker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewRegistrationCommandUsesConnectionStackV2CanonicalEnvelope(t *testing.T) {
	command := testRegistrationCommand(t)
	const wantPayload = `{"bootstrap_id":"bootstrap-0001","requested_region":"us-east-1","stack_arn":"arn:aws:cloudformation:us-east-1:123456789012:stack/DirextalkConnection-0001/01234567-89ab-cdef-0123-456789abcdef"}`
	const wantPayloadSHA256 = "5495eb45bff124f1f8c0c861a5709491362f49d5669a7e285c697aa03cd52742"
	const wantRequestSHA256 = "f1df6d8be5b4740d0962e4ed600abbf7a84e026d87e44c3b2cdcfa3b7c6f5bbb"
	const wantSignatureBase = "dirextalk.aws.command-signature/v2\n" +
		"schema=dirextalk.aws.command/v2\n" +
		"connection_id=connection-0001\n" +
		"command_id=command-0002\n" +
		"node_key_id=node-key-1\n" +
		"issued_at=2026-07-14T12:00:00.123Z\n" +
		"expires_at=2026-07-14T12:04:00.123Z\n" +
		"expected_generation=2\n" +
		"node_counter=8\n" +
		"action=connection.registration.verify\n" +
		"payload_sha256=5495eb45bff124f1f8c0c861a5709491362f49d5669a7e285c697aa03cd52742\n" +
		"approval_binding_sha256=\n" +
		"approval_challenge_id=\n" +
		"approval_signature_sha256=\n"

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
	if _, err := ParseRegistrationCommand(rawCommand); err != nil {
		t.Fatalf("ParseRegistrationCommand canonical envelope: %v", err)
	}
	wrongCase := bytes.Replace(rawCommand, []byte(`"schema"`), []byte(`"Schema"`), 1)
	if _, err := ParseRegistrationCommand(wrongCase); err == nil {
		t.Fatal("ParseRegistrationCommand accepted a differently cased field")
	}
	wrongAction := bytes.Replace(rawCommand, []byte(`"connection.registration.verify"`), []byte(`"quote.request"`), 1)
	if _, err := ParseRegistrationCommand(wrongAction); err == nil {
		t.Fatal("ParseRegistrationCommand accepted quote.request")
	}
}

func TestRegistrationCommandValidateBindingRejectsLogicalDrift(t *testing.T) {
	command := testRegistrationCommand(t)
	request, err := command.RegistrationRequest()
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
	binding := RegistrationCommandBinding{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		IssuedAt: issuedAt, ExpiresAt: expiresAt, Request: request,
	}
	if err := command.ValidateBinding(binding); err != nil {
		t.Fatalf("valid command binding: %v", err)
	}
	binding.Request.StackARN = "arn:aws:cloudformation:us-east-1:123456789012:stack/DirextalkConnection-0002/01234567-89ab-cdef-0123-456789abcdef"
	if err := command.ValidateBinding(binding); err == nil {
		t.Fatal("stack ARN drift must be rejected")
	}
}

func TestClientSubmitRegistrationAcceptsBoundAttestation(t *testing.T) {
	command := testRegistrationCommand(t)
	for _, test := range []struct {
		name        string
		status      string
		disposition string
	}{
		{name: "committed", status: "connection_registered", disposition: "committed"},
		{name: "idempotent", status: "idempotent", disposition: "idempotent"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodPost || request.URL.Path != "/prod/v2/commands" || request.URL.RawQuery != "" || request.TLS == nil {
					t.Errorf("unexpected broker request: method=%s path=%s query=%q tls=%t", request.Method, request.URL.Path, request.URL.RawQuery, request.TLS != nil)
					writer.WriteHeader(http.StatusBadRequest)
					return
				}
				var received RegistrationCommand
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
				result := validRegistrationResult(t, command, "https://"+request.Host+"/prod/v2/commands", test.status, test.disposition)
				writer.Header().Set("Content-Type", "application/json; charset=utf-8")
				if err := json.NewEncoder(writer).Encode(result); err != nil {
					t.Errorf("encode response: %v", err)
				}
			}))
			defer server.Close()

			client := newTestClient(t, server, DefaultMaxResponseBytes)
			got, err := client.SubmitRegistration(context.Background(), command)
			if err != nil {
				t.Fatalf("SubmitRegistration: %v", err)
			}
			if got.Status != test.status || got.Receipt.Disposition != test.disposition || got.Registration.RequestSHA256 != command.RequestSHA256() || got.Registration.BrokerCommandURL != server.URL+"/prod/v2/commands" {
				t.Fatalf("unexpected validated registration result: %#v", got)
			}
		})
	}
}

func TestClientRejectsUnboundRegistrationAttestation(t *testing.T) {
	command := testRegistrationCommand(t)
	for _, test := range []struct {
		name   string
		mutate func(*Registration)
	}{
		{
			name: "node key",
			mutate: func(registration *Registration) {
				registration.NodeKeyID = "other-key-1"
			},
		},
		{
			name: "broker endpoint",
			mutate: func(registration *Registration) {
				registration.BrokerCommandURL = "https://alternate.example/prod/v2/commands"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				result := validRegistrationResult(t, command, "https://"+request.Host+"/prod/v2/commands", "connection_registered", "committed")
				test.mutate(&result.Registration)
				writer.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(writer).Encode(result)
			}))
			defer server.Close()

			_, err := newTestClient(t, server, DefaultMaxResponseBytes).SubmitRegistration(context.Background(), command)
			assertErrorCode(t, err, "invalid_broker_response")
		})
	}
}

func TestStrictRegistrationResponseRejectsDuplicateFields(t *testing.T) {
	result := validRegistrationResult(t, testRegistrationCommand(t), "https://broker.example/prod/v2/commands", "connection_registered", "committed")
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal registration result: %v", err)
	}
	duplicateStatus := bytes.Replace(raw, []byte(`{"status":`), []byte(`{"status":"connection_registered","status":`), 1)
	if _, err := decodeRegistrationResultJSON(duplicateStatus); err == nil {
		t.Fatal("decodeRegistrationResultJSON accepted duplicate response fields")
	}
}

func testRegistrationCommand(t *testing.T) RegistrationCommand {
	t.Helper()
	seed := bytes.Repeat([]byte{0x42}, ed25519.SeedSize)
	command, err := NewRegistrationCommand(RegistrationCommandInput{
		ConnectionID:       "connection-0001",
		CommandID:          "command-0002",
		NodeKeyID:          "node-key-1",
		ExpectedGeneration: 2,
		NodeCounter:        8,
		IssuedAt:           time.Date(2026, time.July, 14, 12, 0, 0, 123456789, time.UTC),
		ExpiresAt:          time.Date(2026, time.July, 14, 12, 4, 0, 123987654, time.UTC),
		Request: RegistrationRequest{
			BootstrapID:     "bootstrap-0001",
			RequestedRegion: "us-east-1",
			StackARN:        "arn:aws:cloudformation:us-east-1:123456789012:stack/DirextalkConnection-0001/01234567-89ab-cdef-0123-456789abcdef",
		},
		PrivateKey: ed25519.NewKeyFromSeed(seed),
	})
	if err != nil {
		t.Fatalf("NewRegistrationCommand: %v", err)
	}
	return command
}

func validRegistrationResult(t *testing.T, command RegistrationCommand, endpoint, status, disposition string) RegistrationResult {
	t.Helper()
	request, err := command.RegistrationRequest()
	if err != nil {
		t.Fatalf("RegistrationRequest: %v", err)
	}
	registration := Registration{
		Schema:               RegistrationSchema,
		BootstrapID:          request.BootstrapID,
		ConnectionID:         command.ConnectionID,
		AccountID:            "123456789012",
		Region:               request.RequestedRegion,
		BrokerCommandURL:     endpoint,
		NodeKeyID:            command.NodeKeyID,
		ConnectionGeneration: command.ExpectedGeneration,
		StackARN:             request.StackARN,
		CommandID:            command.CommandID,
		RequestSHA256:        command.RequestSHA256(),
	}
	return RegistrationResult{
		Status: status,
		Receipt: RegistrationReceipt{
			Schema:             ReceiptSchema,
			Disposition:        disposition,
			ConnectionID:       command.ConnectionID,
			ExpectedGeneration: command.ExpectedGeneration,
			NodeCounter:        command.NodeCounter,
			CommandID:          command.CommandID,
			RequestSHA256:      command.RequestSHA256(),
			Action:             RegistrationAction,
		},
		Registration: registration,
	}
}

func TestRegistrationRequestRejectsNoGenericActionOrAnyAWSCommand(t *testing.T) {
	command := testRegistrationCommand(t)
	command.Action = "deployment.create"
	if err := command.Validate(); err == nil {
		t.Fatal("registration command accepted a generic AWS mutation action")
	}
	command = testRegistrationCommand(t)
	expandedPayload := []byte(`{"bootstrap_id":"bootstrap-0001","requested_region":"us-east-1","stack_arn":"arn:aws:cloudformation:us-east-1:123456789012:stack/DirextalkConnection-0001/01234567-89ab-cdef-0123-456789abcdef","unexpected":"value"}`)
	command.PayloadB64 = base64.StdEncoding.EncodeToString(expandedPayload)
	command.PayloadSHA256 = sha256Hex(expandedPayload)
	if err := command.Validate(); err == nil {
		t.Fatal("registration command accepted an expanded payload")
	}
}
