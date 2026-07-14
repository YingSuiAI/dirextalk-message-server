package cloudworker

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestIMDSv2IdentityProviderFetchesBoundCanonicalProof(t *testing.T) {
	document := []byte(`{"accountId":"123456789012","instanceId":"i-0123456789abcdef0","region":"ap-southeast-1"}`)
	signature := base64.StdEncoding.EncodeToString([]byte("iid-rsa-signature"))
	const token = "imds-v2-token-value"

	var (
		mu    sync.Mutex
		calls []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		calls = append(calls, request.Method+" "+request.URL.Path)
		mu.Unlock()
		switch request.URL.Path {
		case imdsTokenPath:
			if request.Method != http.MethodPut || request.Header.Get(imdsTokenTTLHeader) != imdsTokenTTLSeconds {
				http.Error(writer, "token request", http.StatusBadRequest)
				return
			}
			if request.Header.Get(imdsTokenHeader) != "" || request.Header.Get("X-Forwarded-For") != "" {
				http.Error(writer, "unexpected token request header", http.StatusBadRequest)
				return
			}
			writer.Header().Set("Content-Type", "text/plain")
			_, _ = writer.Write([]byte(token))
		case imdsIdentityDocumentPath:
			if request.Method != http.MethodGet || request.Header.Get(imdsTokenHeader) != token {
				http.Error(writer, "document request", http.StatusUnauthorized)
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write(document)
		case imdsIdentitySignaturePath:
			if request.Method != http.MethodGet || request.Header.Get(imdsTokenHeader) != token {
				http.Error(writer, "signature request", http.StatusUnauthorized)
				return
			}
			writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = writer.Write([]byte("\n" + signature + "\n"))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	provider, err := newIMDSv2IdentityProviderForTest(server.URL, server.Client())
	if err != nil {
		t.Fatalf("newIMDSv2IdentityProviderForTest() error = %v", err)
	}
	proof, err := provider.Fetch(context.Background())
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if proof.DocumentB64 != base64.StdEncoding.EncodeToString(document) {
		t.Fatalf("DocumentB64 = %q, want base64 of the raw IID document", proof.DocumentB64)
	}
	if proof.SignatureB64 != signature {
		t.Fatalf("SignatureB64 = %q, want canonical IMDS signature bytes once", proof.SignatureB64)
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("proof.Validate() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := calls, []string{
		http.MethodPut + " " + imdsTokenPath,
		http.MethodGet + " " + imdsIdentityDocumentPath,
		http.MethodGet + " " + imdsIdentitySignaturePath,
	}; !equalStrings(got, want) {
		t.Fatalf("IMDS calls = %v, want %v", got, want)
	}
}

func TestIMDSv2IdentityProviderRejectsNonCanonicalSignatureAndUnexpectedContent(t *testing.T) {
	tests := []struct {
		name             string
		documentType     string
		signature        string
		signatureType    string
		wantSignatureHit bool
	}{
		{
			name:             "non_canonical_signature",
			documentType:     "application/json",
			signature:        base64.RawStdEncoding.EncodeToString([]byte("iid-rsa-signature")),
			signatureType:    "text/plain",
			wantSignatureHit: true,
		},
		{
			name:             "unexpected_document_content_type",
			documentType:     "text/html",
			signature:        base64.StdEncoding.EncodeToString([]byte("iid-rsa-signature")),
			signatureType:    "text/plain",
			wantSignatureHit: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				mu            sync.Mutex
				signatureHits int
			)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case imdsTokenPath:
					writer.Header().Set("Content-Type", "text/plain")
					_, _ = writer.Write([]byte("imds-v2-token-value"))
				case imdsIdentityDocumentPath:
					writer.Header().Set("Content-Type", test.documentType)
					_, _ = writer.Write([]byte(`{"instanceId":"i-0123456789abcdef0"}`))
				case imdsIdentitySignaturePath:
					mu.Lock()
					signatureHits++
					mu.Unlock()
					writer.Header().Set("Content-Type", test.signatureType)
					_, _ = writer.Write([]byte(test.signature))
				default:
					http.NotFound(writer, request)
				}
			}))
			defer server.Close()

			provider, err := newIMDSv2IdentityProviderForTest(server.URL, server.Client())
			if err != nil {
				t.Fatalf("newIMDSv2IdentityProviderForTest() error = %v", err)
			}
			if _, err := provider.Fetch(context.Background()); err == nil {
				t.Fatal("Fetch() accepted invalid IMDS identity material")
			}
			mu.Lock()
			got := signatureHits > 0
			mu.Unlock()
			if got != test.wantSignatureHit {
				t.Fatalf("signature endpoint called = %t, want %t", got, test.wantSignatureHit)
			}
		})
	}
}

func TestIMDSv2IdentityProviderRejectsRedirectAndNonLoopbackTestEndpoint(t *testing.T) {
	if _, err := newIMDSv2IdentityProviderForTest("http://metadata.example.invalid", nil); err == nil {
		t.Fatal("newIMDSv2IdentityProviderForTest() accepted a non-loopback endpoint")
	}

	var (
		mu         sync.Mutex
		redirected bool
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case imdsTokenPath:
			http.Redirect(writer, request, "/redirect-target", http.StatusFound)
		case "/redirect-target":
			mu.Lock()
			redirected = true
			mu.Unlock()
			writer.Header().Set("Content-Type", "text/plain")
			_, _ = writer.Write([]byte("imds-v2-token-value"))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	provider, err := newIMDSv2IdentityProviderForTest(server.URL, server.Client())
	if err != nil {
		t.Fatalf("newIMDSv2IdentityProviderForTest() error = %v", err)
	}
	if _, err := provider.Fetch(context.Background()); err == nil {
		t.Fatal("Fetch() followed an IMDS redirect")
	}
	mu.Lock()
	wasRedirected := redirected
	mu.Unlock()
	if wasRedirected {
		t.Fatal("IMDS redirect target was requested")
	}
}

func TestIMDSv2IdentityProviderProductionEndpointIsFixed(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://127.0.0.1:9999")
	provider, err := NewIMDSv2IdentityProvider()
	if err != nil {
		t.Fatalf("NewIMDSv2IdentityProvider() error = %v", err)
	}
	if got := provider.endpoint.String(); got != productionIMDSEndpoint {
		t.Fatalf("production IMDS endpoint = %q, want %q", got, productionIMDSEndpoint)
	}
}

func TestIMDSv2IdentityProviderRejectsOversizedIdentityDocument(t *testing.T) {
	oversizedDocument := []byte("{" + strings.Repeat("a", maxIdentityDocumentBytes) + "}")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case imdsTokenPath:
			writer.Header().Set("Content-Type", "text/plain")
			_, _ = writer.Write([]byte("imds-v2-token-value"))
		case imdsIdentityDocumentPath:
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write(oversizedDocument)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	provider, err := newIMDSv2IdentityProviderForTest(server.URL, server.Client())
	if err != nil {
		t.Fatalf("newIMDSv2IdentityProviderForTest() error = %v", err)
	}
	if _, err := provider.Fetch(context.Background()); err == nil {
		t.Fatal("Fetch() accepted an oversized IID document")
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
