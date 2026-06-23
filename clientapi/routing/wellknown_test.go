package routing

import (
	"net/http"
	"testing"
)

func TestWellKnownClientBaseURLAutoUsesRequestHost(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://localhost:18008/.well-known/matrix/client", nil)
	if err != nil {
		t.Fatal(err)
	}

	got := wellKnownClientBaseURL(req, "auto")
	if got != "http://localhost:18008" {
		t.Fatalf("expected request host base URL, got %q", got)
	}
}

func TestWellKnownClientBaseURLAutoUsesForwardedHeaders(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:18008/.well-known/matrix/client", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Forwarded-Host", "matrix.example.com")

	got := wellKnownClientBaseURL(req, "auto")
	if got != "https://matrix.example.com" {
		t.Fatalf("expected forwarded base URL, got %q", got)
	}
}

func TestWellKnownClientBaseURLAutoAcceptsURLPlaceholder(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://10.0.2.2:18008/.well-known/matrix/client", nil)
	if err != nil {
		t.Fatal(err)
	}

	got := wellKnownClientBaseURL(req, "http://auto")
	if got != "http://10.0.2.2:18008" {
		t.Fatalf("expected request host base URL, got %q", got)
	}
}

func TestWellKnownClientBaseURLPreservesConfiguredValue(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://localhost:18008/.well-known/matrix/client", nil)
	if err != nil {
		t.Fatal(err)
	}

	got := wellKnownClientBaseURL(req, "https://matrix.example.com")
	if got != "https://matrix.example.com" {
		t.Fatalf("expected configured base URL, got %q", got)
	}
}
