package p2p

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPMatrixProfileResolverReturnsProfile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_matrix/client/v3/profile/@owner:t7.dirextalk.ai" {
			t.Fatalf("unexpected profile path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"displayname":"liyanan7","avatar_url":"mxc://t7/avatar"}`))
	}))
	defer server.Close()

	resolver := NewHTTPMatrixProfileResolver(server.URL, server.Client())
	profile, err := resolver.ResolveMatrixProfile(context.Background(), "@owner:t7.dirextalk.ai")
	if err != nil {
		t.Fatalf("ResolveMatrixProfile failed: %v", err)
	}
	if profile.DisplayName != "liyanan7" || profile.AvatarURL != "mxc://t7/avatar" {
		t.Fatalf("unexpected profile: %#v", profile)
	}
}

func TestHTTPMatrixProfileResolverTreatsNotFoundAsEmptyProfile(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	resolver := NewHTTPMatrixProfileResolver(server.URL, server.Client())
	profile, err := resolver.ResolveMatrixProfile(context.Background(), "@missing:example.com")
	if err != nil {
		t.Fatalf("ResolveMatrixProfile should ignore missing profiles, got %v", err)
	}
	if profile != (matrixUserProfile{}) {
		t.Fatalf("expected empty profile, got %#v", profile)
	}
}
