package agentclient

import "testing"

func TestConfigFromEnvNormalizesDomain(t *testing.T) {
	t.Setenv("DIREXIO_DOMAIN", "https://example.com/")
	t.Setenv("DIREXIO_AGENT_TOKEN", "agent-token")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Domain != "https://example.com" {
		t.Fatalf("expected domain without trailing slash, got %q", cfg.Domain)
	}
	if cfg.P2PBaseURL() != "https://example.com/_p2p" {
		t.Fatalf("unexpected p2p base: %q", cfg.P2PBaseURL())
	}
	if cfg.MatrixBaseURL() != "https://example.com/_matrix/client" {
		t.Fatalf("unexpected matrix base: %q", cfg.MatrixBaseURL())
	}
}

func TestConfigRejectsRoutePrefixedDomain(t *testing.T) {
	t.Setenv("DIREXIO_DOMAIN", "https://example.com/_p2p")
	t.Setenv("DIREXIO_AGENT_TOKEN", "agent-token")

	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("expected route-prefixed domain to fail")
	}
}

func TestConfigRejectsMissingAgentToken(t *testing.T) {
	t.Setenv("DIREXIO_DOMAIN", "https://example.com")

	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("expected missing agent token to fail")
	}
}
