package setup

import "testing"

func TestProductAgentURLFromEnvPrefersDirextalkName(t *testing.T) {
	t.Setenv("DIREXIO_PRODUCT_AGENT_URL", "http://legacy:8797")
	t.Setenv("DIREXTALK_PRODUCT_AGENT_URL", "http://product-agent:8797")
	if got := productAgentURLFromEnv(); got != "http://product-agent:8797" {
		t.Fatalf("unexpected Product Agent URL %q", got)
	}
}

func TestProductAgentURLFromEnvSupportsCompatibilityAlias(t *testing.T) {
	t.Setenv("DIREXTALK_PRODUCT_AGENT_URL", "")
	t.Setenv("DIREXIO_PRODUCT_AGENT_URL", "http://legacy:8797")
	if got := productAgentURLFromEnv(); got != "http://legacy:8797" {
		t.Fatalf("unexpected Product Agent URL %q", got)
	}
}
