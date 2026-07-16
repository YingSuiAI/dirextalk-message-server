package setup

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/p2p"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
)

func TestP2PDatabaseOptionsUseGlobalDatabaseWhenConfigured(t *testing.T) {
	cfg := &config.Dendrite{}
	cfg.Global.DatabaseOptions.ConnectionString = "postgres://localhost/global?sslmode=disable"
	cfg.RoomServer.Database.ConnectionString = "postgres://localhost/roomserver?sslmode=disable"

	got := p2pDatabaseOptions(cfg)
	if got.ConnectionString != "postgres://localhost/global?sslmode=disable" {
		t.Fatalf("expected global database, got %q", got.ConnectionString)
	}
}

func TestP2PDatabaseOptionsFallbackToRoomserverDatabase(t *testing.T) {
	cfg := &config.Dendrite{}
	cfg.RoomServer.Database.ConnectionString = "postgres://localhost/roomserver?sslmode=disable"

	got := p2pDatabaseOptions(cfg)
	if got.ConnectionString != "postgres://localhost/roomserver?sslmode=disable" {
		t.Fatalf("expected roomserver database fallback, got %q", got.ConnectionString)
	}
}

func TestPersistentP2PServiceRejectsSQLiteInsteadOfFallingBackToMemory(t *testing.T) {
	dbOpts := config.DatabaseOptions{ConnectionString: "file:p2p.db"}

	service, err := newPersistentP2PService(
		context.Background(),
		p2p.Config{ServerName: "example.com"},
		sqlutil.NewConnectionManager(nil, dbOpts),
		&dbOpts,
		nil,
	)

	if err == nil || !strings.Contains(err.Error(), "SQLite") {
		t.Fatalf("expected SQLite-backed startup to fail explicitly, got service=%v err=%v", service, err)
	}
	if service != nil {
		t.Fatalf("expected no in-memory P2P service fallback")
	}
}

func TestP2PEventRetentionFromEnv(t *testing.T) {
	t.Setenv("P2P_EVENT_RETENTION_MAX_ROWS", "5000")
	t.Setenv("P2P_EVENT_RETENTION_PRUNE_ON_WRITE", "true")

	if got := p2pEventRetentionMaxRowsFromEnv(); got != 5000 {
		t.Fatalf("expected max rows 5000, got %d", got)
	}
	if !p2pEventRetentionPruneOnWriteFromEnv() {
		t.Fatalf("expected prune on write to be enabled")
	}
}

func TestP2PEventRetentionInvalidEnvDisablesPruning(t *testing.T) {
	t.Setenv("P2P_EVENT_RETENTION_MAX_ROWS", "-1")
	t.Setenv("P2P_EVENT_RETENTION_PRUNE_ON_WRITE", "not-bool")

	if got := p2pEventRetentionMaxRowsFromEnv(); got != 0 {
		t.Fatalf("expected invalid max rows to disable retention, got %d", got)
	}
	if p2pEventRetentionPruneOnWriteFromEnv() {
		t.Fatalf("expected invalid prune flag to disable pruning")
	}
}

func TestP2PCloudConnectionStackConfigFromEnv(t *testing.T) {
	unsetEnvironmentVariable(t, "P2P_CLOUD_CONNECTION_STACK_TEMPLATE_URL")
	unsetEnvironmentVariable(t, "P2P_CLOUD_CONNECTION_STACK_TEMPLATE_DIGEST")
	t.Setenv("P2P_CLOUD_CONNECTION_TEMPLATE_JSON", `{"schema":"dirextalk.connection-template-reference/v1","mode":"s3_binding","binding":{"schema":"dirextalk.immutable-artifact-binding/v1","kind":"connection_stack_template","version":"v1.1.0-cloud-mvp.20260716.1","bucket":"dirextalk-artifacts","key":"releases/connection-stack/v1.1.0-cloud-mvp.20260716.1/connection-stack-v1.1.0-cloud-mvp.20260716.1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.yaml","version_id":"version-00000001","sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size_bytes":512,"content_type":"application/x-yaml","kms_key_id":"alias/dirextalk-artifacts"}}`)
	t.Setenv("P2P_CLOUD_CONNECTION_STACK_SOURCE_TREE_DIGEST", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	t.Setenv("P2P_CLOUD_CONNECTION_NODE_KEY_ID", "node-key-1")
	t.Setenv("P2P_CLOUD_CONNECTION_NODE_PUBLIC_KEY_SPKI_BASE64", "public-key-material")
	t.Setenv("P2P_CLOUD_CONNECTION_ROLE_PLAN_TTL_SECONDS", "900")

	config := p2pCloudConnectionStackConfigFromEnv()
	if config.TemplateURL != "" || config.ConnectionTemplate.Mode != "s3_binding" ||
		config.TemplateDigest != "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ||
		config.SourceTreeDigest != "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" ||
		config.NodeKeyID != "node-key-1" || config.NodePublicKeySPKIBase64 != "public-key-material" ||
		config.RolePlanTTL.Seconds() != 900 {
		t.Fatalf("connection Stack config = %#v", config)
	}

	t.Setenv("P2P_CLOUD_CONNECTION_ROLE_PLAN_TTL_SECONDS", "0")
	if config := p2pCloudConnectionStackConfigFromEnv(); config.RolePlanTTL != 0 {
		t.Fatalf("invalid role-plan TTL must fail closed: %#v", config)
	}
}

func TestP2PCloudConnectionStackConfigFromEnvRejectsLegacyOrMalformedTemplateSettings(t *testing.T) {
	validTemplate := `{"schema":"dirextalk.connection-template-reference/v1","mode":"publish_intent","publish_intent":{"kind":"connection_stack_template","version":"v1.1.0-cloud-mvp.20260716.1","sha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size_bytes":512,"content_type":"application/x-yaml"}}`
	for name, configure := range map[string]func(*testing.T){
		"legacy_url": func(t *testing.T) {
			t.Setenv("P2P_CLOUD_CONNECTION_TEMPLATE_JSON", validTemplate)
			t.Setenv("P2P_CLOUD_CONNECTION_STACK_TEMPLATE_URL", "https://s3.us-east-1.amazonaws.com/dirextalk-artifacts/template.yaml?versionId=version-00000001")
		},
		"legacy_digest": func(t *testing.T) {
			t.Setenv("P2P_CLOUD_CONNECTION_TEMPLATE_JSON", validTemplate)
			t.Setenv("P2P_CLOUD_CONNECTION_STACK_TEMPLATE_DIGEST", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		},
		"unknown_json_field": func(t *testing.T) {
			t.Setenv("P2P_CLOUD_CONNECTION_TEMPLATE_JSON", validTemplate[:len(validTemplate)-1]+`,"url":"https://mutable.example.invalid/template.yaml"}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			unsetEnvironmentVariable(t, "P2P_CLOUD_CONNECTION_STACK_TEMPLATE_URL")
			unsetEnvironmentVariable(t, "P2P_CLOUD_CONNECTION_STACK_TEMPLATE_DIGEST")
			configure(t)
			if config := p2pCloudConnectionStackConfigFromEnv(); config.ConnectionTemplate.Mode != "" || config.TemplateDigest != "" || config.RolePlanTTL != 0 {
				t.Fatalf("legacy or malformed template configuration was accepted: %#v", config)
			}
		})
	}
}

func unsetEnvironmentVariable(t *testing.T, name string) {
	t.Helper()
	previous, existed := os.LookupEnv(name)
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if existed {
			_ = os.Setenv(name, previous)
			return
		}
		_ = os.Unsetenv(name)
	})
}

func TestP2PCloudConnectionCredentialBootstrapConfigFromEnv(t *testing.T) {
	t.Setenv("P2P_CLOUD_CONNECTION_CREDENTIAL_BOOTSTRAP_ENDPOINT", "https://bootstrap.internal.example/v1/aws-bootstrap/sessions")
	t.Setenv("P2P_CLOUD_CONNECTION_CREDENTIAL_BOOTSTRAP_CA_FILE", "/run/dirextalk/bootstrap-ca.pem")
	t.Setenv("P2P_CLOUD_CONNECTION_CREDENTIAL_BOOTSTRAP_CERT_FILE", "/run/dirextalk/bootstrap-client.pem")
	t.Setenv("P2P_CLOUD_CONNECTION_CREDENTIAL_BOOTSTRAP_KEY_FILE", "/run/dirextalk/bootstrap-client.key")
	t.Setenv("P2P_CLOUD_CONNECTION_CREDENTIAL_BOOTSTRAP_TIMEOUT_SECONDS", "7")

	config := p2pCloudConnectionCredentialBootstrapConfigFromEnv()
	if config.Endpoint != "https://bootstrap.internal.example/v1/aws-bootstrap/sessions" || config.CAFile != "/run/dirextalk/bootstrap-ca.pem" ||
		config.CertificateFile != "/run/dirextalk/bootstrap-client.pem" || config.KeyFile != "/run/dirextalk/bootstrap-client.key" || config.Timeout != 7*time.Second {
		t.Fatalf("credential bootstrap config = %#v", config)
	}
	t.Setenv("P2P_CLOUD_CONNECTION_CREDENTIAL_BOOTSTRAP_TIMEOUT_SECONDS", "31")
	if config := p2pCloudConnectionCredentialBootstrapConfigFromEnv(); config.Timeout >= 0 {
		t.Fatalf("invalid timeout must fail closed: %#v", config)
	}
}
