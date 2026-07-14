package setup

import (
	"context"
	"strings"
	"testing"

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
	t.Setenv("P2P_CLOUD_CONNECTION_STACK_TEMPLATE_URL", "https://artifacts.example.invalid/connection-stack-v2/template.json")
	t.Setenv("P2P_CLOUD_CONNECTION_STACK_TEMPLATE_DIGEST", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	t.Setenv("P2P_CLOUD_CONNECTION_STACK_SOURCE_TREE_DIGEST", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	t.Setenv("P2P_CLOUD_CONNECTION_NODE_KEY_ID", "node-key-1")
	t.Setenv("P2P_CLOUD_CONNECTION_NODE_PUBLIC_KEY_SPKI_BASE64", "public-key-material")
	t.Setenv("P2P_CLOUD_CONNECTION_ROLE_PLAN_TTL_SECONDS", "900")

	config := p2pCloudConnectionStackConfigFromEnv()
	if config.TemplateURL != "https://artifacts.example.invalid/connection-stack-v2/template.json" ||
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
