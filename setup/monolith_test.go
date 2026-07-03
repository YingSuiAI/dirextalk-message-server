package setup

import (
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
)

func TestP2PDatabaseOptionsUseGlobalDatabaseWhenConfigured(t *testing.T) {
	cfg := &config.Dendrite{}
	cfg.Global.DatabaseOptions.ConnectionString = "file:global.db"
	cfg.RoomServer.Database.ConnectionString = "file:roomserver.db"

	got := p2pDatabaseOptions(cfg)
	if got.ConnectionString != "file:global.db" {
		t.Fatalf("expected global database, got %q", got.ConnectionString)
	}
}

func TestP2PDatabaseOptionsFallbackToRoomserverDatabase(t *testing.T) {
	cfg := &config.Dendrite{}
	cfg.RoomServer.Database.ConnectionString = "file:roomserver.db"

	got := p2pDatabaseOptions(cfg)
	if got.ConnectionString != "file:roomserver.db" {
		t.Fatalf("expected roomserver database fallback, got %q", got.ConnectionString)
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
