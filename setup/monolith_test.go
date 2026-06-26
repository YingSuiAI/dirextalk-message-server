package setup

import (
	"testing"

	"github.com/YingSuiAI/direxio-message-server/setup/config"
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
