package p2p

import (
	"context"
	"database/sql"

	"github.com/YingSuiAI/direxio-message-server/internal/sqlutil"
	p2pstorage "github.com/YingSuiAI/direxio-message-server/p2p/storage"
	"github.com/YingSuiAI/direxio-message-server/setup/config"
)

type DatabaseStore = p2pstorage.DatabaseStore

func NewDatabaseStore(ctx context.Context, cm *sqlutil.Connections, dbProperties *config.DatabaseOptions) (*DatabaseStore, error) {
	return p2pstorage.NewDatabaseStore(ctx, cm, dbProperties)
}

func NewUnmigratedDatabaseStore(db *sql.DB, writer sqlutil.Writer) *DatabaseStore {
	return p2pstorage.NewUnmigratedDatabaseStore(db, writer)
}
