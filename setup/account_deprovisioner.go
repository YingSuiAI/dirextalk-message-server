package setup

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/sqlutil"
	"github.com/YingSuiAI/direxio-message-server/setup/config"
	"github.com/YingSuiAI/direxio-message-server/setup/process"
	"github.com/sirupsen/logrus"
)

const accountDeprovisionShutdownDelay = 2 * time.Second

type accountDeprovisioner struct {
	processCtx *process.ProcessContext
	cm         *sqlutil.Connections
	databases  []config.DatabaseOptions
}

func newAccountDeprovisioner(processCtx *process.ProcessContext, cfg *config.Dendrite, cm *sqlutil.Connections) *accountDeprovisioner {
	return &accountDeprovisioner{
		processCtx: processCtx,
		cm:         cm,
		databases:  accountDeprovisionDatabases(cfg),
	}
}

func (d *accountDeprovisioner) DeprovisionAccount(ctx context.Context) error {
	if d == nil || d.cm == nil {
		return fmt.Errorf("database connection manager unavailable")
	}
	for _, dbOptions := range d.databases {
		if strings.TrimSpace(string(dbOptions.ConnectionString)) == "" {
			continue
		}
		options := dbOptions
		db, writer, err := d.cm.Connection(&options)
		if err != nil {
			return fmt.Errorf("open database %q: %w", options.ConnectionString, err)
		}
		if options.ConnectionString.IsSQLite() {
			if err := writer.Do(nil, nil, func(*sql.Tx) error {
				return resetSQLiteDatabase(ctx, db)
			}); err != nil {
				return fmt.Errorf("reset sqlite database %q: %w", options.ConnectionString, err)
			}
			continue
		}
		if err := writer.Do(db, nil, func(txn *sql.Tx) error {
			return resetPostgresDatabase(ctx, txn)
		}); err != nil {
			return fmt.Errorf("reset postgres database %q: %w", options.ConnectionString, err)
		}
	}
	d.scheduleShutdown()
	return nil
}

func (d *accountDeprovisioner) scheduleShutdown() {
	if d.processCtx == nil {
		return
	}
	go func() {
		time.Sleep(accountDeprovisionShutdownDelay)
		logrus.Warn("Direxio account deprovision complete; shutting down message server")
		d.processCtx.ShutdownDendrite()
	}()
}

func accountDeprovisionDatabases(cfg *config.Dendrite) []config.DatabaseOptions {
	if cfg == nil {
		return nil
	}
	if cfg.Global.DatabaseOptions.ConnectionString != "" {
		return []config.DatabaseOptions{cfg.Global.DatabaseOptions}
	}
	candidates := []config.DatabaseOptions{
		cfg.RoomServer.Database,
		cfg.SyncAPI.Database,
		cfg.UserAPI.AccountDatabase,
		cfg.KeyServer.Database,
		cfg.FederationAPI.Database,
		cfg.MediaAPI.Database,
		cfg.RelayAPI.Database,
		cfg.MSCs.Database,
	}
	seen := map[config.DataSource]struct{}{}
	result := make([]config.DatabaseOptions, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.ConnectionString == "" {
			continue
		}
		if _, ok := seen[candidate.ConnectionString]; ok {
			continue
		}
		seen[candidate.ConnectionString] = struct{}{}
		result = append(result, candidate)
	}
	return result
}

func resetPostgresDatabase(ctx context.Context, txn *sql.Tx) error {
	if txn == nil {
		return fmt.Errorf("postgres reset requires transaction")
	}
	var tables sql.NullString
	if err := txn.QueryRowContext(ctx, `
SELECT string_agg(format('%I.%I', schemaname, tablename), ', ')
FROM pg_tables
WHERE schemaname = 'public'
`).Scan(&tables); err != nil {
		return err
	}
	if !tables.Valid || strings.TrimSpace(tables.String) == "" {
		return nil
	}
	_, err := txn.ExecContext(ctx, "TRUNCATE TABLE "+tables.String+" RESTART IDENTITY CASCADE")
	return err
}

func resetSQLiteDatabase(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("sqlite reset requires database")
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		return err
	}
	defer db.ExecContext(context.Background(), "PRAGMA foreign_keys = ON") //nolint:errcheck

	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		return err
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return err
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, table := range tables {
		if _, err := db.ExecContext(ctx, `DELETE FROM `+quoteSQLiteIdentifier(table)); err != nil {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM sqlite_sequence"); err != nil && !strings.Contains(strings.ToLower(err.Error()), "no such table") {
		return err
	}
	return nil
}

func quoteSQLiteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
