// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

//go:build !wasm
// +build !wasm

package storage

import (
	"context"
	"fmt"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/syncapi/storage/postgres"
)

// NewSyncServerDatasource opens a database connection.
func NewSyncServerDatasource(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions) (Database, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return nil, fmt.Errorf("SQLite is not supported; configure PostgreSQL")
	case dbProperties.ConnectionString.IsPostgres():
		return postgres.NewDatabase(ctx, conMan, dbProperties)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
