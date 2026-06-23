// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

//go:build !wasm
// +build !wasm

package storage

import (
	"fmt"

	"github.com/YingSuiAI/direxio-message-server/internal/sqlutil"
	"github.com/YingSuiAI/direxio-message-server/mediaapi/storage/postgres"
	"github.com/YingSuiAI/direxio-message-server/mediaapi/storage/sqlite3"
	"github.com/YingSuiAI/direxio-message-server/setup/config"
)

// NewMediaAPIDatasource opens a database connection.
func NewMediaAPIDatasource(conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions) (Database, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return sqlite3.NewDatabase(conMan, dbProperties)
	case dbProperties.ConnectionString.IsPostgres():
		return postgres.NewDatabase(conMan, dbProperties)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
