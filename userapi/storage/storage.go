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
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/matrix-org/gomatrixserverlib/spec"

	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/userapi/storage/postgres"
)

// NewUserDatabase opens a PostgreSQL database and sets connection parameters.
func NewUserDatabase(
	ctx context.Context,
	conMan *sqlutil.Connections,
	dbProperties *config.DatabaseOptions,
	serverName spec.ServerName,
	bcryptCost int,
	openIDTokenLifetimeMS int64,
	loginTokenLifetime time.Duration,
	serverNoticesLocalpart string,
) (UserDatabase, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return nil, fmt.Errorf("SQLite is not supported; configure PostgreSQL")
	case dbProperties.ConnectionString.IsPostgres():
		return postgres.NewDatabase(ctx, conMan, dbProperties, serverName, bcryptCost, openIDTokenLifetimeMS, loginTokenLifetime, serverNoticesLocalpart)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}

// NewKeyDatabase opens a PostgreSQL database and sets connection parameters.
func NewKeyDatabase(conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions) (KeyDatabase, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return nil, fmt.Errorf("SQLite is not supported; configure PostgreSQL")
	case dbProperties.ConnectionString.IsPostgres():
		return postgres.NewKeyDatabase(conMan, dbProperties)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
