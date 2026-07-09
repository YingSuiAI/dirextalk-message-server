// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

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
	return nil, fmt.Errorf("database storage is not supported in wasm builds")
}

// NewKeyDatabase opens a database connection.
func NewKeyDatabase(conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions) (KeyDatabase, error) {
	return nil, fmt.Errorf("database storage is not supported in wasm builds")
}
