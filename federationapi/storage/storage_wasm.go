// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package storage

import (
	"context"
	"fmt"

	"github.com/YingSuiAI/dirextalk-message-server/internal/caching"
	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

// NewDatabase opens a new database
func NewDatabase(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions, cache caching.FederationCache, isLocalServerName func(spec.ServerName) bool) (Database, error) {
	return nil, fmt.Errorf("database storage is not supported in wasm builds")
}
