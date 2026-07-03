// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package testrig

import (
	"fmt"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/setup/process"
	"github.com/YingSuiAI/dirextalk-message-server/test"
)

func CreateConfig(t *testing.T, dbType test.DBType) (*config.Dendrite, *process.ProcessContext, func()) {
	var cfg config.Dendrite
	cfg.Defaults(config.DefaultOpts{
		Generate:       false,
		SingleDatabase: true,
	})
	cfg.Global.JetStream.InMemory = true
	cfg.FederationAPI.KeyPerspectives = nil
	ctx := process.NewProcessContext()
	switch dbType {
	case test.DBTypePostgres:
		cfg.Global.Defaults(config.DefaultOpts{ // autogen a signing key
			Generate:       true,
			SingleDatabase: true,
		})
		cfg.MediaAPI.Defaults(config.DefaultOpts{ // autogen a media path
			Generate:       true,
			SingleDatabase: true,
		})
		cfg.SyncAPI.Fulltext.Defaults(config.DefaultOpts{ // use in memory fts
			Generate:       true,
			SingleDatabase: true,
		})
		cfg.Global.ServerName = "test"
		// use a distinct prefix else concurrent postgres/sqlite runs will clash since NATS will use
		// the file system event with InMemory=true :(
		cfg.Global.JetStream.TopicPrefix = fmt.Sprintf("Test_%d_", dbType)
		cfg.SyncAPI.Fulltext.InMemory = true

		connStr, closeDb := test.PrepareDBConnectionString(t, dbType)
		cfg.Global.DatabaseOptions = config.DatabaseOptions{
			ConnectionString:       config.DataSource(connStr),
			MaxOpenConnections:     10,
			MaxIdleConnections:     2,
			ConnMaxLifetimeSeconds: 60,
		}
		return &cfg, ctx, func() {
			ctx.ShutdownDendrite()
			ctx.WaitForShutdown()
			closeDb()
		}
	case test.DBTypeSQLite:
		t.Fatalf("sqlite test configs are disabled; use PostgreSQL")
	default:
		t.Fatalf("unknown db type: %v", dbType)
	}
	return &config.Dendrite{}, nil, func() {}
}
