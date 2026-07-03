// Copyright 2024 New Vector Ltd.
// Copyright 2019, 2020 The Matrix.org Foundation C.I.C.
// Copyright 2017, 2018 New Vector Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package postgres

import (
	// Import the postgres database driver.
	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/mediaapi/storage/shared"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	_ "github.com/lib/pq"
)

// NewDatabase opens a postgres database.
func NewDatabase(conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions) (*shared.Database, error) {
	db, writer, err := conMan.Connection(dbProperties)
	if err != nil {
		return nil, err
	}
	mediaRepo, err := NewPostgresMediaRepositoryTable(db)
	if err != nil {
		return nil, err
	}
	thumbnails, err := NewPostgresThumbnailsTable(db)
	if err != nil {
		return nil, err
	}
	return &shared.Database{
		MediaRepository: mediaRepo,
		Thumbnails:      thumbnails,
		DB:              db,
		Writer:          writer,
	}, nil
}
