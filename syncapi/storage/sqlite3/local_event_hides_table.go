// Copyright 2026 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial

package sqlite3

import (
	"context"
	"database/sql"
	"strings"

	"github.com/YingSuiAI/direxio-message-server/internal"
	"github.com/YingSuiAI/direxio-message-server/internal/sqlutil"
	"github.com/YingSuiAI/direxio-message-server/syncapi/storage/tables"
	"github.com/YingSuiAI/direxio-message-server/syncapi/types"
)

const localEventHidesSchema = `
CREATE TABLE IF NOT EXISTS syncapi_local_event_hides (
	user_id TEXT NOT NULL,
	room_id TEXT NOT NULL,
	event_id TEXT NOT NULL,
	hidden_at TEXT NOT NULL,
	PRIMARY KEY(user_id, room_id, event_id)
);

CREATE INDEX IF NOT EXISTS syncapi_local_event_hides_user_room_idx
	ON syncapi_local_event_hides(user_id, room_id);

CREATE TABLE IF NOT EXISTS syncapi_local_room_clears (
	user_id TEXT NOT NULL,
	room_id TEXT NOT NULL,
	through_stream_pos BIGINT NOT NULL,
	hidden_at TEXT NOT NULL,
	PRIMARY KEY(user_id, room_id)
);
`

const insertLocalEventHideSQL = `
INSERT INTO syncapi_local_event_hides (user_id, room_id, event_id, hidden_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT(user_id, room_id, event_id) DO UPDATE SET hidden_at = excluded.hidden_at
`

const upsertLocalRoomClearSQL = `
INSERT INTO syncapi_local_room_clears (user_id, room_id, through_stream_pos, hidden_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT(user_id, room_id) DO UPDATE SET
	through_stream_pos = excluded.through_stream_pos,
	hidden_at = excluded.hidden_at
`

const selectLocalEventHidesSQL = `
SELECT event_id FROM syncapi_local_event_hides
WHERE user_id = $1 AND room_id = $2 AND event_id IN ($3)
`

const selectLocalRoomClearSQL = `
SELECT through_stream_pos FROM syncapi_local_room_clears
WHERE user_id = $1 AND room_id = $2
`

const purgeLocalEventHidesSQL = `
DELETE FROM syncapi_local_event_hides WHERE room_id = $1
`

const purgeLocalRoomClearsSQL = `
DELETE FROM syncapi_local_room_clears WHERE room_id = $1
`

type localEventHidesStatements struct {
	insertLocalEventHideStmt *sql.Stmt
	upsertLocalRoomClearStmt *sql.Stmt
	selectLocalRoomClearStmt *sql.Stmt
	purgeLocalEventHidesStmt *sql.Stmt
	purgeLocalRoomClearsStmt *sql.Stmt
	db                       *sql.DB
}

func NewSqliteLocalEventHidesTable(db *sql.DB) (tables.LocalEventHides, error) {
	if _, err := db.Exec(localEventHidesSchema); err != nil {
		return nil, err
	}
	s := &localEventHidesStatements{db: db}
	return s, sqlutil.StatementList{
		{&s.insertLocalEventHideStmt, insertLocalEventHideSQL},
		{&s.upsertLocalRoomClearStmt, upsertLocalRoomClearSQL},
		{&s.selectLocalRoomClearStmt, selectLocalRoomClearSQL},
		{&s.purgeLocalEventHidesStmt, purgeLocalEventHidesSQL},
		{&s.purgeLocalRoomClearsStmt, purgeLocalRoomClearsSQL},
	}.Prepare(db)
}

func (s *localEventHidesStatements) InsertLocalEventHides(ctx context.Context, txn *sql.Tx, userID, roomID string, eventIDs []string, hiddenAt string) error {
	stmt := sqlutil.TxStmt(txn, s.insertLocalEventHideStmt)
	for _, eventID := range eventIDs {
		if _, err := stmt.ExecContext(ctx, userID, roomID, eventID, hiddenAt); err != nil {
			return err
		}
	}
	return nil
}

func (s *localEventHidesStatements) UpsertLocalRoomClear(ctx context.Context, txn *sql.Tx, userID, roomID string, throughStreamPos types.StreamPosition, hiddenAt string) error {
	_, err := sqlutil.TxStmt(txn, s.upsertLocalRoomClearStmt).ExecContext(ctx, userID, roomID, throughStreamPos, hiddenAt)
	return err
}

func (s *localEventHidesStatements) SelectLocalEventHideState(ctx context.Context, txn *sql.Tx, userID, roomID string, eventIDs []string) (types.LocalEventHideState, error) {
	state := types.LocalEventHideState{EventIDs: map[string]struct{}{}}
	if err := sqlutil.TxStmt(txn, s.selectLocalRoomClearStmt).QueryRowContext(ctx, userID, roomID).Scan(&state.ClearStreamPosition); err != nil && err != sql.ErrNoRows {
		return state, err
	}
	if len(eventIDs) == 0 {
		return state, nil
	}
	params := make([]any, 0, len(eventIDs)+2)
	params = append(params, userID, roomID)
	for _, eventID := range eventIDs {
		params = append(params, eventID)
	}
	query := strings.Replace(selectLocalEventHidesSQL, "($3)", sqlutil.QueryVariadicOffset(len(eventIDs), 2), 1)
	stmt, err := s.db.PrepareContext(ctx, query)
	if err != nil {
		return state, err
	}
	defer internal.CloseAndLogIfError(ctx, stmt, "SelectLocalEventHideState: stmt.close() failed")
	rows, err := sqlutil.TxStmt(txn, stmt).QueryContext(ctx, params...)
	if err != nil {
		return state, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectLocalEventHideState: rows.close() failed")
	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			return state, err
		}
		state.EventIDs[eventID] = struct{}{}
	}
	return state, rows.Err()
}

func (s *localEventHidesStatements) PurgeLocalEventHides(ctx context.Context, txn *sql.Tx, roomID string) error {
	if _, err := sqlutil.TxStmt(txn, s.purgeLocalEventHidesStmt).ExecContext(ctx, roomID); err != nil {
		return err
	}
	_, err := sqlutil.TxStmt(txn, s.purgeLocalRoomClearsStmt).ExecContext(ctx, roomID)
	return err
}
