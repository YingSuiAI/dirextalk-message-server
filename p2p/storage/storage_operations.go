package storage

import (
	"context"
	"database/sql"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/operations"
)

const operationColumns = `
	operation_id, action, status, phase, room_id, current_room_id, user_id,
	peer_mxid, request_id, base_request_id, result_json, error_code, revision, lease_owner, lease_until,
	created_at, updated_at
`

func (s *DatabaseStore) LookupOperation(ctx context.Context, operationID string) (operations.Record, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+operationColumns+` FROM p2p_operations WHERE operation_id = $1`, operationID)
	var record operations.Record
	if err := row.Scan(
		&record.OperationID,
		&record.Action,
		&record.Status,
		&record.Phase,
		&record.RoomID,
		&record.CurrentRoomID,
		&record.UserID,
		&record.PeerMXID,
		&record.RequestID,
		&record.BaseRequestID,
		&record.ResultJSON,
		&record.ErrorCode,
		&record.Revision,
		&record.LeaseOwner,
		&record.LeaseUntil,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err == sql.ErrNoRows {
		return operations.Record{}, false, nil
	} else if err != nil {
		return operations.Record{}, false, err
	}
	return record, true, nil
}

func (s *DatabaseStore) UpsertOperation(ctx context.Context, record operations.Record) error {
	return s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		_, err := txn.ExecContext(ctx, `
			INSERT INTO p2p_operations (
				operation_id, action, status, phase, room_id, current_room_id, user_id,
				peer_mxid, request_id, base_request_id, result_json, error_code, revision, lease_owner, lease_until,
				created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
			ON CONFLICT(operation_id) DO UPDATE SET
				action = EXCLUDED.action,
				status = EXCLUDED.status,
				phase = EXCLUDED.phase,
				room_id = EXCLUDED.room_id,
				current_room_id = EXCLUDED.current_room_id,
				user_id = EXCLUDED.user_id,
				peer_mxid = EXCLUDED.peer_mxid,
				request_id = EXCLUDED.request_id,
				base_request_id = EXCLUDED.base_request_id,
				result_json = EXCLUDED.result_json,
				error_code = EXCLUDED.error_code,
				revision = EXCLUDED.revision,
				lease_owner = EXCLUDED.lease_owner,
				lease_until = EXCLUDED.lease_until,
				created_at = EXCLUDED.created_at,
				updated_at = EXCLUDED.updated_at
		`, record.OperationID, record.Action, record.Status, record.Phase, record.RoomID, record.CurrentRoomID,
			record.UserID, record.PeerMXID, record.RequestID, record.BaseRequestID, record.ResultJSON, record.ErrorCode,
			record.Revision, record.LeaseOwner, record.LeaseUntil, record.CreatedAt, record.UpdatedAt)
		return err
	})
}

func (s *DatabaseStore) ClaimOperation(
	ctx context.Context,
	record operations.Record,
	owner string,
	leaseDurationMillis int64,
) (operations.Record, bool, error) {
	var claimed operations.Record
	found := false
	err := s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		row := txn.QueryRowContext(ctx, `
			INSERT INTO p2p_operations (
				operation_id, action, status, phase, room_id, current_room_id, user_id,
				peer_mxid, request_id, base_request_id, result_json, error_code, revision, lease_owner, lease_until,
				created_at, updated_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 1,
				CASE WHEN $13 <> '' AND $14::bigint > 0 THEN $13 ELSE '' END,
				CASE WHEN $13 <> '' AND $14::bigint > 0
					THEN floor(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint + $14::bigint
					ELSE 0
				END,
				$15, $16
			)
			ON CONFLICT(operation_id) DO UPDATE SET
				revision = p2p_operations.revision + 1,
				lease_owner = EXCLUDED.lease_owner,
				lease_until = EXCLUDED.lease_until
			WHERE (
				p2p_operations.lease_owner = ''
				OR p2p_operations.lease_owner = EXCLUDED.lease_owner
				OR p2p_operations.lease_until <= floor(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint
			)
			AND p2p_operations.action = EXCLUDED.action
			AND (EXCLUDED.room_id = '' OR p2p_operations.room_id = EXCLUDED.room_id)
			AND (EXCLUDED.user_id = '' OR p2p_operations.user_id = EXCLUDED.user_id)
			AND (EXCLUDED.peer_mxid = '' OR p2p_operations.peer_mxid = EXCLUDED.peer_mxid)
			AND (EXCLUDED.request_id = '' OR p2p_operations.request_id = EXCLUDED.request_id)
			RETURNING `+operationColumns+`
		`, record.OperationID, record.Action, record.Status, record.Phase, record.RoomID, record.CurrentRoomID,
			record.UserID, record.PeerMXID, record.RequestID, record.BaseRequestID, record.ResultJSON, record.ErrorCode,
			owner, leaseDurationMillis, record.CreatedAt, record.UpdatedAt)
		if err := scanOperation(row, &claimed); err == sql.ErrNoRows {
			return nil
		} else if err != nil {
			return err
		}
		found = true
		return nil
	})
	if err != nil {
		return operations.Record{}, false, err
	}
	if found {
		return claimed, true, nil
	}
	current, ok, err := s.LookupOperation(ctx, record.OperationID)
	return current, false, func() error {
		if err != nil {
			return err
		}
		if !ok {
			return sql.ErrNoRows
		}
		return nil
	}()
}

func (s *DatabaseStore) CompareAndSwapOperation(
	ctx context.Context,
	record operations.Record,
	expectedRevision int64,
	owner string,
	leaseDurationMillis int64,
) (operations.Record, bool, error) {
	var updated operations.Record
	found := false
	err := s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		row := txn.QueryRowContext(ctx, `
			UPDATE p2p_operations SET
				action = $1, status = $2, phase = $3, room_id = $4, current_room_id = $5,
				user_id = $6, peer_mxid = $7, request_id = $8, base_request_id = $9,
				result_json = $10, error_code = $11, revision = revision + 1,
				lease_owner = CASE WHEN $12 <> '' AND $13::bigint > 0 THEN $12 ELSE '' END,
				lease_until = CASE WHEN $12 <> '' AND $13::bigint > 0
					THEN floor(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint + $13::bigint
					ELSE 0
				END,
				created_at = $14, updated_at = $15
			WHERE operation_id = $16 AND revision = $17 AND lease_owner = $18
				AND lease_until > floor(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint
			RETURNING `+operationColumns+`
		`, record.Action, record.Status, record.Phase, record.RoomID, record.CurrentRoomID,
			record.UserID, record.PeerMXID, record.RequestID, record.BaseRequestID, record.ResultJSON, record.ErrorCode,
			record.LeaseOwner, leaseDurationMillis, record.CreatedAt, record.UpdatedAt,
			record.OperationID, expectedRevision, owner)
		if err := scanOperation(row, &updated); err == sql.ErrNoRows {
			return nil
		} else if err != nil {
			return err
		}
		found = true
		return nil
	})
	return updated, found, err
}

type operationScanner interface {
	Scan(dest ...any) error
}

func scanOperation(row operationScanner, record *operations.Record) error {
	return row.Scan(
		&record.OperationID, &record.Action, &record.Status, &record.Phase, &record.RoomID,
		&record.CurrentRoomID, &record.UserID, &record.PeerMXID, &record.RequestID,
		&record.BaseRequestID, &record.ResultJSON, &record.ErrorCode, &record.Revision, &record.LeaseOwner,
		&record.LeaseUntil, &record.CreatedAt, &record.UpdatedAt,
	)
}
