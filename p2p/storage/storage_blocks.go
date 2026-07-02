package storage

import (
	"context"
	"database/sql"
)

func (s *DatabaseStore) UpsertBlock(ctx context.Context, block blockRecord) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_blocks (
				target_type, target_id, room_id, peer_mxid, display_name, avatar_url, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT(target_type, target_id) DO UPDATE SET
				room_id = EXCLUDED.room_id,
				peer_mxid = EXCLUDED.peer_mxid,
				display_name = EXCLUDED.display_name,
				avatar_url = EXCLUDED.avatar_url,
				created_at = CASE
					WHEN p2p_blocks.created_at > 0 THEN p2p_blocks.created_at
					ELSE EXCLUDED.created_at
				END
		`, block.TargetType, block.TargetID, block.RoomID, block.PeerMXID, block.DisplayName, block.AvatarURL, block.CreatedAt)
		return err
	})
}

func (s *DatabaseStore) DeleteBlock(ctx context.Context, targetType, targetID string) (bool, error) {
	var rowsAffected int64
	err := s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		result, err := s.db.ExecContext(ctx, `DELETE FROM p2p_blocks WHERE target_type = $1 AND target_id = $2`, targetType, targetID)
		if err != nil {
			return err
		}
		rowsAffected, err = result.RowsAffected()
		return err
	})
	return rowsAffected > 0, err
}

func (s *DatabaseStore) ListBlocks(ctx context.Context) ([]blockRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT target_type, target_id, room_id, peer_mxid, display_name, avatar_url, created_at
		FROM p2p_blocks
		ORDER BY target_type ASC, display_name ASC, target_id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	blocks := make([]blockRecord, 0)
	for rows.Next() {
		var block blockRecord
		if err := rows.Scan(
			&block.TargetType,
			&block.TargetID,
			&block.RoomID,
			&block.PeerMXID,
			&block.DisplayName,
			&block.AvatarURL,
			&block.CreatedAt,
		); err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}
	return blocks, rows.Err()
}
