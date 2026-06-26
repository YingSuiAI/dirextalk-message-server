package storage

import (
	"context"
	"database/sql"
	"strings"
)

func (s *DatabaseStore) UpsertFavorite(ctx context.Context, favorite favoriteRecord) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_favorites (id, event_id, room_id, sender_id, sender_name, content, message_type, origin_server_ts, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT(id) DO UPDATE SET
				event_id = EXCLUDED.event_id,
				room_id = EXCLUDED.room_id,
				sender_id = EXCLUDED.sender_id,
				sender_name = EXCLUDED.sender_name,
				content = EXCLUDED.content,
				message_type = EXCLUDED.message_type,
				origin_server_ts = EXCLUDED.origin_server_ts,
				created_at = EXCLUDED.created_at
		`, favorite.ID, favorite.EventID, favorite.RoomID, favorite.SenderID, favorite.SenderName, favorite.Content, favorite.MessageType, favorite.OriginServerTS, favorite.CreatedAt)
		return err
	})
}

func (s *DatabaseStore) FindFavoriteByEvent(ctx context.Context, eventID, roomID string) (favoriteRecord, bool, error) {
	query := listFavoritesSelect + ` WHERE event_id = $1`
	args := []any{eventID}
	if strings.TrimSpace(roomID) != "" {
		query += ` AND room_id = $2`
		args = append(args, roomID)
	}
	query += ` ORDER BY created_at DESC LIMIT 1`
	var favorite favoriteRecord
	err := s.db.QueryRowContext(ctx, query, args...).Scan(
		&favorite.ID,
		&favorite.EventID,
		&favorite.RoomID,
		&favorite.SenderID,
		&favorite.SenderName,
		&favorite.Content,
		&favorite.MessageType,
		&favorite.OriginServerTS,
		&favorite.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return favoriteRecord{}, false, nil
	}
	if err != nil {
		return favoriteRecord{}, false, err
	}
	return favorite, true, nil
}

func (s *DatabaseStore) ListFavorites(ctx context.Context, messageType string) ([]favoriteRecord, error) {
	var rows *sql.Rows
	var err error
	if messageType == "" {
		rows, err = s.db.QueryContext(ctx, listFavoritesSelect+` ORDER BY created_at DESC`)
	} else {
		rows, err = s.db.QueryContext(ctx, listFavoritesSelect+` WHERE message_type = $1 ORDER BY created_at DESC`, messageType)
	}
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	var favorites []favoriteRecord
	for rows.Next() {
		var favorite favoriteRecord
		if err := rows.Scan(&favorite.ID, &favorite.EventID, &favorite.RoomID, &favorite.SenderID, &favorite.SenderName, &favorite.Content, &favorite.MessageType, &favorite.OriginServerTS, &favorite.CreatedAt); err != nil {
			return nil, err
		}
		favorites = append(favorites, favorite)
	}
	return favorites, rows.Err()
}

const listFavoritesSelect = `SELECT id, event_id, room_id, sender_id, sender_name, content, message_type, origin_server_ts, created_at FROM p2p_favorites`

func (s *DatabaseStore) DeleteFavorite(ctx context.Context, id int64) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `DELETE FROM p2p_favorites WHERE id = $1`, id)
		return err
	})
}

func (s *DatabaseStore) InsertReport(ctx context.Context, report reportRecord) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_reports (id, reporter_domain, reported_domain, target_type, reason, images_json, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, report.ID, report.ReporterDomain, report.ReportedDomain, report.TargetType, report.Reason, report.ImagesJSON, report.CreatedAt)
		return err
	})
}

func (s *DatabaseStore) UpsertFollow(ctx context.Context, follow followRecord) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_follows (domain, created_at)
			VALUES ($1, $2)
			ON CONFLICT(domain) DO UPDATE SET created_at = EXCLUDED.created_at
		`, follow.Domain, follow.CreatedAt)
		return err
	})
}

func (s *DatabaseStore) ListFollows(ctx context.Context) ([]followRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT domain, created_at FROM p2p_follows ORDER BY domain ASC`)
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	var follows []followRecord
	for rows.Next() {
		var follow followRecord
		if err := rows.Scan(&follow.Domain, &follow.CreatedAt); err != nil {
			return nil, err
		}
		follows = append(follows, follow)
	}
	return follows, rows.Err()
}

func (s *DatabaseStore) DeleteFollow(ctx context.Context, domain string) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `DELETE FROM p2p_follows WHERE domain = $1`, domain)
		return err
	})
}
