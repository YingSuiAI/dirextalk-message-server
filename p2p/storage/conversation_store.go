package storage

import (
	"context"
	"database/sql"
	"fmt"
)

const conversationColumns = `
	conversation_id, matrix_room_id, kind, lifecycle, created_by_mxid, peer_mxid, title, avatar_url,
	last_event_id, last_message, last_activity_at, projection_state, projection_reason, created_at, updated_at
`

func (s *DatabaseStore) UpsertConversation(ctx context.Context, record conversationRecord) error {
	record = normalizeConversationRecord(record)
	if err := validateConversationRecord(record); err != nil {
		return err
	}
	return s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		return upsertConversation(ctx, txn, record)
	})
}

func validateConversationRecord(record conversationRecord) error {
	record = normalizeConversationRecord(record)
	if record.ConversationID == "" {
		return fmt.Errorf("conversation_id is required")
	}
	if record.MatrixRoomID == "" {
		return fmt.Errorf("matrix_room_id is required")
	}
	if record.Kind == "" {
		return fmt.Errorf("conversation kind is required")
	}
	return nil
}

type conversationDB interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type conversationQueryDB interface {
	conversationDB
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func upsertConversation(ctx context.Context, db conversationDB, record conversationRecord) error {
	record = normalizeConversationRecord(record)
	if err := validateConversationRecord(record); err != nil {
		return err
	}
	var existingID string
	var existingKind conversationKind
	err := db.QueryRowContext(ctx, `
			SELECT conversation_id, kind FROM p2p_conversations WHERE matrix_room_id = $1
		`, record.MatrixRoomID).Scan(&existingID, &existingKind)
	switch {
	case err == nil && existingKind != record.Kind:
		return fmt.Errorf("conversation kind conflict for room %s: existing %s, incoming %s", record.MatrixRoomID, existingKind, record.Kind)
	case err != nil && err != sql.ErrNoRows:
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO p2p_conversations (
			conversation_id, matrix_room_id, kind, lifecycle, created_by_mxid, peer_mxid, title, avatar_url,
			last_event_id, last_message, last_activity_at, projection_state, projection_reason, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		ON CONFLICT(conversation_id) DO UPDATE SET
			matrix_room_id = EXCLUDED.matrix_room_id,
			lifecycle = EXCLUDED.lifecycle,
			created_by_mxid = CASE WHEN EXCLUDED.created_by_mxid <> '' THEN EXCLUDED.created_by_mxid ELSE p2p_conversations.created_by_mxid END,
			peer_mxid = CASE WHEN EXCLUDED.peer_mxid <> '' THEN EXCLUDED.peer_mxid ELSE p2p_conversations.peer_mxid END,
			title = CASE WHEN EXCLUDED.title <> '' THEN EXCLUDED.title ELSE p2p_conversations.title END,
			avatar_url = CASE WHEN EXCLUDED.avatar_url <> '' THEN EXCLUDED.avatar_url ELSE p2p_conversations.avatar_url END,
			last_event_id = CASE WHEN EXCLUDED.last_event_id <> '' THEN EXCLUDED.last_event_id ELSE p2p_conversations.last_event_id END,
			last_message = CASE WHEN EXCLUDED.last_message <> '' THEN EXCLUDED.last_message ELSE p2p_conversations.last_message END,
			last_activity_at = CASE WHEN EXCLUDED.last_activity_at > 0 THEN EXCLUDED.last_activity_at ELSE p2p_conversations.last_activity_at END,
			projection_state = EXCLUDED.projection_state,
			projection_reason = EXCLUDED.projection_reason,
			updated_at = EXCLUDED.updated_at
		`, record.ConversationID, record.MatrixRoomID, record.Kind, record.Lifecycle, record.CreatedByMXID, record.PeerMXID, record.Title,
		record.AvatarURL, record.LastEventID, record.LastMessage, record.LastActivityAt, record.ProjectionState, record.ProjectionReason, record.CreatedAt, record.UpdatedAt)
	return err
}

func (s *DatabaseStore) GetConversationByID(ctx context.Context, conversationID string) (conversationRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+conversationColumns+` FROM p2p_conversations WHERE conversation_id = $1`, conversationID)
	return scanConversation(row)
}

func (s *DatabaseStore) GetConversationByRoomID(ctx context.Context, matrixRoomID string) (conversationRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+conversationColumns+` FROM p2p_conversations WHERE matrix_room_id = $1`, matrixRoomID)
	return scanConversation(row)
}

func (s *DatabaseStore) ListConversations(ctx context.Context) ([]conversationRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+conversationColumns+`
		FROM p2p_conversations
		WHERE lifecycle <> $1
		ORDER BY last_activity_at DESC, updated_at DESC, conversation_id ASC
	`, conversationLifecycleDeleted)
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	var conversations []conversationRecord
	for rows.Next() {
		record, err := scanConversationRow(rows)
		if err != nil {
			return nil, err
		}
		conversations = append(conversations, record)
	}
	return conversations, rows.Err()
}

func (s *DatabaseStore) DeleteConversationByRoomID(ctx context.Context, matrixRoomID string) error {
	return s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		_, err := txn.ExecContext(ctx, `DELETE FROM p2p_conversations WHERE matrix_room_id = $1`, matrixRoomID)
		return err
	})
}

func (s *DatabaseStore) BackfillProductConversations(ctx context.Context) error {
	return s.backfillProductConversations(ctx)
}

func (s *DatabaseStore) backfillProductConversations(ctx context.Context) error {
	return s.writer.Do(s.db, nil, func(txn *sql.Tx) error {
		return backfillProductConversations(ctx, txn)
	})
}

func backfillProductConversations(ctx context.Context, db conversationQueryDB) error {
	if err := backfillContactConversations(ctx, db); err != nil {
		return err
	}
	if err := backfillGroupConversations(ctx, db); err != nil {
		return err
	}
	return backfillChannelConversations(ctx, db)
}

func backfillContactConversations(ctx context.Context, db conversationQueryDB) error {
	ok, err := productTableExists(ctx, db, "p2p_contacts")
	if err != nil || !ok {
		return err
	}
	avatarColumn := "''"
	if hasAvatar, columnErr := productColumnExists(ctx, db, "p2p_contacts", "avatar_url"); columnErr != nil {
		return columnErr
	} else if hasAvatar {
		avatarColumn = "avatar_url"
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT room_id, peer_mxid, display_name, %s, domain, status
		FROM p2p_contacts
		WHERE TRIM(room_id) <> ''
		ORDER BY room_id ASC
	`, avatarColumn))
	if err != nil {
		return err
	}
	contacts := []contactRecord{}
	for rows.Next() {
		var contact contactRecord
		if err := rows.Scan(&contact.RoomID, &contact.PeerMXID, &contact.DisplayName, &contact.AvatarURL, &contact.Domain, &contact.Status); err != nil {
			closeResource(rows)
			return err
		}
		contacts = append(contacts, contact)
	}
	if err := rows.Err(); err != nil {
		closeResource(rows)
		return err
	}
	closeResource(rows)
	for _, contact := range contacts {
		if !contactDeleted(contact.Status) {
			if _, err := db.ExecContext(ctx, `DELETE FROM p2p_conversations WHERE matrix_room_id = $1 AND kind <> $2`, contact.RoomID, conversationKindDirect); err != nil {
				return err
			}
		}
		if err := upsertConversation(ctx, db, conversationFromContact(contact)); err != nil {
			return err
		}
	}
	return nil
}

func backfillGroupConversations(ctx context.Context, db conversationQueryDB) error {
	ok, err := productTableExists(ctx, db, "p2p_groups")
	if err != nil || !ok {
		return err
	}
	rows, err := db.QueryContext(ctx, `
		SELECT g.room_id, g.name, g.topic, g.avatar_url, g.member_count, g.invite_policy, g.muted
		FROM p2p_groups g
		WHERE TRIM(g.room_id) <> ''
			AND NOT EXISTS (
				SELECT 1 FROM p2p_contacts c
				WHERE c.room_id = g.room_id AND LOWER(TRIM(c.status)) <> 'deleted'
			)
		ORDER BY g.room_id ASC
	`)
	if err != nil {
		return err
	}
	groups := []groupRecord{}
	for rows.Next() {
		var group groupRecord
		var muted int64
		if err := rows.Scan(&group.RoomID, &group.Name, &group.Topic, &group.AvatarURL, &group.MemberCount, &group.InvitePolicy, &muted); err != nil {
			closeResource(rows)
			return err
		}
		group.Muted = muted != 0
		groups = append(groups, group)
	}
	if err := rows.Err(); err != nil {
		closeResource(rows)
		return err
	}
	closeResource(rows)
	for _, group := range groups {
		if err := upsertConversation(ctx, db, conversationFromGroup(group)); err != nil {
			return err
		}
	}
	return nil
}

func backfillChannelConversations(ctx context.Context, db conversationQueryDB) error {
	ok, err := productTableExists(ctx, db, "p2p_channels")
	if err != nil || !ok {
		return err
	}
	rows, err := db.QueryContext(ctx, `
		SELECT channel_id, room_id, name, description, avatar_url, visibility,
			join_policy, channel_type, comments_enabled, member_count,
			pending_join_count, muted
		FROM p2p_channels
		WHERE TRIM(room_id) <> ''
		ORDER BY room_id ASC
	`)
	if err != nil {
		return err
	}
	channels := []channel{}
	for rows.Next() {
		var ch channel
		var commentsEnabled, muted int64
		if err := rows.Scan(
			&ch.ChannelID,
			&ch.RoomID,
			&ch.Name,
			&ch.Description,
			&ch.AvatarURL,
			&ch.Visibility,
			&ch.JoinPolicy,
			&ch.ChannelType,
			&commentsEnabled,
			&ch.MemberCount,
			&ch.PendingJoinCount,
			&muted,
		); err != nil {
			closeResource(rows)
			return err
		}
		ch.CommentsEnabled = commentsEnabled != 0
		ch.Muted = muted != 0
		channels = append(channels, ch)
	}
	if err := rows.Err(); err != nil {
		closeResource(rows)
		return err
	}
	closeResource(rows)
	for _, ch := range channels {
		if err := upsertConversation(ctx, db, conversationFromChannel(ch)); err != nil {
			return err
		}
	}
	return nil
}

func productTableExists(ctx context.Context, db conversationDB, table string) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = $1
	`, table).Scan(&count)
	return count > 0, err
}

func productColumnExists(ctx context.Context, db conversationDB, table, column string) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
	`, table, column).Scan(&count)
	return count > 0, err
}

type conversationScanner interface {
	Scan(dest ...any) error
}

func scanConversation(row conversationScanner) (conversationRecord, bool, error) {
	record, err := scanConversationRow(row)
	if err == sql.ErrNoRows {
		return conversationRecord{}, false, nil
	}
	if err != nil {
		return conversationRecord{}, false, err
	}
	return record, true, nil
}

func scanConversationRow(row conversationScanner) (conversationRecord, error) {
	var record conversationRecord
	err := row.Scan(
		&record.ConversationID,
		&record.MatrixRoomID,
		&record.Kind,
		&record.Lifecycle,
		&record.CreatedByMXID,
		&record.PeerMXID,
		&record.Title,
		&record.AvatarURL,
		&record.LastEventID,
		&record.LastMessage,
		&record.LastActivityAt,
		&record.ProjectionState,
		&record.ProjectionReason,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	return record, err
}
