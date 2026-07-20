package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
)

func (s *DatabaseStore) UpsertReaction(ctx context.Context, reaction reactionRecord) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_reactions (
				event_id, target_type, target_id, channel_id, post_id, comment_id, reaction, user_id, active, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT(target_type, target_id, reaction, user_id) DO UPDATE SET
				event_id = EXCLUDED.event_id,
				channel_id = EXCLUDED.channel_id,
				post_id = EXCLUDED.post_id,
				comment_id = EXCLUDED.comment_id,
				active = EXCLUDED.active,
				created_at = EXCLUDED.created_at
		`, reaction.EventID, reaction.TargetType, reaction.TargetID, reaction.ChannelID, reaction.PostID, reaction.CommentID, reaction.Reaction, reaction.UserID, boolInt(reaction.Active), reaction.CreatedAt)
		return err
	})
}

func (s *DatabaseStore) GetReaction(ctx context.Context, targetType, targetID, reaction, userID string) (reactionRecord, bool, error) {
	var record reactionRecord
	var active int64
	err := s.db.QueryRowContext(ctx, `
			SELECT event_id, target_type, target_id, channel_id, post_id, comment_id, reaction, user_id, active, created_at
		FROM p2p_reactions
		WHERE target_type = $1 AND target_id = $2 AND reaction = $3 AND user_id = $4
		`, targetType, targetID, reaction, userID).Scan(&record.EventID, &record.TargetType, &record.TargetID, &record.ChannelID, &record.PostID, &record.CommentID, &record.Reaction, &record.UserID, &active, &record.CreatedAt)
	if err == sql.ErrNoRows {
		return reactionRecord{}, false, nil
	}
	if err != nil {
		return reactionRecord{}, false, err
	}
	record.Active = active == 1
	return record, true, nil
}

func (s *DatabaseStore) DeactivateReactionByEventID(ctx context.Context, eventID string) (bool, error) {
	if strings.TrimSpace(eventID) == "" {
		return false, nil
	}
	var changed bool
	err := s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		result, err := s.db.ExecContext(ctx, `UPDATE p2p_reactions SET active = 0 WHERE event_id = $1 AND active = 1`, eventID)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		changed = rows > 0
		return nil
	})
	return changed, err
}

func (s *DatabaseStore) CountActiveReactions(ctx context.Context, targetType, targetID, reaction string) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM p2p_reactions
		WHERE target_type = $1 AND target_id = $2 AND reaction = $3 AND active = 1
	`, targetType, targetID, reaction).Scan(&count)
	return count, err
}

func (s *DatabaseStore) ListReactions(ctx context.Context, userID string) ([]reactionRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
			SELECT event_id, target_type, target_id, channel_id, post_id, comment_id, reaction, user_id, active, created_at
		FROM p2p_reactions
		WHERE user_id = $1 AND active = 1
		ORDER BY created_at DESC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	var reactions []reactionRecord
	for rows.Next() {
		var record reactionRecord
		var active int64
		if err := rows.Scan(&record.EventID, &record.TargetType, &record.TargetID, &record.ChannelID, &record.PostID, &record.CommentID, &record.Reaction, &record.UserID, &active, &record.CreatedAt); err != nil {
			return nil, err
		}
		record.Active = active == 1
		reactions = append(reactions, record)
	}
	return reactions, rows.Err()
}

func (s *DatabaseStore) UpsertMember(ctx context.Context, member memberRecord) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_members (
				room_id, user_id, channel_id, display_name, avatar_url, domain, membership, role, muted, joined_at, requester_node_base_url, request_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT(room_id, user_id) DO UPDATE SET
				channel_id = EXCLUDED.channel_id,
				display_name = EXCLUDED.display_name,
				avatar_url = EXCLUDED.avatar_url,
				domain = EXCLUDED.domain,
				membership = EXCLUDED.membership,
				role = EXCLUDED.role,
				muted = EXCLUDED.muted,
				requester_node_base_url = CASE
					WHEN EXCLUDED.requester_node_base_url <> '' THEN EXCLUDED.requester_node_base_url
					ELSE p2p_members.requester_node_base_url
				END,
				request_id = CASE
					WHEN EXCLUDED.request_id <> '' THEN EXCLUDED.request_id
					ELSE p2p_members.request_id
				END,
				joined_at = CASE
					WHEN LOWER(BTRIM(EXCLUDED.membership)) IN ('join', 'joined')
						AND LOWER(BTRIM(p2p_members.membership)) NOT IN ('join', 'joined')
						AND EXCLUDED.joined_at > 0
						THEN EXCLUDED.joined_at
					WHEN p2p_members.joined_at > 0 THEN p2p_members.joined_at
					WHEN EXCLUDED.joined_at > 0 THEN EXCLUDED.joined_at
					ELSE p2p_members.joined_at
				END
		`, member.RoomID, member.UserID, member.ChannelID, member.DisplayName, member.AvatarURL, member.Domain, member.Membership, member.Role, boolInt(member.Muted), member.JoinedAt, member.RequesterNodeBaseURL, member.RequestID)
		return err
	})
}

func (s *DatabaseStore) InsertMemberIfAbsent(ctx context.Context, member memberRecord) (bool, error) {
	inserted := false
	err := s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		result, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_members (
				room_id, user_id, channel_id, display_name, avatar_url, domain, membership, role, muted, joined_at, requester_node_base_url, request_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT(room_id, user_id) DO NOTHING
		`, member.RoomID, member.UserID, member.ChannelID, member.DisplayName, member.AvatarURL, member.Domain,
			member.Membership, member.Role, boolInt(member.Muted), member.JoinedAt, member.RequesterNodeBaseURL, member.RequestID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		inserted = count == 1
		return nil
	})
	return inserted, err
}

func (s *DatabaseStore) CompareAndSwapMemberGeneration(
	ctx context.Context,
	member memberRecord,
	expectedRequestID,
	expectedMembership string,
) (bool, error) {
	updated := false
	err := s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		result, err := s.db.ExecContext(ctx, `
			UPDATE p2p_members SET
				channel_id = $1,
				display_name = $2,
				avatar_url = $3,
				domain = $4,
				membership = $5,
				role = $6,
				muted = $7,
				joined_at = $8,
				requester_node_base_url = $9,
				request_id = $10
			WHERE room_id = $11 AND user_id = $12 AND request_id = $13
				AND ($14 = '' OR LOWER(BTRIM(membership)) = LOWER(BTRIM($14)))
		`, member.ChannelID, member.DisplayName, member.AvatarURL, member.Domain, member.Membership,
			member.Role, boolInt(member.Muted), member.JoinedAt, member.RequesterNodeBaseURL,
			member.RequestID, member.RoomID, member.UserID, expectedRequestID, expectedMembership)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		updated = count == 1
		return nil
	})
	return updated, err
}

func (s *DatabaseStore) ListMembers(ctx context.Context, roomID, channelID string) ([]memberRecord, error) {
	var rows *sql.Rows
	var err error
	switch {
	case roomID != "":
		rows, err = s.db.QueryContext(ctx, listMembersSelect+visibleMembersWhere+` AND room_id = $1 ORDER BY joined_at ASC, user_id ASC`, roomID)
	case channelID != "":
		rows, err = s.db.QueryContext(ctx, listMembersSelect+visibleMembersWhere+` AND channel_id = $1 ORDER BY joined_at ASC, user_id ASC`, channelID)
	default:
		rows, err = s.db.QueryContext(ctx, listMembersSelect+visibleMembersWhere+` ORDER BY joined_at ASC, user_id ASC`)
	}
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	var members []memberRecord
	for rows.Next() {
		var member memberRecord
		var muted int64
		if err := rows.Scan(&member.RoomID, &member.UserID, &member.ChannelID, &member.DisplayName, &member.AvatarURL, &member.Domain, &member.Membership, &member.Role, &muted, &member.JoinedAt, &member.RequesterNodeBaseURL, &member.RequestID); err != nil {
			return nil, err
		}
		member.Muted = muted == 1
		members = append(members, member)
	}
	return members, rows.Err()
}

func (s *DatabaseStore) ListMembersForUser(ctx context.Context, userID string) ([]memberRecord, error) {
	rows, err := s.db.QueryContext(ctx, listMembersSelect+visibleMembersWhere+` AND user_id = $1 ORDER BY joined_at ASC, room_id ASC`, userID)
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	return scanMembers(rows)
}

func (s *DatabaseStore) CountJoinedMembers(ctx context.Context, roomID, channelID string) (int64, error) {
	joined, _, err := s.CountProductMembers(ctx, roomID, channelID)
	return joined, err
}

func (s *DatabaseStore) CountProductMembers(ctx context.Context, roomID, channelID string) (int64, int64, error) {
	var joined, pending int64
	var rows *sql.Rows
	var err error
	switch {
	case channelID != "":
		rows, err = s.db.QueryContext(ctx, `
			SELECT membership, COUNT(*)
			FROM p2p_members
			WHERE channel_id = $1 AND membership IN ('join', 'pending')
			GROUP BY membership
		`, channelID)
	case roomID != "":
		rows, err = s.db.QueryContext(ctx, `
			SELECT membership, COUNT(*)
			FROM p2p_members
			WHERE room_id = $1 AND channel_id = '' AND membership IN ('join', 'pending')
			GROUP BY membership
		`, roomID)
	default:
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	defer closeResource(rows)
	for rows.Next() {
		var membership string
		var count int64
		if err := rows.Scan(&membership, &count); err != nil {
			return 0, 0, err
		}
		switch membership {
		case "join":
			joined = count
		case "pending":
			pending = count
		}
	}
	return joined, pending, rows.Err()
}

func (s *DatabaseStore) LookupMember(ctx context.Context, roomID, userID string) (memberRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, listMembersSelect+` WHERE room_id = $1 AND user_id = $2`, roomID, userID)
	var member memberRecord
	var muted int64
	if err := row.Scan(&member.RoomID, &member.UserID, &member.ChannelID, &member.DisplayName, &member.AvatarURL, &member.Domain, &member.Membership, &member.Role, &muted, &member.JoinedAt, &member.RequesterNodeBaseURL, &member.RequestID); err != nil {
		if err == sql.ErrNoRows {
			return memberRecord{}, false, nil
		}
		return memberRecord{}, false, err
	}
	member.Muted = muted == 1
	return member, true, nil
}

const listMembersSelect = `SELECT room_id, user_id, channel_id, display_name, avatar_url, domain, membership, role, muted, joined_at, requester_node_base_url, request_id FROM p2p_members`
const visibleMembersWhere = ` WHERE membership NOT IN ('leave', 'left', 'remove', 'removed', 'reject', 'rejected', 'ban', 'banned')`

func scanMembers(rows *sql.Rows) ([]memberRecord, error) {
	var members []memberRecord
	for rows.Next() {
		var member memberRecord
		var muted int64
		if err := rows.Scan(&member.RoomID, &member.UserID, &member.ChannelID, &member.DisplayName, &member.AvatarURL, &member.Domain, &member.Membership, &member.Role, &muted, &member.JoinedAt, &member.RequesterNodeBaseURL, &member.RequestID); err != nil {
			return nil, err
		}
		member.Muted = muted == 1
		members = append(members, member)
	}
	return members, rows.Err()
}

func (s *DatabaseStore) DeleteChannelPost(ctx context.Context, postID string) (bool, error) {
	var deleted bool
	err := s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		result, err := s.db.ExecContext(ctx, `DELETE FROM p2p_channel_posts WHERE post_id = $1 OR event_id = $1`, postID)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		deleted = affected > 0
		return nil
	})
	return deleted, err
}

func (s *DatabaseStore) DeleteChannelComment(ctx context.Context, commentID string) (bool, error) {
	var deleted bool
	err := s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		result, err := s.db.ExecContext(ctx, `DELETE FROM p2p_channel_comments WHERE comment_id = $1 OR event_id = $1`, commentID)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		deleted = affected > 0
		return nil
	})
	return deleted, err
}

func (s *DatabaseStore) InsertEvent(ctx context.Context, event p2pEvent) (bool, error) {
	payload := event.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	var inserted bool
	err = s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		result, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_events (seq, type, room_id, event_id, dedupe_key, payload_json, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT DO NOTHING
		`, event.Seq, event.Type, event.RoomID, event.EventID, event.DedupeKey, string(payloadJSON), event.CreatedAt)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		inserted = affected > 0
		return nil
	})
	return inserted, err
}

func (s *DatabaseStore) ListEvents(ctx context.Context, since int64, limit int) ([]p2pEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT seq, type, room_id, event_id, dedupe_key, payload_json, created_at
		FROM p2p_events
		WHERE seq > $1
		ORDER BY seq ASC
		LIMIT $2
	`, since, limit)
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	events := []p2pEvent{}
	for rows.Next() {
		var event p2pEvent
		var payloadJSON string
		if err := rows.Scan(&event.Seq, &event.Type, &event.RoomID, &event.EventID, &event.DedupeKey, &payloadJSON, &event.CreatedAt); err != nil {
			return nil, err
		}
		if strings.TrimSpace(payloadJSON) != "" {
			payload := map[string]any{}
			if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
				return nil, err
			}
			event.Payload = payload
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *DatabaseStore) EventBounds(ctx context.Context) (eventBounds, error) {
	var bounds eventBounds
	var minSeq, maxSeq sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `
		SELECT MIN(seq), MAX(seq), COUNT(*)
		FROM p2p_events
	`).Scan(&minSeq, &maxSeq, &bounds.Count); err != nil {
		return eventBounds{}, err
	}
	if minSeq.Valid {
		bounds.MinSeq = minSeq.Int64
	}
	if maxSeq.Valid {
		bounds.MaxSeq = maxSeq.Int64
	}
	return bounds, nil
}

func (s *DatabaseStore) PruneEventsBefore(ctx context.Context, beforeSeq int64) (int64, error) {
	if beforeSeq <= 0 {
		return 0, nil
	}
	var deleted int64
	err := s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		result, err := s.db.ExecContext(ctx, `DELETE FROM p2p_events WHERE seq < $1`, beforeSeq)
		if err != nil {
			return err
		}
		deleted, err = result.RowsAffected()
		return err
	})
	return deleted, err
}

func (s *DatabaseStore) PruneEventsToMaxRows(ctx context.Context, maxRows int64) (int64, error) {
	if maxRows <= 0 {
		return 0, nil
	}
	var deleted int64
	err := s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		result, err := s.db.ExecContext(ctx, `
			DELETE FROM p2p_events
			WHERE seq NOT IN (
				SELECT seq FROM p2p_events
				ORDER BY seq DESC
				LIMIT $1
			)
		`, maxRows)
		if err != nil {
			return err
		}
		deleted, err = result.RowsAffected()
		return err
	})
	return deleted, err
}
