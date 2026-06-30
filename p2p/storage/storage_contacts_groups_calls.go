package storage

import (
	"context"
	"database/sql"
)

func (s *DatabaseStore) UpsertContact(ctx context.Context, contact contactRecord) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_contacts (room_id, peer_mxid, display_name, display_name_override, avatar_url, remark, domain, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT(peer_mxid) DO UPDATE SET
				room_id = EXCLUDED.room_id,
				peer_mxid = EXCLUDED.peer_mxid,
				display_name = EXCLUDED.display_name,
				display_name_override = EXCLUDED.display_name_override,
				avatar_url = CASE
					WHEN EXCLUDED.avatar_url <> '' THEN EXCLUDED.avatar_url
					ELSE p2p_contacts.avatar_url
				END,
				remark = EXCLUDED.remark,
				domain = EXCLUDED.domain,
				status = EXCLUDED.status
		`, contact.RoomID, contact.PeerMXID, contact.DisplayName, contact.DisplayNameOverride, contact.AvatarURL, contact.Remark, contact.Domain, contact.Status)
		return err
	})
}

func (s *DatabaseStore) ListContacts(ctx context.Context) ([]contactRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT room_id, peer_mxid, display_name, display_name_override, avatar_url, remark, domain, status FROM p2p_contacts ORDER BY display_name ASC`)
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	var contacts []contactRecord
	for rows.Next() {
		var contact contactRecord
		if err := rows.Scan(&contact.RoomID, &contact.PeerMXID, &contact.DisplayName, &contact.DisplayNameOverride, &contact.AvatarURL, &contact.Remark, &contact.Domain, &contact.Status); err != nil {
			return nil, err
		}
		contacts = append(contacts, contact)
	}
	return contacts, rows.Err()
}

func (s *DatabaseStore) DeleteContact(ctx context.Context, roomID string) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `DELETE FROM p2p_contacts WHERE room_id = $1`, roomID)
		return err
	})
}

func (s *DatabaseStore) UpsertGroup(ctx context.Context, group groupRecord) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_groups (room_id, name, topic, avatar_url, member_count, invite_policy, muted)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT(room_id) DO UPDATE SET
				name = EXCLUDED.name,
				topic = EXCLUDED.topic,
				avatar_url = EXCLUDED.avatar_url,
				member_count = EXCLUDED.member_count,
				invite_policy = EXCLUDED.invite_policy,
				muted = EXCLUDED.muted
		`, group.RoomID, group.Name, group.Topic, group.AvatarURL, group.MemberCount, group.InvitePolicy, boolInt(group.Muted))
		return err
	})
}

func (s *DatabaseStore) DeleteGroup(ctx context.Context, roomID string) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `DELETE FROM p2p_groups WHERE room_id = $1`, roomID)
		return err
	})
}

func (s *DatabaseStore) ListGroups(ctx context.Context) ([]groupRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT room_id, name, topic, avatar_url, member_count, invite_policy, muted FROM p2p_groups ORDER BY name ASC`)
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	var groups []groupRecord
	for rows.Next() {
		var group groupRecord
		var muted int64
		if err := rows.Scan(&group.RoomID, &group.Name, &group.Topic, &group.AvatarURL, &group.MemberCount, &group.InvitePolicy, &muted); err != nil {
			return nil, err
		}
		group.Muted = muted == 1
		groups = append(groups, group)
	}
	return groups, rows.Err()
}

func (s *DatabaseStore) GetGroupByRoom(ctx context.Context, roomID string) (groupRecord, bool, error) {
	if roomID == "" {
		return groupRecord{}, false, nil
	}
	row := s.db.QueryRowContext(ctx, `SELECT room_id, name, topic, avatar_url, member_count, invite_policy, muted FROM p2p_groups WHERE room_id = $1`, roomID)
	var group groupRecord
	var muted int64
	if err := row.Scan(&group.RoomID, &group.Name, &group.Topic, &group.AvatarURL, &group.MemberCount, &group.InvitePolicy, &muted); err != nil {
		if err == sql.ErrNoRows {
			return groupRecord{}, false, nil
		}
		return groupRecord{}, false, err
	}
	group.Muted = muted == 1
	return group, true, nil
}

func (s *DatabaseStore) ListJoinedGroupsForUser(ctx context.Context, userID string) ([]groupRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT g.room_id, g.name, g.topic, g.avatar_url, g.member_count, g.invite_policy, g.muted
		FROM p2p_groups g
		INNER JOIN p2p_members m ON m.room_id = g.room_id
		WHERE m.user_id = $1 AND m.channel_id = '' AND m.membership = 'join'
		ORDER BY g.name ASC
	`, userID)
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	var groups []groupRecord
	for rows.Next() {
		var group groupRecord
		var muted int64
		if err := rows.Scan(&group.RoomID, &group.Name, &group.Topic, &group.AvatarURL, &group.MemberCount, &group.InvitePolicy, &muted); err != nil {
			return nil, err
		}
		group.Muted = muted == 1
		groups = append(groups, group)
	}
	return groups, rows.Err()
}

func (s *DatabaseStore) UpsertCall(ctx context.Context, call callRecord) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_calls (
				call_id, room_id, room_type, media_type, created_by_mxid, state, created_at,
				answered_at, ended_at, ended_by_mxid, end_reason, duration_ms
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT(call_id) DO UPDATE SET
				room_id = EXCLUDED.room_id,
				room_type = EXCLUDED.room_type,
				media_type = EXCLUDED.media_type,
				created_by_mxid = EXCLUDED.created_by_mxid,
				state = EXCLUDED.state,
				created_at = EXCLUDED.created_at,
				answered_at = EXCLUDED.answered_at,
				ended_at = EXCLUDED.ended_at,
				ended_by_mxid = EXCLUDED.ended_by_mxid,
				end_reason = EXCLUDED.end_reason,
				duration_ms = EXCLUDED.duration_ms
		`, call.CallID, call.RoomID, call.RoomType, call.MediaType, call.CreatedByMXID, call.State, call.CreatedAt, call.AnsweredAt, call.EndedAt, call.EndedByMXID, call.EndReason, call.DurationMS)
		return err
	})
}

func (s *DatabaseStore) ListCalls(ctx context.Context, roomID string, activeOnly bool) ([]callRecord, error) {
	var rows *sql.Rows
	var err error
	switch {
	case roomID != "" && activeOnly:
		rows, err = s.db.QueryContext(ctx, listCallsSelect+` WHERE room_id = $1 AND state NOT IN ('ended', 'rejected', 'missed', 'failed') ORDER BY created_at DESC`, roomID)
	case roomID != "":
		rows, err = s.db.QueryContext(ctx, listCallsSelect+` WHERE room_id = $1 ORDER BY created_at DESC`, roomID)
	case activeOnly:
		rows, err = s.db.QueryContext(ctx, listCallsSelect+` WHERE state NOT IN ('ended', 'rejected', 'missed', 'failed') ORDER BY created_at DESC`)
	default:
		rows, err = s.db.QueryContext(ctx, listCallsSelect+` ORDER BY created_at DESC`)
	}
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	var calls []callRecord
	for rows.Next() {
		var call callRecord
		if err := rows.Scan(&call.CallID, &call.RoomID, &call.RoomType, &call.MediaType, &call.CreatedByMXID, &call.State, &call.CreatedAt, &call.AnsweredAt, &call.EndedAt, &call.EndedByMXID, &call.EndReason, &call.DurationMS); err != nil {
			return nil, err
		}
		calls = append(calls, call)
	}
	return calls, rows.Err()
}

const listCallsSelect = `SELECT call_id, room_id, room_type, media_type, created_by_mxid, state, created_at, answered_at, ended_at, ended_by_mxid, end_reason, duration_ms FROM p2p_calls`
