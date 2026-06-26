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
				target_type, target_id, channel_id, post_id, comment_id, reaction, user_id, active, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT(target_type, target_id, reaction, user_id) DO UPDATE SET
				channel_id = EXCLUDED.channel_id,
				post_id = EXCLUDED.post_id,
				comment_id = EXCLUDED.comment_id,
				active = EXCLUDED.active,
				created_at = EXCLUDED.created_at
		`, reaction.TargetType, reaction.TargetID, reaction.ChannelID, reaction.PostID, reaction.CommentID, reaction.Reaction, reaction.UserID, boolInt(reaction.Active), reaction.CreatedAt)
		return err
	})
}

func (s *DatabaseStore) GetReaction(ctx context.Context, targetType, targetID, reaction, userID string) (reactionRecord, bool, error) {
	var record reactionRecord
	var active int64
	err := s.db.QueryRowContext(ctx, `
		SELECT target_type, target_id, channel_id, post_id, comment_id, reaction, user_id, active, created_at
		FROM p2p_reactions
		WHERE target_type = $1 AND target_id = $2 AND reaction = $3 AND user_id = $4
	`, targetType, targetID, reaction, userID).Scan(&record.TargetType, &record.TargetID, &record.ChannelID, &record.PostID, &record.CommentID, &record.Reaction, &record.UserID, &active, &record.CreatedAt)
	if err == sql.ErrNoRows {
		return reactionRecord{}, false, nil
	}
	if err != nil {
		return reactionRecord{}, false, err
	}
	record.Active = active == 1
	return record, true, nil
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
		SELECT target_type, target_id, channel_id, post_id, comment_id, reaction, user_id, active, created_at
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
		if err := rows.Scan(&record.TargetType, &record.TargetID, &record.ChannelID, &record.PostID, &record.CommentID, &record.Reaction, &record.UserID, &active, &record.CreatedAt); err != nil {
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
				room_id, user_id, channel_id, display_name, avatar_url, domain, membership, role, muted, joined_at, requester_node_base_url
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
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
				joined_at = CASE
					WHEN p2p_members.joined_at > 0 THEN p2p_members.joined_at
					WHEN EXCLUDED.joined_at > 0 THEN EXCLUDED.joined_at
					ELSE p2p_members.joined_at
				END
		`, member.RoomID, member.UserID, member.ChannelID, member.DisplayName, member.AvatarURL, member.Domain, member.Membership, member.Role, boolInt(member.Muted), member.JoinedAt, member.RequesterNodeBaseURL)
		return err
	})
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
		if err := rows.Scan(&member.RoomID, &member.UserID, &member.ChannelID, &member.DisplayName, &member.AvatarURL, &member.Domain, &member.Membership, &member.Role, &muted, &member.JoinedAt, &member.RequesterNodeBaseURL); err != nil {
			return nil, err
		}
		member.Muted = muted == 1
		members = append(members, member)
	}
	return members, rows.Err()
}

func (s *DatabaseStore) LookupMember(ctx context.Context, roomID, userID string) (memberRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, listMembersSelect+` WHERE room_id = $1 AND user_id = $2`, roomID, userID)
	var member memberRecord
	var muted int64
	if err := row.Scan(&member.RoomID, &member.UserID, &member.ChannelID, &member.DisplayName, &member.AvatarURL, &member.Domain, &member.Membership, &member.Role, &muted, &member.JoinedAt, &member.RequesterNodeBaseURL); err != nil {
		if err == sql.ErrNoRows {
			return memberRecord{}, false, nil
		}
		return memberRecord{}, false, err
	}
	member.Muted = muted == 1
	return member, true, nil
}

const listMembersSelect = `SELECT room_id, user_id, channel_id, display_name, avatar_url, domain, membership, role, muted, joined_at, requester_node_base_url FROM p2p_members`
const visibleMembersWhere = ` WHERE membership NOT IN ('leave', 'left', 'remove', 'removed', 'reject', 'rejected', 'ban', 'banned')`

func (s *DatabaseStore) DeleteChannelPost(ctx context.Context, postID string) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `DELETE FROM p2p_channel_posts WHERE post_id = $1 OR event_id = $1`, postID)
		return err
	})
}

func (s *DatabaseStore) DeleteChannelComment(ctx context.Context, commentID string) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `DELETE FROM p2p_channel_comments WHERE comment_id = $1 OR event_id = $1`, commentID)
		return err
	})
}

func (s *DatabaseStore) InsertEvent(ctx context.Context, event p2pEvent) error {
	payload := event.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_events (seq, type, room_id, event_id, payload_json, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT(seq) DO UPDATE SET
				type = EXCLUDED.type,
				room_id = EXCLUDED.room_id,
				event_id = EXCLUDED.event_id,
				payload_json = EXCLUDED.payload_json,
				created_at = EXCLUDED.created_at
		`, event.Seq, event.Type, event.RoomID, event.EventID, string(payloadJSON), event.CreatedAt)
		return err
	})
}

func (s *DatabaseStore) ListEvents(ctx context.Context, since int64, limit int) ([]p2pEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT seq, type, room_id, event_id, payload_json, created_at
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
		if err := rows.Scan(&event.Seq, &event.Type, &event.RoomID, &event.EventID, &payloadJSON, &event.CreatedAt); err != nil {
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
