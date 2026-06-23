package p2p

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/YingSuiAI/direxio-message-server/internal/sqlutil"
	"github.com/YingSuiAI/direxio-message-server/setup/config"
)

type DatabaseStore struct {
	db     *sql.DB
	writer sqlutil.Writer
}

func NewDatabaseStore(ctx context.Context, cm *sqlutil.Connections, dbProperties *config.DatabaseOptions) (*DatabaseStore, error) {
	db, writer, err := cm.Connection(dbProperties)
	if err != nil {
		return nil, err
	}
	store := &DatabaseStore{db: db, writer: writer}
	if err := store.migrate(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *DatabaseStore) Close() error {
	return s.db.Close()
}

func (s *DatabaseStore) migrate(ctx context.Context) error {
	m := sqlutil.NewMigrator(s.db)
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: integrated appservice tables v1",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_portal (
					id TEXT PRIMARY KEY NOT NULL,
					initialized BIGINT NOT NULL,
					password TEXT NOT NULL,
					admin_token TEXT NOT NULL,
					matrix_token TEXT NOT NULL,
					agent_token TEXT NOT NULL,
					owner_mxid TEXT NOT NULL,
					agent_room_id TEXT NOT NULL,
					user_id TEXT NOT NULL,
					display_name TEXT NOT NULL,
					domain TEXT NOT NULL,
					avatar_url TEXT NOT NULL,
					gender TEXT NOT NULL,
					birthday TEXT NOT NULL,
					phone TEXT NOT NULL,
					email TEXT NOT NULL
				)`,
				`CREATE TABLE IF NOT EXISTS p2p_read_markers (
					room_id TEXT PRIMARY KEY NOT NULL,
					event_id TEXT NOT NULL,
					origin_server_ts BIGINT NOT NULL
				)`,
				`CREATE TABLE IF NOT EXISTS p2p_channels (
					channel_id TEXT PRIMARY KEY NOT NULL,
					room_id TEXT NOT NULL,
					name TEXT NOT NULL,
					description TEXT NOT NULL,
					avatar_url TEXT NOT NULL,
					visibility TEXT NOT NULL,
					join_policy TEXT NOT NULL,
					channel_type TEXT NOT NULL,
					comments_enabled BIGINT NOT NULL,
					member_count BIGINT NOT NULL,
					pending_join_count BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_channels_room_idx ON p2p_channels(room_id)`,
				`CREATE INDEX IF NOT EXISTS p2p_channels_type_visibility_idx ON p2p_channels(channel_type, visibility, channel_id)`,
				`CREATE TABLE IF NOT EXISTS p2p_channel_posts (
					post_id TEXT PRIMARY KEY NOT NULL,
					channel_id TEXT NOT NULL,
					room_id TEXT NOT NULL,
					event_id TEXT NOT NULL,
					author_mxid TEXT NOT NULL,
					author_name TEXT NOT NULL,
					body TEXT NOT NULL,
					message_type TEXT NOT NULL,
					media_json TEXT NOT NULL,
					origin_server_ts BIGINT NOT NULL,
					comment_count BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_channel_posts_channel_idx ON p2p_channel_posts(channel_id, origin_server_ts)`,
				`CREATE INDEX IF NOT EXISTS p2p_channel_posts_event_idx ON p2p_channel_posts(event_id)`,
				`CREATE INDEX IF NOT EXISTS p2p_channel_posts_author_idx ON p2p_channel_posts(author_mxid, origin_server_ts)`,
				`CREATE TABLE IF NOT EXISTS p2p_channel_comments (
					comment_id TEXT PRIMARY KEY NOT NULL,
					post_id TEXT NOT NULL,
					channel_id TEXT NOT NULL,
					event_id TEXT NOT NULL,
					author_mxid TEXT NOT NULL,
					author_name TEXT NOT NULL,
					body TEXT NOT NULL,
					message_type TEXT NOT NULL,
					origin_server_ts BIGINT NOT NULL,
					reaction_count BIGINT NOT NULL,
					reacted_by_me BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_channel_comments_post_idx ON p2p_channel_comments(post_id, origin_server_ts)`,
				`CREATE INDEX IF NOT EXISTS p2p_channel_comments_channel_idx ON p2p_channel_comments(channel_id, origin_server_ts)`,
				`CREATE INDEX IF NOT EXISTS p2p_channel_comments_event_idx ON p2p_channel_comments(event_id)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: integrated appservice tables v2",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_contacts (
					room_id TEXT PRIMARY KEY NOT NULL,
					peer_mxid TEXT NOT NULL,
					display_name TEXT NOT NULL,
					remark TEXT NOT NULL DEFAULT '',
					domain TEXT NOT NULL,
					status TEXT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_contacts_peer_idx ON p2p_contacts(peer_mxid)`,
				`CREATE INDEX IF NOT EXISTS p2p_contacts_status_idx ON p2p_contacts(status, domain)`,
				`CREATE TABLE IF NOT EXISTS p2p_groups (
					room_id TEXT PRIMARY KEY NOT NULL,
					name TEXT NOT NULL,
					topic TEXT NOT NULL,
					avatar_url TEXT NOT NULL,
					member_count BIGINT NOT NULL,
					invite_policy TEXT NOT NULL
				)`,
				`CREATE TABLE IF NOT EXISTS p2p_calls (
					call_id TEXT PRIMARY KEY NOT NULL,
					room_id TEXT NOT NULL,
					room_type TEXT NOT NULL,
					media_type TEXT NOT NULL,
					created_by_mxid TEXT NOT NULL,
					state TEXT NOT NULL,
					created_at TEXT NOT NULL,
					answered_at TEXT NOT NULL DEFAULT '',
					ended_at TEXT NOT NULL DEFAULT '',
					ended_by_mxid TEXT NOT NULL DEFAULT '',
					end_reason TEXT NOT NULL DEFAULT '',
					duration_ms BIGINT NOT NULL DEFAULT 0
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_calls_room_idx ON p2p_calls(room_id, created_at)`,
				`CREATE INDEX IF NOT EXISTS p2p_calls_state_idx ON p2p_calls(state, created_at)`,
				`CREATE TABLE IF NOT EXISTS p2p_favorites (
					id BIGINT PRIMARY KEY NOT NULL,
					event_id TEXT NOT NULL,
					room_id TEXT NOT NULL,
					sender_id TEXT NOT NULL,
					sender_name TEXT NOT NULL,
					content TEXT NOT NULL,
					message_type TEXT NOT NULL,
					origin_server_ts BIGINT NOT NULL,
					created_at TEXT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_favorites_type_idx ON p2p_favorites(message_type, created_at)`,
				`CREATE INDEX IF NOT EXISTS p2p_favorites_event_idx ON p2p_favorites(event_id)`,
				`CREATE TABLE IF NOT EXISTS p2p_follows (
					domain TEXT PRIMARY KEY NOT NULL,
					created_at TEXT NOT NULL
				)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: integrated appservice tables v3",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_reactions (
					target_type TEXT NOT NULL,
					target_id TEXT NOT NULL,
					channel_id TEXT NOT NULL,
					post_id TEXT NOT NULL,
					comment_id TEXT NOT NULL,
					reaction TEXT NOT NULL,
					user_id TEXT NOT NULL,
					active BIGINT NOT NULL,
					created_at TEXT NOT NULL,
					PRIMARY KEY (target_type, target_id, reaction, user_id)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_reactions_user_idx ON p2p_reactions(user_id, active)`,
				`CREATE INDEX IF NOT EXISTS p2p_reactions_target_idx ON p2p_reactions(target_type, target_id, reaction, active)`,
				`CREATE TABLE IF NOT EXISTS p2p_members (
					room_id TEXT NOT NULL,
					user_id TEXT NOT NULL,
					channel_id TEXT NOT NULL,
					display_name TEXT NOT NULL,
					domain TEXT NOT NULL,
					membership TEXT NOT NULL,
					role TEXT NOT NULL,
					muted BIGINT NOT NULL,
					PRIMARY KEY (room_id, user_id)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_members_channel_idx ON p2p_members(channel_id, membership)`,
				`CREATE INDEX IF NOT EXISTS p2p_members_room_idx ON p2p_members(room_id, membership)`,
				`CREATE INDEX IF NOT EXISTS p2p_members_user_idx ON p2p_members(user_id, membership)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: integrated appservice tables v4 member avatars",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			_, err := txn.ExecContext(ctx, `ALTER TABLE p2p_members ADD COLUMN avatar_url TEXT NOT NULL DEFAULT ''`)
			return err
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: integrated appservice tables v5 product mute state",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_channels ADD COLUMN muted BIGINT NOT NULL DEFAULT 0`,
				`ALTER TABLE p2p_groups ADD COLUMN muted BIGINT NOT NULL DEFAULT 0`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: integrated appservice tables v6 member join order",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_members ADD COLUMN joined_at BIGINT NOT NULL DEFAULT 0`,
				`CREATE INDEX IF NOT EXISTS p2p_members_room_joined_idx ON p2p_members(room_id, membership, joined_at, user_id)`,
				`CREATE INDEX IF NOT EXISTS p2p_members_channel_joined_idx ON p2p_members(channel_id, membership, joined_at, user_id)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: integrated appservice tables v7 portal matrix device",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			_, err := txn.ExecContext(ctx, `ALTER TABLE p2p_portal ADD COLUMN matrix_device_id TEXT NOT NULL DEFAULT ''`)
			return err
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: integrated appservice tables v8 portal profile initialized",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			if _, err := txn.ExecContext(ctx, `ALTER TABLE p2p_portal ADD COLUMN profile_initialized BIGINT NOT NULL DEFAULT 0`); err != nil {
				return err
			}
			_, err := txn.ExecContext(ctx, `UPDATE p2p_portal SET profile_initialized = 1 WHERE TRIM(display_name) <> ''`)
			return err
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: integrated appservice tables v9 reports",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_reports (
					id TEXT PRIMARY KEY NOT NULL,
					reporter_domain TEXT NOT NULL,
					reported_domain TEXT NOT NULL,
					target_type BIGINT NOT NULL,
					reason TEXT NOT NULL,
					images_json TEXT NOT NULL,
					created_at TEXT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_reports_reported_idx ON p2p_reports(reported_domain, target_type, created_at)`,
				`CREATE INDEX IF NOT EXISTS p2p_reports_reporter_idx ON p2p_reports(reporter_domain, created_at)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: integrated appservice tables v10 portal password initialized",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			if _, err := txn.ExecContext(ctx, `ALTER TABLE p2p_portal ADD COLUMN password_initialized BIGINT NOT NULL DEFAULT 0`); err != nil {
				return err
			}
			_, err := txn.ExecContext(ctx, `UPDATE p2p_portal SET password_initialized = 1 WHERE profile_initialized = 1`)
			return err
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: integrated appservice tables v11 channel comment replies",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_channel_comments ADD COLUMN reply_to_comment_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_channel_comments ADD COLUMN reply_to_author_mxid TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_channel_comments ADD COLUMN mentions_json TEXT NOT NULL DEFAULT '[]'`,
				`CREATE INDEX IF NOT EXISTS p2p_channel_comments_reply_idx ON p2p_channel_comments(post_id, reply_to_comment_id, origin_server_ts)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: integrated appservice tables v12 channel comment media",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			_, err := txn.ExecContext(ctx, `ALTER TABLE p2p_channel_comments ADD COLUMN media_json TEXT NOT NULL DEFAULT ''`)
			return err
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: integrated appservice tables v13 event outbox",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_events (
					seq BIGINT PRIMARY KEY NOT NULL,
					type TEXT NOT NULL,
					room_id TEXT NOT NULL,
					event_id TEXT NOT NULL,
					payload_json TEXT NOT NULL,
					created_at TEXT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_events_room_idx ON p2p_events(room_id, seq)`,
				`CREATE INDEX IF NOT EXISTS p2p_events_type_idx ON p2p_events(type, seq)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: integrated appservice tables v14 channel invite grants",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_channel_invite_grants (
					grant_id TEXT PRIMARY KEY NOT NULL,
					channel_id TEXT NOT NULL,
					room_id TEXT NOT NULL,
					share_room_id TEXT NOT NULL,
					created_by TEXT NOT NULL,
					created_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_channel_invite_grants_channel_idx ON p2p_channel_invite_grants(channel_id, share_room_id)`,
				`CREATE INDEX IF NOT EXISTS p2p_channel_invite_grants_room_idx ON p2p_channel_invite_grants(room_id, share_room_id)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: drop legacy message mirror table v15",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			_, err := txn.ExecContext(ctx, `DROP TABLE IF EXISTS p2p_messages`)
			return err
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: unique contact peer v16",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`DELETE FROM p2p_contacts
				 WHERE room_id IN (
					SELECT room_id FROM (
						SELECT room_id,
							ROW_NUMBER() OVER (
								PARTITION BY peer_mxid
								ORDER BY
									CASE LOWER(TRIM(status))
										WHEN 'accepted' THEN 5
										WHEN 'pending_inbound' THEN 4
										WHEN 'pending_outbound' THEN 3
										WHEN 'rejected' THEN 2
										WHEN 'reject' THEN 2
										WHEN 'deleted' THEN 1
										ELSE 0
									END DESC,
									room_id ASC
							) AS duplicate_rank
						FROM p2p_contacts
					) ranked
					WHERE duplicate_rank > 1
				)`,
				`DROP INDEX IF EXISTS p2p_contacts_peer_idx`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_contacts_peer_idx ON p2p_contacts(peer_mxid)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: product conversations v17",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_conversations (
					conversation_id TEXT PRIMARY KEY NOT NULL,
					matrix_room_id TEXT NOT NULL UNIQUE,
					kind TEXT NOT NULL,
					lifecycle TEXT NOT NULL,
						created_by_mxid TEXT NOT NULL DEFAULT '',
						peer_mxid TEXT NOT NULL DEFAULT '',
						title TEXT NOT NULL DEFAULT '',
						avatar_url TEXT NOT NULL DEFAULT '',
						last_event_id TEXT NOT NULL DEFAULT '',
						last_message TEXT NOT NULL DEFAULT '',
						last_activity_at BIGINT NOT NULL DEFAULT 0,
					projection_state TEXT NOT NULL DEFAULT 'ready',
					projection_reason TEXT NOT NULL DEFAULT '',
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_conversations_kind_idx ON p2p_conversations(kind)`,
				`CREATE INDEX IF NOT EXISTS p2p_conversations_updated_idx ON p2p_conversations(updated_at)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: conversation peer mxid v18",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			_, err := txn.ExecContext(ctx, `ALTER TABLE p2p_conversations ADD COLUMN IF NOT EXISTS peer_mxid TEXT NOT NULL DEFAULT ''`)
			return err
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: backfill product conversations v19",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return backfillProductConversations(ctx, txn)
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: conversation last message v20",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			if _, err := txn.ExecContext(ctx, `ALTER TABLE p2p_conversations ADD COLUMN IF NOT EXISTS last_message TEXT NOT NULL DEFAULT ''`); err != nil {
				return err
			}
			_, err := txn.ExecContext(ctx, `
					DO $$
					BEGIN
						IF to_regclass('public.syncapi_output_room_events') IS NOT NULL THEN
							WITH latest AS (
								SELECT DISTINCT ON (room_id)
									room_id,
									event_id,
									COALESCE((headered_event_json::jsonb->>'origin_server_ts')::bigint, 0) AS origin_server_ts,
									COALESCE(
										NULLIF(TRIM(headered_event_json::jsonb->'content'->>'body'), ''),
										CASE LOWER(TRIM(headered_event_json::jsonb->'content'->>'msgtype'))
											WHEN 'm.image' THEN '图片'
											WHEN 'image' THEN '图片'
											WHEN 'm.video' THEN '视频'
											WHEN 'video' THEN '视频'
											WHEN 'm.audio' THEN '语音'
											WHEN 'audio' THEN '语音'
											WHEN 'm.file' THEN '文件'
											WHEN 'file' THEN '文件'
											ELSE ''
										END
									) AS last_message
								FROM syncapi_output_room_events
								WHERE type = 'm.room.message'
									AND COALESCE(exclude_from_sync, false) = false
								ORDER BY room_id, id DESC
							)
							UPDATE p2p_conversations c
							SET
								last_event_id = CASE WHEN latest.event_id <> '' THEN latest.event_id ELSE c.last_event_id END,
								last_message = CASE WHEN latest.last_message <> '' THEN latest.last_message ELSE c.last_message END,
								last_activity_at = CASE WHEN latest.origin_server_ts > c.last_activity_at THEN latest.origin_server_ts ELSE c.last_activity_at END,
								updated_at = CASE WHEN latest.origin_server_ts > c.updated_at THEN latest.origin_server_ts ELSE c.updated_at END
							FROM latest
							WHERE c.matrix_room_id = latest.room_id
								AND latest.origin_server_ts >= c.last_activity_at;
						END IF;
					END $$;
				`)
			return err
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: member requester node v21",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			exists, err := productTableExists(ctx, txn, "p2p_members")
			if err != nil {
				return err
			}
			if !exists {
				return nil
			}
			_, err = txn.ExecContext(ctx, `ALTER TABLE p2p_members ADD COLUMN requester_node_base_url TEXT NOT NULL DEFAULT ''`)
			return err
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: contact avatars v22",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			exists, err := productTableExists(ctx, txn, "p2p_contacts")
			if err != nil {
				return err
			}
			if !exists {
				return nil
			}
			_, err = txn.ExecContext(ctx, `ALTER TABLE p2p_contacts ADD COLUMN avatar_url TEXT NOT NULL DEFAULT ''`)
			return err
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: call lifecycle fields v23",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			exists, err := productTableExists(ctx, txn, "p2p_calls")
			if err != nil {
				return err
			}
			if !exists {
				return nil
			}
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_calls ADD COLUMN IF NOT EXISTS answered_at TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_calls ADD COLUMN IF NOT EXISTS ended_at TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_calls ADD COLUMN IF NOT EXISTS ended_by_mxid TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_calls ADD COLUMN IF NOT EXISTS end_reason TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_calls ADD COLUMN IF NOT EXISTS duration_ms BIGINT NOT NULL DEFAULT 0`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: contact request remark v24",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			exists, err := productTableExists(ctx, txn, "p2p_contacts")
			if err != nil {
				return err
			}
			if !exists {
				return nil
			}
			_, err = txn.ExecContext(ctx, `ALTER TABLE p2p_contacts ADD COLUMN IF NOT EXISTS remark TEXT NOT NULL DEFAULT ''`)
			return err
		},
	})
	return m.Up(ctx)
}

func execMigrationStatements(ctx context.Context, txn *sql.Tx, statements []string) error {
	for _, statement := range statements {
		if _, err := txn.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *DatabaseStore) LoadPortal(ctx context.Context) (portalState, bool, error) {
	var state portalState
	var initialized, passwordInitialized, profileInitialized int64
	err := s.db.QueryRowContext(ctx, `
		SELECT initialized, password, admin_token, matrix_token, matrix_device_id, agent_token,
			owner_mxid, agent_room_id, password_initialized, profile_initialized, user_id, display_name, domain,
			avatar_url, gender, birthday, phone, email
		FROM p2p_portal WHERE id = $1
	`, "owner").Scan(
		&initialized, &state.Password, &state.AdminToken, &state.MatrixToken, &state.MatrixDeviceID, &state.AgentToken,
		&state.OwnerMXID, &state.AgentRoomID, &passwordInitialized, &profileInitialized, &state.Profile.UserID, &state.Profile.DisplayName, &state.Profile.Domain,
		&state.Profile.AvatarURL, &state.Profile.Gender, &state.Profile.Birthday, &state.Profile.Phone, &state.Profile.Email,
	)
	if err == sql.ErrNoRows {
		return portalState{}, false, nil
	}
	if err != nil {
		return portalState{}, false, err
	}
	state.Initialized = initialized == 1
	state.PasswordInitialized = passwordInitialized == 1
	state.ProfileInitialized = profileInitialized == 1
	return state, true, nil
}

func (s *DatabaseStore) SavePortal(ctx context.Context, state portalState) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_portal (
				id, initialized, password, admin_token, matrix_token, matrix_device_id, agent_token,
				owner_mxid, agent_room_id, password_initialized, profile_initialized, user_id, display_name, domain,
				avatar_url, gender, birthday, phone, email
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
			ON CONFLICT(id) DO UPDATE SET
				initialized = EXCLUDED.initialized,
				password = EXCLUDED.password,
				admin_token = EXCLUDED.admin_token,
				matrix_token = EXCLUDED.matrix_token,
				matrix_device_id = EXCLUDED.matrix_device_id,
				agent_token = EXCLUDED.agent_token,
				owner_mxid = EXCLUDED.owner_mxid,
				agent_room_id = EXCLUDED.agent_room_id,
				password_initialized = EXCLUDED.password_initialized,
				profile_initialized = EXCLUDED.profile_initialized,
				user_id = EXCLUDED.user_id,
				display_name = EXCLUDED.display_name,
				domain = EXCLUDED.domain,
				avatar_url = EXCLUDED.avatar_url,
				gender = EXCLUDED.gender,
				birthday = EXCLUDED.birthday,
				phone = EXCLUDED.phone,
				email = EXCLUDED.email
		`, "owner", boolInt(state.Initialized), state.Password, state.AdminToken, state.MatrixToken, state.MatrixDeviceID, state.AgentToken,
			state.OwnerMXID, state.AgentRoomID, boolInt(state.PasswordInitialized), boolInt(state.ProfileInitialized), state.Profile.UserID, state.Profile.DisplayName, state.Profile.Domain,
			state.Profile.AvatarURL, state.Profile.Gender, state.Profile.Birthday, state.Profile.Phone, state.Profile.Email)
		return err
	})
}

func (s *DatabaseStore) SaveReadMarker(ctx context.Context, marker readMarker) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_read_markers (room_id, event_id, origin_server_ts)
			VALUES ($1, $2, $3)
			ON CONFLICT(room_id) DO UPDATE SET
				event_id = EXCLUDED.event_id,
				origin_server_ts = EXCLUDED.origin_server_ts
		`, marker.RoomID, marker.EventID, marker.OriginServerTS)
		return err
	})
}

func (s *DatabaseStore) UpsertChannel(ctx context.Context, ch channel) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_channels (
				channel_id, room_id, name, description, avatar_url, visibility,
				join_policy, channel_type, comments_enabled, muted, member_count, pending_join_count
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT(channel_id) DO UPDATE SET
				room_id = EXCLUDED.room_id,
				name = EXCLUDED.name,
				description = EXCLUDED.description,
				avatar_url = EXCLUDED.avatar_url,
				visibility = EXCLUDED.visibility,
				join_policy = EXCLUDED.join_policy,
				channel_type = EXCLUDED.channel_type,
				comments_enabled = EXCLUDED.comments_enabled,
				muted = EXCLUDED.muted,
				member_count = EXCLUDED.member_count,
				pending_join_count = EXCLUDED.pending_join_count
		`, ch.ChannelID, ch.RoomID, ch.Name, ch.Description, ch.AvatarURL, ch.Visibility,
			ch.JoinPolicy, ch.ChannelType, boolInt(ch.CommentsEnabled), boolInt(ch.Muted), ch.MemberCount, ch.PendingJoinCount)
		return err
	})
}

func (s *DatabaseStore) DeleteChannel(ctx context.Context, channelID string) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `DELETE FROM p2p_channels WHERE channel_id = $1`, channelID)
		return err
	})
}

func (s *DatabaseStore) ListChannels(ctx context.Context) ([]channel, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT channel_id, room_id, name, description, avatar_url, visibility,
			join_policy, channel_type, comments_enabled, muted, member_count, pending_join_count
		FROM p2p_channels ORDER BY channel_id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	var channels []channel
	for rows.Next() {
		var ch channel
		var commentsEnabled, muted int64
		if err := rows.Scan(&ch.ChannelID, &ch.RoomID, &ch.Name, &ch.Description, &ch.AvatarURL, &ch.Visibility,
			&ch.JoinPolicy, &ch.ChannelType, &commentsEnabled, &muted, &ch.MemberCount, &ch.PendingJoinCount); err != nil {
			return nil, err
		}
		ch.CommentsEnabled = commentsEnabled == 1
		ch.Muted = muted == 1
		channels = append(channels, ch)
	}
	return channels, rows.Err()
}

func (s *DatabaseStore) UpsertChannelInviteGrant(ctx context.Context, grant channelInviteGrant) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_channel_invite_grants (grant_id, channel_id, room_id, share_room_id, created_by, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT(grant_id) DO UPDATE SET
				channel_id = EXCLUDED.channel_id,
				room_id = EXCLUDED.room_id,
				share_room_id = EXCLUDED.share_room_id,
				created_by = EXCLUDED.created_by,
				created_at = EXCLUDED.created_at
		`, grant.GrantID, grant.ChannelID, grant.RoomID, grant.ShareRoomID, grant.CreatedBy, grant.CreatedAt)
		return err
	})
}

func (s *DatabaseStore) ListChannelInviteGrants(ctx context.Context) ([]channelInviteGrant, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT grant_id, channel_id, room_id, share_room_id, created_by, created_at
		FROM p2p_channel_invite_grants ORDER BY created_at DESC, grant_id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	var grants []channelInviteGrant
	for rows.Next() {
		var grant channelInviteGrant
		if err := rows.Scan(&grant.GrantID, &grant.ChannelID, &grant.RoomID, &grant.ShareRoomID, &grant.CreatedBy, &grant.CreatedAt); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

func (s *DatabaseStore) InsertChannelPost(ctx context.Context, post channelPostRecord) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_channel_posts (
				post_id, channel_id, room_id, event_id, author_mxid, author_name,
				body, message_type, media_json, origin_server_ts, comment_count
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT(post_id) DO UPDATE SET
				channel_id = EXCLUDED.channel_id,
				room_id = EXCLUDED.room_id,
				event_id = EXCLUDED.event_id,
				author_mxid = EXCLUDED.author_mxid,
				author_name = EXCLUDED.author_name,
				body = EXCLUDED.body,
				message_type = EXCLUDED.message_type,
				media_json = EXCLUDED.media_json,
				origin_server_ts = EXCLUDED.origin_server_ts,
				comment_count = EXCLUDED.comment_count
		`, post.PostID, post.ChannelID, post.RoomID, post.EventID, post.AuthorMXID, post.AuthorName,
			post.Body, post.MessageType, post.MediaJSON, post.OriginServerTS, post.CommentCount)
		return err
	})
}

func (s *DatabaseStore) ListChannelPosts(ctx context.Context, channelID string) ([]channelPostRecord, error) {
	var rows *sql.Rows
	var err error
	if channelID == "" {
		rows, err = s.db.QueryContext(ctx, listPostsSelect+` ORDER BY origin_server_ts DESC`)
	} else {
		rows, err = s.db.QueryContext(ctx, listPostsSelect+` WHERE channel_id = $1 ORDER BY origin_server_ts DESC`, channelID)
	}
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	var posts []channelPostRecord
	for rows.Next() {
		var post channelPostRecord
		if err := rows.Scan(&post.PostID, &post.ChannelID, &post.RoomID, &post.EventID, &post.AuthorMXID, &post.AuthorName,
			&post.Body, &post.MessageType, &post.MediaJSON, &post.OriginServerTS, &post.CommentCount); err != nil {
			return nil, err
		}
		posts = append(posts, post)
	}
	return posts, rows.Err()
}

const listPostsSelect = `SELECT post_id, channel_id, room_id, event_id, author_mxid, author_name, body, message_type, media_json, origin_server_ts, comment_count FROM p2p_channel_posts`

func (s *DatabaseStore) InsertChannelComment(ctx context.Context, comment channelCommentRecord) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_channel_comments (
				comment_id, post_id, channel_id, event_id, author_mxid, author_name,
				body, message_type, media_json, reply_to_comment_id, reply_to_author_mxid, mentions_json,
				origin_server_ts, reaction_count, reacted_by_me
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
			ON CONFLICT(comment_id) DO UPDATE SET
				post_id = EXCLUDED.post_id,
				channel_id = EXCLUDED.channel_id,
				event_id = EXCLUDED.event_id,
				author_mxid = EXCLUDED.author_mxid,
				author_name = EXCLUDED.author_name,
				body = EXCLUDED.body,
				message_type = EXCLUDED.message_type,
				media_json = EXCLUDED.media_json,
				reply_to_comment_id = EXCLUDED.reply_to_comment_id,
				reply_to_author_mxid = EXCLUDED.reply_to_author_mxid,
				mentions_json = EXCLUDED.mentions_json,
				origin_server_ts = EXCLUDED.origin_server_ts,
				reaction_count = EXCLUDED.reaction_count,
				reacted_by_me = EXCLUDED.reacted_by_me
		`, comment.CommentID, comment.PostID, comment.ChannelID, comment.EventID, comment.AuthorMXID, comment.AuthorName,
			comment.Body, comment.MessageType, comment.MediaJSON, comment.ReplyToCommentID, comment.ReplyToAuthorMXID, fallbackString(comment.MentionsJSON, "[]"),
			comment.OriginServerTS, comment.ReactionCount, boolInt(comment.ReactedByMe))
		return err
	})
}

func (s *DatabaseStore) ListChannelComments(ctx context.Context, postID string) ([]channelCommentRecord, error) {
	var rows *sql.Rows
	var err error
	if postID == "" {
		rows, err = s.db.QueryContext(ctx, listCommentsSelect+` ORDER BY origin_server_ts ASC`)
	} else {
		rows, err = s.db.QueryContext(ctx, listCommentsSelect+` WHERE post_id = $1 ORDER BY origin_server_ts ASC`, postID)
	}
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	var comments []channelCommentRecord
	for rows.Next() {
		var comment channelCommentRecord
		var reacted int64
		if err := rows.Scan(&comment.CommentID, &comment.PostID, &comment.ChannelID, &comment.EventID, &comment.AuthorMXID, &comment.AuthorName,
			&comment.Body, &comment.MessageType, &comment.MediaJSON, &comment.ReplyToCommentID, &comment.ReplyToAuthorMXID, &comment.MentionsJSON,
			&comment.OriginServerTS, &comment.ReactionCount, &reacted); err != nil {
			return nil, err
		}
		comment.ReactedByMe = reacted == 1
		comments = append(comments, comment)
	}
	return comments, rows.Err()
}

const listCommentsSelect = `SELECT comment_id, post_id, channel_id, event_id, author_mxid, author_name, body, message_type, media_json, reply_to_comment_id, reply_to_author_mxid, mentions_json, origin_server_ts, reaction_count, reacted_by_me FROM p2p_channel_comments`

func (s *DatabaseStore) UpsertContact(ctx context.Context, contact contactRecord) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_contacts (room_id, peer_mxid, display_name, avatar_url, remark, domain, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT(peer_mxid) DO UPDATE SET
				room_id = EXCLUDED.room_id,
				peer_mxid = EXCLUDED.peer_mxid,
				display_name = EXCLUDED.display_name,
				avatar_url = CASE
					WHEN EXCLUDED.avatar_url <> '' THEN EXCLUDED.avatar_url
					ELSE p2p_contacts.avatar_url
				END,
				remark = EXCLUDED.remark,
				domain = EXCLUDED.domain,
				status = EXCLUDED.status
		`, contact.RoomID, contact.PeerMXID, contact.DisplayName, contact.AvatarURL, contact.Remark, contact.Domain, contact.Status)
		return err
	})
}

func (s *DatabaseStore) ListContacts(ctx context.Context) ([]contactRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT room_id, peer_mxid, display_name, avatar_url, remark, domain, status FROM p2p_contacts ORDER BY display_name ASC`)
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	var contacts []contactRecord
	for rows.Next() {
		var contact contactRecord
		if err := rows.Scan(&contact.RoomID, &contact.PeerMXID, &contact.DisplayName, &contact.AvatarURL, &contact.Remark, &contact.Domain, &contact.Status); err != nil {
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

func boolInt(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func internalError(err error) *apiError {
	return statusError(500, fmt.Sprintf("internal error: %s", err.Error()))
}
