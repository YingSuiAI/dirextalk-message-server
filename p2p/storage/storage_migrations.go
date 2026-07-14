package storage

import (
	"context"
	"database/sql"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
)

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
						access_token TEXT NOT NULL,
							agent_token TEXT NOT NULL,
							owner_mxid TEXT NOT NULL,
							agent_room_id TEXT NOT NULL,
							system_room_id TEXT NOT NULL DEFAULT '',
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
					display_name_override BOOLEAN NOT NULL DEFAULT FALSE,
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
					dedupe_key TEXT NOT NULL DEFAULT '',
					payload_json TEXT NOT NULL,
					created_at TEXT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_events_room_idx ON p2p_events(room_id, seq)`,
				`CREATE INDEX IF NOT EXISTS p2p_events_type_idx ON p2p_events(type, seq)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_events_dedupe_key_idx ON p2p_events(dedupe_key) WHERE dedupe_key <> ''`,
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
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: owner scoped member indexes v25",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			exists, err := productTableExists(ctx, txn, "p2p_members")
			if err != nil {
				return err
			}
			if !exists {
				return nil
			}
			return execMigrationStatements(ctx, txn, []string{
				`CREATE INDEX IF NOT EXISTS p2p_members_user_room_idx ON p2p_members(user_id, membership, room_id)`,
				`CREATE INDEX IF NOT EXISTS p2p_members_user_channel_idx ON p2p_members(user_id, membership, channel_id)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: public channel visibility index v26",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			exists, err := productTableExists(ctx, txn, "p2p_channels")
			if err != nil {
				return err
			}
			if !exists {
				return nil
			}
			_, err = txn.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS p2p_channels_visibility_idx ON p2p_channels(visibility, channel_id)`)
			return err
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: event dedupe key v27",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			exists, err := productTableExists(ctx, txn, "p2p_events")
			if err != nil {
				return err
			}
			if !exists {
				return nil
			}
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_events ADD COLUMN IF NOT EXISTS dedupe_key TEXT NOT NULL DEFAULT ''`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_events_dedupe_key_idx ON p2p_events(dedupe_key) WHERE dedupe_key <> ''`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: contact display name override v28",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			exists, err := productTableExists(ctx, txn, "p2p_contacts")
			if err != nil {
				return err
			}
			if !exists {
				return nil
			}
			_, err = txn.ExecContext(ctx, `ALTER TABLE p2p_contacts ADD COLUMN IF NOT EXISTS display_name_override BOOLEAN NOT NULL DEFAULT FALSE`)
			return err
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: portal agent config json v29",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			exists, err := productTableExists(ctx, txn, "p2p_portal")
			if err != nil {
				return err
			}
			if !exists {
				return nil
			}
			_, err = txn.ExecContext(ctx, `ALTER TABLE p2p_portal ADD COLUMN IF NOT EXISTS agent_config_json TEXT NOT NULL DEFAULT ''`)
			return err
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: owner blocks v30",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_blocks (
					target_type TEXT NOT NULL,
					target_id TEXT NOT NULL,
					room_id TEXT NOT NULL DEFAULT '',
					peer_mxid TEXT NOT NULL DEFAULT '',
					display_name TEXT NOT NULL DEFAULT '',
					avatar_url TEXT NOT NULL DEFAULT '',
					created_at BIGINT NOT NULL DEFAULT 0,
					PRIMARY KEY (target_type, target_id)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_blocks_type_idx ON p2p_blocks(target_type, display_name, target_id)`,
				`CREATE INDEX IF NOT EXISTS p2p_blocks_room_idx ON p2p_blocks(room_id)`,
				`CREATE INDEX IF NOT EXISTS p2p_blocks_peer_idx ON p2p_blocks(peer_mxid)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: official plugins v31",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_plugins (
					id TEXT PRIMARY KEY NOT NULL,
					name TEXT NOT NULL DEFAULT '',
					version TEXT NOT NULL DEFAULT '',
					image TEXT NOT NULL DEFAULT '',
					digest TEXT NOT NULL DEFAULT '',
					status TEXT NOT NULL DEFAULT '',
					enabled BIGINT NOT NULL DEFAULT 0,
					config_json TEXT NOT NULL DEFAULT '',
					last_job_id TEXT NOT NULL DEFAULT '',
					created_at BIGINT NOT NULL DEFAULT 0,
					updated_at BIGINT NOT NULL DEFAULT 0
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_plugins_status_idx ON p2p_plugins(status, enabled)`,
				`CREATE TABLE IF NOT EXISTS p2p_plugin_jobs (
					job_id TEXT PRIMARY KEY NOT NULL,
					plugin_id TEXT NOT NULL,
					action TEXT NOT NULL,
					status TEXT NOT NULL,
					message TEXT NOT NULL DEFAULT '',
					created_at BIGINT NOT NULL DEFAULT 0,
					updated_at BIGINT NOT NULL DEFAULT 0
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_plugin_jobs_plugin_idx ON p2p_plugin_jobs(plugin_id, created_at)`,
				`CREATE INDEX IF NOT EXISTS p2p_plugin_jobs_status_idx ON p2p_plugin_jobs(status, updated_at)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: plugin secrets v32",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_plugin_secrets (
					plugin_id TEXT NOT NULL,
					name TEXT NOT NULL,
					value TEXT NOT NULL DEFAULT '',
					updated_at BIGINT NOT NULL DEFAULT 0,
					PRIMARY KEY (plugin_id, name)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_plugin_secrets_updated_idx ON p2p_plugin_secrets(plugin_id, updated_at)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: system reports v33",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			exists, err := productTableExists(ctx, txn, "p2p_portal")
			if err != nil {
				return err
			}
			if exists {
				if _, err = txn.ExecContext(ctx, `ALTER TABLE p2p_portal ADD COLUMN IF NOT EXISTS system_room_id TEXT NOT NULL DEFAULT ''`); err != nil {
					return err
				}
			}
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_reports (
					report_id TEXT PRIMARY KEY NOT NULL,
					target_type TEXT NOT NULL,
					target_room_id TEXT NOT NULL,
					target_channel_id TEXT NOT NULL DEFAULT '',
					target_name TEXT NOT NULL DEFAULT '',
					reporter_mxid TEXT NOT NULL DEFAULT '',
					reporter_display_name TEXT NOT NULL DEFAULT '',
					reason TEXT NOT NULL DEFAULT '',
					body TEXT NOT NULL DEFAULT '',
					image_urls_json TEXT NOT NULL DEFAULT '[]',
					system_room_id TEXT NOT NULL DEFAULT '',
					event_id TEXT NOT NULL DEFAULT '',
					origin_server_ts BIGINT NOT NULL DEFAULT 0,
					created_at TEXT NOT NULL DEFAULT ''
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_reports_target_idx ON p2p_reports(target_type, target_room_id, created_at)`,
				`CREATE INDEX IF NOT EXISTS p2p_reports_reporter_idx ON p2p_reports(reporter_mxid, created_at)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: portal client build v34",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			exists, err := productTableExists(ctx, txn, "p2p_portal")
			if err != nil || !exists {
				return err
			}
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_portal ADD COLUMN IF NOT EXISTS client_version TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_portal ADD COLUMN IF NOT EXISTS client_build_number TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_portal ADD COLUMN IF NOT EXISTS client_platform TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_portal ADD COLUMN IF NOT EXISTS client_version_reported_at TEXT NOT NULL DEFAULT ''`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: recoverable operations v35",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			statements := make([]string, 0, 5)
			contactsExist, err := productTableExists(ctx, txn, "p2p_contacts")
			if err != nil {
				return err
			}
			if contactsExist {
				statements = append(statements, `ALTER TABLE p2p_contacts ADD COLUMN IF NOT EXISTS request_id TEXT NOT NULL DEFAULT ''`)
			}
			membersExist, err := productTableExists(ctx, txn, "p2p_members")
			if err != nil {
				return err
			}
			if membersExist {
				statements = append(statements, `ALTER TABLE p2p_members ADD COLUMN IF NOT EXISTS request_id TEXT NOT NULL DEFAULT ''`)
			}
			statements = append(statements,
				`CREATE TABLE IF NOT EXISTS p2p_operations (
					operation_id TEXT PRIMARY KEY NOT NULL,
					action TEXT NOT NULL DEFAULT '',
					status TEXT NOT NULL DEFAULT '',
					phase TEXT NOT NULL DEFAULT '',
					room_id TEXT NOT NULL DEFAULT '',
					current_room_id TEXT NOT NULL DEFAULT '',
					user_id TEXT NOT NULL DEFAULT '',
					peer_mxid TEXT NOT NULL DEFAULT '',
					request_id TEXT NOT NULL DEFAULT '',
					result_json TEXT NOT NULL DEFAULT '',
					error_code TEXT NOT NULL DEFAULT '',
					created_at BIGINT NOT NULL DEFAULT 0,
					updated_at BIGINT NOT NULL DEFAULT 0
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_operations_status_updated_idx ON p2p_operations(status, updated_at)`,
			)
			return execMigrationStatements(ctx, txn, statements)
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: recoverable operation claims v36",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_operations ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 0`,
				`ALTER TABLE p2p_operations ADD COLUMN IF NOT EXISTS lease_owner TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_operations ADD COLUMN IF NOT EXISTS lease_until BIGINT NOT NULL DEFAULT 0`,
				`CREATE INDEX IF NOT EXISTS p2p_operations_lease_idx ON p2p_operations(lease_until) WHERE lease_owner <> ''`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: operation base generations v37",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_operations ADD COLUMN IF NOT EXISTS base_request_id TEXT NOT NULL DEFAULT ''`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: legacy agent invocation reservations v38",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_legacy_agent_invocations (
					matrix_room_id TEXT NOT NULL CHECK (matrix_room_id <> ''),
					request_id UUID NOT NULL,
					matrix_invoke_event_id TEXT NOT NULL CHECK (matrix_invoke_event_id <> ''),
					matrix_input_event_id TEXT NOT NULL CHECK (matrix_input_event_id <> ''),
					tenant_id UUID NOT NULL,
					installation_id UUID NOT NULL,
					conversation_id UUID NOT NULL,
					request_event_id UUID NOT NULL,
					source_digest BYTEA NOT NULL CHECK (octet_length(source_digest) = 32),
					idempotency_digest BYTEA NOT NULL CHECK (octet_length(idempotency_digest) = 32),
					request_digest BYTEA NOT NULL CHECK (octet_length(request_digest) = 32),
					preferred_connector_id UUID,
					required_capabilities TEXT[] NOT NULL DEFAULT '{}'
						CHECK (cardinality(required_capabilities) <= 64),
					dispatch_mode TEXT NOT NULL CHECK (dispatch_mode IN ('single', 'failover')),
					grant_version BIGINT NOT NULL CHECK (grant_version > 0 AND grant_version <= 9007199254740991),
					state TEXT NOT NULL CHECK (state IN ('reserved', 'accepted', 'rejected')),
					run_id UUID,
					routing_state TEXT NOT NULL DEFAULT ''
						CHECK (routing_state IN ('', 'queued', 'offered', 'leased', 'reconcile_required', 'expired')),
					inserted BOOLEAN,
					error_code TEXT NOT NULL DEFAULT '',
					created_at TIMESTAMPTZ NOT NULL,
					updated_at TIMESTAMPTZ NOT NULL,
					CHECK (
						(state = 'reserved' AND run_id IS NULL AND routing_state = '' AND inserted IS NULL AND error_code = '')
						OR (state = 'accepted' AND run_id IS NOT NULL AND routing_state <> '' AND inserted IS NOT NULL AND error_code = '')
						OR (state = 'rejected' AND run_id IS NULL AND routing_state = '' AND inserted IS NULL AND error_code <> '')
					),
					PRIMARY KEY (matrix_room_id, request_id),
					UNIQUE (matrix_invoke_event_id),
					UNIQUE (tenant_id, matrix_room_id, idempotency_digest)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_legacy_agent_invocations_state_updated_idx
					ON p2p_legacy_agent_invocations(state, updated_at)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud orchestrator control plane v39",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_goals (
					goal_id TEXT PRIMARY KEY NOT NULL,
					owner_mxid TEXT NOT NULL,
					prompt TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL DEFAULT '',
					plan_id TEXT NOT NULL,
					status TEXT NOT NULL CHECK (status IN ('researching', 'planned', 'canceled')),
					idempotency_hash TEXT NOT NULL,
					request_digest TEXT NOT NULL,
					revision BIGINT NOT NULL CHECK (revision > 0),
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL,
					UNIQUE (owner_mxid, idempotency_hash)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_goals_owner_updated_idx ON p2p_cloud_goals(owner_mxid, updated_at DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_plans (
					plan_id TEXT PRIMARY KEY NOT NULL,
					goal_id TEXT NOT NULL UNIQUE,
					cloud_connection_id TEXT NOT NULL DEFAULT '',
					status TEXT NOT NULL CHECK (status IN ('researching', 'quoting', 'ready_for_confirmation', 'approved', 'expired', 'superseded')),
					title TEXT NOT NULL DEFAULT '',
					summary TEXT NOT NULL DEFAULT '',
					recipe_digest TEXT NOT NULL DEFAULT '',
					quote_id TEXT NOT NULL DEFAULT '',
					plan_hash TEXT NOT NULL DEFAULT '',
					revision BIGINT NOT NULL CHECK (revision > 0),
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_plans_status_updated_idx ON p2p_cloud_plans(status, updated_at DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_connections (
					cloud_connection_id TEXT PRIMARY KEY NOT NULL,
					provider TEXT NOT NULL,
					account_id TEXT NOT NULL DEFAULT '',
					region TEXT NOT NULL DEFAULT '',
					mode TEXT NOT NULL,
					status TEXT NOT NULL,
					revision BIGINT NOT NULL CHECK (revision > 0),
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_connections_status_updated_idx ON p2p_cloud_connections(status, updated_at DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_deployments (
					deployment_id TEXT PRIMARY KEY NOT NULL,
					plan_id TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL,
					execution_status TEXT NOT NULL,
					outcome_status TEXT NOT NULL,
					resource_status TEXT NOT NULL,
					revision BIGINT NOT NULL CHECK (revision > 0),
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_deployments_plan_updated_idx ON p2p_cloud_deployments(plan_id, updated_at DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_services (
					service_id TEXT PRIMARY KEY NOT NULL,
					deployment_id TEXT NOT NULL,
					recipe_id TEXT NOT NULL DEFAULT '',
					name TEXT NOT NULL,
					service_status TEXT NOT NULL,
					integration_status TEXT NOT NULL,
					revision BIGINT NOT NULL CHECK (revision > 0),
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_services_deployment_updated_idx ON p2p_cloud_services(deployment_id, updated_at DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_recipes (
					recipe_id TEXT PRIMARY KEY NOT NULL,
					name TEXT NOT NULL,
					version TEXT NOT NULL,
					digest TEXT NOT NULL,
					maturity TEXT NOT NULL CHECK (maturity IN ('experimental', 'awaiting_management_acceptance', 'managed')),
					revision BIGINT NOT NULL CHECK (revision > 0),
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL
				)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_recipes_digest_idx ON p2p_cloud_recipes(digest)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_alerts (
					alert_id TEXT PRIMARY KEY NOT NULL,
					deployment_id TEXT NOT NULL DEFAULT '',
					service_id TEXT NOT NULL DEFAULT '',
					severity TEXT NOT NULL,
					code TEXT NOT NULL,
					message TEXT NOT NULL,
					acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
					revision BIGINT NOT NULL CHECK (revision > 0),
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_alerts_open_updated_idx ON p2p_cloud_alerts(acknowledged, updated_at DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_events (
					event_id TEXT PRIMARY KEY NOT NULL,
					type TEXT NOT NULL,
					aggregate_type TEXT NOT NULL,
					aggregate_id TEXT NOT NULL,
					revision BIGINT NOT NULL CHECK (revision > 0),
					summary_json TEXT NOT NULL DEFAULT '{}',
					created_at BIGINT NOT NULL,
					UNIQUE (aggregate_type, aggregate_id, revision, type)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_events_aggregate_revision_idx ON p2p_cloud_events(aggregate_type, aggregate_id, revision DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_outbox (
					outbox_id TEXT PRIMARY KEY NOT NULL,
					kind TEXT NOT NULL,
					aggregate_type TEXT NOT NULL,
					aggregate_id TEXT NOT NULL,
					payload_json TEXT NOT NULL,
					lease_owner TEXT NOT NULL DEFAULT '',
					lease_until BIGINT NOT NULL DEFAULT 0,
					attempts INTEGER NOT NULL DEFAULT 0,
					delivered_at BIGINT NOT NULL DEFAULT 0,
					created_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_outbox_pending_idx ON p2p_cloud_outbox(delivered_at, lease_until, created_at)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud orchestrator runtime v40",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_cloud_outbox ADD COLUMN IF NOT EXISTS lease_token TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_outbox ADD COLUMN IF NOT EXISTS available_at BIGINT NOT NULL DEFAULT 0`,
				`ALTER TABLE p2p_cloud_outbox ADD COLUMN IF NOT EXISTS last_error_code TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_outbox ADD COLUMN IF NOT EXISTS completed_at BIGINT NOT NULL DEFAULT 0`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_outbox_claim_idx ON p2p_cloud_outbox(kind, completed_at, available_at, lease_until, created_at)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_plan_versions (
					plan_id TEXT NOT NULL,
					revision BIGINT NOT NULL CHECK (revision > 0),
					canonical_cbor BYTEA NOT NULL,
					display_json TEXT NOT NULL,
					plan_hash TEXT NOT NULL,
					recipe_digest TEXT NOT NULL,
					quote_id TEXT NOT NULL,
					quote_digest TEXT NOT NULL,
					quote_valid_until BIGINT NOT NULL,
					created_at BIGINT NOT NULL,
					PRIMARY KEY (plan_id, revision)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_plan_versions_plan_created_idx ON p2p_cloud_plan_versions(plan_id, created_at DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_recipe_versions (
					recipe_id TEXT NOT NULL,
					revision BIGINT NOT NULL CHECK (revision > 0),
					canonical_cbor BYTEA NOT NULL,
					display_json TEXT NOT NULL,
					digest TEXT NOT NULL,
					maturity TEXT NOT NULL,
					created_at BIGINT NOT NULL,
					PRIMARY KEY (recipe_id, revision)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_recipe_versions_recipe_created_idx ON p2p_cloud_recipe_versions(recipe_id, created_at DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_quotes (
					quote_id TEXT PRIMARY KEY NOT NULL,
					cloud_connection_id TEXT NOT NULL,
					region TEXT NOT NULL,
					currency TEXT NOT NULL,
					digest TEXT NOT NULL UNIQUE,
					canonical_cbor BYTEA NOT NULL,
					display_json TEXT NOT NULL,
					quoted_at BIGINT NOT NULL,
					valid_until BIGINT NOT NULL,
					created_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_quotes_connection_valid_idx ON p2p_cloud_quotes(cloud_connection_id, valid_until DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_jobs (
					job_id TEXT PRIMARY KEY NOT NULL,
					plan_id TEXT NOT NULL,
					deployment_id TEXT NOT NULL DEFAULT '',
					kind TEXT NOT NULL,
					execution_status TEXT NOT NULL CHECK (execution_status IN ('queued', 'provisioning', 'installing', 'waiting_user', 'verifying', 'finished')),
					outcome_status TEXT NOT NULL CHECK (outcome_status IN ('pending', 'succeeded', 'failed', 'canceled', 'interrupted')),
					checkpoint TEXT NOT NULL DEFAULT '',
					error_code TEXT NOT NULL DEFAULT '',
					revision BIGINT NOT NULL CHECK (revision > 0),
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_jobs_plan_updated_idx ON p2p_cloud_jobs(plan_id, updated_at DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_job_steps (
					job_id TEXT NOT NULL,
					step_id TEXT NOT NULL,
					status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'waiting_user', 'finished', 'failed')),
					summary TEXT NOT NULL,
					checkpoint TEXT NOT NULL DEFAULT '',
					error_code TEXT NOT NULL DEFAULT '',
					revision BIGINT NOT NULL CHECK (revision > 0),
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL,
					PRIMARY KEY (job_id, step_id)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_job_steps_job_updated_idx ON p2p_cloud_job_steps(job_id, updated_at DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_projection_outbox (
					projection_id TEXT PRIMARY KEY NOT NULL,
					cloud_event_id TEXT NOT NULL UNIQUE,
					type TEXT NOT NULL,
					payload_json TEXT NOT NULL,
					lease_owner TEXT NOT NULL DEFAULT '',
					lease_token TEXT NOT NULL DEFAULT '',
					lease_until BIGINT NOT NULL DEFAULT 0,
					attempts INTEGER NOT NULL DEFAULT 0,
					available_at BIGINT NOT NULL DEFAULT 0,
					last_error_code TEXT NOT NULL DEFAULT '',
					completed_at BIGINT NOT NULL DEFAULT 0,
					created_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_projection_outbox_pending_idx ON p2p_cloud_projection_outbox(completed_at, available_at, lease_until, created_at)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud orchestrator broker quote v41",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				// This table contains only the independently verified, non-secret
				// Connection Stack endpoint and public signing-key identity. The
				// corresponding private Ed25519 key is never stored in PostgreSQL.
				`CREATE TABLE IF NOT EXISTS p2p_cloud_connection_brokers (
					cloud_connection_id TEXT PRIMARY KEY NOT NULL,
					broker_command_url TEXT NOT NULL,
					broker_region TEXT NOT NULL,
					connection_generation BIGINT NOT NULL CHECK (connection_generation > 0),
					node_key_id TEXT NOT NULL,
					next_node_counter BIGINT NOT NULL DEFAULT 0 CHECK (next_node_counter >= 0),
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_connection_brokers_region_idx
					ON p2p_cloud_connection_brokers(broker_region, cloud_connection_id)`,
				// Command identity and exact signed envelopes are durable so a lost
				// response can be replayed without a second purchase or counter.
				`CREATE TABLE IF NOT EXISTS p2p_cloud_broker_commands (
					command_id TEXT PRIMARY KEY NOT NULL,
					cloud_connection_id TEXT NOT NULL,
					plan_id TEXT NOT NULL,
					plan_revision BIGINT NOT NULL CHECK (plan_revision > 0),
					quote_request_id TEXT NOT NULL,
					quote_request_digest TEXT NOT NULL,
					command_attempt INTEGER NOT NULL CHECK (command_attempt > 0),
					action TEXT NOT NULL CHECK (action = 'quote.request'),
					node_key_id TEXT NOT NULL,
					expected_generation BIGINT NOT NULL CHECK (expected_generation > 0),
					node_counter BIGINT NOT NULL CHECK (node_counter > 0),
					canonical_payload_json TEXT NOT NULL DEFAULT '',
					payload_sha256 TEXT NOT NULL DEFAULT '',
					request_sha256 TEXT NOT NULL DEFAULT '',
					signed_envelope_json TEXT NOT NULL DEFAULT '',
					issued_at BIGINT NOT NULL DEFAULT 0,
					expires_at BIGINT NOT NULL DEFAULT 0,
					state TEXT NOT NULL CHECK (state IN ('allocated', 'signed', 'indeterminate', 'accepted', 'expired', 'failed')),
					attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
					last_error_code TEXT NOT NULL DEFAULT '',
					receipt_json TEXT NOT NULL DEFAULT '',
					quote_json TEXT NOT NULL DEFAULT '',
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL,
					UNIQUE (cloud_connection_id, node_counter),
					UNIQUE (plan_id, plan_revision, quote_request_digest, command_attempt)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_broker_commands_plan_idx
					ON p2p_cloud_broker_commands(plan_id, plan_revision, quote_request_digest, command_attempt DESC)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud connection stack registration v42",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				// Bootstrap records remain private control-plane state until the
				// independent Orchestrator verifies the signed Broker. In
				// particular, candidate_broker_url and stack_arn never become a
				// ProductCore Connection projection.
				`CREATE TABLE IF NOT EXISTS p2p_cloud_connection_bootstraps (
					bootstrap_id TEXT PRIMARY KEY NOT NULL,
					owner_mxid TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL UNIQUE,
					provider TEXT NOT NULL CHECK (provider = 'aws'),
					requested_region TEXT NOT NULL,
					template_url TEXT NOT NULL,
					template_digest TEXT NOT NULL,
					source_tree_digest TEXT NOT NULL,
					stack_name TEXT NOT NULL,
					node_key_id TEXT NOT NULL,
					node_public_key_spki_base64 TEXT NOT NULL,
					device_approval_key_id TEXT NOT NULL,
					device_approval_public_key_spki_base64 TEXT NOT NULL,
					candidate_broker_url TEXT NOT NULL DEFAULT '',
					stack_arn TEXT NOT NULL DEFAULT '',
					status TEXT NOT NULL CHECK (status IN ('awaiting_stack', 'verification_queued', 'verifying', 'active', 'verification_failed', 'expired')),
					revision BIGINT NOT NULL CHECK (revision > 0),
					idempotency_hash TEXT NOT NULL,
					request_digest TEXT NOT NULL,
					completion_idempotency_hash TEXT NOT NULL DEFAULT '',
					completion_request_digest TEXT NOT NULL DEFAULT '',
					job_id TEXT NOT NULL DEFAULT '',
					next_node_counter BIGINT NOT NULL DEFAULT 0 CHECK (next_node_counter >= 0),
					expires_at BIGINT NOT NULL,
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL,
					UNIQUE (owner_mxid, idempotency_hash)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_connection_bootstraps_status_expiry_idx
					ON p2p_cloud_connection_bootstraps(status, expires_at, updated_at DESC)`,
				// A registration command is persisted before its first network
				// attempt. It has its own counter source because no active Broker
				// row exists until the verification transaction succeeds.
				`CREATE TABLE IF NOT EXISTS p2p_cloud_connection_registration_commands (
					command_id TEXT PRIMARY KEY NOT NULL,
					bootstrap_id TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL,
					command_attempt INTEGER NOT NULL CHECK (command_attempt > 0),
					action TEXT NOT NULL CHECK (action = 'connection.registration.verify'),
					node_key_id TEXT NOT NULL,
					expected_generation BIGINT NOT NULL CHECK (expected_generation > 0),
					node_counter BIGINT NOT NULL CHECK (node_counter > 0),
					canonical_payload_json TEXT NOT NULL DEFAULT '',
					payload_sha256 TEXT NOT NULL DEFAULT '',
					request_sha256 TEXT NOT NULL DEFAULT '',
					signed_envelope_json TEXT NOT NULL DEFAULT '',
					issued_at BIGINT NOT NULL DEFAULT 0,
					expires_at BIGINT NOT NULL DEFAULT 0,
					state TEXT NOT NULL CHECK (state IN ('allocated', 'signed', 'indeterminate', 'accepted', 'expired', 'failed')),
					attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
					last_error_code TEXT NOT NULL DEFAULT '',
					receipt_json TEXT NOT NULL DEFAULT '',
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL,
					UNIQUE (cloud_connection_id, node_counter),
					UNIQUE (bootstrap_id, command_attempt)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_connection_registration_commands_state_idx
					ON p2p_cloud_connection_registration_commands(bootstrap_id, state, updated_at DESC)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud plan confirmation v43",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				// The confirmation record is the durable device-approval boundary.
				// It stores an unsigned, reviewable ApprovalV1 challenge and only a
				// consumed signature after verification. It never stores a private
				// device key, provider credential, Broker endpoint or Worker session.
				`CREATE TABLE IF NOT EXISTS p2p_cloud_plan_approvals (
					approval_id TEXT PRIMARY KEY NOT NULL,
					owner_mxid TEXT NOT NULL,
					plan_id TEXT NOT NULL,
					plan_revision BIGINT NOT NULL CHECK (plan_revision > 0),
					challenge_id TEXT NOT NULL UNIQUE,
					signer_key_id TEXT NOT NULL,
					plan_hash TEXT NOT NULL,
					approval_json TEXT NOT NULL,
					signing_payload_cbor BYTEA NOT NULL,
					expires_at BIGINT NOT NULL,
					status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'expired')),
					prepare_idempotency_hash TEXT NOT NULL,
					prepare_request_digest TEXT NOT NULL,
					approve_idempotency_hash TEXT,
					approve_request_digest TEXT,
					signature TEXT NOT NULL DEFAULT '',
					deployment_id TEXT NOT NULL DEFAULT '',
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL,
					UNIQUE (plan_id, plan_revision),
					UNIQUE (owner_mxid, prepare_idempotency_hash)
				)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_plan_approvals_owner_approve_idempotency_idx
					ON p2p_cloud_plan_approvals(owner_mxid, approve_idempotency_hash)
					WHERE approve_idempotency_hash IS NOT NULL`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_plan_approvals_plan_status_idx
					ON p2p_cloud_plan_approvals(plan_id, status, expires_at DESC)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud worker placement attestation v44",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				// These are private Stack-attested bindings. Empty defaults preserve
				// pre-v44 Connections, but the provision claim path rejects them
				// fail-closed until the Connection is re-attested with a fixed Worker
				// AMI, private placement and resource manifest digest.
				`ALTER TABLE p2p_cloud_connection_brokers ADD COLUMN IF NOT EXISTS worker_artifact_kind TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_connection_brokers ADD COLUMN IF NOT EXISTS worker_ami_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_connection_brokers ADD COLUMN IF NOT EXISTS worker_vpc_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_connection_brokers ADD COLUMN IF NOT EXISTS worker_subnet_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_connection_brokers ADD COLUMN IF NOT EXISTS worker_availability_zone TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_connection_brokers ADD COLUMN IF NOT EXISTS worker_resource_manifest_digest TEXT NOT NULL DEFAULT ''`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud deployment provision commands v45",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				// deployment.create has its own command journal rather than sharing
				// the quote table. It preserves the exact signed envelope across an
				// indeterminate network result, while keeping approval proof and
				// resource receipts outside ProductCore projections.
				`CREATE TABLE IF NOT EXISTS p2p_cloud_deployment_commands (
					command_id TEXT PRIMARY KEY NOT NULL,
					deployment_id TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL,
					plan_id TEXT NOT NULL,
					plan_revision BIGINT NOT NULL CHECK (plan_revision > 0),
					approval_id TEXT NOT NULL,
					request_digest TEXT NOT NULL,
					command_attempt INTEGER NOT NULL CHECK (command_attempt > 0),
					action TEXT NOT NULL CHECK (action = 'deployment.create'),
					node_key_id TEXT NOT NULL,
					expected_generation BIGINT NOT NULL CHECK (expected_generation > 0),
					node_counter BIGINT NOT NULL CHECK (node_counter > 0),
					canonical_payload_json TEXT NOT NULL DEFAULT '',
					payload_sha256 TEXT NOT NULL DEFAULT '',
					request_sha256 TEXT NOT NULL DEFAULT '',
					signed_envelope_json TEXT NOT NULL DEFAULT '',
					issued_at BIGINT NOT NULL DEFAULT 0,
					expires_at BIGINT NOT NULL DEFAULT 0,
					state TEXT NOT NULL CHECK (state IN ('allocated', 'signed', 'indeterminate', 'accepted', 'expired', 'failed')),
					attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
					last_error_code TEXT NOT NULL DEFAULT '',
					receipt_json TEXT NOT NULL DEFAULT '',
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL,
					UNIQUE (cloud_connection_id, node_counter),
					UNIQUE (deployment_id, request_digest, command_attempt)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_deployment_commands_deployment_idx
					ON p2p_cloud_deployment_commands(deployment_id, request_digest, command_attempt DESC)`,
				// Resource identifiers are durable private accounting/cleanup facts,
				// never public Deployment summary fields. A receipt is trusted only
				// after the typed Stack response binds it to the signed command.
				`CREATE TABLE IF NOT EXISTS p2p_cloud_deployment_resources (
					deployment_id TEXT PRIMARY KEY NOT NULL,
					cloud_connection_id TEXT NOT NULL,
					request_sha256 TEXT NOT NULL,
					resource_status TEXT NOT NULL CHECK (resource_status IN ('active', 'retained_tracked', 'destroying', 'verified_destroyed', 'blocked', 'orphaned')),
					instance_id TEXT NOT NULL,
					volume_ids_json TEXT NOT NULL,
					network_interface_ids_json TEXT NOT NULL,
					broker_receipt_json TEXT NOT NULL,
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_deployment_resources_status_idx
					ON p2p_cloud_deployment_resources(resource_status, updated_at DESC)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud worker bootstrap observations v46",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				// A verified Worker observation is private orchestrator evidence. The
				// record intentionally omits every bearer, bootstrap-session ID/hash,
				// IID document, raw Worker event, endpoint, log, and service secret.
				// Its lease fields are independent from the Worker lease reported by
				// the Stack, so an Orchestrator restart cannot duplicate a signed read.
				`CREATE TABLE IF NOT EXISTS p2p_cloud_worker_bootstrap_observations (
					deployment_id TEXT PRIMARY KEY NOT NULL,
					cloud_connection_id TEXT NOT NULL,
					instance_id TEXT NOT NULL,
					worker_session_state TEXT NOT NULL DEFAULT '',
					worker_lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (worker_lease_epoch >= 0),
					worker_lease_expires_at BIGINT NOT NULL DEFAULT 0,
					worker_last_sequence BIGINT NOT NULL DEFAULT 0 CHECK (worker_last_sequence >= 0),
					worker_last_event_at BIGINT NOT NULL DEFAULT 0,
					observed_at BIGINT NOT NULL DEFAULT 0,
					available_at BIGINT NOT NULL DEFAULT 0,
					lease_owner TEXT NOT NULL DEFAULT '',
					lease_token TEXT NOT NULL DEFAULT '',
					lease_until BIGINT NOT NULL DEFAULT 0,
					attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
					last_error_code TEXT NOT NULL DEFAULT '',
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_worker_bootstrap_observations_claim_idx
					ON p2p_cloud_worker_bootstrap_observations(available_at, lease_until, updated_at)`,
				// The exact signed read is journaled separately from deployment.create:
				// it can be replayed after a lost response without reusing a node
				// counter or turning an observation into a provider mutation.
				`CREATE TABLE IF NOT EXISTS p2p_cloud_deployment_observation_commands (
					command_id TEXT PRIMARY KEY NOT NULL,
					deployment_id TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL,
					request_digest TEXT NOT NULL,
					command_attempt INTEGER NOT NULL CHECK (command_attempt > 0),
					action TEXT NOT NULL CHECK (action = 'deployment.observe'),
					node_key_id TEXT NOT NULL,
					expected_generation BIGINT NOT NULL CHECK (expected_generation > 0),
					node_counter BIGINT NOT NULL CHECK (node_counter > 0),
					canonical_payload_json TEXT NOT NULL DEFAULT '',
					payload_sha256 TEXT NOT NULL DEFAULT '',
					request_sha256 TEXT NOT NULL DEFAULT '',
					signed_envelope_json TEXT NOT NULL DEFAULT '',
					issued_at BIGINT NOT NULL DEFAULT 0,
					expires_at BIGINT NOT NULL DEFAULT 0,
					state TEXT NOT NULL CHECK (state IN ('allocated', 'signed', 'indeterminate', 'accepted', 'expired', 'failed')),
					attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
					last_error_code TEXT NOT NULL DEFAULT '',
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL,
					UNIQUE (cloud_connection_id, node_counter),
					UNIQUE (deployment_id, request_digest, command_attempt)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_deployment_observation_commands_deployment_idx
					ON p2p_cloud_deployment_observation_commands(deployment_id, request_digest, command_attempt DESC)`,
			})
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
