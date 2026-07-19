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
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud execution probe task transport v47",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				// Execution probe artifacts and command journals are private
				// orchestrator state. They deliberately never become ProductCore
				// projections: the Worker receives only typed task references and
				// digests, while the signed command is replayable after a lost
				// response without allocating another Broker node counter.
				`CREATE TABLE IF NOT EXISTS p2p_cloud_execution_probe_tasks (
					deployment_id TEXT PRIMARY KEY NOT NULL,
					task_id TEXT NOT NULL UNIQUE,
					plan_id TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL,
					instance_id TEXT NOT NULL,
					execution_manifest_cbor BYTEA NOT NULL,
					execution_manifest_digest TEXT NOT NULL,
					input_cbor BYTEA NOT NULL,
					input_digest TEXT NOT NULL,
					task_status TEXT NOT NULL CHECK (task_status IN ('unissued', 'queued', 'running', 'succeeded', 'failed', 'interrupted')),
					task_attempt BIGINT NOT NULL CHECK (task_attempt > 0),
					last_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
					checkpoint TEXT NOT NULL DEFAULT '',
					error_code TEXT NOT NULL DEFAULT '',
					evidence_digest TEXT NOT NULL DEFAULT '',
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
				`CREATE INDEX IF NOT EXISTS p2p_cloud_execution_probe_tasks_claim_idx
					ON p2p_cloud_execution_probe_tasks(task_status, available_at, lease_until, updated_at)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_execution_probe_commands (
					command_id TEXT PRIMARY KEY NOT NULL,
					task_id TEXT NOT NULL,
					deployment_id TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL,
					request_digest TEXT NOT NULL,
					command_attempt INTEGER NOT NULL CHECK (command_attempt > 0),
					action TEXT NOT NULL CHECK (action IN ('worker.task.issue', 'worker.task.observe')),
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
					UNIQUE (task_id, action, request_digest, command_attempt)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_execution_probe_commands_task_idx
					ON p2p_cloud_execution_probe_commands(task_id, action, request_digest, command_attempt DESC)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud recipe execution confirmation v48",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				// A manifest is registered only by the trusted Orchestrator compiler.
				// ProductCore, Agent, MCP, and Worker paths never write this table.
				// It stores sealed references/digests rather than a command, artifact
				// body, URL, secret value, provider credential, or Worker session.
				`CREATE TABLE IF NOT EXISTS p2p_cloud_recipe_execution_manifests (
					execution_id TEXT PRIMARY KEY NOT NULL,
					deployment_id TEXT NOT NULL UNIQUE,
					plan_id TEXT NOT NULL,
					plan_revision BIGINT NOT NULL CHECK (plan_revision > 0),
					plan_hash TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL,
					manifest_digest TEXT NOT NULL,
					manifest_cbor BYTEA NOT NULL,
					manifest_json TEXT NOT NULL,
					status TEXT NOT NULL CHECK (status IN ('registered', 'approval_prepared', 'approved')),
					revision BIGINT NOT NULL CHECK (revision > 0),
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_recipe_execution_manifests_status_updated_idx
					ON p2p_cloud_recipe_execution_manifests(status, updated_at DESC)`,
				// Approval proof has a separate journal from Plan approval. A consumed
				// execution signature must never be substituted for a purchase approval,
				// and all replay keys remain owner-scoped and durable.
				`CREATE TABLE IF NOT EXISTS p2p_cloud_recipe_execution_approvals (
					approval_id TEXT PRIMARY KEY NOT NULL,
					owner_mxid TEXT NOT NULL,
					execution_id TEXT NOT NULL,
					deployment_id TEXT NOT NULL,
					deployment_revision BIGINT NOT NULL CHECK (deployment_revision > 0),
					plan_id TEXT NOT NULL,
					plan_revision BIGINT NOT NULL CHECK (plan_revision > 0),
					signer_key_id TEXT NOT NULL,
					manifest_digest TEXT NOT NULL,
					approval_json TEXT NOT NULL,
					signing_payload_cbor BYTEA NOT NULL,
					expires_at BIGINT NOT NULL,
					status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'expired')),
					prepare_idempotency_hash TEXT NOT NULL,
					prepare_request_digest TEXT NOT NULL,
					approve_idempotency_hash TEXT,
					approve_request_digest TEXT,
					signature TEXT NOT NULL DEFAULT '',
					job_id TEXT NOT NULL DEFAULT '',
					created_at BIGINT NOT NULL,
					updated_at BIGINT NOT NULL,
					UNIQUE (owner_mxid, prepare_idempotency_hash)
				)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_recipe_execution_approvals_owner_approve_idempotency_idx
					ON p2p_cloud_recipe_execution_approvals(owner_mxid, approve_idempotency_hash)
					WHERE approve_idempotency_hash IS NOT NULL`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_recipe_execution_approvals_execution_active_idx
					ON p2p_cloud_recipe_execution_approvals(execution_id)
					WHERE status IN ('pending', 'approved')`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_recipe_execution_approvals_execution_status_idx
					ON p2p_cloud_recipe_execution_approvals(execution_id, status, expires_at DESC)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud recipe install runner v49",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_recipe_install_tasks (
					execution_id TEXT PRIMARY KEY NOT NULL, task_id TEXT NOT NULL UNIQUE, deployment_id TEXT NOT NULL,
					plan_id TEXT NOT NULL, cloud_connection_id TEXT NOT NULL, instance_id TEXT NOT NULL,
					manifest_digest TEXT NOT NULL, input_digest TEXT NOT NULL, checkpoint_sequence_json TEXT NOT NULL,
					task_status TEXT NOT NULL CHECK (task_status IN ('unissued','queued','running','succeeded','failed','interrupted')),
					task_attempt BIGINT NOT NULL DEFAULT 1 CHECK (task_attempt > 0), last_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
					last_checkpoint TEXT NOT NULL DEFAULT '', error_code TEXT NOT NULL DEFAULT '',
					available_at BIGINT NOT NULL DEFAULT 0, lease_owner TEXT NOT NULL DEFAULT '', lease_token TEXT NOT NULL DEFAULT '', lease_until BIGINT NOT NULL DEFAULT 0,
					attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0), last_error_code TEXT NOT NULL DEFAULT '', created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_recipe_install_tasks_claim_idx ON p2p_cloud_recipe_install_tasks(task_status, available_at, lease_until, updated_at)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_recipe_install_commands (
					command_id TEXT PRIMARY KEY NOT NULL, execution_id TEXT NOT NULL, deployment_id TEXT NOT NULL, task_id TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL, request_digest TEXT NOT NULL, command_attempt INTEGER NOT NULL CHECK (command_attempt > 0),
					action TEXT NOT NULL CHECK (action IN ('worker.recipe_task.issue','worker.recipe_task.observe')),
					node_key_id TEXT NOT NULL, expected_generation BIGINT NOT NULL CHECK (expected_generation > 0), node_counter BIGINT NOT NULL CHECK (node_counter > 0),
					canonical_payload_json TEXT NOT NULL DEFAULT '', payload_sha256 TEXT NOT NULL DEFAULT '', request_sha256 TEXT NOT NULL DEFAULT '', signed_envelope_json TEXT NOT NULL DEFAULT '',
					issued_at BIGINT NOT NULL DEFAULT 0, expires_at BIGINT NOT NULL DEFAULT 0,
					state TEXT NOT NULL CHECK (state IN ('allocated','signed','indeterminate','accepted','expired','failed')),
					last_error_code TEXT NOT NULL DEFAULT '', created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
					UNIQUE (cloud_connection_id, node_counter), UNIQUE (execution_id, action, request_digest, command_attempt)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_recipe_install_commands_execution_idx ON p2p_cloud_recipe_install_commands(execution_id, action, command_attempt DESC)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud service readiness runner v50",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_readiness_tasks (
					task_id TEXT PRIMARY KEY NOT NULL, execution_id TEXT NOT NULL, deployment_id TEXT NOT NULL, service_id TEXT NOT NULL UNIQUE,
					cloud_connection_id TEXT NOT NULL, instance_id TEXT NOT NULL,
					recipe_execution_manifest_digest TEXT NOT NULL, install_evidence_digest TEXT NOT NULL, semantic_expectation_digest TEXT NOT NULL,
					task_status TEXT NOT NULL CHECK (task_status IN ('unissued','queued','running','succeeded','failed','interrupted')),
					task_attempt BIGINT NOT NULL DEFAULT 1 CHECK (task_attempt > 0), last_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
					checkpoint TEXT NOT NULL DEFAULT '', challenge_digest TEXT NOT NULL DEFAULT '', semantic_evidence_digest TEXT NOT NULL DEFAULT '', stack_observation_digest TEXT NOT NULL DEFAULT '', error_code TEXT NOT NULL DEFAULT '',
					available_at BIGINT NOT NULL DEFAULT 0, lease_owner TEXT NOT NULL DEFAULT '', lease_token TEXT NOT NULL DEFAULT '', lease_until BIGINT NOT NULL DEFAULT 0,
					attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0), last_error_code TEXT NOT NULL DEFAULT '', created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_readiness_tasks_claim_idx ON p2p_cloud_service_readiness_tasks(task_status, available_at, lease_until, updated_at)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_readiness_commands (
					command_id TEXT PRIMARY KEY NOT NULL, task_id TEXT NOT NULL, execution_id TEXT NOT NULL, deployment_id TEXT NOT NULL, service_id TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL, request_digest TEXT NOT NULL, command_attempt INTEGER NOT NULL CHECK (command_attempt > 0),
					action TEXT NOT NULL CHECK (action IN ('worker.service_readiness.issue','worker.service_readiness.observe')),
					node_key_id TEXT NOT NULL, expected_generation BIGINT NOT NULL CHECK (expected_generation > 0), node_counter BIGINT NOT NULL CHECK (node_counter > 0),
					canonical_payload_json TEXT NOT NULL DEFAULT '', payload_sha256 TEXT NOT NULL DEFAULT '', request_sha256 TEXT NOT NULL DEFAULT '', signed_envelope_json TEXT NOT NULL DEFAULT '',
					issued_at BIGINT NOT NULL DEFAULT 0, expires_at BIGINT NOT NULL DEFAULT 0,
					state TEXT NOT NULL CHECK (state IN ('allocated','signed','indeterminate','accepted','expired','failed')),
					last_error_code TEXT NOT NULL DEFAULT '', created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
					UNIQUE (cloud_connection_id, node_counter), UNIQUE (task_id, action, request_digest, command_attempt)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_readiness_commands_task_idx ON p2p_cloud_service_readiness_commands(task_id, action, command_attempt DESC)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud service destroy approval v51",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_destroy_approvals (
					approval_id TEXT PRIMARY KEY NOT NULL, challenge_id TEXT NOT NULL UNIQUE, owner_mxid TEXT NOT NULL,
					service_id TEXT NOT NULL, service_revision BIGINT NOT NULL CHECK (service_revision > 0),
					deployment_id TEXT NOT NULL, deployment_revision BIGINT NOT NULL CHECK (deployment_revision > 0),
					cloud_connection_id TEXT NOT NULL, recipe_id TEXT NOT NULL, recipe_digest TEXT NOT NULL,
					instance_id TEXT NOT NULL, volume_ids_json TEXT NOT NULL, network_interface_ids_json TEXT NOT NULL,
					signer_key_id TEXT NOT NULL, approval_json TEXT NOT NULL, signing_payload BYTEA NOT NULL,
					service_json TEXT NOT NULL, deployment_json TEXT NOT NULL,
					result_service_json TEXT NOT NULL DEFAULT '', result_deployment_json TEXT NOT NULL DEFAULT '', result_job_json TEXT NOT NULL DEFAULT '',
					status TEXT NOT NULL CHECK (status IN ('pending','approved','expired')),
					prepare_idempotency_hash TEXT NOT NULL, prepare_request_digest TEXT NOT NULL,
					approve_idempotency_hash TEXT, approve_request_digest TEXT, signature TEXT NOT NULL DEFAULT '', job_id TEXT NOT NULL DEFAULT '',
					expires_at BIGINT NOT NULL, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
					UNIQUE (owner_mxid, prepare_idempotency_hash)
				)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_destroy_approvals_approve_idempotency_idx
					ON p2p_cloud_service_destroy_approvals(owner_mxid, approve_idempotency_hash) WHERE approve_idempotency_hash IS NOT NULL`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_destroy_approvals_service_idx
					ON p2p_cloud_service_destroy_approvals(service_id, created_at DESC)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud service destroy commands v52",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_destroy_commands (
					command_id TEXT PRIMARY KEY NOT NULL, approval_id TEXT NOT NULL, service_id TEXT NOT NULL, deployment_id TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL, request_digest TEXT NOT NULL, command_attempt INTEGER NOT NULL CHECK (command_attempt > 0),
					action TEXT NOT NULL CHECK (action = 'deployment.destroy'), node_key_id TEXT NOT NULL,
					expected_generation BIGINT NOT NULL CHECK (expected_generation > 0), node_counter BIGINT NOT NULL CHECK (node_counter > 0),
					canonical_payload_json TEXT NOT NULL DEFAULT '', payload_sha256 TEXT NOT NULL DEFAULT '', request_sha256 TEXT NOT NULL DEFAULT '', signed_envelope_json TEXT NOT NULL DEFAULT '',
					issued_at BIGINT NOT NULL DEFAULT 0, expires_at BIGINT NOT NULL DEFAULT 0,
					state TEXT NOT NULL CHECK (state IN ('allocated','signed','indeterminate','accepted','failed')),
					receipt_json TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0), last_error_code TEXT NOT NULL DEFAULT '',
					created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
					UNIQUE (cloud_connection_id, node_counter), UNIQUE (approval_id, request_digest, command_attempt)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_destroy_commands_service_idx ON p2p_cloud_service_destroy_commands(service_id, command_attempt DESC)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud managed service operations v53",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_operation_approvals (
					approval_id TEXT PRIMARY KEY NOT NULL, challenge_id TEXT NOT NULL UNIQUE, owner_mxid TEXT NOT NULL,
					service_id TEXT NOT NULL, service_revision BIGINT NOT NULL CHECK (service_revision > 0), operation TEXT NOT NULL CHECK (operation IN ('start','stop','restart')),
					deployment_id TEXT NOT NULL, deployment_revision BIGINT NOT NULL CHECK (deployment_revision > 0), cloud_connection_id TEXT NOT NULL,
					recipe_id TEXT NOT NULL, recipe_digest TEXT NOT NULL, installed_manifest_digest TEXT NOT NULL, artifact_digest TEXT NOT NULL, action_id TEXT NOT NULL,
					approval_json TEXT NOT NULL, signing_payload BYTEA NOT NULL, signer_key_id TEXT NOT NULL, service_json TEXT NOT NULL, deployment_json TEXT NOT NULL,
					status TEXT NOT NULL CHECK (status IN ('pending','approved','expired')), prepare_idempotency_hash TEXT NOT NULL, prepare_request_digest TEXT NOT NULL,
					approve_idempotency_hash TEXT, approve_request_digest TEXT, signature TEXT, operation_id TEXT NOT NULL DEFAULT '', job_id TEXT NOT NULL DEFAULT '',
					result_service_json TEXT NOT NULL DEFAULT '', result_job_json TEXT NOT NULL DEFAULT '', expires_at BIGINT NOT NULL, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
					UNIQUE(owner_mxid,prepare_idempotency_hash)
				)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_operation_approvals_approve_idempotency_idx ON p2p_cloud_service_operation_approvals(owner_mxid,approve_idempotency_hash) WHERE approve_idempotency_hash IS NOT NULL`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_operation_approvals_service_idx ON p2p_cloud_service_operation_approvals(service_id,created_at DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_operation_tasks (
					operation_id TEXT PRIMARY KEY NOT NULL, approval_id TEXT NOT NULL UNIQUE, service_id TEXT NOT NULL, service_revision BIGINT NOT NULL CHECK (service_revision > 0),
					expected_service_status TEXT NOT NULL CHECK (expected_service_status IN ('experimental','active','stopped','degraded')),
					operation TEXT NOT NULL CHECK (operation IN ('start','stop','restart')), execution_id TEXT NOT NULL UNIQUE, deployment_id TEXT NOT NULL, plan_id TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL, instance_id TEXT NOT NULL, manifest_digest TEXT NOT NULL, input_digest TEXT NOT NULL, manifest_json TEXT NOT NULL,
					checkpoint_sequence_json TEXT NOT NULL, task_id TEXT NOT NULL UNIQUE, job_id TEXT NOT NULL UNIQUE,
					task_status TEXT NOT NULL CHECK (task_status IN ('queued','running','succeeded','failed','interrupted')),
					task_attempt BIGINT NOT NULL DEFAULT 1 CHECK (task_attempt > 0), last_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
					last_checkpoint TEXT NOT NULL DEFAULT '', error_code TEXT NOT NULL DEFAULT '', available_at BIGINT NOT NULL DEFAULT 0,
					lease_owner TEXT NOT NULL DEFAULT '', lease_token TEXT NOT NULL DEFAULT '', lease_until BIGINT NOT NULL DEFAULT 0,
					attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0), last_error_code TEXT NOT NULL DEFAULT '', created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_operation_tasks_claim_idx ON p2p_cloud_service_operation_tasks(task_status,available_at,lease_until,updated_at)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_operation_tasks_active_service_idx ON p2p_cloud_service_operation_tasks(service_id) WHERE task_status IN ('queued','running')`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud service backups v54",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_backup_approvals (
					approval_id TEXT PRIMARY KEY NOT NULL, challenge_id TEXT NOT NULL UNIQUE, owner_mxid TEXT NOT NULL,
					backup_id TEXT NOT NULL UNIQUE, service_id TEXT NOT NULL, service_revision BIGINT NOT NULL CHECK(service_revision>0),
					deployment_id TEXT NOT NULL, deployment_revision BIGINT NOT NULL CHECK(deployment_revision>0), cloud_connection_id TEXT NOT NULL,
					recipe_id TEXT NOT NULL, recipe_digest TEXT NOT NULL, instance_id TEXT NOT NULL, volume_ids_json TEXT NOT NULL, retention_policy TEXT NOT NULL CHECK(retention_policy='manual'),
					signer_key_id TEXT NOT NULL, approval_json TEXT NOT NULL, signing_payload BYTEA NOT NULL, service_json TEXT NOT NULL, deployment_json TEXT NOT NULL,
					status TEXT NOT NULL CHECK(status IN('pending','approved','expired')), prepare_idempotency_hash TEXT NOT NULL, prepare_request_digest TEXT NOT NULL,
					approve_idempotency_hash TEXT, approve_request_digest TEXT, signature TEXT NOT NULL DEFAULT '', job_id TEXT NOT NULL DEFAULT '',
					result_service_json TEXT NOT NULL DEFAULT '', result_backup_json TEXT NOT NULL DEFAULT '', result_job_json TEXT NOT NULL DEFAULT '',
					expires_at BIGINT NOT NULL, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL, UNIQUE(owner_mxid,prepare_idempotency_hash)
				)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_backup_approvals_approve_idempotency_idx ON p2p_cloud_service_backup_approvals(owner_mxid,approve_idempotency_hash) WHERE approve_idempotency_hash IS NOT NULL`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_backup_approvals_service_idx ON p2p_cloud_service_backup_approvals(service_id,created_at DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_backups (
					backup_id TEXT PRIMARY KEY NOT NULL, approval_id TEXT NOT NULL UNIQUE, service_id TEXT NOT NULL, service_revision BIGINT NOT NULL CHECK(service_revision>0),
					deployment_id TEXT NOT NULL, deployment_revision BIGINT NOT NULL CHECK(deployment_revision>0), plan_id TEXT NOT NULL, cloud_connection_id TEXT NOT NULL,
					instance_id TEXT NOT NULL, volume_ids_json TEXT NOT NULL, retention_policy TEXT NOT NULL CHECK(retention_policy='manual'), job_id TEXT NOT NULL UNIQUE,
					backup_status TEXT NOT NULL CHECK(backup_status IN('queued','running','available','failed')), image_id TEXT NOT NULL DEFAULT '', snapshots_json TEXT NOT NULL DEFAULT '[]', receipt_json TEXT NOT NULL DEFAULT '',
					revision BIGINT NOT NULL DEFAULT 1 CHECK(revision>0), last_error_code TEXT NOT NULL DEFAULT '', created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_backups_claim_idx ON p2p_cloud_service_backups(backup_status,updated_at)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_backups_active_service_idx ON p2p_cloud_service_backups(service_id) WHERE backup_status IN('queued','running')`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_backup_commands (
					command_id TEXT PRIMARY KEY NOT NULL, backup_id TEXT NOT NULL, approval_id TEXT NOT NULL, service_id TEXT NOT NULL, deployment_id TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL, request_digest TEXT NOT NULL, command_attempt INTEGER NOT NULL CHECK(command_attempt>0), action TEXT NOT NULL CHECK(action='service.backup'),
					node_key_id TEXT NOT NULL, expected_generation BIGINT NOT NULL CHECK(expected_generation>0), node_counter BIGINT NOT NULL CHECK(node_counter>0),
					canonical_payload_json TEXT NOT NULL DEFAULT '', payload_sha256 TEXT NOT NULL DEFAULT '', request_sha256 TEXT NOT NULL DEFAULT '', signed_envelope_json TEXT NOT NULL DEFAULT '',
					issued_at BIGINT NOT NULL DEFAULT 0, expires_at BIGINT NOT NULL DEFAULT 0, state TEXT NOT NULL CHECK(state IN('allocated','signed','indeterminate','accepted','failed')),
					receipt_json TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0), last_error_code TEXT NOT NULL DEFAULT '', created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
					UNIQUE(cloud_connection_id,node_counter), UNIQUE(backup_id,request_digest,command_attempt)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_backup_commands_backup_idx ON p2p_cloud_service_backup_commands(backup_id,command_attempt DESC)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud service restore plans v55",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_restore_plans (
					restore_plan_id TEXT PRIMARY KEY NOT NULL, owner_mxid TEXT NOT NULL, service_id TEXT NOT NULL, service_revision BIGINT NOT NULL CHECK(service_revision>0),
					deployment_id TEXT NOT NULL, deployment_revision BIGINT NOT NULL CHECK(deployment_revision>0), plan_id TEXT NOT NULL, cloud_connection_id TEXT NOT NULL,
					backup_id TEXT NOT NULL, backup_revision BIGINT NOT NULL CHECK(backup_revision>0), recipe_id TEXT NOT NULL, recipe_digest TEXT NOT NULL,
					instance_id TEXT NOT NULL, region TEXT NOT NULL, image_id TEXT NOT NULL, snapshot_refs_json TEXT NOT NULL,
					plan_status TEXT NOT NULL CHECK(plan_status IN('planning','ready_for_confirmation','failed','expired','approved')),
					availability_zone TEXT NOT NULL DEFAULT '', quote_id TEXT NOT NULL DEFAULT '', currency TEXT NOT NULL DEFAULT '', estimated_hourly_minor BIGINT NOT NULL DEFAULT 0,
					estimated_thirty_day_minor BIGINT NOT NULL DEFAULT 0, quoted_at BIGINT NOT NULL DEFAULT 0, valid_until BIGINT NOT NULL DEFAULT 0,
					unincluded_json TEXT NOT NULL DEFAULT '[]', volume_swaps_json TEXT NOT NULL DEFAULT '[]', broker_receipt_json TEXT NOT NULL DEFAULT '',
					job_id TEXT NOT NULL UNIQUE, revision BIGINT NOT NULL DEFAULT 1 CHECK(revision>0), last_error_code TEXT NOT NULL DEFAULT '',
					idempotency_hash TEXT NOT NULL, request_digest TEXT NOT NULL, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
					UNIQUE(owner_mxid,idempotency_hash)
				)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_restore_plans_active_service_idx ON p2p_cloud_service_restore_plans(service_id) WHERE plan_status IN('planning','ready_for_confirmation','approved')`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_restore_plans_backup_idx ON p2p_cloud_service_restore_plans(backup_id,created_at DESC)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_restore_plan_commands (
					command_id TEXT PRIMARY KEY NOT NULL, restore_plan_id TEXT NOT NULL, cloud_connection_id TEXT NOT NULL, request_digest TEXT NOT NULL,
					command_attempt INTEGER NOT NULL CHECK(command_attempt>0), action TEXT NOT NULL CHECK(action='service.restore.plan'), node_key_id TEXT NOT NULL,
					expected_generation BIGINT NOT NULL CHECK(expected_generation>0), node_counter BIGINT NOT NULL CHECK(node_counter>0), canonical_payload_json TEXT NOT NULL DEFAULT '',
					payload_sha256 TEXT NOT NULL DEFAULT '', request_sha256 TEXT NOT NULL DEFAULT '', signed_envelope_json TEXT NOT NULL DEFAULT '', issued_at BIGINT NOT NULL DEFAULT 0,
					expires_at BIGINT NOT NULL DEFAULT 0, state TEXT NOT NULL CHECK(state IN('allocated','signed','indeterminate','expired','accepted','failed')),
					receipt_json TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0), last_error_code TEXT NOT NULL DEFAULT '', created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
					UNIQUE(cloud_connection_id,node_counter), UNIQUE(restore_plan_id,request_digest,command_attempt)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_restore_plan_commands_plan_idx ON p2p_cloud_service_restore_plan_commands(restore_plan_id,command_attempt DESC)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud service restore approvals v56",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_restore_approvals (
					approval_id TEXT PRIMARY KEY NOT NULL, challenge_id TEXT NOT NULL UNIQUE, owner_mxid TEXT NOT NULL, restore_plan_id TEXT NOT NULL UNIQUE,
					restore_plan_revision BIGINT NOT NULL CHECK(restore_plan_revision>0), service_id TEXT NOT NULL, service_revision BIGINT NOT NULL CHECK(service_revision>0),
					deployment_id TEXT NOT NULL, deployment_revision BIGINT NOT NULL CHECK(deployment_revision>0), backup_id TEXT NOT NULL, backup_revision BIGINT NOT NULL CHECK(backup_revision>0),
					cloud_connection_id TEXT NOT NULL, signer_key_id TEXT NOT NULL, approval_json TEXT NOT NULL, signing_payload BYTEA NOT NULL,
					service_json TEXT NOT NULL, deployment_json TEXT NOT NULL, restore_plan_json TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN('pending','approved','expired')),
					prepare_idempotency_hash TEXT NOT NULL, prepare_request_digest TEXT NOT NULL, approve_idempotency_hash TEXT, approve_request_digest TEXT,
					signature TEXT NOT NULL DEFAULT '', job_id TEXT NOT NULL DEFAULT '', result_service_json TEXT NOT NULL DEFAULT '', result_restore_json TEXT NOT NULL DEFAULT '', result_job_json TEXT NOT NULL DEFAULT '',
					expires_at BIGINT NOT NULL, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL, UNIQUE(owner_mxid,prepare_idempotency_hash)
				)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_restore_approvals_approve_idempotency_idx ON p2p_cloud_service_restore_approvals(owner_mxid,approve_idempotency_hash) WHERE approve_idempotency_hash IS NOT NULL`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_restores (
					restore_id TEXT PRIMARY KEY NOT NULL, restore_plan_id TEXT NOT NULL UNIQUE, approval_id TEXT NOT NULL UNIQUE, service_id TEXT NOT NULL, service_revision BIGINT NOT NULL CHECK(service_revision>0),
					deployment_id TEXT NOT NULL, deployment_revision BIGINT NOT NULL CHECK(deployment_revision>0), backup_id TEXT NOT NULL, backup_revision BIGINT NOT NULL CHECK(backup_revision>0),
					plan_id TEXT NOT NULL, cloud_connection_id TEXT NOT NULL, instance_id TEXT NOT NULL, region TEXT NOT NULL, availability_zone TEXT NOT NULL,
					volume_swaps_json TEXT NOT NULL, original_volume_retention TEXT NOT NULL CHECK(original_volume_retention='manual'), failure_policy TEXT NOT NULL CHECK(failure_policy='reattach_original'),
					job_id TEXT NOT NULL UNIQUE, restore_status TEXT NOT NULL CHECK(restore_status IN('queued','running','verifying','succeeded','failed','restore_blocked')),
					revision BIGINT NOT NULL DEFAULT 1 CHECK(revision>0), last_error_code TEXT NOT NULL DEFAULT '', created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL
				)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_restores_active_service_idx ON p2p_cloud_service_restores(service_id) WHERE restore_status IN('queued','running','verifying','restore_blocked')`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud service restore commands v57",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_cloud_service_restores ADD COLUMN IF NOT EXISTS receipt_json TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_service_restores ADD COLUMN IF NOT EXISTS original_volume_ids_json TEXT NOT NULL DEFAULT '[]'`,
				`ALTER TABLE p2p_cloud_service_restores ADD COLUMN IF NOT EXISTS replacement_volume_ids_json TEXT NOT NULL DEFAULT '[]'`,
				`ALTER TABLE p2p_cloud_service_readiness_tasks DROP CONSTRAINT IF EXISTS p2p_cloud_service_readiness_tasks_service_id_key`,
				`ALTER TABLE p2p_cloud_service_readiness_tasks ADD COLUMN IF NOT EXISTS purpose TEXT NOT NULL DEFAULT 'install'`,
				`ALTER TABLE p2p_cloud_service_readiness_tasks ADD COLUMN IF NOT EXISTS restore_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_service_readiness_tasks ADD COLUMN IF NOT EXISTS job_id TEXT NOT NULL DEFAULT ''`,
				`UPDATE p2p_cloud_service_readiness_tasks task SET job_id=(SELECT job.job_id FROM p2p_cloud_jobs job WHERE job.deployment_id=task.deployment_id AND job.kind='install' ORDER BY job.created_at DESC,job.job_id LIMIT 1) WHERE task.job_id='' AND EXISTS(SELECT 1 FROM p2p_cloud_jobs job WHERE job.deployment_id=task.deployment_id AND job.kind='install')`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_readiness_tasks_install_service_idx ON p2p_cloud_service_readiness_tasks(service_id) WHERE purpose='install'`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_readiness_tasks_restore_idx ON p2p_cloud_service_readiness_tasks(restore_id) WHERE purpose='restore'`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_restore_commands (
					command_id TEXT PRIMARY KEY NOT NULL, restore_id TEXT NOT NULL, approval_id TEXT NOT NULL, service_id TEXT NOT NULL, deployment_id TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL, request_digest TEXT NOT NULL, command_attempt INTEGER NOT NULL CHECK(command_attempt>0), action TEXT NOT NULL CHECK(action='service.restore'),
					node_key_id TEXT NOT NULL, expected_generation BIGINT NOT NULL CHECK(expected_generation>0), node_counter BIGINT NOT NULL CHECK(node_counter>0),
					canonical_payload_json TEXT NOT NULL DEFAULT '', payload_sha256 TEXT NOT NULL DEFAULT '', request_sha256 TEXT NOT NULL DEFAULT '', signed_envelope_json TEXT NOT NULL DEFAULT '',
					issued_at BIGINT NOT NULL DEFAULT 0, expires_at BIGINT NOT NULL DEFAULT 0, state TEXT NOT NULL CHECK(state IN('allocated','signed','indeterminate','expired','accepted','failed')),
					receipt_json TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0), last_error_code TEXT NOT NULL DEFAULT '', created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
					UNIQUE(cloud_connection_id,node_counter), UNIQUE(restore_id,request_digest,command_attempt)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_restore_commands_restore_idx ON p2p_cloud_service_restore_commands(restore_id,command_attempt DESC)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: cloud service management acceptance v58",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_management_acceptances (
					acceptance_id TEXT PRIMARY KEY NOT NULL, approval_id TEXT NOT NULL UNIQUE, challenge_id TEXT NOT NULL, owner_mxid TEXT NOT NULL,
					service_id TEXT NOT NULL, service_revision BIGINT NOT NULL CHECK(service_revision>0), deployment_id TEXT NOT NULL, deployment_revision BIGINT NOT NULL CHECK(deployment_revision>0),
					recipe_id TEXT NOT NULL, recipe_revision BIGINT NOT NULL CHECK(recipe_revision>0), signer_key_id TEXT NOT NULL,
					target_json TEXT NOT NULL, approval_json TEXT NOT NULL, signing_payload BYTEA NOT NULL, service_json TEXT NOT NULL, recipe_json TEXT NOT NULL,
					status TEXT NOT NULL CHECK(status IN('pending','approved','expired')), prepare_idempotency_hash TEXT NOT NULL, prepare_request_digest TEXT NOT NULL,
					approve_idempotency_hash TEXT, approve_request_digest TEXT NOT NULL DEFAULT '', result_service_json TEXT NOT NULL DEFAULT '', result_recipe_json TEXT NOT NULL DEFAULT '', result_acceptance_json TEXT NOT NULL DEFAULT '',
					expires_at BIGINT NOT NULL, revision BIGINT NOT NULL DEFAULT 1 CHECK(revision>0), created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
					UNIQUE(owner_mxid,prepare_idempotency_hash)
				)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_management_acceptances_approve_idempotency_idx ON p2p_cloud_service_management_acceptances(owner_mxid,approve_idempotency_hash) WHERE approve_idempotency_hash IS NOT NULL`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_management_acceptances_pending_service_idx ON p2p_cloud_service_management_acceptances(service_id) WHERE status='pending'`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: legacy agent terminal delivery v59",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_legacy_agent_invocations ADD COLUMN IF NOT EXISTS terminal_kind TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_legacy_agent_invocations ADD COLUMN IF NOT EXISTS terminal_digest BYTEA`,
				`ALTER TABLE p2p_legacy_agent_invocations ADD COLUMN IF NOT EXISTS terminal_cursor TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_legacy_agent_invocations ADD COLUMN IF NOT EXISTS terminal_event_type TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_legacy_agent_invocations ADD COLUMN IF NOT EXISTS terminal_content_json BYTEA`,
				`ALTER TABLE p2p_legacy_agent_invocations ADD COLUMN IF NOT EXISTS matrix_transaction_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_legacy_agent_invocations ADD COLUMN IF NOT EXISTS matrix_terminal_event_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_legacy_agent_invocations ADD COLUMN IF NOT EXISTS terminal_phase TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_legacy_agent_invocations ADD CONSTRAINT p2p_legacy_agent_invocations_terminal_check CHECK ((terminal_phase='' AND terminal_kind='' AND terminal_digest IS NULL AND terminal_cursor='' AND terminal_event_type='' AND terminal_content_json IS NULL AND matrix_transaction_id='' AND matrix_terminal_event_id='') OR (state='accepted' AND terminal_phase IN ('send_intent','sent','committed','source_ack') AND terminal_kind IN ('result','error') AND octet_length(terminal_digest)=32 AND terminal_cursor<>'' AND terminal_event_type<>'' AND terminal_content_json IS NOT NULL AND matrix_transaction_id<>'' AND matrix_terminal_event_id<>''))`,
				`CREATE UNIQUE INDEX p2p_legacy_agent_invocations_terminal_cursor_idx ON p2p_legacy_agent_invocations(terminal_cursor) WHERE terminal_cursor<>''`,
				`CREATE UNIQUE INDEX p2p_legacy_agent_invocations_terminal_txn_idx ON p2p_legacy_agent_invocations(matrix_transaction_id) WHERE matrix_transaction_id<>''`,
				`CREATE UNIQUE INDEX p2p_legacy_agent_invocations_terminal_event_idx ON p2p_legacy_agent_invocations(matrix_terminal_event_id) WHERE matrix_terminal_event_id<>''`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: trusted compiled recipe artifacts v59",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_recipe_artifacts (
					artifact_digest TEXT PRIMARY KEY NOT NULL, descriptor_digest TEXT NOT NULL UNIQUE,
					recipe_id TEXT NOT NULL, recipe_revision BIGINT NOT NULL CHECK(recipe_revision>0), recipe_digest TEXT NOT NULL,
					worker_resource_manifest_digest TEXT NOT NULL, canonical_cbor BYTEA NOT NULL, descriptor_json TEXT NOT NULL,
					status TEXT NOT NULL CHECK(status='verified'), revision BIGINT NOT NULL DEFAULT 1 CHECK(revision>0),
					created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
					UNIQUE(recipe_id,recipe_revision)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_recipe_artifacts_recipe_digest_idx ON p2p_cloud_recipe_artifacts(recipe_digest,artifact_digest)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: service secret bootstrap approvals v60",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				// This is a non-secret confirmation journal. It deliberately stores
				// no Broker URL, session token, ciphertext, envelope, provider path,
				// provider version, or secret value.
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_secret_bootstrap_approvals (
					approval_id TEXT PRIMARY KEY NOT NULL, challenge_id TEXT NOT NULL UNIQUE, session_id TEXT NOT NULL UNIQUE,
					owner_mxid TEXT NOT NULL, deployment_id TEXT NOT NULL, deployment_revision BIGINT NOT NULL CHECK(deployment_revision>0),
					plan_id TEXT NOT NULL, plan_revision BIGINT NOT NULL CHECK(plan_revision>0), cloud_connection_id TEXT NOT NULL,
					task_id TEXT NOT NULL, execution_id TEXT NOT NULL, manifest_digest TEXT NOT NULL, recipe_digest TEXT NOT NULL, artifact_digest TEXT NOT NULL,
					slot_id TEXT NOT NULL, secret_ref TEXT NOT NULL, purpose TEXT NOT NULL, delivery TEXT NOT NULL CHECK(delivery IN('file','environment')),
					context_digest TEXT NOT NULL, signer_key_id TEXT NOT NULL, approval_json TEXT NOT NULL, signing_payload_cbor BYTEA NOT NULL,
					status TEXT NOT NULL CHECK(status IN('pending','expired')), prepare_idempotency_hash TEXT NOT NULL, prepare_request_digest TEXT NOT NULL,
					expires_at BIGINT NOT NULL, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
					UNIQUE(owner_mxid,prepare_idempotency_hash)
				)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_secret_bootstrap_pending_slot_idx ON p2p_cloud_service_secret_bootstrap_approvals(deployment_id,slot_id) WHERE status='pending'`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: selectable private recipe research v61",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_cloud_goals ADD COLUMN IF NOT EXISTS selected_recipe_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_goals ADD COLUMN IF NOT EXISTS selected_recipe_revision BIGINT NOT NULL DEFAULT 0 CHECK(selected_recipe_revision>=0)`,
				`ALTER TABLE p2p_cloud_goals ADD COLUMN IF NOT EXISTS selected_recipe_digest TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_goals ADD CONSTRAINT p2p_cloud_goals_selected_recipe_check CHECK ((selected_recipe_id='' AND selected_recipe_revision=0 AND selected_recipe_digest='') OR (selected_recipe_id<>'' AND selected_recipe_revision>0 AND selected_recipe_digest<>''))`,
				`ALTER TABLE p2p_cloud_plans ADD COLUMN IF NOT EXISTS recipe_id TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_plans ADD COLUMN IF NOT EXISTS recipe_revision BIGINT NOT NULL DEFAULT 0 CHECK(recipe_revision>=0)`,
				`ALTER TABLE p2p_cloud_plans ADD CONSTRAINT p2p_cloud_plans_selected_recipe_check CHECK ((recipe_id='' AND recipe_revision=0) OR (recipe_id<>'' AND recipe_revision>0))`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: service secret observer journal v62",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`DROP INDEX IF EXISTS p2p_cloud_service_secret_bootstrap_pending_slot_idx`,
				`ALTER TABLE p2p_cloud_service_secret_bootstrap_approvals DROP CONSTRAINT IF EXISTS p2p_cloud_service_secret_bootstrap_approvals_status_check`,
				`ALTER TABLE p2p_cloud_service_secret_bootstrap_approvals ADD CONSTRAINT p2p_cloud_service_secret_bootstrap_approvals_status_check CHECK(status IN('pending','observing','ready','expired','failed'))`,
				`ALTER TABLE p2p_cloud_service_secret_bootstrap_approvals ADD COLUMN IF NOT EXISTS updated_marker TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_service_secret_bootstrap_approvals ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1 CHECK(revision>0)`,
				`ALTER TABLE p2p_cloud_service_secret_bootstrap_approvals ADD COLUMN IF NOT EXISTS available_at BIGINT NOT NULL DEFAULT 0`,
				`ALTER TABLE p2p_cloud_service_secret_bootstrap_approvals ADD COLUMN IF NOT EXISTS lease_owner TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_service_secret_bootstrap_approvals ADD COLUMN IF NOT EXISTS lease_token TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_service_secret_bootstrap_approvals ADD COLUMN IF NOT EXISTS lease_until BIGINT NOT NULL DEFAULT 0`,
				`ALTER TABLE p2p_cloud_service_secret_bootstrap_approvals ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0)`,
				`ALTER TABLE p2p_cloud_service_secret_bootstrap_approvals ADD COLUMN IF NOT EXISTS last_error_code TEXT NOT NULL DEFAULT ''`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_secret_bootstrap_active_slot_idx ON p2p_cloud_service_secret_bootstrap_approvals(deployment_id,slot_id) WHERE status IN('pending','observing','ready')`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_secret_bootstrap_updated_marker_idx ON p2p_cloud_service_secret_bootstrap_approvals(updated_marker) WHERE updated_marker<>''`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_secret_bootstrap_observe_claim_idx ON p2p_cloud_service_secret_bootstrap_approvals(status,available_at,lease_until,expires_at)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_secret_observe_commands(
					command_id TEXT PRIMARY KEY NOT NULL,approval_id TEXT NOT NULL,session_id TEXT NOT NULL,deployment_id TEXT NOT NULL,task_id TEXT NOT NULL,execution_id TEXT NOT NULL,
					cloud_connection_id TEXT NOT NULL,manifest_digest TEXT NOT NULL,secret_ref TEXT NOT NULL,context_digest TEXT NOT NULL,request_digest TEXT NOT NULL,
					command_attempt INTEGER NOT NULL CHECK(command_attempt>0),action TEXT NOT NULL CHECK(action='service.secret.observe'),node_key_id TEXT NOT NULL,
					expected_generation BIGINT NOT NULL CHECK(expected_generation>0),node_counter BIGINT NOT NULL CHECK(node_counter>0),canonical_payload_json TEXT NOT NULL DEFAULT '',
					payload_sha256 TEXT NOT NULL DEFAULT '',request_sha256 TEXT NOT NULL DEFAULT '',signed_envelope_json TEXT NOT NULL DEFAULT '',issued_at BIGINT NOT NULL DEFAULT 0,expires_at BIGINT NOT NULL DEFAULT 0,
					state TEXT NOT NULL CHECK(state IN('allocated','signed','indeterminate','accepted','expired','failed')),last_error_code TEXT NOT NULL DEFAULT '',created_at BIGINT NOT NULL,updated_at BIGINT NOT NULL,
					UNIQUE(approval_id,request_digest,command_attempt),UNIQUE(cloud_connection_id,node_counter)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_secret_observe_commands_approval_idx ON p2p_cloud_service_secret_observe_commands(approval_id,command_attempt DESC)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: service destroy secret references v63",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_cloud_service_secret_bootstrap_approvals DROP COLUMN IF EXISTS provider_version`,
				`ALTER TABLE p2p_cloud_service_secret_bootstrap_approvals DROP CONSTRAINT IF EXISTS p2p_cloud_service_secret_bootstrap_approvals_status_check`,
				`ALTER TABLE p2p_cloud_service_secret_bootstrap_approvals ADD CONSTRAINT p2p_cloud_service_secret_bootstrap_approvals_status_check CHECK(status IN('pending','observing','ready','expired','failed','destroyed'))`,
				`ALTER TABLE p2p_cloud_service_destroy_approvals ADD COLUMN IF NOT EXISTS secret_refs_json TEXT NOT NULL DEFAULT '[]'`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: typed OCI semantic readiness v64",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_cloud_service_readiness_tasks ADD COLUMN IF NOT EXISTS artifact_digest TEXT NOT NULL DEFAULT ''`,
				`ALTER TABLE p2p_cloud_service_readiness_tasks ADD COLUMN IF NOT EXISTS semantic_probe_json TEXT NOT NULL DEFAULT ''`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: durable recipe artifact transfer v65",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_recipe_artifact_transfers(
					execution_id TEXT PRIMARY KEY NOT NULL,deployment_id TEXT NOT NULL,task_id TEXT NOT NULL,cloud_connection_id TEXT NOT NULL,
					recipe_digest TEXT NOT NULL,artifact_digest TEXT NOT NULL,manifest_digest TEXT NOT NULL,archive_sha256 TEXT NOT NULL,
					size_bytes BIGINT NOT NULL CHECK(size_bytes>0),media_type TEXT NOT NULL CHECK(media_type='application/x-tar'),version_id TEXT NOT NULL DEFAULT '',
					state TEXT NOT NULL CHECK(state IN('pending','uploaded','verified','failed')),last_error_code TEXT NOT NULL DEFAULT '',created_at BIGINT NOT NULL,updated_at BIGINT NOT NULL,
					UNIQUE(deployment_id,task_id),UNIQUE(cloud_connection_id,execution_id)
				)`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_recipe_artifact_commands(
					command_id TEXT PRIMARY KEY NOT NULL,execution_id TEXT NOT NULL,deployment_id TEXT NOT NULL,task_id TEXT NOT NULL,cloud_connection_id TEXT NOT NULL,
					phase TEXT NOT NULL CHECK(phase IN('prepare','complete')),request_digest TEXT NOT NULL,action TEXT NOT NULL CHECK(action='artifact.put'),
					node_key_id TEXT NOT NULL,expected_generation BIGINT NOT NULL CHECK(expected_generation>0),node_counter BIGINT NOT NULL CHECK(node_counter>0),
					canonical_payload_json TEXT NOT NULL DEFAULT '',payload_sha256 TEXT NOT NULL DEFAULT '',request_sha256 TEXT NOT NULL DEFAULT '',signed_envelope_json TEXT NOT NULL DEFAULT '',
					issued_at BIGINT NOT NULL DEFAULT 0,expires_at BIGINT NOT NULL DEFAULT 0,state TEXT NOT NULL CHECK(state IN('allocated','signed','indeterminate','accepted','expired','failed')),
					last_error_code TEXT NOT NULL DEFAULT '',created_at BIGINT NOT NULL,updated_at BIGINT NOT NULL,
					UNIQUE(execution_id,phase),UNIQUE(cloud_connection_id,node_counter)
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_recipe_artifact_commands_execution_idx ON p2p_cloud_recipe_artifact_commands(execution_id,phase)`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: deployment pairing resume approval v66",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_pairing_resume_approvals(
					approval_id TEXT PRIMARY KEY NOT NULL,challenge_id TEXT NOT NULL UNIQUE,owner_mxid TEXT NOT NULL,deployment_id TEXT NOT NULL,
					deployment_revision BIGINT NOT NULL CHECK(deployment_revision>0),plan_id TEXT NOT NULL,cloud_connection_id TEXT NOT NULL,
					execution_id TEXT NOT NULL,manifest_digest TEXT NOT NULL,job_id TEXT NOT NULL,job_revision BIGINT NOT NULL CHECK(job_revision>0),
					signer_key_id TEXT NOT NULL,approval_json TEXT NOT NULL,signing_payload_cbor BYTEA NOT NULL,prepare_deployment_json TEXT NOT NULL,prepare_job_json TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN('pending','approved','expired')),
					prepare_idempotency_hash TEXT NOT NULL,prepare_request_digest TEXT NOT NULL,approve_idempotency_hash TEXT,approve_request_digest TEXT,
					signature TEXT NOT NULL DEFAULT '',result_deployment_json TEXT NOT NULL DEFAULT '',result_job_json TEXT NOT NULL DEFAULT '',expires_at BIGINT NOT NULL,created_at BIGINT NOT NULL,updated_at BIGINT NOT NULL,
					UNIQUE(owner_mxid,prepare_idempotency_hash)
				)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_pairing_resume_approve_idempotency_idx ON p2p_cloud_pairing_resume_approvals(owner_mxid,approve_idempotency_hash) WHERE approve_idempotency_hash IS NOT NULL`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_pairing_resume_active_deployment_idx ON p2p_cloud_pairing_resume_approvals(deployment_id) WHERE status='pending'`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: continuous service monitor v67",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_service_monitors(
					service_id TEXT PRIMARY KEY NOT NULL,deployment_id TEXT NOT NULL,
					monitor_status TEXT NOT NULL CHECK(monitor_status IN('idle','checking')),
					generation BIGINT NOT NULL DEFAULT 0 CHECK(generation>=0),current_task_id TEXT NOT NULL DEFAULT '',
					healthy_service_status TEXT NOT NULL DEFAULT '' CHECK(healthy_service_status IN('','active','experimental')),
					degraded_by_monitor BOOLEAN NOT NULL DEFAULT FALSE,consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK(consecutive_failures>=0),
					next_check_at BIGINT NOT NULL DEFAULT 0,last_success_at BIGINT NOT NULL DEFAULT 0,last_failure_at BIGINT NOT NULL DEFAULT 0,
					lease_owner TEXT NOT NULL DEFAULT '',lease_token TEXT NOT NULL DEFAULT '',lease_until BIGINT NOT NULL DEFAULT 0,
					attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),last_error_code TEXT NOT NULL DEFAULT '',created_at BIGINT NOT NULL,updated_at BIGINT NOT NULL
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_cloud_service_monitors_claim_idx ON p2p_cloud_service_monitors(monitor_status,next_check_at,lease_until,updated_at)`,
				`ALTER TABLE p2p_cloud_service_readiness_tasks ADD COLUMN IF NOT EXISTS monitor_generation BIGINT NOT NULL DEFAULT 0 CHECK(monitor_generation>=0)`,
				`ALTER TABLE p2p_cloud_service_readiness_tasks ADD COLUMN IF NOT EXISTS monitor_service_revision BIGINT NOT NULL DEFAULT 0 CHECK(monitor_service_revision>=0)`,
				`ALTER TABLE p2p_cloud_service_readiness_tasks ADD COLUMN IF NOT EXISTS monitor_deployment_revision BIGINT NOT NULL DEFAULT 0 CHECK(monitor_deployment_revision>=0)`,
				`ALTER TABLE p2p_cloud_service_readiness_tasks ADD COLUMN IF NOT EXISTS monitor_resource_status TEXT NOT NULL DEFAULT '' CHECK(monitor_resource_status IN('','active','retained_tracked'))`,
				`ALTER TABLE p2p_cloud_service_readiness_tasks ADD COLUMN IF NOT EXISTS worker_lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK(worker_lease_epoch>=0)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_service_readiness_tasks_monitor_generation_idx ON p2p_cloud_service_readiness_tasks(service_id,monitor_generation) WHERE purpose='monitor'`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: device approved cloud job cancellation v68",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`ALTER TABLE p2p_cloud_job_steps DROP CONSTRAINT IF EXISTS p2p_cloud_job_steps_status_check`,
				`ALTER TABLE p2p_cloud_job_steps ADD CONSTRAINT p2p_cloud_job_steps_status_check CHECK(status IN('queued','running','waiting_user','finished','failed','canceled'))`,
				`CREATE TABLE IF NOT EXISTS p2p_cloud_job_cancel_approvals(
					approval_id TEXT PRIMARY KEY NOT NULL,challenge_id TEXT NOT NULL UNIQUE,owner_mxid TEXT NOT NULL,
					job_id TEXT NOT NULL,job_revision BIGINT NOT NULL CHECK(job_revision>0),job_kind TEXT NOT NULL CHECK(job_kind IN('provision','install','verify')),
					plan_id TEXT NOT NULL,deployment_id TEXT NOT NULL,deployment_revision BIGINT NOT NULL CHECK(deployment_revision>0),
					cloud_connection_id TEXT NOT NULL,resource_status TEXT NOT NULL CHECK(resource_status IN('none','active','retained_tracked')),
					signer_key_id TEXT NOT NULL,approval_json TEXT NOT NULL,signing_payload_cbor BYTEA NOT NULL,
					prepare_job_json TEXT NOT NULL,prepare_deployment_json TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN('pending','approved','expired')),
					prepare_idempotency_hash TEXT NOT NULL,prepare_request_digest TEXT NOT NULL,approve_idempotency_hash TEXT,approve_request_digest TEXT,
					signature TEXT NOT NULL DEFAULT '',result_job_json TEXT NOT NULL DEFAULT '',result_deployment_json TEXT NOT NULL DEFAULT '',
					expires_at BIGINT NOT NULL,created_at BIGINT NOT NULL,updated_at BIGINT NOT NULL,
					UNIQUE(owner_mxid,prepare_idempotency_hash)
				)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_job_cancel_approve_idempotency_idx ON p2p_cloud_job_cancel_approvals(owner_mxid,approve_idempotency_hash) WHERE approve_idempotency_hash IS NOT NULL`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_job_cancel_active_job_idx ON p2p_cloud_job_cancel_approvals(job_id) WHERE status='pending'`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: service independent deployment destruction v69",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_cloud_deployment_destroy_approvals(
					approval_id TEXT PRIMARY KEY NOT NULL,challenge_id TEXT NOT NULL UNIQUE,owner_mxid TEXT NOT NULL,
					deployment_id TEXT NOT NULL,deployment_revision BIGINT NOT NULL CHECK(deployment_revision>0),plan_id TEXT NOT NULL,cloud_connection_id TEXT NOT NULL,
					resource_status TEXT NOT NULL CHECK(resource_status IN('active','retained_tracked','blocked','orphaned')),instance_id TEXT NOT NULL,
					volume_ids_json TEXT NOT NULL,network_interface_ids_json TEXT NOT NULL,secret_refs_json TEXT NOT NULL DEFAULT '[]',
					signer_key_id TEXT NOT NULL,approval_json TEXT NOT NULL,signing_payload_cbor BYTEA NOT NULL,prepare_deployment_json TEXT NOT NULL,
					status TEXT NOT NULL CHECK(status IN('pending','approved','expired')),prepare_idempotency_hash TEXT NOT NULL,prepare_request_digest TEXT NOT NULL,
					approve_idempotency_hash TEXT,approve_request_digest TEXT,signature TEXT NOT NULL DEFAULT '',job_id TEXT NOT NULL DEFAULT '',
					result_deployment_json TEXT NOT NULL DEFAULT '',result_job_json TEXT NOT NULL DEFAULT '',expires_at BIGINT NOT NULL,created_at BIGINT NOT NULL,updated_at BIGINT NOT NULL,
					UNIQUE(owner_mxid,prepare_idempotency_hash)
				)`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_deployment_destroy_approve_idempotency_idx ON p2p_cloud_deployment_destroy_approvals(owner_mxid,approve_idempotency_hash) WHERE approve_idempotency_hash IS NOT NULL`,
				`CREATE UNIQUE INDEX IF NOT EXISTS p2p_cloud_deployment_destroy_active_idx ON p2p_cloud_deployment_destroy_approvals(deployment_id) WHERE status='pending'`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: root credential bootstrap permit v70",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				// This is an owner-approved, non-secret role-plan capability. It
				// never stores an ARN, access key, secret, session token, or receipt.
				`ALTER TABLE p2p_cloud_connection_bootstraps ADD COLUMN IF NOT EXISTS allow_root_credential_bootstrap BOOLEAN NOT NULL DEFAULT FALSE`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: immutable connection template reference v71",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				// A raw URL cannot survive a restart as the execution authority.
				// Existing pre-v71 rows deliberately receive an empty value and fail
				// closed when read; new role plans persist the complete closed union.
				`ALTER TABLE p2p_cloud_connection_bootstraps ADD COLUMN IF NOT EXISTS connection_template_json TEXT NOT NULL DEFAULT ''`,
			})
		},
	})
	m.AddMigrations(sqlutil.Migration{
		Version: "p2p: independent Agent event projection cursor v72",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			return execMigrationStatements(ctx, txn, []string{
				`CREATE TABLE IF NOT EXISTS p2p_agent_event_cursors(
					agent_instance_id UUID NOT NULL,caller_id TEXT NOT NULL CHECK(caller_id<>''),
					after_seq BIGINT NOT NULL DEFAULT 0 CHECK(after_seq>=0),updated_at BIGINT NOT NULL,
					PRIMARY KEY(agent_instance_id,caller_id)
				)`,
				`CREATE TABLE IF NOT EXISTS p2p_agent_projection_revisions(
					agent_instance_id UUID NOT NULL,caller_id TEXT NOT NULL CHECK(caller_id<>''),event_type TEXT NOT NULL,
					aggregate_type TEXT NOT NULL,aggregate_id UUID NOT NULL,revision BIGINT NOT NULL CHECK(revision>0),
					source_event_seq BIGINT NOT NULL CHECK(source_event_seq>0),projected_event_seq BIGINT NOT NULL CHECK(projected_event_seq>0),updated_at BIGINT NOT NULL,
					PRIMARY KEY(agent_instance_id,caller_id,event_type,aggregate_id),
					FOREIGN KEY(agent_instance_id,caller_id) REFERENCES p2p_agent_event_cursors(agent_instance_id,caller_id) ON DELETE CASCADE
				)`,
				`CREATE INDEX IF NOT EXISTS p2p_agent_projection_revisions_aggregate_idx ON p2p_agent_projection_revisions(agent_instance_id,caller_id,aggregate_type,aggregate_id)`,
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
