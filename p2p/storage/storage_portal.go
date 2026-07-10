package storage

import (
	"context"
	"database/sql"
	"encoding/json"
)

func (s *DatabaseStore) LoadPortal(ctx context.Context) (portalState, bool, error) {
	var state portalState
	var initialized int64
	var agentConfigJSON string
	err := s.db.QueryRowContext(ctx, `
			SELECT initialized, password, access_token, matrix_device_id, agent_token,
				owner_mxid, agent_room_id, system_room_id, user_id, display_name, domain,
				avatar_url, gender, birthday, phone, email, agent_config_json,
				client_version, client_build_number, client_platform, client_version_reported_at
			FROM p2p_portal WHERE id = $1
		`, "owner").Scan(
		&initialized, &state.Password, &state.AccessToken, &state.MatrixDeviceID, &state.AgentToken,
		&state.OwnerMXID, &state.AgentRoomID, &state.SystemRoomID, &state.Profile.UserID, &state.Profile.DisplayName, &state.Profile.Domain,
		&state.Profile.AvatarURL, &state.Profile.Gender, &state.Profile.Birthday, &state.Profile.Phone, &state.Profile.Email,
		&agentConfigJSON,
		&state.ClientBuild.Version, &state.ClientBuild.BuildNumber, &state.ClientBuild.Platform, &state.ClientBuild.ReportedAt,
	)
	if err == sql.ErrNoRows {
		return portalState{}, false, nil
	}
	if err != nil {
		return portalState{}, false, err
	}
	state.Initialized = initialized == 1
	if agentConfigJSON != "" {
		if err := json.Unmarshal([]byte(agentConfigJSON), &state.AgentConfig); err != nil {
			return portalState{}, false, err
		}
	}
	return state, true, nil
}

func (s *DatabaseStore) SavePortal(ctx context.Context, state portalState) error {
	agentConfigJSON, err := json.Marshal(state.AgentConfig)
	if err != nil {
		return err
	}
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
				INSERT INTO p2p_portal (
					id, initialized, password, access_token, matrix_device_id, agent_token,
					owner_mxid, agent_room_id, system_room_id, user_id, display_name, domain,
					avatar_url, gender, birthday, phone, email, agent_config_json,
					client_version, client_build_number, client_platform, client_version_reported_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
				ON CONFLICT(id) DO UPDATE SET
					initialized = EXCLUDED.initialized,
					password = EXCLUDED.password,
					access_token = EXCLUDED.access_token,
					matrix_device_id = EXCLUDED.matrix_device_id,
					agent_token = EXCLUDED.agent_token,
					owner_mxid = EXCLUDED.owner_mxid,
					agent_room_id = EXCLUDED.agent_room_id,
					system_room_id = EXCLUDED.system_room_id,
					user_id = EXCLUDED.user_id,
					display_name = EXCLUDED.display_name,
					domain = EXCLUDED.domain,
				avatar_url = EXCLUDED.avatar_url,
				gender = EXCLUDED.gender,
				birthday = EXCLUDED.birthday,
				phone = EXCLUDED.phone,
					email = EXCLUDED.email,
					agent_config_json = EXCLUDED.agent_config_json,
					client_version = EXCLUDED.client_version,
					client_build_number = EXCLUDED.client_build_number,
					client_platform = EXCLUDED.client_platform,
					client_version_reported_at = EXCLUDED.client_version_reported_at
			`, "owner", boolInt(state.Initialized), state.Password, state.AccessToken, state.MatrixDeviceID, state.AgentToken,
			state.OwnerMXID, state.AgentRoomID, state.SystemRoomID, state.Profile.UserID, state.Profile.DisplayName, state.Profile.Domain,
			state.Profile.AvatarURL, state.Profile.Gender, state.Profile.Birthday, state.Profile.Phone, state.Profile.Email,
			string(agentConfigJSON), state.ClientBuild.Version, state.ClientBuild.BuildNumber, state.ClientBuild.Platform, state.ClientBuild.ReportedAt)
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
