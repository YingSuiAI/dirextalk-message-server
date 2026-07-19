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
					client_version = CASE WHEN p2p_portal.matrix_device_id IS DISTINCT FROM EXCLUDED.matrix_device_id THEN EXCLUDED.client_version ELSE p2p_portal.client_version END,
					client_build_number = CASE WHEN p2p_portal.matrix_device_id IS DISTINCT FROM EXCLUDED.matrix_device_id THEN EXCLUDED.client_build_number ELSE p2p_portal.client_build_number END,
					client_platform = CASE WHEN p2p_portal.matrix_device_id IS DISTINCT FROM EXCLUDED.matrix_device_id THEN EXCLUDED.client_platform ELSE p2p_portal.client_platform END,
					client_version_reported_at = CASE WHEN p2p_portal.matrix_device_id IS DISTINCT FROM EXCLUDED.matrix_device_id THEN EXCLUDED.client_version_reported_at ELSE p2p_portal.client_version_reported_at END
			`, "owner", boolInt(state.Initialized), state.Password, state.AccessToken, state.MatrixDeviceID, state.AgentToken,
			state.OwnerMXID, state.AgentRoomID, state.SystemRoomID, state.Profile.UserID, state.Profile.DisplayName, state.Profile.Domain,
			state.Profile.AvatarURL, state.Profile.Gender, state.Profile.Birthday, state.Profile.Phone, state.Profile.Email,
			string(agentConfigJSON), state.ClientBuild.Version, state.ClientBuild.BuildNumber, state.ClientBuild.Platform, state.ClientBuild.ReportedAt)
		return err
	})
}

func (s *DatabaseStore) SaveClientBuild(ctx context.Context, expectedDeviceID string, build clientBuild) (bool, error) {
	updated := false
	err := s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		result, err := s.db.ExecContext(ctx, `
			UPDATE p2p_portal SET
				client_version = $1,
				client_build_number = $2,
				client_platform = $3,
				client_version_reported_at = $4
			WHERE id = $5 AND matrix_device_id = $6
		`, build.Version, build.BuildNumber, build.Platform, build.ReportedAt, "owner", expectedDeviceID)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		updated = rows == 1
		return nil
	})
	return updated, err
}

func (s *DatabaseStore) SaveReadMarker(ctx context.Context, marker readMarker) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_read_markers (
				room_id, event_id, origin_server_ts, topological_position, stream_position
			)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT(room_id) DO UPDATE SET
				event_id = EXCLUDED.event_id,
				origin_server_ts = EXCLUDED.origin_server_ts,
				topological_position = EXCLUDED.topological_position,
				stream_position = EXCLUDED.stream_position
			WHERE (p2p_read_markers.topological_position, p2p_read_markers.stream_position) <
				(EXCLUDED.topological_position, EXCLUDED.stream_position)
		`, marker.RoomID, marker.EventID, marker.OriginServerTS, marker.TopologicalPosition, marker.StreamPosition)
		return err
	})
}

func (s *DatabaseStore) GetReadMarker(ctx context.Context, roomID string) (readMarker, bool, error) {
	var marker readMarker
	err := s.db.QueryRowContext(ctx, `
		SELECT room_id, event_id, origin_server_ts, topological_position, stream_position
		FROM p2p_read_markers
		WHERE room_id = $1
	`, roomID).Scan(
		&marker.RoomID, &marker.EventID, &marker.OriginServerTS,
		&marker.TopologicalPosition, &marker.StreamPosition,
	)
	if err == sql.ErrNoRows {
		return readMarker{}, false, nil
	}
	if err != nil {
		return readMarker{}, false, err
	}
	return marker, true, nil
}

func (s *DatabaseStore) ListReadMarkers(ctx context.Context) ([]readMarker, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT room_id, event_id, origin_server_ts, topological_position, stream_position
		FROM p2p_read_markers
		ORDER BY room_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	markers := make([]readMarker, 0)
	for rows.Next() {
		var marker readMarker
		if err := rows.Scan(
			&marker.RoomID, &marker.EventID, &marker.OriginServerTS,
			&marker.TopologicalPosition, &marker.StreamPosition,
		); err != nil {
			return nil, err
		}
		markers = append(markers, marker)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return markers, nil
}
