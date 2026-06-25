package storage

import (
	"context"
	"database/sql"
)

func (s *DatabaseStore) LoadPortal(ctx context.Context) (portalState, bool, error) {
	var state portalState
	var initialized int64
	err := s.db.QueryRowContext(ctx, `
		SELECT initialized, password, access_token, matrix_device_id, agent_token,
			owner_mxid, agent_room_id, user_id, display_name, domain,
			avatar_url, gender, birthday, phone, email
		FROM p2p_portal WHERE id = $1
	`, "owner").Scan(
		&initialized, &state.Password, &state.AccessToken, &state.MatrixDeviceID, &state.AgentToken,
		&state.OwnerMXID, &state.AgentRoomID, &state.Profile.UserID, &state.Profile.DisplayName, &state.Profile.Domain,
		&state.Profile.AvatarURL, &state.Profile.Gender, &state.Profile.Birthday, &state.Profile.Phone, &state.Profile.Email,
	)
	if err == sql.ErrNoRows {
		return portalState{}, false, nil
	}
	if err != nil {
		return portalState{}, false, err
	}
	state.Initialized = initialized == 1
	return state, true, nil
}

func (s *DatabaseStore) SavePortal(ctx context.Context, state portalState) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_portal (
				id, initialized, password, access_token, matrix_device_id, agent_token,
				owner_mxid, agent_room_id, user_id, display_name, domain,
				avatar_url, gender, birthday, phone, email
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
			ON CONFLICT(id) DO UPDATE SET
				initialized = EXCLUDED.initialized,
				password = EXCLUDED.password,
				access_token = EXCLUDED.access_token,
				matrix_device_id = EXCLUDED.matrix_device_id,
				agent_token = EXCLUDED.agent_token,
				owner_mxid = EXCLUDED.owner_mxid,
				agent_room_id = EXCLUDED.agent_room_id,
				user_id = EXCLUDED.user_id,
				display_name = EXCLUDED.display_name,
				domain = EXCLUDED.domain,
				avatar_url = EXCLUDED.avatar_url,
				gender = EXCLUDED.gender,
				birthday = EXCLUDED.birthday,
				phone = EXCLUDED.phone,
				email = EXCLUDED.email
		`, "owner", boolInt(state.Initialized), state.Password, state.AccessToken, state.MatrixDeviceID, state.AgentToken,
			state.OwnerMXID, state.AgentRoomID, state.Profile.UserID, state.Profile.DisplayName, state.Profile.Domain,
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
