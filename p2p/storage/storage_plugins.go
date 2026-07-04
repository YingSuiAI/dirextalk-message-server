package storage

import (
	"context"
	"database/sql"
	"encoding/json"
)

func (s *DatabaseStore) UpsertPlugin(ctx context.Context, plugin pluginInstance) error {
	configJSON, err := json.Marshal(plugin.Config)
	if err != nil {
		return err
	}
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_plugins (
				id, name, version, image, digest, status, enabled, config_json,
				last_job_id, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT(id) DO UPDATE SET
				name = EXCLUDED.name,
				version = EXCLUDED.version,
				image = EXCLUDED.image,
				digest = EXCLUDED.digest,
				status = EXCLUDED.status,
				enabled = EXCLUDED.enabled,
				config_json = EXCLUDED.config_json,
				last_job_id = EXCLUDED.last_job_id,
				created_at = CASE
					WHEN p2p_plugins.created_at > 0 THEN p2p_plugins.created_at
					ELSE EXCLUDED.created_at
				END,
				updated_at = EXCLUDED.updated_at
		`, plugin.ID, plugin.Name, plugin.Version, plugin.Image, plugin.Digest, plugin.Status, boolInt(plugin.Enabled),
			string(configJSON), plugin.LastJobID, plugin.CreatedAt, plugin.UpdatedAt)
		return err
	})
}

func (s *DatabaseStore) ListPlugins(ctx context.Context) ([]pluginInstance, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, version, image, digest, status, enabled, config_json,
			last_job_id, created_at, updated_at
		FROM p2p_plugins
		ORDER BY name ASC, id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	plugins := make([]pluginInstance, 0)
	for rows.Next() {
		plugin, err := scanPlugin(rows)
		if err != nil {
			return nil, err
		}
		plugins = append(plugins, plugin)
	}
	return plugins, rows.Err()
}

func (s *DatabaseStore) GetPlugin(ctx context.Context, id string) (pluginInstance, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, name, version, image, digest, status, enabled, config_json,
			last_job_id, created_at, updated_at
		FROM p2p_plugins
		WHERE id = $1
	`, id)
	plugin, err := scanPlugin(row)
	if err == sql.ErrNoRows {
		return pluginInstance{}, false, nil
	}
	if err != nil {
		return pluginInstance{}, false, err
	}
	return plugin, true, nil
}

type pluginScanner interface {
	Scan(dest ...any) error
}

func scanPlugin(scanner pluginScanner) (pluginInstance, error) {
	var plugin pluginInstance
	var enabled int64
	var configJSON string
	if err := scanner.Scan(
		&plugin.ID,
		&plugin.Name,
		&plugin.Version,
		&plugin.Image,
		&plugin.Digest,
		&plugin.Status,
		&enabled,
		&configJSON,
		&plugin.LastJobID,
		&plugin.CreatedAt,
		&plugin.UpdatedAt,
	); err != nil {
		return pluginInstance{}, err
	}
	plugin.Enabled = enabled == 1
	if configJSON != "" {
		if err := json.Unmarshal([]byte(configJSON), &plugin.Config); err != nil {
			return pluginInstance{}, err
		}
	}
	if plugin.Config == nil {
		plugin.Config = map[string]any{}
	}
	return plugin, nil
}

func (s *DatabaseStore) UpsertPluginJob(ctx context.Context, job pluginJob) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_plugin_jobs (
				job_id, plugin_id, action, status, message, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT(job_id) DO UPDATE SET
				plugin_id = EXCLUDED.plugin_id,
				action = EXCLUDED.action,
				status = EXCLUDED.status,
				message = EXCLUDED.message,
				updated_at = EXCLUDED.updated_at
		`, job.JobID, job.PluginID, job.Action, job.Status, job.Message, job.CreatedAt, job.UpdatedAt)
		return err
	})
}

func (s *DatabaseStore) GetPluginJob(ctx context.Context, jobID string) (pluginJob, bool, error) {
	var job pluginJob
	err := s.db.QueryRowContext(ctx, `
		SELECT job_id, plugin_id, action, status, message, created_at, updated_at
		FROM p2p_plugin_jobs
		WHERE job_id = $1
	`, jobID).Scan(&job.JobID, &job.PluginID, &job.Action, &job.Status, &job.Message, &job.CreatedAt, &job.UpdatedAt)
	if err == sql.ErrNoRows {
		return pluginJob{}, false, nil
	}
	if err != nil {
		return pluginJob{}, false, err
	}
	return job, true, nil
}

func (s *DatabaseStore) UpsertPluginSecret(ctx context.Context, secret pluginSecret) error {
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_plugin_secrets (plugin_id, name, value, updated_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT(plugin_id, name) DO UPDATE SET
				value = EXCLUDED.value,
				updated_at = EXCLUDED.updated_at
		`, secret.PluginID, secret.Name, secret.Value, secret.UpdatedAt)
		return err
	})
}

func (s *DatabaseStore) GetPluginSecret(ctx context.Context, pluginID, name string) (pluginSecret, bool, error) {
	var secret pluginSecret
	err := s.db.QueryRowContext(ctx, `
		SELECT plugin_id, name, value, updated_at
		FROM p2p_plugin_secrets
		WHERE plugin_id = $1 AND name = $2
	`, pluginID, name).Scan(&secret.PluginID, &secret.Name, &secret.Value, &secret.UpdatedAt)
	if err == sql.ErrNoRows {
		return pluginSecret{}, false, nil
	}
	if err != nil {
		return pluginSecret{}, false, err
	}
	return secret, true, nil
}
