package dirextalkplugin

type CatalogEntry struct {
	ID             string         `json:"id"`
	Name           string         `json:"name"`
	Version        string         `json:"version"`
	Description    string         `json:"description"`
	Image          string         `json:"image"`
	Digest         string         `json:"digest"`
	MinBaseVersion string         `json:"min_base_version"`
	Permissions    []string       `json:"permissions"`
	Events         []string       `json:"events"`
	Actions        []string       `json:"actions"`
	ConfigSchema   map[string]any `json:"config_schema,omitempty"`
}

type Instance struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Version   string         `json:"version"`
	Image     string         `json:"image"`
	Digest    string         `json:"digest"`
	Status    string         `json:"status"`
	Enabled   bool           `json:"enabled"`
	Config    map[string]any `json:"config,omitempty"`
	LastJobID string         `json:"last_job_id,omitempty"`
	CreatedAt int64          `json:"created_at"`
	UpdatedAt int64          `json:"updated_at"`
}

type Job struct {
	JobID     string `json:"job_id"`
	PluginID  string `json:"plugin_id"`
	Action    string `json:"action"`
	Status    string `json:"status"`
	Message   string `json:"message,omitempty"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type Secret struct {
	PluginID  string `json:"plugin_id"`
	Name      string `json:"name"`
	Value     string `json:"-"`
	UpdatedAt int64  `json:"updated_at"`
}
