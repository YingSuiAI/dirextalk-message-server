package agentclient

import (
	"errors"
	"net/url"
	"os"
	"strings"
)

type Config struct {
	Domain     string
	AgentToken string
}

func ConfigFromEnv() (Config, error) {
	return NewConfig(os.Getenv("DIREXIO_DOMAIN"), os.Getenv("DIREXIO_AGENT_TOKEN"))
}

func NewConfig(domain, agentToken string) (Config, error) {
	domain = strings.TrimRight(strings.TrimSpace(domain), "/")
	agentToken = strings.TrimSpace(agentToken)
	if domain == "" {
		return Config{}, errors.New("DIREXIO_DOMAIN is required")
	}
	if agentToken == "" {
		return Config{}, errors.New("DIREXIO_AGENT_TOKEN is required")
	}
	u, err := url.Parse(domain)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return Config{}, errors.New("DIREXIO_DOMAIN must be an absolute site origin such as https://example.com")
	}
	path := strings.Trim(u.Path, "/")
	if path != "" {
		return Config{}, errors.New("DIREXIO_DOMAIN must be the site origin without a route prefix")
	}
	u.Path = ""
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return Config{Domain: strings.TrimRight(u.String(), "/"), AgentToken: agentToken}, nil
}

func (c Config) P2PBaseURL() string {
	return c.Domain + "/_p2p"
}

func (c Config) MatrixBaseURL() string {
	return c.Domain + "/_matrix/client"
}
