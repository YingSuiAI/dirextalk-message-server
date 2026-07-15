package cloud

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	connectionCredentialBootstrapPath      = "/v1/aws-bootstrap/sessions"
	maxConnectionCredentialBootstrapJSON   = 64 << 10
	maxConnectionCredentialBootstrapTLSPEM = 1 << 20
)

type ConnectionCredentialBootstrapHTTPConfig struct {
	Endpoint        string
	CAFile          string
	CertificateFile string
	KeyFile         string
	Timeout         time.Duration
}

type connectionCredentialBootstrapHTTPClient struct {
	endpoint string
	client   *http.Client
}

func NewConnectionCredentialBootstrapHTTPClient(config ConnectionCredentialBootstrapHTTPConfig) (ConnectionCredentialBootstrapClient, error) {
	endpoint := strings.TrimSpace(config.Endpoint)
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" || parsed.Path != connectionCredentialBootstrapPath {
		return nil, errors.New("cloud connection credential bootstrap endpoint is invalid")
	}
	caPEM, err := readConnectionCredentialBootstrapTLSFile(config.CAFile)
	if err != nil {
		return nil, errors.New("cloud connection credential bootstrap TLS configuration is invalid")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("cloud connection credential bootstrap TLS configuration is invalid")
	}
	certificatePEM, err := readConnectionCredentialBootstrapTLSFile(config.CertificateFile)
	if err != nil {
		return nil, errors.New("cloud connection credential bootstrap TLS configuration is invalid")
	}
	keyPEM, err := readConnectionCredentialBootstrapTLSFile(config.KeyFile)
	if err != nil {
		return nil, errors.New("cloud connection credential bootstrap TLS configuration is invalid")
	}
	certificate, err := tls.X509KeyPair(certificatePEM, keyPEM)
	if err != nil {
		return nil, errors.New("cloud connection credential bootstrap TLS configuration is invalid")
	}
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	if timeout < 0 || timeout > 30*time.Second {
		return nil, errors.New("cloud connection credential bootstrap timeout is invalid")
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DisableCompression:    true,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: timeout,
		TLSClientConfig: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			RootCAs:      roots,
			Certificates: []tls.Certificate{certificate},
			ServerName:   parsed.Hostname(),
		},
	}
	return &connectionCredentialBootstrapHTTPClient{
		endpoint: endpoint,
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (client *connectionCredentialBootstrapHTTPClient) CreateSession(ctx context.Context, input ConnectionCredentialBootstrapRequest) (ConnectionCredentialBootstrapSession, error) {
	if client == nil || client.client == nil || ctx == nil {
		return ConnectionCredentialBootstrapSession{}, errors.New("cloud connection credential bootstrap client is unavailable")
	}
	body, err := json.Marshal(input)
	if err != nil || len(body) > maxConnectionCredentialBootstrapJSON {
		return ConnectionCredentialBootstrapSession{}, ErrConnectionCredentialBootstrapUpstreamRejected
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint, bytes.NewReader(body))
	if err != nil {
		return ConnectionCredentialBootstrapSession{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := client.client.Do(request)
	if err != nil {
		return ConnectionCredentialBootstrapSession{}, err
	}
	defer response.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, maxConnectionCredentialBootstrapJSON+1))
	if readErr != nil || len(raw) == 0 || len(raw) > maxConnectionCredentialBootstrapJSON {
		return ConnectionCredentialBootstrapSession{}, ErrConnectionCredentialBootstrapUpstreamInvalid
	}
	if !strings.Contains(strings.ToLower(response.Header.Get("Cache-Control")), "no-store") {
		return ConnectionCredentialBootstrapSession{}, ErrConnectionCredentialBootstrapUpstreamInvalid
	}
	mediaType, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		return ConnectionCredentialBootstrapSession{}, ErrConnectionCredentialBootstrapUpstreamInvalid
	}
	switch response.StatusCode {
	case http.StatusCreated:
		var session ConnectionCredentialBootstrapSession
		if err := strictConnectionCredentialBootstrapJSON(raw, &session); err != nil {
			return ConnectionCredentialBootstrapSession{}, ErrConnectionCredentialBootstrapUpstreamInvalid
		}
		return session, nil
	case http.StatusConflict:
		if err := validateConnectionCredentialBootstrapHTTPError(raw); err != nil {
			return ConnectionCredentialBootstrapSession{}, ErrConnectionCredentialBootstrapUpstreamInvalid
		}
		return ConnectionCredentialBootstrapSession{}, ErrConnectionCredentialBootstrapUpstreamConflict
	case http.StatusBadRequest:
		if err := validateConnectionCredentialBootstrapHTTPError(raw); err != nil {
			return ConnectionCredentialBootstrapSession{}, ErrConnectionCredentialBootstrapUpstreamInvalid
		}
		return ConnectionCredentialBootstrapSession{}, ErrConnectionCredentialBootstrapUpstreamRejected
	default:
		if err := validateConnectionCredentialBootstrapHTTPError(raw); err != nil {
			return ConnectionCredentialBootstrapSession{}, ErrConnectionCredentialBootstrapUpstreamInvalid
		}
		return ConnectionCredentialBootstrapSession{}, errors.New("cloud connection credential bootstrap request failed")
	}
}

func validateConnectionCredentialBootstrapHTTPError(raw []byte) error {
	var value struct {
		Error string `json:"error"`
	}
	if err := strictConnectionCredentialBootstrapJSON(raw, &value); err != nil || strings.TrimSpace(value.Error) == "" || len(value.Error) > 128 {
		return ErrConnectionCredentialBootstrapUpstreamInvalid
	}
	return nil
}

func readConnectionCredentialBootstrapTLSFile(path string) ([]byte, error) {
	path = strings.TrimSpace(path)
	if path == "" || strings.ContainsAny(path, "\r\n\x00") {
		return nil, errors.New("TLS material path is invalid")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxConnectionCredentialBootstrapTLSPEM {
		return nil, errors.New("TLS material file is invalid")
	}
	return os.ReadFile(path)
}

func strictConnectionCredentialBootstrapJSON(raw []byte, target any) error {
	if err := rejectConnectionCredentialBootstrapDuplicateKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrConnectionCredentialBootstrapUpstreamInvalid
	}
	return nil
}

func rejectConnectionCredentialBootstrapDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanConnectionCredentialBootstrapJSON(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return ErrConnectionCredentialBootstrapUpstreamInvalid
	}
	return nil
}

func scanConnectionCredentialBootstrapJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return ErrConnectionCredentialBootstrapUpstreamInvalid
			}
			if _, duplicate := seen[key]; duplicate {
				return ErrConnectionCredentialBootstrapUpstreamInvalid
			}
			seen[key] = struct{}{}
			if err := scanConnectionCredentialBootstrapJSON(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return ErrConnectionCredentialBootstrapUpstreamInvalid
		}
	case '[':
		for decoder.More() {
			if err := scanConnectionCredentialBootstrapJSON(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return ErrConnectionCredentialBootstrapUpstreamInvalid
		}
	default:
		return ErrConnectionCredentialBootstrapUpstreamInvalid
	}
	return nil
}
