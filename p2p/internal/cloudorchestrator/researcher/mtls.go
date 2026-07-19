package researcher

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode"
)

const maxTLSMaterialBytes = 1 << 20

// MutualTLSClientConfig names the mounted, process-local identity material
// used only between the Orchestrator and the private researcher. It never
// carries a model or cloud credential.
type MutualTLSClientConfig struct {
	CAFile          string
	CertificateFile string
	KeyFile         string
	ServerName      string
	Timeout         time.Duration
}

// MutualTLSServerConfig names a private researcher's server identity and the
// CA that is allowed to authenticate an Orchestrator client.
type MutualTLSServerConfig struct {
	CertificateFile string
	KeyFile         string
	ClientCAFile    string
}

// NewMutualTLSClient fails closed unless a dedicated CA, client identity, and
// expected server name are present. It intentionally ignores proxy settings
// so private research goals cannot be redirected by process environment.
func NewMutualTLSClient(cfg MutualTLSClientConfig) (*http.Client, error) {
	roots, err := loadCertificatePool(cfg.CAFile)
	if err != nil {
		return nil, errors.New("cloud researcher client TLS configuration is invalid")
	}
	certificate, err := loadKeyPair(cfg.CertificateFile, cfg.KeyFile)
	if err != nil || !validTLSName(cfg.ServerName) {
		return nil, errors.New("cloud researcher client TLS configuration is invalid")
	}
	timeout := cfg.Timeout
	if timeout <= 0 || timeout > 90*time.Second {
		timeout = defaultResearchLimit
	}
	transport := &http.Transport{
		Proxy:                 nil,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: timeout,
		TLSClientConfig: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			RootCAs:      roots,
			Certificates: []tls.Certificate{certificate},
			ServerName:   strings.TrimSpace(cfg.ServerName),
		},
	}
	return &http.Client{Timeout: timeout, Transport: transport}, nil
}

// LoadMutualTLSServerConfig produces the server-side mTLS policy. A caller
// must install it on a TLS listener; the HTTP handler does not treat headers
// as an identity substitute.
func LoadMutualTLSServerConfig(cfg MutualTLSServerConfig) (*tls.Config, error) {
	certificate, err := loadKeyPair(cfg.CertificateFile, cfg.KeyFile)
	if err != nil {
		return nil, errors.New("cloud researcher server TLS configuration is invalid")
	}
	clientCAs, err := loadCertificatePool(cfg.ClientCAFile)
	if err != nil {
		return nil, errors.New("cloud researcher server TLS configuration is invalid")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	}, nil
}

func loadCertificatePool(path string) (*x509.CertPool, error) {
	content, err := readTLSMaterial(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(content) {
		return nil, errors.New("invalid certificate authority")
	}
	return pool, nil
}

func loadKeyPair(certificatePath, keyPath string) (tls.Certificate, error) {
	certificatePEM, err := readTLSMaterial(certificatePath)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEM, err := readTLSMaterial(keyPath)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(certificatePEM, keyPEM)
}

func readTLSMaterial(path string) ([]byte, error) {
	path = strings.TrimSpace(path)
	if path == "" || strings.ContainsAny(path, "\r\n\x00") {
		return nil, errors.New("TLS material path is invalid")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxTLSMaterialBytes {
		return nil, errors.New("TLS material file is invalid")
	}
	return os.ReadFile(path)
}

func validTLSName(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 253 || strings.ContainsAny(value, " \r\n\t\x00") {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
