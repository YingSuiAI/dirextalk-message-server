package cloud

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConnectionCredentialBootstrapHTTPClientUsesStrictMTLSWithoutProxy(t *testing.T) {
	directory := t.TempDir()
	caPEM, caKey, caCertificate := credentialBootstrapTestCA(t)
	serverPEM, serverKeyPEM := credentialBootstrapTestCertificate(t, caKey, caCertificate, "bootstrap-server", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, true)
	clientPEM, clientKeyPEM := credentialBootstrapTestCertificate(t, caKey, caCertificate, "message-server", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, false)
	serverCertificate, err := tls.X509KeyPair(serverPEM, serverKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	clientRoots := x509.NewCertPool()
	clientRoots.AppendCertsFromPEM(caPEM)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.TLS == nil || request.TLS.Version != tls.VersionTLS13 || len(request.TLS.PeerCertificates) != 1 || request.TLS.PeerCertificates[0].Subject.CommonName != "message-server" {
			t.Fatalf("mTLS identity=%#v", request.TLS)
		}
		raw, _ := io.ReadAll(request.Body)
		var input ConnectionCredentialBootstrapRequest
		if err := strictConnectionCredentialBootstrapJSON(raw, &input); err != nil || request.URL.Path != connectionCredentialBootstrapPath {
			t.Fatalf("request path=%q body=%s err=%v", request.URL.Path, raw, err)
		}
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(writer).Encode(ConnectionCredentialBootstrapSession{
			Schema: connectionCredentialBootstrapResponseSchema, Status: "awaiting_upload", RequestID: input.RequestID,
			SessionID: "aws-bootstrap-http-test-0001", ConnectionID: input.RolePlan.ConnectionID,
			ServerX25519PublicKey: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", UploadBearer: "transient-bearer",
			UploadURL: "https://upload.example.invalid/v1/aws-bootstrap/sessions/aws-bootstrap-http-test-0001",
			ExpiresAt: "2026-07-16T05:10:00Z", HKDF: connectionCredentialBootstrapHKDF, AAD: "fixed-aad",
		})
	}))
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverCertificate},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientRoots,
	}
	server.StartTLS()
	defer server.Close()

	caFile := credentialBootstrapWriteTestFile(t, directory, "ca.pem", caPEM)
	clientFile := credentialBootstrapWriteTestFile(t, directory, "client.pem", clientPEM)
	keyFile := credentialBootstrapWriteTestFile(t, directory, "client.key", clientKeyPEM)
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	created, err := NewConnectionCredentialBootstrapHTTPClient(ConnectionCredentialBootstrapHTTPConfig{
		Endpoint: server.URL + connectionCredentialBootstrapPath, CAFile: caFile, CertificateFile: clientFile, KeyFile: keyFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	httpClient := created.(*connectionCredentialBootstrapHTTPClient)
	transport := httpClient.client.Transport.(*http.Transport)
	if transport.Proxy != nil || transport.TLSClientConfig.MinVersion != tls.VersionTLS13 || len(transport.TLSClientConfig.Certificates) != 1 {
		t.Fatalf("transport does not enforce dedicated TLS: %#v", transport)
	}
	request := ConnectionCredentialBootstrapRequest{Schema: connectionCredentialBootstrapCreateSchema, RequestID: "019f6a80-1234-7abc-8def-0123456789ab", RolePlan: ConnectionCredentialBootstrapRolePlanWire{ConnectionID: "connection-http-test-0001"}}
	response, err := created.CreateSession(context.Background(), request)
	if err != nil || response.UploadBearer != "transient-bearer" || response.RequestID != request.RequestID {
		t.Fatalf("response=%#v err=%v", response, err)
	}

	if _, err := NewConnectionCredentialBootstrapHTTPClient(ConnectionCredentialBootstrapHTTPConfig{Endpoint: "http://bootstrap.invalid" + connectionCredentialBootstrapPath}); err == nil {
		t.Fatal("non-HTTPS bootstrap endpoint was accepted")
	}
}

func credentialBootstrapTestCA(t *testing.T) ([]byte, *ecdsa.PrivateKey, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "credential-bootstrap-test-ca"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), key, certificate
}

func credentialBootstrapTestCertificate(t *testing.T, caKey *ecdsa.PrivateKey, ca *x509.Certificate, commonName string, usages []x509.ExtKeyUsage, server bool) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{SerialNumber: big.NewInt(now.UnixNano()), Subject: pkix.Name{CommonName: commonName}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usages}
	if server {
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func credentialBootstrapWriteTestFile(t *testing.T, directory, name string, content []byte) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
