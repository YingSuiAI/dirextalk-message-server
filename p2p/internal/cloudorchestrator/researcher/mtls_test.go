package researcher

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestMutualTLSClientAuthenticatesBothEnds(t *testing.T) {
	directory := t.TempDir()
	caCertificate, caKey := newTestCA(t)
	serverCertificate, serverKey := issueTestCertificate(t, caCertificate, caKey, "cloud-researcher.test", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientCertificate, clientKey := issueTestCertificate(t, caCertificate, caKey, "cloud-orchestrator.test", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	caFile := writeTestFile(t, directory, "ca.pem", caCertificate)
	serverCertFile := writeTestFile(t, directory, "server.pem", serverCertificate)
	serverKeyFile := writeTestFile(t, directory, "server.key", serverKey)
	clientCertFile := writeTestFile(t, directory, "client.pem", clientCertificate)
	clientKeyFile := writeTestFile(t, directory, "client.key", clientKey)

	serverTLS, err := LoadMutualTLSServerConfig(MutualTLSServerConfig{
		CertificateFile: serverCertFile,
		KeyFile:         serverKeyFile,
		ClientCAFile:    caFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.TLS == nil || len(request.TLS.PeerCertificates) != 1 || request.TLS.PeerCertificates[0].Subject.CommonName != "cloud-orchestrator.test" {
			t.Fatalf("mutual TLS peer = %#v", request.TLS)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = serverTLS
	server.StartTLS()
	defer server.Close()

	client, err := NewMutualTLSClient(MutualTLSClientConfig{
		CAFile:          caFile,
		CertificateFile: clientCertFile,
		KeyFile:         clientKeyFile,
		ServerName:      "cloud-researcher.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Get(server.URL + cloudResearchPath)
	if err != nil {
		t.Fatalf("authenticated request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("authenticated status = %d", response.StatusCode)
	}

	if _, err := NewMutualTLSClient(MutualTLSClientConfig{CAFile: caFile, ServerName: "cloud-researcher.test"}); err == nil {
		t.Fatal("mutual TLS client must require a client certificate and key")
	}
}

func TestMutualTLSHTTPPlannerCompletesAResearchRoundTrip(t *testing.T) {
	directory := t.TempDir()
	caCertificate, caKey := newTestCA(t)
	serverCertificate, serverKey := issueTestCertificate(t, caCertificate, caKey, "cloud-researcher.test", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientCertificate, clientKey := issueTestCertificate(t, caCertificate, caKey, "cloud-orchestrator.test", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	caFile := writeTestFile(t, directory, "ca.pem", caCertificate)
	serverCertFile := writeTestFile(t, directory, "server.pem", serverCertificate)
	serverKeyFile := writeTestFile(t, directory, "server.key", serverKey)
	clientCertFile := writeTestFile(t, directory, "client.pem", clientCertificate)
	clientKeyFile := writeTestFile(t, directory, "client.key", clientKey)
	serverTLS, err := LoadMutualTLSServerConfig(MutualTLSServerConfig{CertificateFile: serverCertFile, KeyFile: serverKeyFile, ClientCAFile: caFile})
	if err != nil {
		t.Fatal(err)
	}
	input := runtime.ResearchInput{GoalID: "goal-1", PlanID: "plan-1", ConnectionID: "connection-1", PlanRevision: 1, Prompt: "Deploy a private knowledge workload."}
	server := httptest.NewUnstartedServer(NewResearchHTTPHandler(&recordingResearchPlanner{output: validResearchOutput(t, time.Now().UTC(), input)}))
	server.TLS = serverTLS
	server.StartTLS()
	defer server.Close()
	client, err := NewMutualTLSClient(MutualTLSClientConfig{CAFile: caFile, CertificateFile: clientCertFile, KeyFile: clientKeyFile, ServerName: "cloud-researcher.test"})
	if err != nil {
		t.Fatal(err)
	}
	planner, err := NewHTTP(HTTPConfig{Endpoint: server.URL + cloudResearchPath, Client: client})
	if err != nil {
		t.Fatal(err)
	}
	output, err := planner.Research(context.Background(), input)
	if err != nil || output.Draft.Region == "" {
		t.Fatalf("mTLS research output valid=%t err=%v", output.Draft.Region != "", err)
	}
}

func newTestCA(t *testing.T) ([]byte, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "cloud-research-test-ca"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), key
}

func issueTestCertificate(t *testing.T, caPEM []byte, caKey *ecdsa.PrivateKey, commonName string, usages []x509.ExtKeyUsage) ([]byte, []byte) {
	t.Helper()
	block, _ := pem.Decode(caPEM)
	if block == nil {
		t.Fatal("decode test CA")
	}
	ca, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		Subject:      pkix.Name{CommonName: commonName},
		DNSNames:     []string{commonName},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
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

func writeTestFile(t *testing.T, directory, name string, content []byte) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
