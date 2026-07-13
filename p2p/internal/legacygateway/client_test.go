package legacygateway

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/url"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidateClientCertificateRequiresExactLegacyGatewayIdentity(t *testing.T) {
	identity, err := LegacyGatewaySPIFFEIdentity(fixtureTenant)
	if err != nil {
		t.Fatal(err)
	}
	valid := makeClientCertificate(t, "", identity, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	if err := ValidateClientCertificate(valid, fixtureTenant); err != nil {
		t.Fatalf("valid client certificate rejected: %v", err)
	}

	cases := map[string]tls.Certificate{
		"common name":     makeClientCertificate(t, "legacy-gateway", identity, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}),
		"wrong tenant":    makeClientCertificate(t, "", "spiffe://dirextalk.internal/v1/tenants/01890f00-0000-7000-8000-000000000399/services/legacy-matrix-gateway", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}),
		"server auth eku": makeClientCertificate(t, "", identity, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}),
	}
	for name, certificate := range cases {
		t.Run(name, func(t *testing.T) {
			if err := ValidateClientCertificate(certificate, fixtureTenant); err == nil {
				t.Fatal("invalid client certificate accepted")
			}
		})
	}
}

func TestIngressErrorsAreSanitizedAndClassified(t *testing.T) {
	permanent := sanitizedIngressError(status.Error(codes.PermissionDenied, "provider secret detail"))
	if got := permanent.Error(); got != "legacy gateway ingress failed: permission_denied" {
		t.Fatalf("sanitized permanent error = %q", got)
	}
	if !IsPermanentError(permanent) {
		t.Fatal("permission denial must be permanent")
	}
	transient := sanitizedIngressError(status.Error(codes.ResourceExhausted, "backend detail"))
	if IsPermanentError(transient) {
		t.Fatal("resource exhaustion must remain retryable")
	}
	if got := IngressErrorCodeOf(transient); got != IngressErrorResourceExhausted {
		t.Fatalf("error code = %q", got)
	}
}

func makeClientCertificate(
	t *testing.T,
	commonName, identity string,
	extendedKeyUsage []x509.ExtKeyUsage,
) tls.Certificate {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityURI, err := url.Parse(identity)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  extendedKeyUsage,
		URIs:         []*url.URL{identityURI},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: privateKey}
}
