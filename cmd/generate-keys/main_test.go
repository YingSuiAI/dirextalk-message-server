// Copyright 2026 YingSuiAI
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial

package main

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestGenerateTLSKeyUsesServerNameAsVerifiableSAN(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	certificatePath := filepath.Join(directory, "agent.crt")
	keyPath := filepath.Join(directory, "agent.key")
	if err := generateTLSKey("agent", keyPath, certificatePath, "", "", 1024); err != nil {
		t.Fatalf("generate TLS key: %v", err)
	}

	encoded, err := os.ReadFile(certificatePath)
	if err != nil {
		t.Fatalf("read generated certificate: %v", err)
	}
	block, rest := pem.Decode(encoded)
	if block == nil || len(rest) != 0 || block.Type != "CERTIFICATE" {
		t.Fatal("generated certificate is not exactly one PEM certificate")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse generated certificate: %v", err)
	}
	if got, want := certificate.DNSNames, []string{"agent"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("generated certificate DNS SANs=%v, want %v", got, want)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	if _, err := certificate.Verify(x509.VerifyOptions{
		Roots:     roots,
		DNSName:   "agent",
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Fatalf("generated exact-leaf trust cannot verify agent SAN: %v", err)
	}
}
