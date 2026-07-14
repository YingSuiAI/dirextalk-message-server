package cloudworker

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

const testDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func validTestManifest(endpoint string) BootstrapManifest {
	return BootstrapManifest{
		Schema:                 BootstrapManifestV1Schema,
		ConnectionID:           "connection-v2-0001",
		DeploymentID:           "deployment-v2-0001",
		BootstrapSessionID:     "worker-session-v2-01",
		BootstrapEndpoint:      endpoint,
		WorkerImageDigest:      testDigest,
		ArtifactManifestDigest: testDigest,
		ExpiresAt:              "2026-07-14T07:04:00.000Z",
	}
}

func validManifestContext(endpoint string) ManifestValidationContext {
	return ManifestValidationContext{
		Now:                       time.Date(2026, 7, 14, 7, 0, 0, 0, time.UTC),
		MaxLifetime:               5 * time.Minute,
		ExpectedConnectionID:      "connection-v2-0001",
		ExpectedBootstrapEndpoint: endpoint,
	}
}

func TestBootstrapManifestV1IsStrictAndCompatible(t *testing.T) {
	const endpoint = "https://broker.example.invalid/v2/worker-sessions"
	manifest := validTestManifest(endpoint)
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("MarshalJSON() error = %v", err)
	}
	parsed, err := ParseBootstrapManifest(raw, validManifestContext(endpoint))
	if err != nil {
		t.Fatalf("ParseBootstrapManifest() error = %v", err)
	}
	if parsed != manifest {
		t.Fatalf("parsed manifest = %#v, want %#v", parsed, manifest)
	}

	for _, forbidden := range []string{"ssh_key_name", "iam_instance_profile", "aws_access_key_id", "user_data"} {
		t.Run(forbidden, func(t *testing.T) {
			mutated := strings.TrimSuffix(string(raw), "}") + `,"` + forbidden + `":"forbidden"}`
			if _, err := ParseBootstrapManifest([]byte(mutated), validManifestContext(endpoint)); err == nil {
				t.Fatalf("ParseBootstrapManifest() accepted forbidden %q field", forbidden)
			}
		})
	}

	duplicate := strings.TrimSuffix(string(raw), "}") + `,"connection_id":"connection-v2-0001"}`
	if _, err := ParseBootstrapManifest([]byte(duplicate), validManifestContext(endpoint)); err == nil {
		t.Fatal("ParseBootstrapManifest() accepted a duplicate field")
	}
}

func TestBootstrapManifestV1RejectsExpiredOrUnboundValues(t *testing.T) {
	const endpoint = "https://broker.example.invalid/v2/worker-sessions"
	manifest := validTestManifest(endpoint)
	context := validManifestContext(endpoint)
	for name, mutate := range map[string]func(*BootstrapManifest, *ManifestValidationContext){
		"expired": func(manifest *BootstrapManifest, _ *ManifestValidationContext) {
			manifest.ExpiresAt = "2026-07-14T07:00:00.000Z"
		},
		"too_long": func(manifest *BootstrapManifest, _ *ManifestValidationContext) {
			manifest.ExpiresAt = "2026-07-14T07:06:00.000Z"
		},
		"wrong_connection": func(manifest *BootstrapManifest, _ *ManifestValidationContext) {
			manifest.ConnectionID = "connection-v2-other"
		},
		"wrong_endpoint": func(manifest *BootstrapManifest, _ *ManifestValidationContext) {
			manifest.BootstrapEndpoint = "https://other.example.invalid/v2/worker-sessions"
		},
		"invalid_digest": func(manifest *BootstrapManifest, _ *ManifestValidationContext) {
			manifest.ArtifactManifestDigest = "sha256:not-a-digest"
		},
	} {
		t.Run(name, func(t *testing.T) {
			gotManifest, gotContext := manifest, context
			mutate(&gotManifest, &gotContext)
			if err := gotManifest.Validate(gotContext); err == nil {
				t.Fatalf("Validate() accepted %s", name)
			}
		})
	}
}
