package releasecontrol

import (
	"fmt"
	"strings"
	"testing"
)

const validDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestValidateManifestAcceptsPinnedRelease(t *testing.T) {
	manifest, err := ValidateManifest(testManifestJSON(
		"v1.1.0",
		"dirextalk/message-server:v1.1.0",
		validDigest,
		`[">=1.0.0 <1.1.0"]`,
	))
	if err != nil {
		t.Fatalf("ValidateManifest() error = %v", err)
	}
	if manifest.Version != "v1.1.0" {
		t.Fatalf("Version = %q, want v1.1.0", manifest.Version)
	}
}

func TestValidateManifestRejectsImageTagMismatch(t *testing.T) {
	_, err := ValidateManifest(testManifestJSON(
		"v1.1.0",
		"dirextalk/message-server:latest",
		validDigest,
		`[">=1.0.0 <1.1.0"]`,
	))
	if err == nil || !strings.Contains(err.Error(), "image tag") {
		t.Fatalf("ValidateManifest() error = %v, want image tag error", err)
	}
}

func TestValidateManifestRejectsMalformedDigest(t *testing.T) {
	_, err := ValidateManifest(testManifestJSON(
		"v1.1.0",
		"dirextalk/message-server:v1.1.0",
		"sha256:not-a-digest",
		`[">=1.0.0 <1.1.0"]`,
	))
	if err == nil || !strings.Contains(err.Error(), "image_digest") {
		t.Fatalf("ValidateManifest() error = %v, want image_digest error", err)
	}
}

func TestValidateManifestRejectsMalformedUpgradeRange(t *testing.T) {
	_, err := ValidateManifest(testManifestJSON(
		"v1.1.0",
		"dirextalk/message-server:v1.1.0",
		validDigest,
		`["not a semver range"]`,
	))
	if err == nil || !strings.Contains(err.Error(), "upgrade_from") {
		t.Fatalf("ValidateManifest() error = %v, want upgrade_from error", err)
	}
}

func TestValidateManifestRequiresBackup(t *testing.T) {
	valid := string(testManifestJSON(
		"v1.1.0",
		"dirextalk/message-server:v1.1.0",
		validDigest,
		`[">=1.0.0 <1.1.0"]`,
	))

	for _, tc := range []struct {
		name string
		data string
	}{
		{
			name: "false",
			data: strings.Replace(valid, `"backup_required": true`, `"backup_required": false`, 1),
		},
		{
			name: "missing",
			data: strings.Replace(valid, `"backup_required": true,`, "", 1),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateManifest([]byte(tc.data))
			if err == nil || !strings.Contains(err.Error(), "backup_required") {
				t.Fatalf("ValidateManifest() error = %v, want backup_required error", err)
			}
		})
	}
}

func TestManifestRejectsUndeclaredUpgradePath(t *testing.T) {
	manifest, err := ValidateManifest(testManifestJSON(
		"v1.1.0",
		"dirextalk/message-server:v1.1.0",
		validDigest,
		`[">=1.0.0 <1.1.0"]`,
	))
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		current string
		want    bool
	}{
		{current: "v1.0.0", want: true},
		{current: "v0.15.2", want: false},
		{current: "v1.1.0", want: false},
	} {
		got, err := manifest.AllowsUpgradeFrom(tc.current)
		if err != nil {
			t.Fatalf("AllowsUpgradeFrom(%q) error = %v", tc.current, err)
		}
		if got != tc.want {
			t.Fatalf("AllowsUpgradeFrom(%q) = %v, want %v", tc.current, got, tc.want)
		}
	}
}

func TestManifestChecksClientCompatibilityWindow(t *testing.T) {
	manifest, err := ValidateManifest(testManifestJSON(
		"v1.1.0",
		"dirextalk/message-server:v1.1.0",
		validDigest,
		`[">=1.0.0 <1.1.0"]`,
	))
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		client string
		want   bool
	}{
		{client: "v1.2.0", want: true},
		{client: "1.2.0", want: true},
		{client: "v1.0.9", want: false},
		{client: "1.0.9", want: false},
		{client: "v2.0.0", want: false},
	} {
		got, err := manifest.SupportsClient(tc.client)
		if err != nil {
			t.Fatalf("SupportsClient(%q) error = %v", tc.client, err)
		}
		if got != tc.want {
			t.Fatalf("SupportsClient(%q) = %v, want %v", tc.client, got, tc.want)
		}
	}
}

func testManifestJSON(version, image, digest, upgradeFrom string) []byte {
	return []byte(fmt.Sprintf(`{
		"manifest_version": 1,
		"version": %q,
		"image": %q,
		"image_digest": %q,
		"upgrade_from": %s,
		"schema_version": 2,
		"schema_compat_version": 1,
		"minimum_client_version": "v1.1.0",
		"maximum_client_version_exclusive": "v2.0.0",
		"backup_required": true,
		"rollback_supported": true,
		"rollback_mode": "restore_backup",
		"release_notes_url": "https://github.com/YingSuiAI/dirextalk-message-server/releases/tag/%s"
	}`, version, image, digest, upgradeFrom, version))
}
