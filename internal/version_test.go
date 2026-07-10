package internal

import "testing"

func TestCurrentBuildInfoUsesCanonicalReleaseVersion(t *testing.T) {
	got := CurrentBuildInfo()

	if got.Version != "v1.0.0" {
		t.Fatalf("Version = %q, want v1.0.0", got.Version)
	}
	if got.SchemaVersion != 1 {
		t.Fatalf("SchemaVersion = %d, want 1", got.SchemaVersion)
	}
	if got.SchemaCompatVersion != 1 {
		t.Fatalf("SchemaCompatVersion = %d, want 1", got.SchemaCompatVersion)
	}
	if VersionString() != got.Version {
		t.Fatalf("VersionString() = %q, want %q", VersionString(), got.Version)
	}
}

func TestCurrentBuildInfoKeepsCommitAndBuildTimeSeparate(t *testing.T) {
	oldVersion, oldCommit, oldBuildTime := version, commit, buildTime
	version = "v1.2.3"
	commit = "0123456789abcdef0123456789abcdef01234567"
	buildTime = "2026-07-10T08:09:10Z"
	t.Cleanup(func() {
		version, commit, buildTime = oldVersion, oldCommit, oldBuildTime
	})

	got := CurrentBuildInfo()
	if got.Version != version {
		t.Fatalf("Version = %q, want %q", got.Version, version)
	}
	if got.Commit != commit {
		t.Fatalf("Commit = %q, want %q", got.Commit, commit)
	}
	if got.BuildTime != buildTime {
		t.Fatalf("BuildTime = %q, want %q", got.BuildTime, buildTime)
	}
	if got.Version == got.Commit || got.Version == got.BuildTime {
		t.Fatalf("build metadata must not be folded into Version: %#v", got)
	}
}
