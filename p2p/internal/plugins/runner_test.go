package plugins

import "testing"

func TestNewEnvironmentRunnerSelection(t *testing.T) {
	tests := []struct {
		name    string
		enabled string
		check   func(*testing.T, Runner)
	}{
		{
			name: "noop by default",
			check: func(t *testing.T, runner Runner) {
				if _, ok := runner.(NoopRunner); !ok {
					t.Fatal("expected noop plugin runner when Docker is disabled")
				}
			},
		},
		{
			name:    "Docker when enabled",
			enabled: "true",
			check: func(t *testing.T, runner Runner) {
				docker, ok := runner.(DockerRunner)
				if !ok {
					t.Fatal("expected Docker plugin runner when enabled")
				}
				if docker.binary != "docker-test" {
					t.Fatalf("configured Docker binary = %q", docker.binary)
				}
				if docker.network != "dirextalk-p2p_default" {
					t.Fatalf("configured Docker network = %q", docker.network)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("P2P_PLUGIN_DOCKER_ENABLED", tc.enabled)
			t.Setenv("P2P_PLUGIN_DOCKER_BIN", "docker-test")
			t.Setenv("P2P_PLUGIN_DOCKER_NETWORK", "dirextalk-p2p_default")
			tc.check(t, NewEnvironmentRunner())
		})
	}
}

func TestValidateOfficialOperation(t *testing.T) {
	tests := []struct {
		name    string
		op      RunnerOperation
		wantErr bool
	}{
		{
			name: "official image without digest",
			op: RunnerOperation{
				Action: "install", PluginID: "io.dirextalk.backup",
				Image: "docker.io/dirextalk/backup-plugin:latest",
			},
		},
		{
			name: "non-Dirextalk image",
			op: RunnerOperation{
				Action: "install", PluginID: "io.dirextalk.backup",
				Image: "docker.io/example/backup-plugin:latest",
			},
			wantErr: true,
		},
		{
			name: "invalid optional digest",
			op: RunnerOperation{
				Action: "install", PluginID: "io.dirextalk.backup",
				Image: "dirextalk/backup-plugin:latest", Digest: "latest",
			},
			wantErr: true,
		},
		{
			name: "non-Ops Docker socket",
			op: RunnerOperation{
				Action: "enable", PluginID: "io.dirextalk.backup",
				Image:   "docker.io/dirextalk/backup-plugin:latest",
				Volumes: []string{"/var/run/docker.sock:/var/run/docker.sock"},
			},
			wantErr: true,
		},
		{
			name: "non-Ops data volume",
			op: RunnerOperation{
				Action: "enable", PluginID: "io.dirextalk.backup",
				Image:   "docker.io/dirextalk/backup-plugin:latest",
				Volumes: []string{"dirextalk_backup_data:/var/lib/dirextalk-backup"},
			},
			wantErr: true,
		},
		{
			name: "Ops privileged mounts",
			op: RunnerOperation{
				Action: "enable", PluginID: OpsPluginID,
				Image: "docker.io/dirextalk/ops-plugin:latest",
				Volumes: []string{
					"/var/run/docker.sock:/var/run/docker.sock",
					"dirextalk_ops_backups:/var/lib/dirextalk-ops",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateOfficialOperation(tc.op)
			if tc.wantErr && err == nil {
				t.Fatal("expected operation validation to fail")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected operation validation to pass, got %v", err)
			}
		})
	}
}

func TestImageReferenceUsesDigestOnlyWhenPresent(t *testing.T) {
	if got := ImageReference(" docker.io/dirextalk/backup-plugin:latest ", ""); got != "docker.io/dirextalk/backup-plugin:latest" {
		t.Fatalf("tag image reference = %q", got)
	}
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if got := ImageReference("dirextalk/backup-plugin:latest", digest); got != "dirextalk/backup-plugin:latest@"+digest {
		t.Fatalf("digest image reference = %q", got)
	}
}
