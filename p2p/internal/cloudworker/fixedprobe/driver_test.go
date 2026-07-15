package fixedprobe

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

func TestDriverExecutesOnlyTheFixedProbeRecipe(t *testing.T) {
	host := &fakeHost{uid: 0}
	reporter := &recordingReporter{}
	driver := NewDriver(host)

	err := driver.Execute(context.Background(), validRequest(), reporter)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if host.writePath != UnitPath || host.writeMode != 0o644 || string(host.writeContents) != UnitContents {
		t.Fatalf("unit write = (%q, %o, %q)", host.writePath, host.writeMode, host.writeContents)
	}
	wantCommands := []commandCall{
		{executable: SystemctlPath, arguments: []string{"daemon-reload"}},
		{executable: SystemctlPath, arguments: []string{"enable", "--now", UnitName}},
	}
	if !reflect.DeepEqual(host.commands, wantCommands) {
		t.Fatalf("commands = %#v, want %#v", host.commands, wantCommands)
	}
	if !reflect.DeepEqual(host.healthURLs, []string{ReadinessURL}) {
		t.Fatalf("health URLs = %#v, want %q", host.healthURLs, ReadinessURL)
	}
	if !reflect.DeepEqual(reporter.checkpoints, CheckpointSequence()) {
		t.Fatalf("checkpoints = %#v, want %#v", reporter.checkpoints, CheckpointSequence())
	}
}

func TestDriverResumesAfterTheExactDurableCheckpoint(t *testing.T) {
	tests := []struct {
		name            string
		resumeAfter     string
		wantWrite       bool
		wantCommands    []commandCall
		wantCheckpoints []string
	}{
		{
			name: "unit installed", resumeAfter: CheckpointUnitInstalled,
			wantCommands:    []commandCall{{executable: SystemctlPath, arguments: []string{"enable", "--now", UnitName}}},
			wantCheckpoints: []string{CheckpointServiceStarted, CheckpointHealthVerified},
		},
		{
			name: "service started", resumeAfter: CheckpointServiceStarted,
			wantCheckpoints: []string{CheckpointHealthVerified},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			host := &fakeHost{uid: 0}
			reporter := &recordingReporter{}
			request := validRequest()
			request.ResumeAfter = test.resumeAfter
			if err := NewDriver(host).Execute(context.Background(), request, reporter); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if (host.writePath != "") != test.wantWrite {
				t.Fatalf("unit write occurred = %t, want %t", host.writePath != "", test.wantWrite)
			}
			if !reflect.DeepEqual(host.commands, test.wantCommands) {
				t.Fatalf("commands = %#v, want %#v", host.commands, test.wantCommands)
			}
			if !reflect.DeepEqual(reporter.checkpoints, test.wantCheckpoints) {
				t.Fatalf("checkpoints = %#v, want %#v", reporter.checkpoints, test.wantCheckpoints)
			}
		})
	}
}

func TestDriverRequiresRootAndTheRootApprovedScope(t *testing.T) {
	tests := []struct {
		name    string
		uid     int
		mutate  func(*recipeexec.ActionRequest)
		wantErr error
	}{
		{name: "worker is not root", uid: 1000, wantErr: ErrRootRequired},
		{name: "manifest did not approve root", uid: 0, mutate: func(request *recipeexec.ActionRequest) { request.RootRequired = false }, wantErr: ErrRootRequired},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			host := &fakeHost{uid: test.uid}
			request := validRequest()
			if test.mutate != nil {
				test.mutate(&request)
			}
			if err := NewDriver(host).Execute(context.Background(), request, &recordingReporter{}); !errors.Is(err, test.wantErr) {
				t.Fatalf("Execute() error = %v, want %v", err, test.wantErr)
			}
			assertHostUnused(t, host)
		})
	}
}

func TestDriverRejectsEveryUncompiledActionAndDynamicSlot(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*recipeexec.ActionRequest)
	}{
		{name: "other action", mutate: func(request *recipeexec.ActionRequest) { request.ActionID = "install_user_service" }},
		{name: "other artifact", mutate: func(request *recipeexec.ActionRequest) {
			request.Artifact.ArtifactDigest = "sha256:" + strings.Repeat("f", 64)
		}},
		{name: "unknown resume checkpoint", mutate: func(request *recipeexec.ActionRequest) { request.ResumeAfter = "user_checkpoint" }},
		{name: "volume slot", mutate: func(request *recipeexec.ActionRequest) {
			request.VolumeSlots = append(request.VolumeSlots, cloudorchestrator.VolumeSlotV1{})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			host := &fakeHost{uid: 0}
			request := validRequest()
			test.mutate(&request)
			if err := NewDriver(host).Execute(context.Background(), request, &recordingReporter{}); !errors.Is(err, ErrUnsupportedScope) {
				t.Fatalf("Execute() error = %v, want %v", err, ErrUnsupportedScope)
			}
			assertHostUnused(t, host)
		})
	}
}

func TestDriverDoesNotReportHealthWhenLoopbackReadinessFails(t *testing.T) {
	healthErr := errors.New("probe unavailable")
	host := &fakeHost{uid: 0, healthErr: healthErr}
	reporter := &recordingReporter{}
	request := validRequest()
	request.ResumeAfter = CheckpointServiceStarted

	err := NewDriver(host).Execute(context.Background(), request, reporter)
	if !errors.Is(err, healthErr) {
		t.Fatalf("Execute() error = %v, want %v", err, healthErr)
	}
	if len(reporter.checkpoints) != 0 {
		t.Fatalf("checkpoints after failed readiness = %#v", reporter.checkpoints)
	}
}

func TestLocalHostRequiresTheExactReadinessDocument(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
		wantErr     bool
	}{
		{name: "exact", status: http.StatusOK, contentType: ReadinessContentType, body: ReadinessBody},
		{name: "redirect", status: http.StatusFound, contentType: ReadinessContentType, body: ReadinessBody, wantErr: true},
		{name: "content type parameters", status: http.StatusOK, contentType: "application/json; charset=utf-8", body: ReadinessBody, wantErr: true},
		{name: "extra body", status: http.StatusOK, contentType: ReadinessContentType, body: ReadinessBody + "\n", wantErr: true},
		{name: "wrong schema", status: http.StatusOK, contentType: ReadinessContentType, body: `{"schema":"other","status":"ready"}`, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			host := &LocalHost{client: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if request.Method != http.MethodGet || request.URL.String() != ReadinessURL {
					t.Fatalf("request = %s %s", request.Method, request.URL)
				}
				return &http.Response{
					StatusCode: test.status,
					Header:     http.Header{"Content-Type": []string{test.contentType}},
					Body:       io.NopCloser(strings.NewReader(test.body)),
					Request:    request,
				}, nil
			})}}
			err := host.CheckLoopback(context.Background(), ReadinessURL)
			if (err != nil) != test.wantErr {
				t.Fatalf("CheckLoopback() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func TestProductionDriverPreflightRequiresLinuxRootAndFixedExecutables(t *testing.T) {
	executable := staticFileInfo{mode: 0o755}
	validStat := func(string) (fs.FileInfo, error) { return executable, nil }
	if err := validateProductionHost("linux", 0, validStat); err != nil {
		t.Fatalf("validateProductionHost() error = %v", err)
	}

	tests := []struct {
		name string
		goos string
		uid  int
		stat func(string) (fs.FileInfo, error)
	}{
		{name: "non linux", goos: "windows", uid: 0, stat: validStat},
		{name: "non root", goos: "linux", uid: 1000, stat: validStat},
		{name: "symlink", goos: "linux", uid: 0, stat: func(string) (fs.FileInfo, error) { return staticFileInfo{mode: fs.ModeSymlink | 0o777}, nil }},
		{name: "not executable", goos: "linux", uid: 0, stat: func(string) (fs.FileInfo, error) { return staticFileInfo{mode: 0o644}, nil }},
		{name: "missing", goos: "linux", uid: 0, stat: func(string) (fs.FileInfo, error) { return nil, fs.ErrNotExist }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateProductionHost(test.goos, test.uid, test.stat); !errors.Is(err, ErrDriverConfiguration) {
				t.Fatalf("validateProductionHost() error = %v, want %v", err, ErrDriverConfiguration)
			}
		})
	}
}

func validRequest() recipeexec.ActionRequest {
	return recipeexec.ActionRequest{ActionID: ActionID, Artifact: recipeexec.FixedProbeBundle(), RootRequired: true}
}

type commandCall struct {
	executable string
	arguments  []string
}

type fakeHost struct {
	uid           int
	writePath     string
	writeContents []byte
	writeMode     fs.FileMode
	commands      []commandCall
	healthURLs    []string
	healthErr     error
}

func (host *fakeHost) EffectiveUID() int { return host.uid }

func (host *fakeHost) WriteFile(path string, contents []byte, mode fs.FileMode) error {
	host.writePath, host.writeContents, host.writeMode = path, append([]byte(nil), contents...), mode
	return nil
}

func (host *fakeHost) Run(_ context.Context, executable string, arguments ...string) error {
	host.commands = append(host.commands, commandCall{executable: executable, arguments: append([]string(nil), arguments...)})
	return nil
}

func (host *fakeHost) CheckLoopback(_ context.Context, url string) error {
	host.healthURLs = append(host.healthURLs, url)
	return host.healthErr
}

type recordingReporter struct{ checkpoints []string }

func (reporter *recordingReporter) Checkpoint(_ context.Context, checkpoint string) error {
	reporter.checkpoints = append(reporter.checkpoints, checkpoint)
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

type staticFileInfo struct{ mode fs.FileMode }

func (info staticFileInfo) Name() string       { return "fixed" }
func (info staticFileInfo) Size() int64        { return 1 }
func (info staticFileInfo) Mode() fs.FileMode  { return info.mode }
func (info staticFileInfo) ModTime() time.Time { return time.Time{} }
func (info staticFileInfo) IsDir() bool        { return false }
func (info staticFileInfo) Sys() any           { return nil }

func assertHostUnused(t *testing.T, host *fakeHost) {
	t.Helper()
	if host.writePath != "" || len(host.commands) != 0 || len(host.healthURLs) != 0 {
		t.Fatalf("host was used: %#v", host)
	}
}
