package base_test

import (
	"bytes"
	"context"
	"embed"
	"html/template"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal"
	"github.com/YingSuiAI/dirextalk-message-server/internal/httputil"
	basepkg "github.com/YingSuiAI/dirextalk-message-server/setup/base"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/setup/process"
	"github.com/stretchr/testify/assert"
)

//go:embed static/*.gotmpl
var staticContent embed.FS

func TestLandingPage_Tcp(t *testing.T) {
	// generate the expected result
	tmpl := template.Must(template.ParseFS(staticContent, "static/*.gotmpl"))
	expectedRes := &bytes.Buffer{}
	err := tmpl.ExecuteTemplate(expectedRes, "index.gotmpl", map[string]string{
		"Version": internal.VersionString(),
	})
	assert.NoError(t, err)

	processCtx := process.NewProcessContext()
	routers := httputil.NewRouters()
	cfg := config.Dendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})

	// hack: create a server and close it immediately, just to get a random port assigned
	s := httptest.NewServer(nil)
	s.Close()

	// start base with the listener and wait for it to be started
	address, err := config.HTTPAddress(s.URL)
	assert.NoError(t, err)
	go basepkg.SetupAndServeHTTP(processCtx, &cfg, routers, address, nil, nil)
	time.Sleep(time.Millisecond * 10)

	// When hitting /, we should be redirected to /_matrix/static, which should contain the landing page
	var resp *http.Response
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		req, reqErr := http.NewRequest(http.MethodGet, s.URL, nil)
		assert.NoError(t, reqErr)
		resp, err = s.Client().Do(req)
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	defer resp.Body.Close()

	// read the response
	buf := &bytes.Buffer{}
	_, err = buf.ReadFrom(resp.Body)
	assert.NoError(t, err)

	// Using .String() for user friendly output
	assert.Equal(t, expectedRes.String(), buf.String(), "response mismatch")
}

func TestMCPRoute_Tcp(t *testing.T) {
	processCtx := process.NewProcessContext()
	routers := httputil.NewRouters()
	routers.MCP.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	cfg := config.Dendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})

	s := httptest.NewServer(nil)
	s.Close()

	address, err := config.HTTPAddress(s.URL)
	assert.NoError(t, err)
	go basepkg.SetupAndServeHTTP(processCtx, &cfg, routers, address, nil, nil)
	time.Sleep(time.Millisecond * 10)

	var resp *http.Response
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		req, reqErr := http.NewRequest(http.MethodPost, s.URL+"/mcp", nil)
		assert.NoError(t, reqErr)
		resp, err = s.Client().Do(req)
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestLandingPage_UnixSocket(t *testing.T) {
	// generate the expected result
	tmpl := template.Must(template.ParseFS(staticContent, "static/*.gotmpl"))
	expectedRes := &bytes.Buffer{}
	err := tmpl.ExecuteTemplate(expectedRes, "index.gotmpl", map[string]string{
		"Version": internal.VersionString(),
	})
	assert.NoError(t, err)

	processCtx := process.NewProcessContext()
	routers := httputil.NewRouters()
	cfg := config.Dendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})

	tempDir := t.TempDir()
	socket := path.Join(tempDir, "socket")
	// start base with the listener and wait for it to be started
	address, err := config.UnixSocketAddress(socket, "755")
	assert.NoError(t, err)
	go basepkg.SetupAndServeHTTP(processCtx, &cfg, routers, address, nil, nil)
	time.Sleep(time.Millisecond * 100)

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", socket)
			},
		},
	}
	resp, err := client.Get("http://unix/")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// read the response
	buf := &bytes.Buffer{}
	_, err = buf.ReadFrom(resp.Body)
	assert.NoError(t, err)

	// Using .String() for user friendly output
	assert.Equal(t, expectedRes.String(), buf.String(), "response mismatch")
}
