package routing

import (
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/httputil"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/gorilla/mux"
)

func TestResumableUploadRoutesUseVendorUnstableNamespace(t *testing.T) {
	routers := httputil.NewRouters()
	cfg := &config.Dendrite{}
	cfg.Defaults(config.DefaultOpts{})
	Setup(routers, cfg, nil, nil, nil, nil, nil)

	templates := map[string]bool{}
	if err := routers.Media.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		template, err := route.GetPathTemplate()
		if err == nil {
			templates[template] = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if templates["/_matrix/media/{apiversion:(?:r0|v1|v3)}/upload/resumable"] {
		t.Fatalf("stable resumable upload start route is registered")
	}
	want := []string{
		"/_matrix/media/unstable/io.dirextalk/upload/resumable",
		"/_matrix/media/unstable/io.dirextalk/upload/resumable/{uploadID}",
		"/_matrix/media/unstable/io.dirextalk/upload/resumable/{uploadID}/chunk",
		"/_matrix/media/unstable/io.dirextalk/upload/resumable/{uploadID}/complete",
	}
	for _, template := range want {
		if !templates[template] {
			t.Fatalf("missing route template %q in %#v", template, templates)
		}
	}
}
