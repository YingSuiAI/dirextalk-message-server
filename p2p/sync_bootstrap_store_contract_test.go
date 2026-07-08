package p2p

import (
	"bytes"
	"os"
	"testing"
)

func TestSyncBootstrapUsesStoreAdapters(t *testing.T) {
	source, err := os.ReadFile("service_profile_sync.go")
	if err != nil {
		t.Fatal(err)
	}
	start := bytes.Index(source, []byte("func (s *Service) syncBootstrap"))
	if start < 0 {
		t.Fatal("syncBootstrap function not found")
	}
	end := bytes.Index(source[start:], []byte("func hasPendingGroupInvite"))
	if end < 0 {
		t.Fatal("syncBootstrap function end not found")
	}
	body := source[start : start+end]
	if bytes.Contains(body, []byte("s."+"store")) {
		t.Fatal("syncBootstrap must access durable state through focused store adapters")
	}
}
