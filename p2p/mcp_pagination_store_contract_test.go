package p2p

import (
	"bytes"
	"os"
	"testing"
)

func TestMCPPaginationUsesSocialStoreForFavorites(t *testing.T) {
	source, err := os.ReadFile("mcp_pagination.go")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(source, []byte("s."+"store.ListFavorites")) {
		t.Fatal("mcp pagination must read favorites through socialStore")
	}
}
