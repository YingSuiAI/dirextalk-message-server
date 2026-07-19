package main

import (
	"errors"
	"io"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/stackteardown"
)

func TestParseRequestAcceptsOnlyConnectionAndRegion(t *testing.T) {
	t.Parallel()
	request, err := parseRequest([]string{"--connection-id", "connection-cleanup-0001", "--region", "us-east-1"}, io.Discard)
	if err != nil || request.ConnectionID != "connection-cleanup-0001" || request.Region != "us-east-1" {
		t.Fatalf("request=%#v err=%v", request, err)
	}
	for _, args := range [][]string{
		{"--connection-id", "connection-cleanup-0001", "--region", "us-east-1", "--stack-id", "arn:aws:cloudformation:us-east-1:123456789012:stack/other/id"},
		{"--connection-id", "connection-cleanup-0001", "--region", "us-east-1", "--resource", "some-table"},
	} {
		if _, err := parseRequest(args, io.Discard); !errors.Is(err, stackteardown.ErrInvalidRequest) {
			t.Fatalf("args=%q error=%v", args, err)
		}
	}
}
