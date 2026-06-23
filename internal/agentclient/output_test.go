package agentclient

import (
	"bytes"
	"testing"
)

func TestWriteJSONPrettyByDefault(t *testing.T) {
	var out bytes.Buffer
	if err := WriteJSON(&out, map[string]any{"ok": true}, false); err != nil {
		t.Fatal(err)
	}
	if out.String() != "{\n  \"ok\": true\n}\n" {
		t.Fatalf("unexpected pretty JSON: %q", out.String())
	}
}

func TestWriteJSONRaw(t *testing.T) {
	var out bytes.Buffer
	if err := WriteJSON(&out, map[string]any{"ok": true}, true); err != nil {
		t.Fatal(err)
	}
	if out.String() != "{\"ok\":true}\n" {
		t.Fatalf("unexpected raw JSON: %q", out.String())
	}
}

func TestWriteNDJSONWritesOneLine(t *testing.T) {
	var out bytes.Buffer
	if err := WriteNDJSON(&out, map[string]any{"type": "m.room.message"}); err != nil {
		t.Fatal(err)
	}
	if out.String() != "{\"type\":\"m.room.message\"}\n" {
		t.Fatalf("unexpected ndjson output: %q", out.String())
	}
}

func TestWriteErrorUsesStderrShape(t *testing.T) {
	var out bytes.Buffer
	WriteError(&out, "direxio: failed")
	if out.String() != "direxio: failed\n" {
		t.Fatalf("unexpected error output: %q", out.String())
	}
}
