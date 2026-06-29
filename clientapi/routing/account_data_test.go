package routing

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNormalizeDirexioPushContextUsesServerExpiry(t *testing.T) {
	now := time.UnixMilli(1700000000000)
	body := []byte(`{
		"foreground": true,
		"expires_at_ms": 4102444800000,
		"expires_in_ms": 999999
	}`)

	normalized, err := normalizeDirexioPushContextAccountData(
		"",
		direxioPushContextAccountDataType,
		body,
		now,
	)
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]interface{}
	if err := json.Unmarshal(normalized, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]interface{}{
		"foreground":    true,
		"expires_at_ms": float64(now.Add(direxioPushContextExpiry).UnixMilli()),
	}
	if len(got) != len(want) {
		t.Fatalf("unexpected normalized content: got %#v want %#v", got, want)
	}
	for key, wantValue := range want {
		if got[key] != wantValue {
			t.Fatalf("unexpected %s: got %#v want %#v", key, got[key], wantValue)
		}
	}
}

func TestNormalizeDirexioPushContextStoresBackgroundWithoutExpiry(t *testing.T) {
	normalized, err := normalizeDirexioPushContextAccountData(
		"",
		direxioPushContextAccountDataType,
		[]byte(`{"foreground": false, "expires_at_ms": 4102444800000}`),
		time.UnixMilli(1700000000000),
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(normalized) != `{"foreground":false}` {
		t.Fatalf("unexpected background context: %s", normalized)
	}
}
