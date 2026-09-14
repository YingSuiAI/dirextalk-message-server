package dendrite

import (
	"fmt"
	"strings"
	"testing"
)

func TestGroupAgentMentionUserIDsPrefersStandardMentions(t *testing.T) {
	standard := groupAgentMentionUserIDs(
		[]string{"@ying:example.test"},
		[]legacyGroupAgentMention{{UserID: "@legacy:example.test"}},
		`[{"user_id":"@json:example.test"}]`,
	)
	if len(standard) != 1 || standard[0] != "@ying:example.test" {
		t.Fatalf("standard mentions = %#v", standard)
	}
}

func TestGroupAgentMentionUserIDsAcceptsLegacyClientFields(t *testing.T) {
	cases := map[string]struct {
		legacy []legacyGroupAgentMention
		raw    string
		want   []string
	}{
		"legacy array": {
			legacy: []legacyGroupAgentMention{{UserID: "@ying:example.test"}, {UserID: "  "}},
			want:   []string{"@ying:example.test"},
		},
		"legacy json": {
			raw:  `[{"user_id":"@ying:example.test","display_name":"Ying"}]`,
			want: []string{"@ying:example.test"},
		},
		"unparsable json": {raw: `{"user_id":"@ying:example.test"}`, want: nil},
		"oversized json":  {raw: "[" + strings.Repeat(`{"user_id":"@ying:example.test"},`, 400) + `{"user_id":"@ying:example.test"}]`, want: nil},
		"absent":          {want: nil},
	}
	for name, tc := range cases {
		got := groupAgentMentionUserIDs(nil, tc.legacy, tc.raw)
		if len(got) != len(tc.want) {
			t.Fatalf("%s: mentions = %#v", name, got)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("%s: mentions = %#v", name, got)
			}
		}
	}
}

func TestGroupAgentMentionUserIDsBoundsLegacyEntries(t *testing.T) {
	legacy := make([]legacyGroupAgentMention, 0, legacyGroupAgentMentionLimit+10)
	for i := 0; i < legacyGroupAgentMentionLimit+10; i++ {
		legacy = append(legacy, legacyGroupAgentMention{UserID: fmt.Sprintf("@user%d:example.test", i)})
	}
	got := groupAgentMentionUserIDs(nil, legacy, "")
	if len(got) != legacyGroupAgentMentionLimit {
		t.Fatalf("bounded mentions = %d", len(got))
	}
}
