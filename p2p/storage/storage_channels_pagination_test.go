package storage

import (
	"reflect"
	"testing"
)

func TestChannelPageQueryPreservesPostAndCommentKeysets(t *testing.T) {
	tests := []struct {
		name        string
		selectSQL   string
		filterField string
		filterValue string
		idField     string
		cursorTS    int64
		cursorID    string
		wantQuery   string
		wantArgs    []any
	}{
		{
			name:        "filtered posts with cursor",
			selectSQL:   "SELECT posts FROM p2p_channel_posts",
			filterField: "channel_id",
			filterValue: "channel-1",
			idField:     "post_id",
			cursorTS:    30,
			cursorID:    "post-3",
			wantQuery:   "SELECT posts FROM p2p_channel_posts WHERE origin_server_ts >= $1 AND origin_server_ts <= $2 AND channel_id = $3 AND (origin_server_ts < $4 OR (origin_server_ts = $4 AND post_id < $5)) ORDER BY origin_server_ts DESC, post_id DESC LIMIT $6",
			wantArgs:    []any{int64(10), int64(40), "channel-1", int64(30), "post-3", 3},
		},
		{
			name:        "unfiltered comments without cursor",
			selectSQL:   "SELECT comments FROM p2p_channel_comments",
			filterField: "post_id",
			idField:     "comment_id",
			wantQuery:   "SELECT comments FROM p2p_channel_comments WHERE origin_server_ts >= $1 AND origin_server_ts <= $2 ORDER BY origin_server_ts DESC, comment_id DESC LIMIT $3",
			wantArgs:    []any{int64(10), int64(40), 3},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			query, args := channelPageQuery(
				testCase.selectSQL,
				testCase.filterField,
				testCase.filterValue,
				testCase.idField,
				10,
				40,
				testCase.cursorTS,
				testCase.cursorID,
				2,
			)
			if query != testCase.wantQuery {
				t.Fatalf("query = %q, want %q", query, testCase.wantQuery)
			}
			if !reflect.DeepEqual(args, testCase.wantArgs) {
				t.Fatalf("args = %#v, want %#v", args, testCase.wantArgs)
			}
		})
	}
}
