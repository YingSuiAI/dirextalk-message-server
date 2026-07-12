package p2p

import "testing"

func TestMCPChannelRecordPaginationSharesStableKeysetSemantics(t *testing.T) {
	page := mcpMessagePage{FromTS: 10, SnapshotTS: 30, CursorTS: 20, CursorID: "b", Limit: 2}

	posts, postsMore := mcpPagePostRecords([]channelPostRecord{
		{PostID: "outside_new", OriginServerTS: 31},
		{PostID: "b", OriginServerTS: 20},
		{PostID: "a", OriginServerTS: 20},
		{PostID: "newer_old", OriginServerTS: 19},
		{PostID: "older", OriginServerTS: 18},
		{PostID: "outside_old", OriginServerTS: 9},
	}, page)
	if !postsMore || len(posts) != 2 || posts[0].PostID != "a" || posts[1].PostID != "newer_old" {
		t.Fatalf("posts = %#v, hasMore=%v", posts, postsMore)
	}

	comments, commentsMore := mcpPageCommentRecords([]channelCommentRecord{
		{CommentID: "outside_new", OriginServerTS: 31},
		{CommentID: "b", OriginServerTS: 20},
		{CommentID: "a", OriginServerTS: 20},
		{CommentID: "newer_old", OriginServerTS: 19},
		{CommentID: "older", OriginServerTS: 18},
		{CommentID: "outside_old", OriginServerTS: 9},
	}, page)
	if !commentsMore || len(comments) != 2 || comments[0].CommentID != "a" || comments[1].CommentID != "newer_old" {
		t.Fatalf("comments = %#v, hasMore=%v", comments, commentsMore)
	}
}
