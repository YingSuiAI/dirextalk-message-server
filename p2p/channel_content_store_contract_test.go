package p2p

import "context"

type channelContentOnlyStore struct{}

func (channelContentOnlyStore) InsertChannelPost(context.Context, channelPostStorageRecord) error {
	return nil
}

func (channelContentOnlyStore) GetChannelPostByID(context.Context, string, string) (channelPostStorageRecord, bool, error) {
	return channelPostStorageRecord{}, false, nil
}

func (channelContentOnlyStore) GetChannelPostByEventID(context.Context, string, string) (channelPostStorageRecord, bool, error) {
	return channelPostStorageRecord{}, false, nil
}

func (channelContentOnlyStore) ListChannelPosts(context.Context, string) ([]channelPostStorageRecord, error) {
	return nil, nil
}

func (channelContentOnlyStore) ListChannelPostsPage(context.Context, string, int64, int64, int64, string, int) ([]channelPostStorageRecord, bool, error) {
	return nil, false, nil
}

func (channelContentOnlyStore) InsertChannelComment(context.Context, channelCommentRecord) error {
	return nil
}

func (channelContentOnlyStore) GetChannelCommentByID(context.Context, string, string) (channelCommentRecord, bool, error) {
	return channelCommentRecord{}, false, nil
}

func (channelContentOnlyStore) GetChannelCommentByEventID(context.Context, string, string) (channelCommentRecord, bool, error) {
	return channelCommentRecord{}, false, nil
}

func (channelContentOnlyStore) ListChannelComments(context.Context, string) ([]channelCommentRecord, error) {
	return nil, nil
}

func (channelContentOnlyStore) ListChannelCommentsPage(context.Context, string, int64, int64, int64, string, int) ([]channelCommentRecord, bool, error) {
	return nil, false, nil
}

func (channelContentOnlyStore) DeleteChannelPost(context.Context, string) (bool, error) {
	return false, nil
}

func (channelContentOnlyStore) DeleteChannelComment(context.Context, string) (bool, error) {
	return false, nil
}

var _ channelContentStore = channelContentOnlyStore{}
