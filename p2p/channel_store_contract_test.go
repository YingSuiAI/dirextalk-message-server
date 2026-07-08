package p2p

import "context"

type channelOnlyStore struct{}

func (channelOnlyStore) UpsertChannel(context.Context, channel) error {
	return nil
}

func (channelOnlyStore) DeleteChannel(context.Context, string) error {
	return nil
}

func (channelOnlyStore) ListChannels(context.Context) ([]channel, error) {
	return nil, nil
}

func (channelOnlyStore) GetChannelByIDOrRoom(context.Context, string, string) (channel, bool, error) {
	return channel{}, false, nil
}

func (channelOnlyStore) ListJoinedChannelsForUser(context.Context, string) ([]channel, error) {
	return nil, nil
}

func (channelOnlyStore) SearchPublicChannels(context.Context, string, int) ([]channel, error) {
	return nil, nil
}

func (channelOnlyStore) ListPublicChannelsForOwner(context.Context, string) ([]channel, error) {
	return nil, nil
}

var _ channelStore = channelOnlyStore{}
