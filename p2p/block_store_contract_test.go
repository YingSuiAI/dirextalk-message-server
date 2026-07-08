package p2p

import "context"

type blockOnlyStore struct{}

func (blockOnlyStore) UpsertBlock(context.Context, blockRecord) error {
	return nil
}

func (blockOnlyStore) DeleteBlock(context.Context, string, string) (bool, error) {
	return false, nil
}

func (blockOnlyStore) ListBlocks(context.Context) ([]blockRecord, error) {
	return nil, nil
}

var _ blockStore = blockOnlyStore{}
