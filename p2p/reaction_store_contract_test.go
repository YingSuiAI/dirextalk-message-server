package p2p

import "context"

type reactionOnlyStore struct{}

func (reactionOnlyStore) UpsertReaction(context.Context, reactionRecord) error {
	return nil
}

func (reactionOnlyStore) GetReaction(context.Context, string, string, string, string) (reactionRecord, bool, error) {
	return reactionRecord{}, false, nil
}

func (reactionOnlyStore) CountActiveReactions(context.Context, string, string, string) (int64, error) {
	return 0, nil
}

func (reactionOnlyStore) ListReactions(context.Context, string) ([]reactionRecord, error) {
	return nil, nil
}

var _ reactionStore = reactionOnlyStore{}
