package p2p

import "context"

type socialOnlyStore struct{}

func (socialOnlyStore) UpsertFavorite(context.Context, favoriteRecord) error {
	return nil
}

func (socialOnlyStore) FindFavoriteByEvent(context.Context, string, string) (favoriteRecord, bool, error) {
	return favoriteRecord{}, false, nil
}

func (socialOnlyStore) ListFavorites(context.Context, string) ([]favoriteRecord, error) {
	return nil, nil
}

func (socialOnlyStore) DeleteFavorite(context.Context, int64) error {
	return nil
}

func (socialOnlyStore) UpsertFollow(context.Context, followRecord) error {
	return nil
}

func (socialOnlyStore) ListFollows(context.Context) ([]followRecord, error) {
	return nil, nil
}

func (socialOnlyStore) DeleteFollow(context.Context, string) error {
	return nil
}

var _ socialStore = socialOnlyStore{}
