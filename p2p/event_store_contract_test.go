package p2p

import "context"

type eventOnlyStore struct{}

func (eventOnlyStore) InsertEvent(context.Context, p2pEvent) (bool, error) {
	return false, nil
}

func (eventOnlyStore) ListEvents(context.Context, int64, int) ([]p2pEvent, error) {
	return nil, nil
}

func (eventOnlyStore) EventBounds(context.Context) (eventBounds, error) {
	return eventBounds{}, nil
}

func (eventOnlyStore) PruneEventsToMaxRows(context.Context, int64) (int64, error) {
	return 0, nil
}

var _ eventStore = eventOnlyStore{}
