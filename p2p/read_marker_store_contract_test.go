package p2p

import "context"

type readMarkerOnlyStore struct{}

func (readMarkerOnlyStore) SaveReadMarker(context.Context, readMarker) error {
	return nil
}

var _ readMarkerStore = readMarkerOnlyStore{}
