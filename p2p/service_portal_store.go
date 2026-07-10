package p2p

import "context"

type portalStore interface {
	LoadPortal(ctx context.Context) (portalState, bool, error)
	SavePortal(ctx context.Context, state portalState) error
	SaveClientBuild(ctx context.Context, expectedDeviceID string, build clientBuild) (bool, error)
}

func portalStoreFrom(store Store) portalStore {
	return store
}

func (s *Service) portalStore() portalStore {
	if s.store == nil {
		return nil
	}
	return s.store
}
