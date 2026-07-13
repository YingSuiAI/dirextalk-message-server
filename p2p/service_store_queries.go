package p2p

import (
	"context"
)

func (s *Service) listGroups(ctx context.Context) ([]groupRecord, error) {
	return s.groupsModule.List(ctx)
}

func (s *Service) listChannels(ctx context.Context) ([]channel, error) {
	return s.channelsModule.List(ctx)
}
