package p2p

import "context"

type reportOnlyStore struct{}

func (reportOnlyStore) InsertReport(context.Context, reportRecord) error {
	return nil
}

var _ reportStore = reportOnlyStore{}
