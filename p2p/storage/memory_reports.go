package storage

import "context"

func (s *MemoryStore) InsertReport(ctx context.Context, report reportRecord) error {
	report = cloneReport(report)
	s.mu.Lock()
	s.reports[report.ReportID] = report
	s.mu.Unlock()
	return nil
}
