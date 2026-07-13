package realtimews

import (
	"testing"
	"time"
)

func TestConsumeTicketRejectsInactiveAccount(t *testing.T) {
	now := time.Date(2026, 7, 14, 8, 0, 0, 0, time.UTC)
	module := New(Dependencies{
		TicketActive: func(Ticket) bool { return false },
	}, Config{
		Now:      func() time.Time { return now },
		NewToken: func(string) string { return "ticket" },
	})
	ticket := module.IssueTicket(Ticket{Role: "owner"})["ticket"].(string)
	if _, err := module.ConsumeTicket(ticket); err == nil {
		t.Fatal("inactive account ticket was accepted")
	}
}
