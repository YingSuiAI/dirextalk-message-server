package action

import (
	"context"
	"testing"
	"time"
)

func TestSettlementContextDetachesClientDeadlineWithoutExtendingNestedSettlement(t *testing.T) {
	clientCtx, cancelClient := context.WithTimeout(context.Background(), time.Second)
	defer cancelClient()
	clientDeadline, _ := clientCtx.Deadline()

	settlementCtx, cancelSettlement := SettlementContext(clientCtx)
	defer cancelSettlement()
	settlementDeadline, ok := settlementCtx.Deadline()
	if !ok || !settlementDeadline.After(clientDeadline) {
		t.Fatalf("settlement deadline = %v, want detached deadline after client deadline %v", settlementDeadline, clientDeadline)
	}

	nestedCtx, cancelNested := SettlementContext(settlementCtx)
	defer cancelNested()
	nestedDeadline, ok := nestedCtx.Deadline()
	if !ok || !nestedDeadline.Equal(settlementDeadline) {
		t.Fatalf("nested settlement deadline = %v, want %v", nestedDeadline, settlementDeadline)
	}

	shortCtx, cancelShort := context.WithTimeout(settlementCtx, time.Second)
	defer cancelShort()
	shortDeadline, _ := shortCtx.Deadline()
	nestedShortCtx, cancelNestedShort := SettlementContext(shortCtx)
	defer cancelNestedShort()
	nestedShortDeadline, ok := nestedShortCtx.Deadline()
	if !ok || !nestedShortDeadline.Equal(shortDeadline) {
		t.Fatalf("nested short settlement deadline = %v, want earliest %v", nestedShortDeadline, shortDeadline)
	}
}
