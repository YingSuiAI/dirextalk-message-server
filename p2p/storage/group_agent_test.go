package storage

import (
	"context"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
	"github.com/google/uuid"
)

func TestGroupAgentStoreMemory(t *testing.T) { testGroupAgentStore(t, NewMemoryStore()) }
func TestGroupAgentStorePostgresReopen(t *testing.T) {
	ctx := context.Background()
	conn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer cleanup()
	opts := config.DatabaseOptions{ConnectionString: config.DataSource(conn)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, opts), &opts)
	if err != nil {
		t.Fatal(err)
	}
	testGroupAgentStore(t, store)
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, opts), &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	b, found, err := store.GetGroupAgentBinding(ctx, "!group:example.test")
	if err != nil || !found || b.Revision != 3 || !b.Enabled {
		t.Fatalf("reopened binding=%#v found=%t err=%v", b, found, err)
	}
	requests, err := store.ListGroupAgentRequests(ctx, b.OwnerMXID, b.AccountGeneration, 10, "")
	if err != nil || len(requests) != 0 {
		t.Fatalf("old epoch revived after reopen: %#v %v", requests, err)
	}
}

func testGroupAgentStore(t *testing.T, store dirextalkdomain.GroupAgentStore) {
	t.Helper()
	ctx := context.Background()
	b := dirextalkdomain.GroupAgentBinding{RoomID: "!group:example.test", Enabled: true, OwnerMXID: "@owner:example.test", AgentMXID: "@ying:example.test", Revision: 1, AccountGeneration: 7, EnabledAt: time.Now().UnixMilli()}
	if err := store.MutateGroupAgentBinding(ctx, b.RoomID, func(value *dirextalkdomain.GroupAgentBinding) error { *value = b; return nil }); err != nil {
		t.Fatal(err)
	}
	r := dirextalkdomain.GroupAgentRequest{RequestID: uuid.NewString(), RoomID: b.RoomID, EventID: "$request", SenderMXID: "@member:remote.test", OwnerMXID: b.OwnerMXID, AgentMXID: b.AgentMXID, BindingRevision: 1, AccountGeneration: 7, OriginServerTS: b.EnabledAt, Body: "must never be stored"}
	if inserted, err := store.EnqueueGroupAgentRequest(ctx, r); err != nil || !inserted {
		t.Fatalf("enqueue: %t %v", inserted, err)
	}
	if inserted, err := store.EnqueueGroupAgentRequest(ctx, r); err != nil || inserted {
		t.Fatalf("dedupe: %t %v", inserted, err)
	}
	for i := 0; i < 2; i++ {
		rows, err := store.ListGroupAgentRequests(ctx, b.OwnerMXID, 7, 10, "")
		if err != nil || len(rows) != 1 || rows[0].Body != "" {
			t.Fatalf("pull must be body-free at-least-once read: %#v %v", rows, err)
		}
	}
	for _, owner := range []string{"@foreign:example.test", b.OwnerMXID} {
		generation := int64(7)
		if owner == b.OwnerMXID {
			generation++
		}
		rows, err := store.ListGroupAgentRequests(ctx, owner, generation, 10, "")
		if err != nil || len(rows) != 0 {
			t.Fatalf("owner/generation fence: %#v %v", rows, err)
		}
	}
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- store.MutateGroupAgentRequest(ctx, r.RequestID, func(_ dirextalkdomain.GroupAgentBinding, _ *dirextalkdomain.GroupAgentRequest) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	disabled := make(chan error, 1)
	go func() {
		disabled <- store.MutateGroupAgentBinding(ctx, b.RoomID, func(value *dirextalkdomain.GroupAgentBinding) error {
			value.Enabled = false
			value.Revision++
			return nil
		})
	}()
	select {
	case err := <-disabled:
		t.Fatalf("revocation did not serialize with publication: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if err := <-disabled; err != nil {
		t.Fatal(err)
	}
	stored, found, err := store.GetGroupAgentRequest(ctx, r.RequestID)
	if err != nil || !found || stored.Status != "cancelled" {
		t.Fatalf("queue not cancelled: %#v %v", stored, err)
	}
	if err = store.MutateGroupAgentBinding(ctx, b.RoomID, func(value *dirextalkdomain.GroupAgentBinding) error {
		value.Enabled = true
		value.Revision++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if inserted, err := store.EnqueueGroupAgentRequest(ctx, r); err != nil || inserted {
		t.Fatalf("old revision accepted: %t %v", inserted, err)
	}
}
