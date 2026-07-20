package p2p

import (
	"context"
	"testing"
	"time"
)

type memberSaveOperationKey struct{}

type memberWriteBarrierStore struct {
	Store
	removeUpsertStarted chan struct{}
	releaseRemoveUpsert chan struct{}
	leaveLookupStarted  chan struct{}
}

func (s *memberWriteBarrierStore) LookupMember(ctx context.Context, roomID, userID string) (memberRecord, bool, error) {
	if ctx.Value(memberSaveOperationKey{}) == "leave" {
		select {
		case <-s.leaveLookupStarted:
		default:
			close(s.leaveLookupStarted)
		}
	}
	return s.Store.LookupMember(ctx, roomID, userID)
}

func (s *memberWriteBarrierStore) UpsertMember(ctx context.Context, member memberRecord) error {
	if ctx.Value(memberSaveOperationKey{}) == "remove" {
		close(s.removeUpsertStarted)
		<-s.releaseRemoveUpsert
	}
	return s.Store.UpsertMember(ctx, member)
}

func TestSaveMemberSerializesMergeAndUpsertByMember(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	baseStore := service.store
	initial := memberRecord{
		RoomID: "!room:example.com", ChannelID: "channel", UserID: "@alice:example.com",
		Membership: "join", Role: "member", JoinedAt: 1,
	}
	if err := baseStore.UpsertMember(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	barrier := &memberWriteBarrierStore{
		Store:               baseStore,
		removeUpsertStarted: make(chan struct{}),
		releaseRemoveUpsert: make(chan struct{}),
		leaveLookupStarted:  make(chan struct{}),
	}
	service.store = barrier

	errs := make(chan error, 2)
	removed := initial
	removed.Membership = "remove"
	go func() {
		errs <- service.saveMember(context.WithValue(context.Background(), memberSaveOperationKey{}, "remove"), removed)
	}()
	<-barrier.removeUpsertStarted

	left := initial
	left.Membership = "leave"
	leaveCallStarted := make(chan struct{})
	go func() {
		close(leaveCallStarted)
		errs <- service.saveMember(context.WithValue(context.Background(), memberSaveOperationKey{}, "leave"), left)
	}()
	<-leaveCallStarted
	select {
	case <-barrier.leaveLookupStarted:
		close(barrier.releaseRemoveUpsert)
		t.Fatal("leave read raced ahead of the in-flight remove upsert")
	case <-time.After(50 * time.Millisecond):
	}

	close(barrier.releaseRemoveUpsert)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	stored, ok, err := baseStore.LookupMember(context.Background(), initial.RoomID, initial.UserID)
	if err != nil || !ok || stored.Membership != "remove" {
		t.Fatalf("concurrent remove/leave stored %#v, ok=%v err=%v", stored, ok, err)
	}
}

func TestMergeMemberPersistenceStartsNewRequestGenerationAfterTerminalState(t *testing.T) {
	member := memberRecord{Membership: "invite", JoinedAt: 200}
	mergeMemberPersistence(&member, memberRecord{Membership: "rejected", JoinedAt: 100})
	if member.JoinedAt != 200 {
		t.Fatalf("new invitation generation kept stale timestamp: %#v", member)
	}

	replay := memberRecord{Membership: "joining", JoinedAt: 300}
	mergeMemberPersistence(&replay, memberRecord{Membership: "pending", JoinedAt: 200})
	if replay.JoinedAt != 200 {
		t.Fatalf("same request generation changed timestamp: %#v", replay)
	}
}

func TestMergeMemberPersistenceUsesConfirmedJoinTransitionTime(t *testing.T) {
	joining := memberRecord{Membership: "join", JoinedAt: 300}
	mergeMemberPersistence(&joining, memberRecord{Membership: "invite", JoinedAt: 100})
	if joining.JoinedAt != 300 {
		t.Fatalf("confirmed join kept invitation timestamp: %#v", joining)
	}

	profileUpdate := memberRecord{Membership: "join", JoinedAt: 400}
	mergeMemberPersistence(&profileUpdate, memberRecord{Membership: "joined", JoinedAt: 300})
	if profileUpdate.JoinedAt != 300 {
		t.Fatalf("joined profile update changed join timestamp: %#v", profileUpdate)
	}
}
