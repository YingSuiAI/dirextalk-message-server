package consumers

import (
	"context"
	"crypto/ed25519"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/caching"
	"github.com/YingSuiAI/direxio-message-server/internal/pushgateway"
	"github.com/YingSuiAI/direxio-message-server/internal/sqlutil"
	"github.com/YingSuiAI/direxio-message-server/roomserver"
	"github.com/YingSuiAI/direxio-message-server/roomserver/types"
	"github.com/YingSuiAI/direxio-message-server/setup/jetstream"
	"github.com/YingSuiAI/direxio-message-server/test/testrig"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/bcrypt"

	"github.com/YingSuiAI/direxio-message-server/internal/pushrules"
	rsapi "github.com/YingSuiAI/direxio-message-server/roomserver/api"
	"github.com/YingSuiAI/direxio-message-server/setup/config"
	"github.com/YingSuiAI/direxio-message-server/test"
	"github.com/YingSuiAI/direxio-message-server/userapi/api"
	"github.com/YingSuiAI/direxio-message-server/userapi/storage"
	userAPITypes "github.com/YingSuiAI/direxio-message-server/userapi/types"
)

const (
	direxioIOSAppID     = "io.direxio.app.ios"
	direxioAndroidAppID = "io.direxio.app.android"
	direxioAPNsPushKey  = "apns-device-token"
	direxioFCMPushKey   = "fcm-device-token"
)

type recordingPushGateway struct {
	url  string
	req  *pushgateway.NotifyRequest
	resp pushgateway.NotifyResponse
}

func (g *recordingPushGateway) Notify(ctx context.Context, url string, req *pushgateway.NotifyRequest, resp *pushgateway.NotifyResponse) error {
	g.url = url
	g.req = req
	*resp = g.resp
	return nil
}

func mustCreateDatabase(t *testing.T, dbType test.DBType) (storage.UserDatabase, func()) {
	t.Helper()
	connStr, close := test.PrepareDBConnectionString(t, dbType)
	cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
	db, err := storage.NewUserDatabase(context.Background(), cm, &config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, "", 4, 0, 0, "")
	if err != nil {
		t.Fatalf("failed to create new user db: %v", err)
	}
	return db, func() {
		close()
	}
}

func mustCreateEvent(t *testing.T, content string) *types.HeaderedEvent {
	t.Helper()
	ev, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10).NewEventFromTrustedJSON([]byte(content), false)
	if err != nil {
		t.Fatalf("failed to create event: %v", err)
	}
	return &types.HeaderedEvent{PDU: ev}
}

type FakeUserRoomserverAPI struct{ rsapi.UserRoomserverAPI }

func (f *FakeUserRoomserverAPI) QueryUserIDForSender(ctx context.Context, roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(senderID), true)
}

func TestNotifyHTTPEventIDOnlySendsDirexioIOSAPNsPusherAndReturnsRejectedDevice(t *testing.T) {
	ctx := context.Background()
	gateway := &recordingPushGateway{
		resp: pushgateway.NotifyResponse{Rejected: []string{direxioAPNsPushKey}},
	}
	consumer := OutputRoomEventConsumer{pgClient: gateway}
	event := mustCreateEvent(t, `{
		"type":"m.room.message",
		"room_id":"!room:example.com",
		"sender":"@alice:example.com",
		"content":{"body":"hello","msgtype":"m.text"}
	}`)
	devices := []*pushgateway.Device{
		{
			AppID:   direxioIOSAppID,
			PushKey: direxioAPNsPushKey,
			Data: map[string]interface{}{
				"format":   "event_id_only",
				"provider": "apns",
				"platform": "ios",
			},
		},
	}

	rejected, err := consumer.notifyHTTP(ctx, event, "https://push.direxio.ai/_matrix/push/v1/notify", "event_id_only", devices, "alice", "Direxio", 7)
	if err != nil {
		t.Fatalf("notifyHTTP returned error: %v", err)
	}
	if gateway.url != "https://push.direxio.ai/_matrix/push/v1/notify" {
		t.Fatalf("unexpected push gateway URL: %q", gateway.url)
	}
	if gateway.req == nil {
		t.Fatal("expected push gateway request")
	}
	notification := gateway.req.Notification
	if notification.EventID != event.EventID() {
		t.Fatalf("unexpected event_id: %q", notification.EventID)
	}
	if notification.RoomID != event.RoomID().String() {
		t.Fatalf("unexpected room_id: %q", notification.RoomID)
	}
	if notification.Counts == nil || notification.Counts.Unread != 7 {
		t.Fatalf("unexpected notification counts: %#v", notification.Counts)
	}
	if len(notification.Devices) != 1 {
		t.Fatalf("expected one device, got %d", len(notification.Devices))
	}
	device := notification.Devices[0]
	if device.AppID != direxioIOSAppID || device.PushKey != direxioAPNsPushKey {
		t.Fatalf("unexpected iOS APNs device: %#v", device)
	}
	wantData := map[string]interface{}{
		"format":   "event_id_only",
		"provider": "apns",
		"platform": "ios",
	}
	if !reflect.DeepEqual(device.Data, wantData) {
		t.Fatalf("unexpected iOS APNs device data:\n got: %#v\nwant: %#v", device.Data, wantData)
	}
	if len(rejected) != 1 || rejected[0].AppID != direxioIOSAppID || rejected[0].PushKey != direxioAPNsPushKey {
		t.Fatalf("unexpected rejected devices: %#v", rejected)
	}
}

func TestDeleteRejectedPushersRemovesOnlyRejectedDirexioIOSAppID(t *testing.T) {
	ctx := context.Background()
	localpart := "alice"
	serverName := spec.ServerName("localhost")

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, close := mustCreateDatabase(t, dbType)
		defer close()
		consumer := OutputRoomEventConsumer{db: db}

		for _, pusher := range []struct {
			appID    string
			pushKey  string
			provider string
			platform string
		}{
			{appID: direxioIOSAppID, pushKey: direxioAPNsPushKey, provider: "apns", platform: "ios"},
			{appID: direxioAndroidAppID, pushKey: direxioFCMPushKey, provider: "fcm", platform: "android"},
		} {
			if err := db.UpsertPusher(ctx, api.Pusher{
				Kind:    api.HTTPKind,
				AppID:   pusher.appID,
				PushKey: pusher.pushKey,
				Data: map[string]interface{}{
					"format":   "event_id_only",
					"url":      "https://push.direxio.ai/_matrix/push/v1/notify",
					"provider": pusher.provider,
					"platform": pusher.platform,
				},
			}, localpart, serverName); err != nil {
				t.Fatal(err)
			}
		}

		consumer.deleteRejectedPushers(ctx, []*pushgateway.Device{{
			AppID:   direxioIOSAppID,
			PushKey: direxioAPNsPushKey,
		}}, localpart, serverName)

		pushers, err := db.GetPushers(ctx, localpart, serverName)
		if err != nil {
			t.Fatal(err)
		}
		if len(pushers) != 1 || pushers[0].AppID != direxioAndroidAppID || pushers[0].PushKey != direxioFCMPushKey {
			t.Fatalf("expected only Android pusher to remain, got %#v", pushers)
		}
	})
}

func Test_evaluatePushRules(t *testing.T) {
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, close := mustCreateDatabase(t, dbType)
		defer close()
		consumer := OutputRoomEventConsumer{db: db, rsAPI: &FakeUserRoomserverAPI{}}

		testCases := []struct {
			name         string
			eventContent string
			wantAction   pushrules.ActionKind
			wantActions  []*pushrules.Action
			wantNotify   bool
		}{
			{
				name:         "m.receipt doesn't notify",
				eventContent: `{"type":"m.receipt","room_id":"!room:example.com"}`,
				wantAction:   pushrules.UnknownAction,
				wantActions:  nil,
			},
			{
				name:         "m.reaction doesn't notify",
				eventContent: `{"type":"m.reaction","room_id":"!room:example.com"}`,
				wantAction:   pushrules.UnknownAction,
				wantActions:  []*pushrules.Action{},
			},
			{
				name:         "m.room.message notifies",
				eventContent: `{"type":"m.room.message","room_id":"!room:example.com"}`,
				wantNotify:   true,
				wantAction:   pushrules.NotifyAction,
				wantActions: []*pushrules.Action{
					{Kind: pushrules.NotifyAction},
				},
			},
			{
				name:         "m.room.message highlights",
				eventContent: `{"type":"m.room.message", "content": {"body": "test"},"room_id":"!room:example.com"}`,
				wantNotify:   true,
				wantAction:   pushrules.NotifyAction,
				wantActions: []*pushrules.Action{
					{Kind: pushrules.NotifyAction},
					{
						Kind:  pushrules.SetTweakAction,
						Tweak: pushrules.SoundTweak,
						Value: "default",
					},
					{
						Kind:  pushrules.SetTweakAction,
						Tweak: pushrules.HighlightTweak,
					},
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				actions, err := consumer.evaluatePushRules(ctx, mustCreateEvent(t, tc.eventContent), &localMembership{
					UserID:    "@test:localhost",
					Localpart: "test",
					Domain:    "localhost",
				}, 10)
				if err != nil {
					t.Fatalf("failed to evaluate push rules: %v", err)
				}
				assert.Equal(t, tc.wantActions, actions)
				gotAction, _, err := pushrules.ActionsToTweaks(actions)
				if err != nil {
					t.Fatalf("failed to get actions: %v", err)
				}
				if gotAction != tc.wantAction {
					t.Fatalf("expected action to be '%s', got '%s'", tc.wantAction, gotAction)
				}
				// this is taken from `notifyLocal`
				if tc.wantNotify && gotAction != pushrules.NotifyAction {
					t.Fatalf("expected to notify but didn't")
				}
			})

		}
	})
}

func TestLocalRoomMembers(t *testing.T) {
	alice := test.NewUser(t)
	_, sk, err := ed25519.GenerateKey(nil)
	assert.NoError(t, err)
	bob := test.NewUser(t, test.WithSigningServer("notlocalhost", "ed25519:abc", sk))
	charlie := test.NewUser(t, test.WithSigningServer("notlocalhost", "ed25519:abc", sk))

	room := test.NewRoom(t, alice)
	room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]string{"membership": spec.Join}, test.WithStateKey(bob.ID))
	room.CreateAndInsert(t, charlie, spec.MRoomMember, map[string]string{"membership": spec.Join}, test.WithStateKey(charlie.ID))

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, close := testrig.CreateConfig(t, dbType)
		defer close()

		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		natsInstance := &jetstream.NATSInstance{}
		caches := caching.NewRistrettoCache(8*1024*1024, time.Hour, caching.DisableMetrics)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		db, err := storage.NewUserDatabase(processCtx.Context(), cm, &cfg.UserAPI.AccountDatabase, cfg.Global.ServerName, bcrypt.MinCost, 1000, 1000, "")
		assert.NoError(t, err)

		err = rsapi.SendEvents(processCtx.Context(), rsAPI, rsapi.KindNew, room.Events(), "", "test", "test", nil, false)
		assert.NoError(t, err)

		consumer := OutputRoomEventConsumer{db: db, rsAPI: rsAPI, serverName: "test", cfg: &cfg.UserAPI}
		members, count, err := consumer.localRoomMembers(processCtx.Context(), room.ID)
		assert.NoError(t, err)
		assert.Equal(t, 3, count)
		expectedLocalMember := &localMembership{UserID: alice.ID, Localpart: alice.Localpart, Domain: "test", MemberContent: gomatrixserverlib.MemberContent{Membership: spec.Join}}
		assert.Equal(t, expectedLocalMember, members[0])
	})

}

func TestMessageStats(t *testing.T) {
	type args struct {
		eventType   string
		eventSender string
		roomID      string
	}
	tests := []struct {
		name           string
		args           args
		ourServer      spec.ServerName
		lastUpdate     time.Time
		initRoomCounts map[spec.ServerName]map[string]bool
		wantStats      userAPITypes.MessageStats
	}{
		{
			name:      "m.room.create does not count as a message",
			ourServer: "localhost",
			args: args{
				eventType:   "m.room.create",
				eventSender: "@alice:localhost",
			},
		},
		{
			name:      "our server - message",
			ourServer: "localhost",
			args: args{
				eventType:   "m.room.message",
				eventSender: "@alice:localhost",
				roomID:      "normalRoom",
			},
			wantStats: userAPITypes.MessageStats{Messages: 1, SentMessages: 1},
		},
		{
			name:      "our server - E2EE message",
			ourServer: "localhost",
			args: args{
				eventType:   "m.room.encrypted",
				eventSender: "@alice:localhost",
				roomID:      "encryptedRoom",
			},
			wantStats: userAPITypes.MessageStats{Messages: 1, SentMessages: 1, MessagesE2EE: 1, SentMessagesE2EE: 1},
		},

		{
			name:      "remote server - message",
			ourServer: "localhost",
			args: args{
				eventType:   "m.room.message",
				eventSender: "@alice:remote",
				roomID:      "normalRoom",
			},
			wantStats: userAPITypes.MessageStats{Messages: 2, SentMessages: 1, MessagesE2EE: 1, SentMessagesE2EE: 1},
		},
		{
			name:      "remote server - E2EE message",
			ourServer: "localhost",
			args: args{
				eventType:   "m.room.encrypted",
				eventSender: "@alice:remote",
				roomID:      "encryptedRoom",
			},
			wantStats: userAPITypes.MessageStats{Messages: 2, SentMessages: 1, MessagesE2EE: 2, SentMessagesE2EE: 1},
		},
		{
			name:       "day change creates a new room map",
			ourServer:  "localhost",
			lastUpdate: time.Now().Add(-time.Hour * 24),
			initRoomCounts: map[spec.ServerName]map[string]bool{
				"localhost": {"encryptedRoom": true},
			},
			args: args{
				eventType:   "m.room.encrypted",
				eventSender: "@alice:remote",
				roomID:      "someOtherRoom",
			},
			wantStats: userAPITypes.MessageStats{Messages: 2, SentMessages: 1, MessagesE2EE: 3, SentMessagesE2EE: 1},
		},
	}

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, close := mustCreateDatabase(t, dbType)
		defer close()

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if tt.lastUpdate.IsZero() {
					tt.lastUpdate = time.Now()
				}
				if tt.initRoomCounts == nil {
					tt.initRoomCounts = map[spec.ServerName]map[string]bool{}
				}
				s := &OutputRoomEventConsumer{
					db:         db,
					msgCounts:  map[spec.ServerName]userAPITypes.MessageStats{},
					roomCounts: tt.initRoomCounts,
					countsLock: sync.Mutex{},
					lastUpdate: tt.lastUpdate,
					serverName: tt.ourServer,
				}
				s.storeMessageStats(context.Background(), tt.args.eventType, tt.args.eventSender, tt.args.roomID)
				t.Logf("%+v", s.roomCounts)
				gotStats, activeRooms, activeE2EERooms, err := db.DailyRoomsMessages(context.Background(), tt.ourServer)
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
				if !reflect.DeepEqual(gotStats, tt.wantStats) {
					t.Fatalf("expected %+v, got %+v", tt.wantStats, gotStats)
				}
				if tt.args.eventType == "m.room.encrypted" && activeE2EERooms != 1 {
					t.Fatalf("expected room to be activeE2EE")
				}
				if tt.args.eventType == "m.room.message" && activeRooms != 1 {
					t.Fatalf("expected room to be active")
				}
			})
		}
	})
}

func BenchmarkLocalRoomMembers(b *testing.B) {
	t := &testing.T{}

	cfg, processCtx, close := testrig.CreateConfig(t, test.DBTypePostgres)
	defer close()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	natsInstance := &jetstream.NATSInstance{}
	caches := caching.NewRistrettoCache(8*1024*1024, time.Hour, caching.DisableMetrics)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	db, err := storage.NewUserDatabase(processCtx.Context(), cm, &cfg.UserAPI.AccountDatabase, cfg.Global.ServerName, bcrypt.MinCost, 1000, 1000, "")
	assert.NoError(b, err)

	consumer := OutputRoomEventConsumer{db: db, rsAPI: rsAPI, serverName: "test", cfg: &cfg.UserAPI}
	_, sk, err := ed25519.GenerateKey(nil)
	assert.NoError(b, err)

	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)

	for i := 0; i < 100; i++ {
		user := test.NewUser(t, test.WithSigningServer("notlocalhost", "ed25519:abc", sk))
		room.CreateAndInsert(t, user, spec.MRoomMember, map[string]string{"membership": spec.Join}, test.WithStateKey(user.ID))
	}

	err = rsapi.SendEvents(processCtx.Context(), rsAPI, rsapi.KindNew, room.Events(), "", "test", "test", nil, false)
	assert.NoError(b, err)

	expectedLocalMember := &localMembership{UserID: alice.ID, Localpart: alice.Localpart, Domain: "test", MemberContent: gomatrixserverlib.MemberContent{Membership: spec.Join}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		members, count, err := consumer.localRoomMembers(processCtx.Context(), room.ID)
		assert.NoError(b, err)
		assert.Equal(b, 101, count)
		assert.Equal(b, expectedLocalMember, members[0])
	}
}
