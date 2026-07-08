package util_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/syncapi/synctypes"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"golang.org/x/crypto/bcrypt"

	"github.com/YingSuiAI/dirextalk-message-server/internal/pushgateway"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
	"github.com/YingSuiAI/dirextalk-message-server/userapi/api"
	"github.com/YingSuiAI/dirextalk-message-server/userapi/storage"
	userUtil "github.com/YingSuiAI/dirextalk-message-server/userapi/util"
)

const (
	dirextalkIOSAppID        = "com.direxio.app"
	dirextalkAndroidAppID    = "com.dirextalk.app"
	dirextalkPushGatewayURL  = "https://push.dirextalk.ai/_matrix/push/v1/notify"
	dirextalkAPNsPushKey     = "apns-device-token"
	dirextalkFCMPushKey      = "fcm-device-token"
	dirextalkPusherDataURL   = "url"
	dirextalkPusherFormatKey = "format"
)

func queryUserIDForSender(senderID spec.SenderID) (*spec.UserID, error) {
	if senderID == "" {
		return nil, nil
	}

	return spec.NewUserID(string(senderID), true)
}

func mustCreateUserDatabase(t *testing.T, ctx context.Context, dbType test.DBType) (storage.UserDatabase, func()) {
	t.Helper()

	connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
	cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
	db, err := storage.NewUserDatabase(ctx, cm, &config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, "test", bcrypt.MinCost, 0, 0, "")
	if err != nil {
		t.Fatalf("failed to create user database: %v", err)
	}
	return db, closeDB
}

func TestGetPushDevicesPreservesDirextalkIOSAPNsPusherData(t *testing.T) {
	ctx := context.Background()
	alice := test.NewUser(t)
	aliceLocalpart, serverName, err := gomatrixserverlib.SplitID('@', alice.ID)
	if err != nil {
		t.Fatal(err)
	}

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, closeDB := mustCreateUserDatabase(t, ctx, dbType)
		defer closeDB()

		pusherData := map[string]interface{}{
			dirextalkPusherDataURL:   dirextalkPushGatewayURL,
			dirextalkPusherFormatKey: "event_id_only",
			"provider":               "apns",
			"platform":               "ios",
			"default_payload": map[string]interface{}{
				"aps": map[string]interface{}{
					"content-available": float64(1),
				},
			},
		}
		if err := db.UpsertPusher(ctx, api.Pusher{
			Kind:              api.HTTPKind,
			AppID:             dirextalkIOSAppID,
			AppDisplayName:    "Dirextalk",
			DeviceDisplayName: "iPhone",
			PushKey:           dirextalkAPNsPushKey,
			PushKeyTS:         12345,
			Language:          "zh-CN",
			Data:              pusherData,
		}, aliceLocalpart, serverName); err != nil {
			t.Fatal(err)
		}

		devices, err := userUtil.GetPushDevices(ctx, aliceLocalpart, serverName, map[string]interface{}{"sound": "default"}, db)
		if err != nil {
			t.Fatal(err)
		}
		if len(devices) != 1 {
			t.Fatalf("expected one push device, got %d", len(devices))
		}

		got := devices[0]
		if got.URL != dirextalkPushGatewayURL {
			t.Fatalf("unexpected gateway URL: %q", got.URL)
		}
		if got.Format != "event_id_only" {
			t.Fatalf("unexpected pusher format: %q", got.Format)
		}
		if got.Device.AppID != dirextalkIOSAppID {
			t.Fatalf("unexpected app_id: %q", got.Device.AppID)
		}
		if got.Device.PushKey != dirextalkAPNsPushKey {
			t.Fatalf("unexpected APNs pushkey: %q", got.Device.PushKey)
		}
		if _, ok := got.Device.Data[dirextalkPusherDataURL]; ok {
			t.Fatalf("push gateway device data must not include data.url: %#v", got.Device.Data)
		}
		wantData := map[string]interface{}{
			dirextalkPusherFormatKey: "event_id_only",
			"provider":               "apns",
			"platform":               "ios",
			"default_payload": map[string]interface{}{
				"aps": map[string]interface{}{
					"content-available": float64(1),
				},
			},
		}
		if !reflect.DeepEqual(got.Device.Data, wantData) {
			t.Fatalf("unexpected push gateway device data:\n got: %#v\nwant: %#v", got.Device.Data, wantData)
		}
	})
}

func TestNotifyUserCountsAsync(t *testing.T) {
	alice := test.NewUser(t)
	aliceLocalpart, serverName, err := gomatrixserverlib.SplitID('@', alice.ID)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()

	// Create a test room, just used to provide events
	room := test.NewRoom(t, alice)
	dummyEvent := room.Events()[len(room.Events())-1]

	appID := util.RandomString(8)
	pushKey := util.RandomString(8)

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		receivedRequest := make(chan bool, 1)
		// create a test server which responds to our /notify call
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var data pushgateway.NotifyRequest
			if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
				t.Error(err)
			}
			notification := data.Notification
			// Validate the request
			if notification.Counts == nil {
				t.Fatal("no unread notification counts in request")
			}
			if unread := notification.Counts.Unread; unread != 1 {
				t.Errorf("expected one unread notification, got %d", unread)
			}

			if len(notification.Devices) == 0 {
				t.Fatal("expected devices in request")
			}

			// We only created one push device, so access it directly
			device := notification.Devices[0]
			if device.AppID != appID {
				t.Errorf("unexpected app_id: %s, want %s", device.AppID, appID)
			}
			if device.PushKey != pushKey {
				t.Errorf("unexpected push_key: %s, want %s", device.PushKey, pushKey)
			}

			// Return empty result, otherwise the call is handled as failed
			if _, err := w.Write([]byte("{}")); err != nil {
				t.Error(err)
			}
			close(receivedRequest)
		}))
		defer srv.Close()

		// Create DB and Dendrite base
		connStr, close := test.PrepareDBConnectionString(t, dbType)
		defer close()
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		db, err := storage.NewUserDatabase(ctx, cm, &config.DatabaseOptions{
			ConnectionString: config.DataSource(connStr),
		}, "test", bcrypt.MinCost, 0, 0, "")
		if err != nil {
			t.Error(err)
		}

		// Prepare pusher with our test server URL
		if err = db.UpsertPusher(ctx, api.Pusher{
			Kind:    api.HTTPKind,
			AppID:   appID,
			PushKey: pushKey,
			Data: map[string]interface{}{
				"url": srv.URL,
			},
		}, aliceLocalpart, serverName); err != nil {
			t.Error(err)
		}

		// Insert a dummy event
		ev, err := synctypes.ToClientEvent(dummyEvent, synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return queryUserIDForSender(senderID)
		})
		if err != nil {
			t.Error(err)
		}
		if err := db.InsertNotification(ctx, aliceLocalpart, serverName, dummyEvent.EventID(), 0, nil, &api.Notification{
			Event: *ev,
		}); err != nil {
			t.Error(err)
		}

		// Notify the user about a new notification
		if err := userUtil.NotifyUserCountsAsync(ctx, pushgateway.NewHTTPClient(true), aliceLocalpart, serverName, db); err != nil {
			t.Error(err)
		}
		select {
		case <-time.After(time.Second * 5):
			t.Error("timed out waiting for response")
		case <-receivedRequest:
		}
	})

}

func TestNotifyUserCountsAsyncRemovesRejectedPusher(t *testing.T) {
	alice := test.NewUser(t)
	aliceLocalpart, serverName, err := gomatrixserverlib.SplitID('@', alice.ID)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()

	room := test.NewRoom(t, alice)
	dummyEvent := room.Events()[len(room.Events())-1]

	appID := util.RandomString(8)
	pushKey := util.RandomString(8)

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		receivedRequest := make(chan bool, 1)
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var data pushgateway.NotifyRequest
			if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
				t.Error(err)
			}
			if len(data.Notification.Devices) != 1 {
				t.Fatalf("expected one device, got %d", len(data.Notification.Devices))
			}
			if got := data.Notification.Devices[0].PushKey; got != pushKey {
				t.Fatalf("unexpected push_key: %s", got)
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(pushgateway.NotifyResponse{Rejected: []string{pushKey}}); err != nil {
				t.Error(err)
			}
			close(receivedRequest)
		}))
		defer srv.Close()

		connStr, close := test.PrepareDBConnectionString(t, dbType)
		defer close()
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		db, err := storage.NewUserDatabase(ctx, cm, &config.DatabaseOptions{
			ConnectionString: config.DataSource(connStr),
		}, "test", bcrypt.MinCost, 0, 0, "")
		if err != nil {
			t.Error(err)
		}

		if err = db.UpsertPusher(ctx, api.Pusher{
			Kind:    api.HTTPKind,
			AppID:   appID,
			PushKey: pushKey,
			Data: map[string]interface{}{
				"url": srv.URL,
			},
		}, aliceLocalpart, serverName); err != nil {
			t.Error(err)
		}

		ev, err := synctypes.ToClientEvent(dummyEvent, synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return queryUserIDForSender(senderID)
		})
		if err != nil {
			t.Error(err)
		}
		if err := db.InsertNotification(ctx, aliceLocalpart, serverName, dummyEvent.EventID(), 0, nil, &api.Notification{
			Event: *ev,
		}); err != nil {
			t.Error(err)
		}

		if err := userUtil.NotifyUserCountsAsync(ctx, pushgateway.NewHTTPClient(true), aliceLocalpart, serverName, db); err != nil {
			t.Error(err)
		}
		select {
		case <-time.After(time.Second * 5):
			t.Fatal("timed out waiting for response")
		case <-receivedRequest:
		}

		deadline := time.Now().Add(5 * time.Second)
		for {
			pushers, err := db.GetPushers(ctx, aliceLocalpart, serverName)
			if err != nil {
				t.Fatal(err)
			}
			if len(pushers) == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("expected rejected pusher to be removed, got %#v", pushers)
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

func TestNotifyUserCountsAsyncSendsLatestDirextalkPusherOnly(t *testing.T) {
	alice := test.NewUser(t)
	aliceLocalpart, serverName, err := gomatrixserverlib.SplitID('@', alice.ID)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()

	room := test.NewRoom(t, alice)
	dummyEvent := room.Events()[len(room.Events())-1]

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		receivedAndroidRequest := make(chan bool, 1)
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var data pushgateway.NotifyRequest
			if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
				t.Error(err)
			}
			if len(data.Notification.Devices) != 1 {
				t.Fatalf("expected one device per push gateway request, got %d", len(data.Notification.Devices))
			}
			device := data.Notification.Devices[0]
			if device.AppID != dirextalkAndroidAppID {
				t.Fatalf("unexpected app_id: %q", device.AppID)
			}
			if device.PushKey != dirextalkFCMPushKey {
				t.Fatalf("unexpected Android FCM pushkey: %q", device.PushKey)
			}
			if device.Data["provider"] != "fcm" || device.Data["platform"] != "android" {
				t.Fatalf("unexpected Android push gateway data: %#v", device.Data)
			}
			if _, err := w.Write([]byte("{}")); err != nil {
				t.Error(err)
			}
			close(receivedAndroidRequest)
		}))
		defer srv.Close()

		db, closeDB := mustCreateUserDatabase(t, ctx, dbType)
		defer closeDB()

		if err = db.UpsertPusher(ctx, api.Pusher{
			Kind:              api.HTTPKind,
			AppID:             dirextalkIOSAppID,
			AppDisplayName:    "Dirextalk",
			DeviceDisplayName: "iPhone",
			PushKey:           dirextalkAPNsPushKey,
			Data: map[string]interface{}{
				dirextalkPusherDataURL:   srv.URL,
				dirextalkPusherFormatKey: "event_id_only",
				"provider":               "apns",
				"platform":               "ios",
			},
		}, aliceLocalpart, serverName); err != nil {
			t.Error(err)
		}
		if err = db.UpsertPusher(ctx, api.Pusher{
			Kind:              api.HTTPKind,
			AppID:             dirextalkAndroidAppID,
			AppDisplayName:    "Dirextalk",
			DeviceDisplayName: "Android",
			PushKey:           dirextalkFCMPushKey,
			Data: map[string]interface{}{
				dirextalkPusherDataURL:   srv.URL,
				dirextalkPusherFormatKey: "event_id_only",
				"provider":               "fcm",
				"platform":               "android",
			},
		}, aliceLocalpart, serverName); err != nil {
			t.Error(err)
		}

		ev, err := synctypes.ToClientEvent(dummyEvent, synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return queryUserIDForSender(senderID)
		})
		if err != nil {
			t.Error(err)
		}
		if err := db.InsertNotification(ctx, aliceLocalpart, serverName, dummyEvent.EventID(), 0, nil, &api.Notification{
			Event: *ev,
		}); err != nil {
			t.Error(err)
		}

		if err := userUtil.NotifyUserCountsAsync(ctx, pushgateway.NewHTTPClient(true), aliceLocalpart, serverName, db); err != nil {
			t.Error(err)
		}
		select {
		case <-time.After(time.Second * 5):
			t.Fatal("timed out waiting for Android push gateway request")
		case <-receivedAndroidRequest:
		}

		pushers, err := db.GetPushers(ctx, aliceLocalpart, serverName)
		if err != nil {
			t.Fatal(err)
		}
		if len(pushers) != 1 || pushers[0].AppID != dirextalkAndroidAppID || pushers[0].PushKey != dirextalkFCMPushKey {
			t.Fatalf("expected latest Android pusher to remain, got %#v", pushers)
		}
	})
}
