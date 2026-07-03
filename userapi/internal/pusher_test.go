package internal_test

import (
	"context"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
	"github.com/YingSuiAI/dirextalk-message-server/userapi/api"
	"github.com/YingSuiAI/dirextalk-message-server/userapi/internal"
	"github.com/YingSuiAI/dirextalk-message-server/userapi/storage"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"golang.org/x/crypto/bcrypt"
)

func mustCreateUserDatabase(t *testing.T, dbType test.DBType) (storage.UserDatabase, func()) {
	t.Helper()

	connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
	cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
	db, err := storage.NewUserDatabase(context.Background(), cm, &config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, "localhost", bcrypt.MinCost, time.Minute.Milliseconds(), time.Minute, "_server")
	if err != nil {
		t.Fatalf("failed to create new user db: %v", err)
	}
	return db, closeDB
}

func TestPerformPusherSetReplacesSameUserPushers(t *testing.T) {
	ctx := context.Background()
	localpart := "alice"
	otherLocalpart := "bob"
	serverName := spec.ServerName("localhost")
	androidAppID := "com.dirextalk.ai"
	iosAppID := "com.dirextalk.app"

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, closeDB := mustCreateUserDatabase(t, dbType)
		defer closeDB()

		userAPI := &internal.UserInternalAPI{DB: db}
		for _, pushKey := range []string{"old-token", "new-token"} {
			err := userAPI.PerformPusherSet(ctx, &api.PerformPusherSetRequest{
				Pusher: api.Pusher{
					PushKey:           pushKey,
					Kind:              api.HTTPKind,
					AppID:             androidAppID,
					AppDisplayName:    "Dirextalk",
					DeviceDisplayName: "Android",
					Language:          "zh-CN",
					Data: map[string]interface{}{
						"format": "event_id_only",
						"url":    "https://push.dirextalk.ai/_matrix/push/v1/notify",
					},
				},
				Localpart:  localpart,
				ServerName: serverName,
			}, &struct{}{})
			if err != nil {
				t.Fatalf("PerformPusherSet returned error: %v", err)
			}
		}

		err := userAPI.PerformPusherSet(ctx, &api.PerformPusherSetRequest{
			Pusher: api.Pusher{
				PushKey:           "ios-token",
				Kind:              api.HTTPKind,
				AppID:             iosAppID,
				AppDisplayName:    "Dirextalk",
				DeviceDisplayName: "iPhone",
				Language:          "zh-CN",
				Data: map[string]interface{}{
					"format": "event_id_only",
					"url":    "https://push.dirextalk.ai/_matrix/push/v1/notify",
				},
			},
			Localpart:  localpart,
			ServerName: serverName,
		}, &struct{}{})
		if err != nil {
			t.Fatalf("PerformPusherSet returned error for other app: %v", err)
		}

		err = userAPI.PerformPusherSet(ctx, &api.PerformPusherSetRequest{
			Pusher: api.Pusher{
				PushKey:           "bob-token",
				Kind:              api.HTTPKind,
				AppID:             androidAppID,
				AppDisplayName:    "Dirextalk",
				DeviceDisplayName: "Android",
				Language:          "zh-CN",
				Data: map[string]interface{}{
					"format": "event_id_only",
					"url":    "https://push.dirextalk.ai/_matrix/push/v1/notify",
				},
			},
			Localpart:  otherLocalpart,
			ServerName: serverName,
		}, &struct{}{})
		if err != nil {
			t.Fatalf("PerformPusherSet returned error for other user: %v", err)
		}

		pushers, err := db.GetPushers(ctx, localpart, serverName)
		if err != nil {
			t.Fatalf("GetPushers returned error: %v", err)
		}
		if len(pushers) != 1 {
			t.Fatalf("expected 1 pusher, got %d: %+v", len(pushers), pushers)
		}

		if pushers[0].AppID != iosAppID || pushers[0].PushKey != "ios-token" {
			t.Fatalf("expected latest iOS pusher only, got %+v", pushers[0])
		}

		otherPushers, err := db.GetPushers(ctx, otherLocalpart, serverName)
		if err != nil {
			t.Fatalf("GetPushers returned error for other user: %v", err)
		}
		if len(otherPushers) != 1 || otherPushers[0].PushKey != "bob-token" {
			t.Fatalf("expected other user's pusher to remain, got %+v", otherPushers)
		}
	})
}

func TestPerformPusherSetStoresLatestDirextalkPusherData(t *testing.T) {
	ctx := context.Background()
	localpart := "alice"
	serverName := spec.ServerName("localhost")

	const (
		iosAppID       = "com.dirextalk.app"
		androidAppID   = "com.dirextalk.ai"
		gatewayURL     = "https://push.dirextalk.ai/_matrix/push/v1/notify"
		oldAPNsToken   = "old-apns-device-token"
		newAPNsToken   = "new-apns-device-token"
		androidFCMKey  = "android-fcm-device-token"
		expectedFormat = "event_id_only"
	)

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, closeDB := mustCreateUserDatabase(t, dbType)
		defer closeDB()

		userAPI := &internal.UserInternalAPI{DB: db}
		for _, pushKey := range []string{oldAPNsToken, newAPNsToken} {
			err := userAPI.PerformPusherSet(ctx, &api.PerformPusherSetRequest{
				Pusher: api.Pusher{
					PushKey:           pushKey,
					Kind:              api.HTTPKind,
					AppID:             iosAppID,
					AppDisplayName:    "Dirextalk",
					DeviceDisplayName: "iPhone",
					Language:          "zh-CN",
					Data: map[string]interface{}{
						"format":   expectedFormat,
						"url":      gatewayURL,
						"provider": "apns",
						"platform": "ios",
					},
				},
				Localpart:  localpart,
				ServerName: serverName,
			}, &struct{}{})
			if err != nil {
				t.Fatalf("PerformPusherSet returned error for iOS APNs pusher: %v", err)
			}
		}

		err := userAPI.PerformPusherSet(ctx, &api.PerformPusherSetRequest{
			Pusher: api.Pusher{
				PushKey:           androidFCMKey,
				Kind:              api.HTTPKind,
				AppID:             androidAppID,
				AppDisplayName:    "Dirextalk",
				DeviceDisplayName: "Android",
				Language:          "zh-CN",
				Data: map[string]interface{}{
					"format":   expectedFormat,
					"url":      gatewayURL,
					"provider": "fcm",
					"platform": "android",
				},
			},
			Localpart:  localpart,
			ServerName: serverName,
		}, &struct{}{})
		if err != nil {
			t.Fatalf("PerformPusherSet returned error for Android FCM pusher: %v", err)
		}

		pushers, err := db.GetPushers(ctx, localpart, serverName)
		if err != nil {
			t.Fatalf("GetPushers returned error: %v", err)
		}
		if len(pushers) != 1 {
			t.Fatalf("expected 1 pusher, got %d: %+v", len(pushers), pushers)
		}

		latestPusher := pushers[0]
		if latestPusher.AppID != androidAppID || latestPusher.PushKey != androidFCMKey {
			t.Fatalf("expected latest Android pusher only, got %+v", latestPusher)
		}
		if latestPusher.Data["url"] != gatewayURL ||
			latestPusher.Data["format"] != expectedFormat ||
			latestPusher.Data["provider"] != "fcm" ||
			latestPusher.Data["platform"] != "android" {
			t.Fatalf("unexpected latest pusher data: %#v", latestPusher.Data)
		}
	})
}
