package internal_test

import (
	"context"
	"testing"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/sqlutil"
	"github.com/YingSuiAI/direxio-message-server/setup/config"
	"github.com/YingSuiAI/direxio-message-server/test"
	"github.com/YingSuiAI/direxio-message-server/userapi/api"
	"github.com/YingSuiAI/direxio-message-server/userapi/internal"
	"github.com/YingSuiAI/direxio-message-server/userapi/storage"
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

func TestPerformPusherSetReplacesSameUserAppID(t *testing.T) {
	ctx := context.Background()
	localpart := "alice"
	serverName := spec.ServerName("localhost")
	appID := "com.direxio.ai"

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, closeDB := mustCreateUserDatabase(t, dbType)
		defer closeDB()

		userAPI := &internal.UserInternalAPI{DB: db}
		for _, pushKey := range []string{"old-token", "new-token"} {
			err := userAPI.PerformPusherSet(ctx, &api.PerformPusherSetRequest{
				Pusher: api.Pusher{
					PushKey:           pushKey,
					Kind:              api.HTTPKind,
					AppID:             appID,
					AppDisplayName:    "Direxio",
					DeviceDisplayName: "Android",
					Language:          "zh-CN",
					Data: map[string]interface{}{
						"format": "event_id_only",
						"url":    "https://push.direxio.ai/_matrix/push/v1/notify",
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
				PushKey:           "web-token",
				Kind:              api.HTTPKind,
				AppID:             "io.direxio.app.web",
				AppDisplayName:    "Direxio",
				DeviceDisplayName: "Web",
				Language:          "zh-CN",
				Data: map[string]interface{}{
					"format": "event_id_only",
					"url":    "https://push.direxio.ai/_matrix/push/v1/notify",
				},
			},
			Localpart:  localpart,
			ServerName: serverName,
		}, &struct{}{})
		if err != nil {
			t.Fatalf("PerformPusherSet returned error for other app: %v", err)
		}

		pushers, err := db.GetPushers(ctx, localpart, serverName)
		if err != nil {
			t.Fatalf("GetPushers returned error: %v", err)
		}
		if len(pushers) != 2 {
			t.Fatalf("expected 2 pushers, got %d: %+v", len(pushers), pushers)
		}

		seen := map[string]string{}
		for _, pusher := range pushers {
			seen[pusher.AppID] = pusher.PushKey
		}
		if seen[appID] != "new-token" {
			t.Fatalf("expected Android pusher to use new-token, got %q", seen[appID])
		}
		if seen["io.direxio.app.web"] != "web-token" {
			t.Fatalf("expected other app pusher to remain, got %q", seen["io.direxio.app.web"])
		}
	})
}

func TestPerformPusherSetStoresDirexioIOSAPNsPusherAndKeepsAndroidPusher(t *testing.T) {
	ctx := context.Background()
	localpart := "alice"
	serverName := spec.ServerName("localhost")

	const (
		iosAppID       = "io.direxio.app.ios"
		androidAppID   = "io.direxio.app.android"
		gatewayURL     = "https://push.direxio.ai/_matrix/push/v1/notify"
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
					AppDisplayName:    "Direxio",
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
				AppDisplayName:    "Direxio",
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
		if len(pushers) != 2 {
			t.Fatalf("expected 2 pushers, got %d: %+v", len(pushers), pushers)
		}

		byAppID := map[string]api.Pusher{}
		for _, pusher := range pushers {
			byAppID[pusher.AppID] = pusher
		}

		iosPusher := byAppID[iosAppID]
		if iosPusher.PushKey != newAPNsToken {
			t.Fatalf("expected iOS APNs pusher to rotate to %q, got %q", newAPNsToken, iosPusher.PushKey)
		}
		if iosPusher.Data["url"] != gatewayURL ||
			iosPusher.Data["format"] != expectedFormat ||
			iosPusher.Data["provider"] != "apns" ||
			iosPusher.Data["platform"] != "ios" {
			t.Fatalf("unexpected iOS APNs pusher data: %#v", iosPusher.Data)
		}

		androidPusher := byAppID[androidAppID]
		if androidPusher.PushKey != androidFCMKey {
			t.Fatalf("expected Android pusher to remain, got %q", androidPusher.PushKey)
		}
	})
}
