# Direxio Push Gateway Integration

Direxio uses the Matrix Push Gateway API for offline device notifications. The push gateway itself lives in the separate `push-gateway` project. This Direxio Message Server fork only needs to keep Matrix pusher registration and notification delivery compatible with that gateway.

## Runtime Flow

```text
Matrix room event on the recipient homeserver
  -> userapi notification evaluation
  -> userapi pusher lookup
  -> POST /_matrix/push/v1/notify on the configured gateway URL
  -> APNs / FCM / Huawei provider delivery
  -> app wakes and fetches /_matrix/client/v3/sync
```

The gateway should default to Matrix `event_id_only` behavior. Push payloads are wake-up hints, not a message storage or sync channel. Clients must fetch message and call details from their own homeserver after receiving a system push.

## Client Pusher Registration

After login or device-token refresh, the client registers its device token with the local homeserver:

```http
POST /_matrix/client/v3/pushers/set
Authorization: Bearer <access_token>
Content-Type: application/json
```

Direxio HTTP pushers must use the client build identifiers as Matrix `app_id` values: `com.direxio.ai` for Android FCM and `com.direxio.app` for iOS APNs.

```json
{
  "kind": "http",
  "app_id": "com.direxio.app",
  "app_display_name": "Direxio",
  "device_display_name": "iPhone",
  "pushkey": "<apns-or-fcm-device-token>",
  "lang": "en",
  "data": {
    "url": "https://push.direxio.ai/_matrix/push/v1/notify",
    "format": "event_id_only"
  }
}
```

Use a regional gateway URL when required, for example `https://push-eu.direxio.ai/_matrix/push/v1/notify` or `https://push-sea.direxio.ai/_matrix/push/v1/notify`.

## Server Responsibilities

- Ordinary chat messages remain Matrix-native. Do not add a second P2P message push path.
- `userapi/consumers/roomserver.go` handles event notifications and removes pushers rejected by the gateway.
- `userapi/util/notify.go` sends unread-count refreshes and also removes rejected pushers.
- The Push Gateway must return Matrix-compatible responses:

```json
{
  "rejected": ["<expired-or-invalid-device-token>"]
}
```

Rejected pushkeys are removed from the local user database for that `app_id`.

## Push Gateway Project

The standalone gateway should provide:

- `POST /_matrix/push/v1/notify`
- `GET /healthz`
- `GET /readyz`
- `GET /metrics`
- APNs and FCM provider configuration
- optional Huawei Push Kit provider for HMS devices
- no message-body persistence
- delivery logs limited to request ID, app ID, provider, status, latency, and provider error code

The first implementation can be based on Sygnal, then branded and configured as Direxio Push Gateway.

## Local Verification

Use an HTTPS test server or the standalone gateway's development mode as the pusher `data.url`, then run:

```powershell
go test ./userapi -run TestNotifyUserCountsAsync -count=1
go build ./cmd/dendrite
```

For end-to-end validation, register a mobile pusher, send a message while the target app is offline, and confirm the app receives a system push then refreshes through `/sync`.
