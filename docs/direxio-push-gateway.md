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

Direxio Message Server extends Matrix event pushes with optional display and routing metadata when the room has Direxio Matrix-native product state. A normal direct/group/text-channel message notification sent to the gateway includes:

```json
{
  "notification": {
    "event_id": "$event:server",
    "room_id": "!room:server",
    "title": "Conversation name",
    "room_type": "direct",
    "push_type": "message",
    "counts": {
      "unread": 1
    },
    "devices": []
  }
}
```

`room_type` is one of `direct`, `group`, or `channel`, derived from `io.direxio.room.profile.room_type` with `m.room.create.content.type` as a fallback. The gateway uses `title` for the visible notification title and sets the visible body to `Send you a new message`. Post-channel rooms, identified by `io.direxio.room.profile.channel_type=post`, are not sent to the HTTP push gateway in this phase.

For Matrix `m.call.invite` events in Direxio rooms, the notification uses `push_type=call` and includes `room_id`, `event_id`, `room_type`, `call_id`, and `call_kind=voice` as flat fields under `notification`. Product `calls.create` / `calls.incoming` actions currently emit P2P events and durable call records; they are not yet a separate HTTP push gateway path unless represented as Matrix call invite events.

## Client Pusher Registration

After login or device-token refresh, the client registers its device token with the local homeserver:

```http
POST /_matrix/client/v3/pushers/set
Authorization: Bearer <access_token>
Content-Type: application/json
```

Direxio HTTP pushers must use the client build identifiers as Matrix `app_id` values: `com.direxio.ai` for Android FCM and `com.direxio.app` for iOS APNs.
Each Matrix user keeps only one active Direxio pusher. Registering a new Android or iOS token replaces the user's previous pusher, even when the new token uses the other platform's `app_id`.

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

## Client Foreground State

The homeserver cannot reliably infer whether a mobile app is foreground or background from `/sync`, read receipts, or pusher registration. Direxio clients should report app lifecycle state with global Matrix account data:

```http
PUT /_matrix/client/v3/user/{userId}/account_data/io.direxio.push.context
Authorization: Bearer <access_token>
Content-Type: application/json
```

```json
{
  "foreground": true,
  "expires_at_ms": 4102444800000
}
```

When `foreground=true` and `expires_at_ms` is in the future, the server suppresses Matrix push-rule notifications for that user: it does not create a new unread notification row and does not call the HTTP push gateway. Missing, malformed, expired, or `foreground=false` state keeps normal background push behavior.

Mobile clients must implement this lifecycle write. While foreground, refresh the account data every 30 seconds and set `expires_at_ms` about 60 seconds in the future to tolerate network jitter. When entering background, immediately write:

```json
{
  "foreground": false
}
```

If the background write is missed because the app is suspended, the previous foreground state naturally expires and pushes resume.

The configured agents room defaults to no system push. During startup or repair, the message server ensures the portal owner has a room-level Matrix push rule for the real `agent_room_id` with empty actions, while preserving any existing explicit rule for that room.

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

Rejected pushkeys are removed from the local user database for the rejected device's `app_id`. If the client later receives a fresh platform token, registering it through `/pushers/set` becomes the user's new sole active pusher.

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
go test ./userapi/util -run "Test(GetPushDevicesPreservesDirexioIOSAPNsPusherData|NotifyUserCountsAsyncSendsLatestDirexioPusherOnly|NotifyUserCountsAsync)" -count=1
go test ./userapi/internal -run "TestPerformPusherSet" -count=1
go build ./cmd/direxio-message-server
```

For end-to-end validation, register a mobile pusher, send a message while the target app is offline, and confirm the app receives a system push then refreshes through `/sync`.
