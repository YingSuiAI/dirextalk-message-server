# Dirextalk Push Gateway Integration

Dirextalk uses the Matrix Push Gateway API for offline device notifications. The push gateway itself lives in the separate `push-gateway` project. This Dirextalk Message Server fork only needs to keep Matrix pusher registration and notification delivery compatible with that gateway.

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

Dirextalk Message Server extends Matrix event pushes with optional display and routing metadata when the room has Dirextalk Matrix-native product state. A normal direct/group/text-channel message notification sent to the gateway includes:

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

`room_type` is one of `direct`, `group`, or `channel`, derived from `io.dirextalk.room.profile.room_type` with `m.room.create.content.type` as a fallback. The gateway uses `title` for the visible notification title and sets the visible body to `Send you a new message`. Post-channel rooms, identified by `io.dirextalk.room.profile.channel_type=post`, are not sent to the HTTP push gateway in this phase.

For Matrix `m.call.invite` events in Dirextalk rooms, the notification uses `push_type=call` and includes `room_id`, `event_id`, `room_type`, `call_id`, and `call_kind=voice` as flat fields under `notification`. Product `calls.create` / `calls.incoming` actions currently emit P2P events and durable call records; they are not yet a separate HTTP push gateway path unless represented as Matrix call invite events.

## Client Pusher Registration

After login or device-token refresh, the client registers its device token with the local homeserver:

```http
POST /_matrix/client/v3/pushers/set
Authorization: Bearer <access_token>
Content-Type: application/json
```

Dirextalk HTTP pushers must use the client build identifiers as Matrix `app_id` values: `com.dirextalk.ai` for Android FCM and `com.dirextalk.app` for iOS APNs.
Each Matrix user keeps only one active Dirextalk pusher. Registering a new Android or iOS token replaces the user's previous pusher, even when the new token uses the other platform's `app_id`.

```json
{
  "kind": "http",
  "app_id": "com.dirextalk.app",
  "app_display_name": "Dirextalk",
  "device_display_name": "iPhone",
  "pushkey": "<apns-or-fcm-device-token>",
  "lang": "en",
  "data": {
    "url": "https://push.dirextalk.ai/_matrix/push/v1/notify",
    "format": "event_id_only"
  }
}
```

Use a regional gateway URL when required, for example `https://push-eu.dirextalk.ai/_matrix/push/v1/notify` or `https://push-sea.dirextalk.ai/_matrix/push/v1/notify`.

## Client Foreground And Focus State

The homeserver cannot reliably infer whether a mobile app is foreground or background from `/sync`, read receipts, or pusher registration. Current Dirextalk clients should report app lifecycle and focused room over `GET /_p2p/ws` after creating a `realtime.ws_ticket.create` ticket:

```json
{
  "type": "client.lifecycle",
  "foreground": true,
  "state": "resumed",
  "hidden": false,
  "flags": {
    "foreground": true,
    "background": false,
    "hidden": false
  }
}
```

```json
{
  "type": "client.focus",
  "room_id": "!room:server",
  "focused": true,
  "flags": {
    "focused": true
  }
}
```

A fresh foreground, non-hidden WS session suppresses Matrix push-rule notifications only for the same focused room: the server does not create a new unread notification row and does not call the HTTP push gateway for that room. Missing focus, different-room focus, hidden, background, disconnected, or expired session state keeps normal background push behavior. WS session freshness is stamped with server time and expires after 60 seconds.

During migration, clients without a fresh WS session may still report a coarse foreground fallback with global Matrix account data:

```http
PUT /_matrix/client/v3/user/{userId}/account_data/io.dirextalk.push.context
Authorization: Bearer <access_token>
Content-Type: application/json
```

```json
{
  "foreground": true
}
```

When `foreground=true` is written and no fresh WS session exists for the user, the server stamps the account data with a 60-second expiry based on the server clock. While that stamped fallback is fresh, the server suppresses Matrix push-rule notifications for that user. Missing, malformed, expired, or `foreground=false` state keeps normal background push behavior.

Mobile clients should prefer WS lifecycle/focus reporting. During migration they may continue this lifecycle write: while foreground, write `{"foreground": true}` immediately and refresh the account data every 30 seconds. When entering background, immediately write:

```json
{
  "foreground": false
}
```

If the background write is missed because the app is suspended, the previous foreground state naturally expires after 60 seconds and pushes resume.

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

The first implementation can be based on Sygnal, then branded and configured as Dirextalk Push Gateway.

## Local Verification

Use an HTTPS test server or the standalone gateway's development mode as the pusher `data.url`, then run:

```powershell
go test ./userapi/util -run "Test(GetPushDevicesPreservesDirextalkIOSAPNsPusherData|NotifyUserCountsAsyncSendsLatestDirextalkPusherOnly|NotifyUserCountsAsync)" -count=1
go test ./userapi/internal -run "TestPerformPusherSet" -count=1
go build ./cmd/dirextalk-message-server
```

For end-to-end validation, register a mobile pusher, send a message while the target app is offline, and confirm the app receives a system push then refreshes through `/sync`.
