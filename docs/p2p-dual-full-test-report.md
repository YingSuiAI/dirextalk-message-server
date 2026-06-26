# P2P Dual Full Regression Test Report

Date: 2026-06-21

Scope: clean dual-node Docker rebuild plus full `scripts/p2p-dual-smoke.ps1` coverage for contacts, profile/search names, direct messages, groups, channels, moderation, deletion, reapply blocking, kicks, dissolves, posts/comments/reactions, favorites, follows, reports, calls, Agent/MCP auth, and full `p2p.Service.Handle` action coverage.

## Current Status

Status: passed

The dual-node environment was rebuilt from deleted Docker volumes, both nodes became healthy, and the full regression passed.

## Environment Commands

```powershell
$env:P2P_DUAL_PUBLIC_HOST='host.docker.internal'
docker compose -f docker-compose.p2p-dual.yml config --quiet
docker compose -f docker-compose.p2p-dual.yml down -v --remove-orphans
docker compose -f docker-compose.p2p-dual.yml up -d --build --force-recreate dendrite-a dendrite-b
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\p2p-dual-smoke.ps1 -FederationWaitSeconds 90
```

## Findings

### P2P-DUAL-001: Matrix search is disabled in generated dual-node config

Severity: blocker for full test script

Observed:

```text
Matrix search failed on http://127.0.0.1:18008 room=... term=hello matrix smoke ...: The remote server returned an error: (501) Not Implemented.
```

Impact: local-delete search verification cannot run because `/_matrix/client/v3/search` returns 501.

Expected: the dual-node test configuration should allow the full regression script to exercise Matrix search.

Resolution: enabled fulltext search in the generated A/B dual-node configs.

Status: closed

### P2P-DUAL-002: Deleted contact can still send Matrix message in direct room

Severity: functional failure

Observed after temporarily enabling search for diagnosis:

```text
deleted contact room allowed Matrix sending a non-friend message
```

Impact: after `contacts.delete`, ordinary Matrix send in the former direct room is still allowed. This violates the required test scope: deleted-friend messaging must be blocked.

Expected: after contact deletion, Matrix send in that direct room should return 403 or equivalent policy rejection.

Resolution: `contacts.delete` now leaves the Matrix direct room through transport, and accepted contact request mutations no longer pre-delete the accepted contact before the delete path runs.

Status: closed

Second validation after first worker fix:

```text
deleted contact room allowed Matrix sending a non-friend message
```

Runtime evidence on B:

```text
p2p_contacts:
!DO81tlTBeBwl049t:host.docker.internal:18448 | @owner:host.docker.internal:18448 | deleted

p2p_members:
!DO81tlTBeBwl049t:host.docker.internal:18448 | @owner:host.docker.internal:28448 | join | member

syncapi_output_room_events:
id=63 type=m.room.message sender=@owner:host.docker.internal:28448
```

Diagnosis: the script first uses the accepted contact room to cover `contacts.requests.reject` and `contacts.requests.delete`, which stores the contact as `deleted` without making the local Matrix user leave the direct room. A later `contacts.delete` sees the existing deleted status and skips the first worker's `LeaveRoom` path, so Matrix still allows send.

### P2P-DUAL-003: Dual smoke rejects an already accepted contact request

Severity: test-script failure after business guard

Observed after second worker fix:

```text
contacts.requests.reject did not return rejected
```

Impact: the full regression stops before reaching the deleted-contact Matrix send assertion. The script is using the accepted direct contact room to cover `contacts.requests.reject`, but the corrected behavior preserves accepted contacts instead of downgrading them to rejected.

Expected: the smoke script should not require `contacts.requests.reject` to mutate an accepted contact. It should either cover reject/delete on a pending request or assert that request mutation on an accepted contact is non-destructive while still recording action coverage.

Resolution: updated the smoke script to treat request reject/delete on an accepted contact as non-destructive action coverage and to assert that the contact remains accepted before `contacts.delete`.

Status: closed

## Final Validation

Final full smoke output:

```json
{
  "status": "ok",
  "suffix": 1781973803195,
  "contact_room": "!tV2Zzew63h9slNUH:host.docker.internal:18448",
  "channel_id": "ch_channel_c04fef227dd80394",
  "channel_room": "!b5t5vi8rRVnG7tfJ:host.docker.internal:18448",
  "a_member_rows": 2,
  "b_projected_message_rooms": 0,
  "api_actions_checked": 74,
  "p2p_actions_checked": 81,
  "docker_image": "direxio/message-server@sha256:cba43665ef20cbf629072725579c3b8b7645850e16f8c27ee273639e16c17625"
}
```

Commands verified:

```powershell
go test ./p2p -run 'Test(AcceptedContactRequestMutationsDoNotBypassDeleteLeave|ContactDeleteLeavesDirectRoomThroughTransport|ContactAcceptJoinsDirectRoomThroughTransport|ContactRequestCreatesDirectInviteRoomThroughTransport|DeletedContactCannotMessageOrRequestAgain|DeletedContactStillBlocksMessageAndRequestAfterReload)' -count=1
$env:P2P_DUAL_PUBLIC_HOST='host.docker.internal'; docker compose -f docker-compose.p2p-dual.yml config --quiet
git diff --check
$env:P2P_DUAL_PUBLIC_HOST='host.docker.internal'; docker compose -f docker-compose.p2p-dual.yml down -v --remove-orphans
$env:P2P_DUAL_PUBLIC_HOST='host.docker.internal'; docker compose -f docker-compose.p2p-dual.yml up -d --build --force-recreate dendrite-a dendrite-b
$env:P2P_DUAL_PUBLIC_HOST='host.docker.internal'; powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\p2p-dual-smoke.ps1 -FederationWaitSeconds 90
```

## Coverage Reached Before Failure

- Portal auth/setup/profile initialization reached.
- Profile update and user directory nickname/avatar validation reached.
- Friend request projection and acceptance reached.
- Contact remark update reached.
- Public/private channel creation and join request flows reached.
- Remote channel join and Matrix message delivery reached.
- Local delete and recall checks reached when search was enabled.
- Channel posts/comments/reactions and sync projection reached.
- Favorites, calls, follows, and reports reached.
- Group creation/update/invite/join/message reached.

The final run completed through group/channel remove and dissolve, portal password rotation, Matrix key upload checks, and final all-action coverage assertion.

## Acceptance Criteria

- `docker compose -f docker-compose.p2p-dual.yml config --quiet` exits 0.
- Clean rebuild with `down -v` and `up -d --build --force-recreate dendrite-a dendrite-b` succeeds.
- `scripts/p2p-dual-smoke.ps1 -FederationWaitSeconds 90` exits 0 and prints `"status": "ok"`.
- Final report marks all findings closed only after a fresh full dual-node run passes.
