---
name: direxio-event-state-tracer
description: Trace Direxio server behavior through Matrix events, state, membership, profile data, redactions, sync output, federation, product policy, transport writes, consumers, and projected read models. Use before changing event-driven behavior in roomserver, clientapi, federationapi, syncapi, userapi, appservice, internal/productpolicy, p2p transport/projector/consumer, or multi-node flows.
---

# Direxio Event State Tracer

Use this skill when correctness depends on Matrix events or state becoming visible through another API, node, read model, or client sync.

## Trace Paths

For user/API-originated writes:

1. HTTP route or action handler.
2. Authorization and product policy.
3. Matrix write path: Client-Server route or `p2p.Transport`.
4. Roomserver input and output event.
5. Consumers in `syncapi`, `federationapi`, `userapi`, `appservice`, and Direxio projection.
6. Persistent stores and read models.
7. Client-visible read path: `/sync`, history, search, federation response, Direxio action read, `/_p2p/events`, or CLI command.

For inbound or federated behavior, start at the roomserver/federation output and trace consumers forward to visible state.

## State Rules

- Product room type lives in `m.room.create.content.type` with Direxio room types for direct, group, and channel rooms.
- Product metadata uses `io.direxio.room.profile`.
- Member policy uses `io.direxio.member.policy`.
- Public channel approval uses `io.direxio.join_request`.
- Matrix `m.room.member membership=join` is the joined fact. Approval state is not joined state.
- Malformed optional product metadata must not block unrelated later projection events.
- Non-product Matrix rooms must not pollute Direxio product lists unless the bridge is intentional.
- Profile changes must keep Matrix-facing profile storage and Direxio profile/member views aligned when both are read.

## Behavior Checks

- For membership and invitations, verify owner, requester, local user, remote user, leave/kick/ban, deleted direct-contact recovery, and already-applied idempotent paths. Deleted direct contacts keep the old direct room for recovery, but a peer re-request must stay pending until the deleting side explicitly accepts.
- For redaction, distinguish local hiding from Matrix redaction. Local delete hides for one user; recall/redaction propagates as Matrix redaction.
- For ordinary timeline messages, do not create a second product message source of truth.
- For channel posts, comments, and reactions, verify Matrix event content, projection rows, redaction behavior, and owner history.
- For public channel join, remote approval callbacks must not report joined until the requester node performs the Matrix join successfully.
- For federation, use real users from the compose topology. Do not fabricate remote Matrix users in multi-node tests.

## Test Strategy

- Use unit tests close to the changed consumer, projector, policy, storage, route, or transport path.
- Use `scripts/p2p-three-node-regression.py` for changed cross-node lookup, federation, public join request, remote room join, profile/member propagation, redaction, or projection behavior.
- Inspect Docker logs and mounted runtime files before assuming a code bug when behavior only fails in a running stack.

After editing, run `direxio-targeted-verification`.
