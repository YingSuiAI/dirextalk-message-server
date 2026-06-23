# Direxio P2P Action Inventory

Use this reference when a needed API is not exposed as a first-class `direxio` command. Call it with:

```bash
direxio p2p action <action> --params '<json>'
```

Most read actions are safe to call without confirmation. Ask before mutation, moderation, deletion, dissolution, password rotation, permission changes, or recall.

## First-Class CLI Commands

```text
direxio auth status
direxio init
direxio p2p action <action> --params '{}'
direxio p2p apis
direxio p2p sync-bootstrap
direxio contacts list
direxio channels list
direxio channels public-search
direxio groups list
direxio matrix session init
direxio matrix messages send --room-id ROOM --text TEXT
direxio matrix messages list --room-id ROOM --limit 50
direxio matrix sync --timeout 30s
direxio matrix listen
```

## Body Actions

Route hint is `query` for read-style calls and `command` for mutation-style calls. The CLI fallback currently posts to `/_p2p/command`; the service dispatches by `action`.

| Action | Route hint | Area |
| --- | --- | --- |
| `agent.config.get` | `query` | agent |
| `agent.config.update` | `command` | agent |
| `agent.matrix_session.create` | `command` | agent |
| `agent.password` | `query` | agent |
| `agent.status` | `query` | agent |
| `apis.list` | `query` | apis |
| `apis.status` | `command` | apis |
| `calls.active` | `query` | calls |
| `calls.create` | `command` | calls |
| `calls.event` | `command` | calls |
| `calls.get` | `query` | calls |
| `calls.incoming` | `command` | calls |
| `calls.list` | `query` | calls |
| `channels.comment_reaction.toggle` | `command` | channels.reactions |
| `channels.comments.create` | `command` | channels.comments |
| `channels.comments.list` | `query` | channels.comments |
| `channels.comments.recall` | `command` | channels.comments |
| `channels.create` | `command` | channels |
| `channels.dissolve` | `command` | channels |
| `channels.invite` | `command` | channels |
| `channels.invite_grant.create` | `command` | channels |
| `channels.join` | `command` | channels |
| `channels.join_request.approve` | `command` | channels |
| `channels.join_request.reject` | `command` | channels |
| `channels.leave` | `command` | channels |
| `channels.list` | `query` | channels |
| `channels.member.mute` | `command` | channels |
| `channels.member.remove` | `command` | channels |
| `channels.member.unmute` | `command` | channels |
| `channels.members` | `query` | channels |
| `channels.mute` | `command` | channels |
| `channels.my_comments` | `query` | channels |
| `channels.my_reactions` | `query` | channels.reactions |
| `channels.post_reaction.toggle` | `command` | channels.reactions |
| `channels.posts.create` | `command` | channels.posts |
| `channels.posts.list` | `query` | channels.posts |
| `channels.posts.recall` | `command` | channels.posts |
| `channels.public.get` | `query` | channels.public |
| `channels.public.join_request` | `command` | channels.public |
| `channels.public.join_result` | `query` | channels.public |
| `channels.public.search` | `query` | channels.public |
| `channels.read_marker` | `command` | channels |
| `channels.unmute` | `command` | channels |
| `channels.update` | `command` | channels |
| `contacts.delete` | `command` | contacts |
| `contacts.list` | `query` | contacts |
| `contacts.reactivate` | `query` | contacts |
| `contacts.request` | `command` | contacts |
| `contacts.requests.accept` | `command` | contacts.requests |
| `contacts.requests.delete` | `command` | contacts.requests |
| `contacts.requests.reject` | `command` | contacts.requests |
| `contacts.update` | `command` | contacts |
| `conversations.get` | `query` | conversations |
| `conversations.list` | `query` | conversations |
| `favorites.add` | `command` | favorites |
| `favorites.delete` | `command` | favorites |
| `favorites.delete_batch` | `command` | favorites |
| `favorites.list` | `query` | favorites |
| `follows.add` | `command` | follows |
| `follows.list` | `query` | follows |
| `follows.remove` | `command` | follows |
| `groups.create` | `command` | groups |
| `groups.dissolve` | `command` | groups |
| `groups.invite` | `command` | groups |
| `groups.invite.reject` | `command` | groups |
| `groups.invite_policy.update` | `command` | groups |
| `groups.join` | `command` | groups |
| `groups.leave` | `command` | groups |
| `groups.list` | `query` | groups |
| `groups.member.mute` | `command` | groups |
| `groups.member.remove` | `command` | groups |
| `groups.member.unmute` | `command` | groups |
| `groups.members` | `query` | groups |
| `groups.mute` | `command` | groups |
| `groups.unmute` | `command` | groups |
| `groups.update` | `command` | groups |
| `mcp.channel_comments.create` | `command` | mcp |
| `mcp.channel_comments.list` | `query` | mcp |
| `mcp.channel_posts.list` | `query` | mcp |
| `mcp.messages.list` | `query` | mcp |
| `mcp.messages.send` | `command` | mcp |
| `mcp.rooms.search` | `query` | mcp |
| `portal.auth` | `query` | portal |
| `portal.bootstrap` | `query` | portal |
| `portal.password` | `command` | portal |
| `portal.status` | `query` | portal |
| `profile.get` | `query` | profile |
| `profile.update` | `command` | profile |
| `reports.submit` | `command` | reports |
| `sync.bootstrap` | `query` | sync |
| `sync.read_marker` | `command` | sync |
| `users.public_channels` | `query` | users |
