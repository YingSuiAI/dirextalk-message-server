# Dirextalk Message Server 当前项目文档

本文是当前代码与接口的事实源。历史变更记录只用于审计，不作为当前接口或实现依据。

## 1. 项目定位

本仓库是 Dirextalk 对 Element Dendrite 的集成式 fork：同一个 Go monolith 同时提供 Matrix homeserver 能力和 Dirextalk P2P 产品 API。

当前架构原则：

- Matrix 事件与房间状态是好友、群、频道、成员、普通消息的事实源。
- P2P action 是产品语义 facade，负责鉴权、参数校验、远端转发、Matrix 写入编排和投影读取。
- P2P 数据表保留为 projection/read model，不作为成员关系和普通消息的最终事实源。
- 产品代码不得直接写 Matrix SQL 底表；房间、成员、消息、redaction 等 Matrix 行为必须通过 `p2p.Transport` 或 Matrix Client-Server API 进入 Dirextalk Message Server。

## 2. 对外入口

Matrix 协议入口保持 Dirextalk Message Server 原有路径：

- `/_matrix/*`
- `/_synapse/*`
- `/_dendrite/*`
- `/.well-known/matrix/*`

Dirextalk 产品 API 以 body-action surface 为主；标准 MCP 客户端使用单独的 Streamable HTTP endpoint：

- `GET /_p2p/health`
- `POST /_p2p/query`
- `POST /_p2p/command`
- `POST /mcp`
- `GET /_p2p/ws`
- `GET /.well-known/portal/owner.json`

请求 envelope：

```json
{
  "action": "channels.public.get",
  "params": {
    "room_id": "!room:dendrite-a:8448",
    "remote_node_base_url": "https://dendrite-a:8448/_p2p"
  }
}
```

Protected action 通过 HTTP route 调用时需要 `Authorization: Bearer <access_token>`。登录后的客户端 product action 在 WS 已收到 `server.ready` 时优先走 `GET /_p2p/ws` 上的 `client.request`/`server.response`；点击时 WS 未 ready 或已断线时，当前 action 立即用 `POST /_p2p/query` 或 `POST /_p2p/command` 作为 owner HTTP fallback，同时 realtime WS 在后台继续重连。已发出 WS request 后响应丢失时，只对可安全重复的 action 做 HTTP fallback。`portal.account.delete` 是 owner `access_token` 保护的 HTTP-only 注销命令，不能通过 WS 调用。`realtime.ws_ticket.create` 只接受 owner `access_token` 创建 owner WS ticket。`agent_token` 只允许通过 product body-action 访问 `agent.matrix_session.create`，并可访问标准 `POST /mcp` MCP endpoint，不能通过 HTTP fallback 调用 owner product action。固定 `mcp.*` HTTP body action 已从 `/_p2p/query` 和 `/_p2p/command` 删除；外部 MCP 客户端必须使用 `POST /mcp` JSON-RPC。标准 MCP endpoint 与 `agent.matrix_session.create` 不迁移到 WS `client.request`。`GET /_p2p/ws` 只接受短期单次 owner WS ticket，不直接接受 bearer token。当前 public action 是：

- `portal.bootstrap`
- `portal.auth`
- `portal.status`
- `contacts.reactivate`
- `rooms.reactivate`
- `reports.submit`
- `channels.public.search`
- `channels.public.get`
- `channels.public.join_request`
- `channels.public.join_result`
- `users.public_channels`

Action auth and transport metadata is generated from `p2p/serviceapi.ActionSpecs` into `docs/product-action-contract.json`; contract-critical docs and clients should treat that generated file as the checkable action list.

`portal.bootstrap`、`portal.auth`、`portal.password` 响应只暴露一个初始化状态：`initialized`。它只表示用户是否已通过 `portal.password` 修改过初始密码；profile 是否填写不影响该状态。`portal.account.delete` 要求 `params.confirm="delete_account"`，会在清库前向 accepted direct contacts 发布带 `account_deleted` 的 `io.dirextalk.room.profile` 解散状态，让对端隐藏已注销联系人；随后退出直聊、解散 owner 创建的群聊和频道、退出 owner 只是成员的群聊/频道、停用本地 owner/agent Matrix 账号并写入非密钥 deprovision 标记。若关键退出/解散/停用步骤失败，不清库。该动作只清理本机数据库并关闭 message-server 进程，不销毁 AWS/云服务器实例。

`rooms.reactivate` 与 `channels.public.join_result` 是 HTTP-only 节点间回调，不是 WS `client.request` 或客户端常规入口。`rooms.reactivate` 只用于在群/私有频道成员节点重建后恢复对方节点上的邀请/待加入提示，不能让对方静默加入；最终加入仍由对方客户端调用 `groups.join` 或 `channels.join`。

Plugin management is owner-only and stays on the same body-action surface for non-Agent plugins. `plugins.catalog.list`、`plugins.installed.list`、`plugins.install`、`plugins.enable`、`plugins.disable`、`plugins.uninstall`、`plugins.config.get`、`plugins.config.update`、`plugins.job.get`、`plugins.health`、`plugins.logs.tail`、`plugins.invoke`、`plugins.invoke.stream` 都需要 owner `access_token`；`agent_token` 不能调用这些 action。`io.dirextalk.agent` is no longer exposed through plugin catalog/list/lifecycle/config/invoke/health/logs. Ops and future non-Agent Docker plugins only appear and run when the Docker plugin runner is enabled. Non-Agent Docker plugins still must use official catalog metadata whose Docker image belongs to the `dirextalk` Docker Hub organization; digest is optional audit/rollback metadata.

Native Agent is the message-server embedded runtime behind first-class owner `agent.*` actions. Clients call `agent.chat`、`agent.models.list`、`agent.runtime.inspect/install/which/run`、`agent.skills.*`、`agent.mcp.*`、`agent.context.compress`、`agent.config.propose_patch` and built-in Dirextalk tool actions directly through `/query` or `/command` with owner `access_token`; `agent_token` is limited to `agent.matrix_session.create` through the product body-action surface and `POST /mcp`. Model provider, API URL, model ID, parameters, and API key remain request-scoped Native Agent inputs, commonly under `params.model_profile`; `agent.models.list` uses the request-scoped provider/base URL/API key to fetch supported providers' real model list and returns `models[]`, while the message-server must not persist, return, or inject model API keys into plugin/runtime env. Streaming Native Agent chat uses owner realtime WS `client.native_agent_stream` and `client.native_agent_stream.cancel`, returning `server.native_agent_stream.event/error/cancelled`. This Native stream is independent of the real Matrix `agent_room_id`; Online Agent bridge messages still use Matrix Client-Server APIs and are not mirrored through Native stream frames. Native Agent runtime config is stored in the native portal Agent config JSON, including shared display/avatar settings, `mcp_blocked_room_ids`, skills, MCP server metadata, runtime tool metadata, memory settings, and model profiles without API key fields. Startup performs an idempotent sanitized import from old hidden `io.dirextalk.agent` plugin config when present. Runtime CLI files, installed skill files, MCP working data, and memory artifacts stay under `P2P_NATIVE_AGENT_DATA_DIR`. Agent knowledge action names remain reserved and return unsupported in the first version.

当前 x1 本机部署固定使用单节点 `docker-compose.p2p.yml` 的 `message-server` 容器，单节点 compose 默认启用 Docker plugin runner、挂载 `/var/run/docker.sock`，插件后端内网地址为 `http://message-server:8008`。`dendrite-a`、`dendrite-b`、`dendrite-c` 只属于三节点回归环境，不作为 x1 实际服务入口，也不安装或承载插件。

官方 Ops 插件 `io.dirextalk.ops` 面向单机私有部署运维，动作包括 `ops.status.get`、`ops.containers.list`、`ops.logs.tail`、`ops.backups.list`、`ops.backup.create`、`ops.backup.status`、`ops.backup.download_chunk`、`ops.backup.delete`、`ops.cleanup.plan`、`ops.cleanup.run`、`ops.rooms.cleanup.plan`、`ops.rooms.cleanup.run`、`ops.media.orphans.plan`、`ops.migration.export`、`ops.restore.plan`、`ops.restore.run`。Ops 是唯一允许由 Docker runner 挂载 Docker socket 和专用备份 volume 的官方插件；启用时注入 `OPS_BACKUP_ROOT`、`OPS_MAX_BACKUPS`、`OPS_MESSAGE_SERVER_CONTAINER`、`OPS_POSTGRES_CONTAINER`、`OPS_POSTGRES_USER`、`OPS_POSTGRES_PASSWORD`。备份创建可异步返回任务并通过 `ops.backup.status` 轮询进度；备份下载通过 `ops.backup.download_chunk` 分片返回，客户端本地保存文件。`ops.restore.run` 必须显式传入 `confirm="restore_backup"`，用于从已有备份包恢复 Postgres dump。第一版清理必须先 plan 后 confirm：聊天记录清理只做本地缓存、隐藏/归档计划和受控安全操作，不允许 Ops 插件直接 SQL 删除 Matrix 事件表；媒体清理默认只清缓存或明确孤儿文件，仍被消息/频道引用的媒体不删除。

## 3. 运行时结构

核心入口：

- `cmd/dirextalk-message-server`：生产服务入口，monolith 模式运行。
- `setup/monolith.go`：装配 client、federation、media、sync、relay、P2P routes。
- `p2p/action_registry.go`：P2P action 到业务 handler 的注册表。
- `p2p/service_*.go`：P2P 业务编排。
- `p2p/storage`：P2P projection/read model 持久化。
- `internal/dirextalktransport`：产品 Matrix 写入 transport contract。
- `internal/dirextalktransport/dendrite`：真实 Matrix roomserver 写入适配层；`p2p/dendrite_transport.go` 仅保留 facade 构造入口。
- `internal/dirextalkmatrix`：Matrix Client-Server HTTP profile/history reader；`p2p/matrix_profile_resolver.go`、`p2p/matrix_history_reader.go` 和 `p2p/matrixhistory` 仅保留 facade/compatibility aliases。
- `internal/dirextalkprojection`：projection-only helper，例如成员 joined/pending 统计；P2P action 和 conversation view 只调用该 helper，不复制计算逻辑。
- `internal/dirextalkstate`：产品 Matrix state event content builder，例如 `io.dirextalk.room.profile`、`io.dirextalk.member.policy`、`io.dirextalk.join_request`；P2P action 仍负责决定何时通过 transport 发布。
- `internal/dirextalkdomain`：跨包共享的产品 value records 和纯 domain helper，例如 portal/agent config、conversation records、member/channel records、blocks、calls、favorites、reports、P2P event bounds 等；`p2p/domain` 保留带 `Operation`/`ConversationView` 的 response-shaped facade 类型和兼容 alias。
- `internal/dirextalkplugin`：非 Agent plugin catalog/instance/job/secret record shapes；`p2p` 继续拥有 plugin action orchestration、Docker runner 和 Native Agent/plugin 隔离规则。
- `p2p/projector_*.go`、`p2p/projection`：roomserver output 到 P2P projection 的投影。
- `p2p/consumer.go`：订阅 roomserver 输出并调用 projector。
- `internal/productpolicy`：Matrix Client-Server 写入前的 Dirextalk 产品策略校验。

生产持久化优先使用全局 Dirextalk Message Server 数据库配置；未配置时 P2P store 会回退到 roomserver 数据库。Docker 开发栈使用 PostgreSQL 18。

## 4. Matrix Native State

当前产品房间只使用 native Dirextalk state：

- `m.room.create.content.type`
  - `io.dirextalk.room.direct`
  - `io.dirextalk.room.group`
  - `io.dirextalk.room.channel`
- `io.dirextalk.room.profile`
  - direct/group/channel 的产品元数据。
- `io.dirextalk.member.policy`
  - role、mute 等成员策略。
- `io.dirextalk.join_request`
  - public channel 申请审批状态。
- 新建 group room 会在创建时写 `m.room.history_visibility=joined`，新成员只从自己的 Matrix join 之后接收普通 timeline 消息。新建 channel room 是统一的帖子+聊天频道，创建频道或将已有 room 绑定为频道时显式写 `m.room.history_visibility=shared`，成员需要能看到当前频道已有的所有帖子和评论。`channel_type` 是旧字段兼容元数据，创建后不可修改，`channels.update` 会忽略旧客户端传入的 `channel_type`，当前频道行为不再按 `chat`/`post` 分流。当前规则不回溯修改已有房间。

投影规则：

- `io.dirextalk.room.profile` 投影到 groups/channels read model。
- direct invite 的 `io.dirextalk.room.profile` stripped state 投影为 inbound contact request，但联系人身份以 Matrix membership event 的真实 sender 为准；`requester_mxid`、`domain` 或 profile 展示字段不能把申请伪造成另一个用户。
- `io.dirextalk.member.policy` 投影成员角色与禁言。
- `io.dirextalk.join_request` 投影申请审批状态。
- Matrix `m.room.member membership=join` 是最终 joined 事实。
- 普通 Matrix timeline 不复制到 P2P 普通消息表；普通消息读写走 Matrix Client-Server API。配置的 agents room 也保持 Matrix-native：本地 bridge 使用 `@agent:<server>` Matrix session 通过 `/sync` 接收消息，通过 Matrix send/edit 写入预览和最终回复，不投影为 `agent_room.message`，也不使用 `client.agent_stream` 或 `server.agent_stream`。

## 5. 用户请求生命周期

P2P action 生命周期：

1. 登录后客户端在 WS 已收到 `server.ready` 时通过 `GET /_p2p/ws` 发送 `client.request`；点击时 WS 未 ready 或断线时，同一 `{ "action": "...", "params": ... }` envelope 立即通过 HTTP `/query` 或 `/command` 作为 owner fallback，realtime WS 后台重连恢复事件流。portal/auth/password/account-delete、WS ticket、`POST /mcp`、`agent.matrix_session.create`、public/callback action 仍按各自 HTTP/WS 边界执行；固定 `mcp.*` body action 已删除。
2. route 或 WS request 处理器调用 `Service.Authorize`：
   - public action 直接放行；
   - protected action 校验 owner access token；`agent_token` 仅允许 product body-action `agent.matrix_session.create` 和标准 `POST /mcp`。
3. `Service.Handle` 分发到对应业务函数。
4. 业务函数校验参数、所有者/成员/策略权限。
5. 需要 Matrix 事实写入时调用 `p2p.Transport`。
6. Dirextalk Message Server roomserver 产生 output event。
7. `p2p.consumer` 调用 `ProjectRoomEvent` 更新 P2P read model。
8. `/_p2p/ws` 发送产品投影事件和通用 `server.response`。Owner WS 通过 `client.request` 执行登录后 product 查询/命令，但不包含 MCP action；旧 `client.command` 兼容别名已移除，客户端必须发送 `client.request`。Agents room 消息、预览和回复走 Matrix Client-Server，不通过 P2P event 或 WS stream 转发。
9. 客户端普通消息、历史、搜索、redaction 继续通过 Matrix Client-Server API。

同步策略：

- `sync.bootstrap` 是冷启动、登录后恢复、本地缓存不可用或事件缺口兜底用的基线快照；不要在每个事件后全量刷新。
- 日常弱网/断线恢复使用 `GET /_p2p/ws` 增量追平。客户端先通过 `realtime.ws_ticket.create` 创建 ticket，连接后发送 `client.hello` 的 `since=<last_seq>`，并持久保存最后处理的 `seq`，对已知事件类型做本地 reducer 更新；只有遇到未知事件、解析失败、缺口无法确认或本地缓存损坏时才拉一次 `sync.bootstrap`。WS ready 时可通过 `client.request` 拉取；WS 不可用时可通过 owner HTTP fallback 立即拉取。
- 如果 `since` 是非零旧 cursor 且已经早于服务端保留的 `p2p_events` 最小序号，WS 会先发送 `server.cursor_reset`。控制事件 payload 包含 `type`、`since`、`min_seq`、`max_seq`、`count`、`recovery: "bootstrap_required"`；客户端收到后应清理本地产品缓存，优先通过 WS `client.request` 调用一次 `sync.bootstrap`，WS 不 ready 时可用 owner HTTP fallback 拉取，再用最新 `seq` 继续订阅增量。

Matrix Client-Server 写入生命周期：

1. 客户端调用 Matrix send/state/member/redaction API。
2. Dirextalk product policy 读取当前 Matrix state。
3. 如果房间是 Dirextalk product room，则校验 dissolved、member join、mute、role、join policy 等规则。
4. 合法事件进入 Dirextalk Message Server 原生 roomserver。
5. roomserver output 再投影回 P2P read model。

## 6. 频道公开申请生命周期

客户端可见状态统一为：

- `pending`
- `rejected`
- `approved`
- `joining`
- `joined`
- `join_failed`

`channels.public.join_request`：

1. 申请人节点先在本地保存 `pending` projection。
2. 如果频道属于远端 room server，申请人节点把申请转发给频道主节点。
3. 频道主节点写 `io.dirextalk.join_request status=pending`。
4. 频道主节点 projection 中成员为 `pending`。

`channels.join_request.reject`：

1. 频道主节点写 `io.dirextalk.join_request status=rejected`。
2. 本地 projection 更新为 `reject`。
3. 如果申请人是远端用户，频道主节点调用申请人节点的 `channels.public.join_result`。
4. 申请人节点更新为 `rejected` 并发送 P2P event。

`channels.join_request.approve`：

1. 频道主节点写 `io.dirextalk.join_request status=approved`。
2. 如果申请人属于本节点，频道主节点调用 `Transport.JoinRoom`。
3. 如果申请人属于远端节点，频道主节点调用申请人节点的 `channels.public.join_result`。
4. 申请人节点以申请人身份调用 `Transport.JoinRoom`，并带上 `server_names`。
5. Matrix `membership=join` 成功后，projection 才进入 `join`。
6. join 失败时 projection 为 `join_failed`，不得返回或投影成 joined。

公开 open channel 与审批通过走同一套自动 join 流程。Matrix invite 可以作为底层协议事件存在于其他邀请场景，但公开频道申请审批不把 invite 暴露成产品流程。

频道主节点不得直接把远端用户写成 joined；远端用户 join 必须由该用户所在 homeserver 发起。

## 7. 业务结构

Portal/Profile：

- 默认启动时自动初始化 portal owner、owner token、agent token、默认密码和 owner profile。
- `P2P_PORTAL_PASSWORD` 可覆盖默认密码。
- `P2P_PORTAL_CREDENTIALS_FILE` 用于启动、密码变更和 session token 变更后的 credential JSON 写出。
- `portal.bootstrap`、`portal.auth`、`portal.password` 创建新的 portal owner Matrix session 后，会删除该 owner 的其他 Matrix devices，只保留本次登录 device；旧设备后续 Matrix 请求应收到 `M_UNKNOWN_TOKEN` 并回到手动登录。`agent.matrix_session.create` 是本地 bridge bootstrap action，可由 owner `access_token` 或 `agent_token` 调用，返回本地 `@agent:<server>` 的 Matrix session，不删除用户手机 device。
- profile update 同步 P2P profile/member projection，并写入 Matrix-facing profile storage。

Contacts：

- 发起联系人请求会创建 direct Matrix room，并邀请对方。
- inbound/outbound request 来自 Matrix invite/member projection。
- accept 通过 Matrix join 进入 direct room。
- `contacts.update` 设置的是 owner 本地联系人备注名；返回的 contact 可带 `display_name_override=true`。该标记存在时，后续远端 Matrix member display name 更新不能覆盖 contact `display_name`，但 avatar 仍可按远端 profile 更新。没有本地备注名的 accepted contact 继续跟随远端 Matrix member display name。
- delete 后保留原 direct room 身份用于恢复。删除方主动重新添加时，如果对方仍保留 accepted 关系，可以通过 `contacts.reactivate` 复用旧房间；如果请求方本地联系人数据被清理并创建了新的 direct invite，而目标方仍保留 accepted 旧关系，目标方优先重新邀请真实 sender 回旧房间，不采纳新房间里的伪造身份资料。如果清库重建导致旧 invite-only direct room 无法重新加入，则双方改用真实 sender 创建的新 direct room 作为 accepted 关系，旧房间历史不会复制到新房间。双方都已离开旧房间或对端不再保留旧关系时，再次申请会创建新的 direct request room；对历史遗留的旧 room pending request，accept 如果无法 rejoin 旧房间，也会创建新的 direct room 并接受关系。
- 群聊和私有频道成员节点清库重建后，群主/频道主再次邀请该成员时，如果 Matrix 侧显示成员仍在旧房间，owner 节点会先移除该 stale joined membership，再发送新的真实 Matrix invite，并调用成员节点 `rooms.reactivate` 恢复 pending invite/card；成员节点不能静默加入，必须由用户点击后调用 `groups.join` 或 `channels.join`，Matrix join 成功后才写 joined 投影。公开频道不使用 `rooms.reactivate`，重建成员应重新调用 `channels.public.join_request`，并继续遵守 open/approval policy；如果 owner 节点仍保留该公开频道成员的 stale joined membership，owner 节点会先移除并发送新的 Matrix invite，再通过 `channels.public.join_result` 让申请节点完成 join。
- reject/delete 只改变产品 projection 与对应 Matrix leave/kick 行为，不制造普通消息副本；如果 Matrix membership 已经是 leave，`contacts.delete` 仍按幂等删除处理并更新产品 projection。

Blocks：

- `blocks.add`、`blocks.list`、`blocks.remove` 是 owner protected action，用于管理当前用户拉黑的联系人，不提供群聊或频道拉黑。
- 联系人拉黑使用 `target_type=contact` 与 `peer_mxid`/`mxid`；`target_type=group/channel/room` 不是当前产品能力，应返回参数错误。
- 每条黑名单记录保存 `display_name` 与 `avatar_url` 展示快照；客户端没有传昵称时，服务端从现有好友资料回填，仍为空则回退到目标 id，避免黑名单只展示 id。
- `blocks.list` 只返回 `contacts` 列表，供用户设置页展示；客户端可在好友设置页调用 `blocks.add`，在黑名单列表中调用 `blocks.remove` 取消拉黑。
- 对已拉黑联系人发起好友申请或邀请已拉黑用户加入群聊/频道时，服务端在 Matrix 写入前返回 `403 already blocked`，客户端应提示“已经拉黑”。
- 被拉黑联系人对应的 inbound Matrix direct invite 不会投影成 pending 好友申请。

Groups：

- group create 写 Matrix room type 与 `io.dirextalk.room.profile`。
- invite/join/leave/remove/mute/unmute/dissolve 通过 `p2p.Transport` 与 native state 进入 Matrix。
- member list 来自 P2P projection，但最终事实是 Matrix membership。
- 群聊和频道只有 `owner` 与 `member` 两种产品角色。

Channels：

- channel create/update 写 Matrix room type 与 `io.dirextalk.room.profile`。
- public search/get 是只读发现，不创建占位记录。
- invite grant 用于私有或分享卡片加入。
- public join request 使用上面的申请审批自动 join 生命周期。
- channel member、mute、read marker、dissolve 都保持 Matrix-first。
- 频道 `is_owned`、管理能力和发帖能力只来自 `owner` 角色。

Channel posts/comments/reactions：

- 仍是产品内容 projection。
- 使用 Matrix `m.room.message` 携带 `p2p_kind=channel_post` 或 `p2p_kind=channel_comment`。
- reaction 使用 Matrix reaction/内容字段投影到 P2P reaction read model；点赞开关事件携带 `active`，因此取消点赞也会覆盖到其他节点的 read model。
- 新成员加入 channel 后，服务端会从 Matrix `/messages` 历史回填当前频道已有 posts/comments/reactions 到本节点 projection，客户端可通过 product list 接口和 Matrix history 同时看到入群前内容。普通聊天消息仍走 Matrix timeline，不写入帖子/评论 projection。
- recall 通过 Matrix redaction。

Calls/Favorites/Follows：

- calls 是产品会话 read model，支持 create/incoming/get/list/active/event，持久化接通/结束时间、结束方和原因，并通过 `call.changed` P2P event 推送实时状态。
- favorites 和 follows 是 P2P product state，使用 P2P store 持久化。好友举报和官方举报仍走 signed imadmin public API；群聊/频道所有者举报通过 ProductCore `reports.submit` 写入 owner 节点 `p2p_reports`，并向 `system_room_id` 发送 `msg_type=report` 的 Matrix 通知。

Push：

- 系统推送仍使用 Matrix Push Gateway API。客户端用 `/pushers/set` 注册 APNs/FCM pusher，普通 direct/group 消息、call invite 等通知由 `userapi/consumers/roomserver.go` 按 Matrix push rules 评估后发送到 gateway。所有 channel room 事件不投递 HTTP Push Gateway。
- 服务端不能从 `/sync`、read receipt 或 pusher 注册可靠判断 App 是否处于前台。Dirextalk 客户端通过 `GET /_p2p/ws` 上报 `client.lifecycle` 和 `client.focus`：`client.lifecycle` 至少包含 `foreground`，并可携带 `state`、`hidden` 和 `flags`；`client.focus` 至少包含 `room_id`，并可携带 `focused` 和 `flags`。前台、未 hidden、且 focused room 等于收到消息的 room 时，服务端不新增 unread notification，也不调用 HTTP push gateway；后台、hidden、断线、60 秒会话过期、未聚焦或聚焦到其他 room 时继续按后台推送处理。迁移期保留全局 Matrix account data `io.dirextalk.push.context` 作为无新鲜 WS session 时的兜底，服务端按服务端时间保存 60 秒过期时间。

Agent/API：

- Agent token 不再有动态权限表，只能通过 product body-action 访问 `agent.matrix_session.create`，并可访问标准 `POST /mcp` MCP endpoint，不能调用 `realtime.ws_ticket.create` 创建 WS ticket；其他 protected action 只认 owner `access_token`。本地 bridge 使用 `agent.matrix_session.create` 得到的 Matrix session 监听 agents room 并回写消息。
- MCP capability 是 owner-scoped 代理能力：`agent_token` 只负责授权标准 MCP endpoint，联系人列表/搜索、房间搜索、成员身份列表、普通消息默认发送/读取、频道帖子/评论读取和评论创建都按 portal owner 视角操作；普通消息发送不能发送到配置的 `agent_room_id`，agent room 回复只能由 gateway 使用 `agent_gateway`/`gateway_source` 标记路径以 `@agent:<server>` 发出；普通消息读取复用当前 owner `access_token` 读取 Matrix history，不创建 `DIREXTALK_MATRIX_HISTORY` 设备，也不刷新 Matrix session，因此不会导致 owner 手机/浏览器 session 被踢下线。标准 MCP 工具返回 `created_at`、`sender_mxid`、`sender_display_name`、`sender_domain`、`sender_localpart` 和成员 `joined_at` 等可读字段；MCP 读接口使用 `from_time`/`to_time`、`cursor` 和 `limit` 按 newest-first 稳定分页，拒绝旧的 `from_ts`/`to_ts`；cursor 固定首次查询快照，新插入内容只会出现在新的无 cursor 查询中。频道帖子工具返回帖子 `comment_count`、`like_count`、当前 owner 本地 `favorite_count` 和 `favorited_by_me`；频道普通聊天仍用消息工具读取。`agent.config.get/update` 返回和持久化 `avatar_url` 与 `mcp_blocked_room_ids`；黑名单房间不会出现在 MCP 房间搜索，其他直接定位黑名单房间或其频道帖子的 MCP 读写会返回 403。内部实现由 `internal/dirextalkmcp` 统一拥有 MCP registry、schemas、pagination、room authorization、DTOs、errors 和 invocation；Native Agent 内置 Dirextalk tools 和标准 `POST /mcp` 都调用同一个 service，`p2p` 只适配 Store、Transport、Matrix history、profile resolver、owner context 和 blocklist。标准 `POST /mcp` 使用 MCP Streamable HTTP 的 JSON-RPC POST：支持 `initialize`、`tools/list`、`tools/call`，第一版只接受 `Authorization: Bearer <agent_token>`，不接受 query-string token，校验 `Origin`，在不需要 server-to-client streaming 时 GET/SSE 返回 405，并且不会把入站 bearer token 传给下游 capability。Native Agent 在 message-server 内负责标准 MCP client、skills、模型平台配置和 Agent orchestration；后端保留 owner-only `plugins.*` 管理/调用边界。固定 `mcp.*` body action 已删除，`mcp.*` 字符串只作为 `internal/dirextalkmcp` 内部 capability action id 和 p2p adapter 测试标识存在。
- Native Agent 对话是 server-backed native runtime 业务，独立于旧 connect/Codex bridge room 会话。普通 Native Agent 调用直接使用 owner-protected `agent.*` body action；流式对话通过 `client.native_agent_stream` 发送 `id`、`action` 和 `params`，服务端会把 `agent.chat` 自动映射到 native runtime 的 `agent.chat.stream`，并以 `server.native_agent_stream.event` 持续返回 `delta`、`trace`、`done`；客户端取消时发送 `client.native_agent_stream.cancel`，服务端返回 `server.native_agent_stream.cancelled`。Native Agent 还提供 `agent.models.list`、`agent.runtime.inspect`、`agent.runtime.install`、`agent.runtime.which`、`agent.runtime.run`、`agent.skills.registry.search`、`agent.skills.list/install/enable/disable/uninstall`、`agent.mcp.servers.list/install/enable/disable/uninstall`、`agent.mcp.registry.search` 和 `agent.config.propose_patch`；`agent.models.list` 使用本次请求传入的 provider/base_url/api_key 调用支持的厂商模型列表接口，返回真实 `models[]`，不保存也不回显 API Key。Agent 知识库 action 名称保留但第一版返回 unsupported，不加载向量索引依赖。Runtime CLI、skills、MCP server metadata 和 memory 均落在 `P2P_NATIVE_AGENT_DATA_DIR`。
- `agent.matrix_session.create` 使用 owner `access_token` 或 `agent_token` 调用，用于本地 cc-connect/gateway 获取 `@agent:<server>` 的 Matrix Client-Server session；它不返回 owner Matrix session，也不回显 `agent_token` 或 portal password。
- Agent 在线状态对 owner 客户端只暴露一个 Matrix 房间状态字段：真实 `agent_room_id` 内的 `io.dirextalk.agent.status`，state key 为 `@agent:<server>`，content 只含 `online`。运行中的本地 bridge 通过 `@agent:<server>` Matrix session 发布 `online=true/false`；服务端不能从 Agent 配置、`/sync` 或 WS session 推断在线，只在启动/修复 agents room 或禁用 Agent 配置时写 `online=false` 兜底。`sync.bootstrap` 只返回 `agent_room_id` 供客户端定位房间，不再返回 `agent_online`；WS `server.event` 不发送 `agent.presence`。`agent.status`/`agents.status` 已删除，客户端不得再调用。
- Agent 预览和最终可恢复正文都通过 Matrix 消息/编辑回写；客户端展示 Matrix timeline 的聚合编辑结果，不消费 `server.agent_stream`。
- 服务初始化会创建真实私有 Matrix agents room，把 owner 和本地 `@agent:<server>` 加入同一房间，并把 `agent_room_id` 写入 bootstrap credentials；`portal.bootstrap`、`portal.auth`、`sync.bootstrap` 都会返回当前真实 `agent_room_id`，客户端可用它在重启后恢复 Agent 会话；部署和插件必须使用真实 room id，不使用 legacy `!agent:<domain>`。服务会给 owner 的真实 `agent_room_id` 写入默认 room-level 空 actions push rule，使 agent room 默认不走系统推送；已存在的显式同房间 push rule 会保留。
- 新增 MCP capability 时必须先更新 `internal/dirextalkmcp` registry/schema，再同步 Agent allowlist、Postman、接口变更记录和相关测试。

Multi-node：

- 房间、成员、消息、redaction、state 通过 Matrix federation。
- public channel discovery、user public channels 和 join request 使用 `remote_node_base_url` 显式指定目标 owner 节点 P2P base URL。
- 后端校验远端 URL；本地自签名双节点开发可用 `P2P_REMOTE_NODE_INSECURE_SKIP_TLS_VERIFY=true`。

## 8. 配置与开发命令

当前工具链：

- Go 1.26.4。
- 命令从仓库根目录执行。
- Windows 使用 PowerShell；Linux、macOS 或 WSL 使用 Bash/Zsh。文档命令应按当前环境给出，不应强制限定为 WSL。

单节点 Docker：

```bash
docker compose -f docker-compose.p2p.yml up --build
docker compose -f docker-compose.p2p.yml exec message-server cat /var/dirextalk-message-server/p2p/bootstrap.json
```

多节点 regression。

PowerShell：

```powershell
$env:P2P_DUAL_PUBLIC_HOST = if ($env:P2P_DUAL_PUBLIC_HOST) { $env:P2P_DUAL_PUBLIC_HOST } else { "host.docker.internal" }
docker compose -f docker-compose.p2p-dual.yml up -d --force-recreate dendrite-a dendrite-b dendrite-c
python scripts/p2p-three-node-regression.py
```

Bash：

```bash
export P2P_DUAL_PUBLIC_HOST="${P2P_DUAL_PUBLIC_HOST:-host.docker.internal}"
docker compose -f docker-compose.p2p-dual.yml up -d --force-recreate dendrite-a dendrite-b dendrite-c
python3 scripts/p2p-three-node-regression.py
```

本机 PostgreSQL 测试环境变量：

PowerShell：

```powershell
$env:POSTGRES_USER = "postgres"
$env:POSTGRES_PASSWORD = "123789"
$env:POSTGRES_HOST = "127.0.0.1"
$env:POSTGRES_PORT = "5432"
$env:POSTGRES_DB = "postgres"
```

Bash：

```bash
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=123789
export POSTGRES_HOST=127.0.0.1
export POSTGRES_PORT=5432
export POSTGRES_DB=postgres
```

Windows Docker Desktop users should prefer `127.0.0.1` over `localhost` for PostgreSQL ports published from containers. `localhost` may resolve to IPv6 `::1` first and wait for a failed connection before falling back to IPv4.

测试 helper 会创建相互隔离的 `dendrite_test_*` 数据库，并在对应测试结束后删除创建的测试库。

常用验证：

```bash
gofmt -w <touched go files>
go test ./p2p ./internal/productpolicy -count=1
go test ./internal/httputil ./setup -count=1
go build ./cmd/dirextalk-message-server
python3 -m json.tool docs/postman/dirextalk-message-server.postman_collection.json >/dev/null
python3 -m json.tool docs/postman/dirextalk-plugins.postman_collection.json >/dev/null
git diff --check
docker compose -f docker-compose.p2p-dual.yml config
```

## 9. 代码规范

- Go 代码必须 `gofmt`。
- 先从全局 Dirextalk server 视角梳理入口、鉴权、policy、storage、roomserver output、consumer/projection、sync/federation、docs 和验证路径，再把改动落在最小 owning package。
- 不新增 URL-shaped 产品接口；当前明确例外是标准 MCP Streamable HTTP endpoint `POST /mcp`。其它新增产品能力优先使用稳定 action 和 params schema。
- 不静默改变请求/响应字段；接口变化必须更新 `docs/api-interface-change-record.md`。
- 必须持久化的产品状态不得放内存-only；扩展 `p2p.Store` 和 migration。
- Matrix 侧房间、成员、消息、redaction 不绕过 `p2p.Transport`。
- remote public lookup 不从 room ID 推导 P2P URL，必须使用请求提供的 `remote_node_base_url`。
- public channel membership 不得在 Matrix join 前标记为 joined。
- local delete 与 recall 保持语义独立：local delete 是本地隐藏；recall 是 Matrix redaction。
- Postman JSON 必须保持可导入。
- 项目本地技能 `.codex/skills/*/SKILL.md` 与 AGENTS.md 必须随业务规则同步更新，并只承载 Dirextalk 项目专属事实、路径、检查矩阵和业务约束，不重复系统通用技能。
- 项目 skills 必须按全局工作面维护，不再按 P2P/Matrix/Dirextalk Message Server 层名拆分。当前全局技能是：`dirextalk-backend-change-orchestrator`（全链路影响图）、`dirextalk-backend-contract-state-storage`（合同、Matrix 事件状态、持久化和 migration 规则）和 `dirextalk-backend-verification`（本仓库验证命令选择）。

## 10. 文档规则

- README/AGENTS 级文档只描述当前运行与开发规则，不维护继承自 Dendrite 的站点式安装、管理、FAQ 或历史计划文档。
- 本文件是当前项目事实源。
- `docs/api-interface-change-record.md` 记录接口变更审计。
- `docs/api-audit-and-optimization.md` 记录当前审计与优化结论。
- `docs/p2p-integrated-as-implementation.md` 记录实现细节。
- `docs/dirextalk-message-server.md` 记录 Docker 镜像和运行说明，`docs/dirextalk-push-gateway.md` 记录 Push Gateway 合约，`docs/postman/` 只保留可导入 JSON 示例。
- 不在活文档、Postman、技能规则或示例中保留旧接口作为当前可用能力。
