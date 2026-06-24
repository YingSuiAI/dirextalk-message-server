# Direxio Message Server 当前项目文档

本文是当前代码与接口的事实源。历史变更记录只用于审计，不作为当前接口或实现依据。

## 1. 项目定位

本仓库是 Direxio 对 Element Dendrite 的集成式 fork：同一个 Go monolith 同时提供 Matrix homeserver 能力和 Direxio P2P 产品 API。

当前架构原则：

- Matrix 事件与房间状态是好友、群、频道、成员、普通消息的事实源。
- P2P action 是产品语义 facade，负责鉴权、参数校验、远端转发、Matrix 写入编排和投影读取。
- P2P 数据表保留为 projection/read model，不作为成员关系和普通消息的最终事实源。
- 产品代码不得直接写 Matrix SQL 底表；房间、成员、消息、redaction 等 Matrix 行为必须通过 `p2p.Transport` 或 Matrix Client-Server API 进入 Direxio Message Server。

## 2. 对外入口

Matrix 协议入口保持 Direxio Message Server 原有路径：

- `/_matrix/*`
- `/_synapse/*`
- `/_dendrite/*`
- `/.well-known/matrix/*`

Direxio 产品 API 只暴露 body-action surface：

- `GET /_p2p/health`
- `POST /_p2p/query`
- `POST /_p2p/command`
- `GET /_p2p/events`
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

Protected action 需要 `Authorization: Bearer <access_token>`，或启用对应 action 权限的 `agent_token`。当前 public action 是：

- `portal.bootstrap`
- `portal.auth`
- `portal.status`
- `contacts.reactivate`
- `channels.public.search`
- `channels.public.get`
- `channels.public.join_request`
- `channels.public.join_result`
- `users.public_channels`

`channels.public.join_result` 是节点间审批结果回调，不是客户端常规入口。

## 3. 运行时结构

核心入口：

- `cmd/direxio-message-server`：生产服务入口，monolith 模式运行。
- `setup/monolith.go`：装配 client、federation、media、sync、relay、P2P routes。
- `p2p/service.go`：P2P action 分发与业务编排。
- `p2p/storage.go`：P2P projection/read model 持久化。
- `p2p/dendrite_transport.go`：真实 Matrix 写入适配层。
- `p2p/projector.go`：roomserver output 到 P2P projection 的投影。
- `p2p/consumer.go`：订阅 roomserver 输出并调用 projector。
- `internal/productpolicy`：Matrix Client-Server 写入前的 Direxio 产品策略校验。

生产持久化优先使用全局 Direxio Message Server 数据库配置；未配置时 P2P store 会回退到 roomserver 数据库。Docker 开发栈使用 PostgreSQL 18。

## 4. Matrix Native State

当前产品房间只使用 native Direxio state：

- `m.room.create.content.type`
  - `io.direxio.room.direct`
  - `io.direxio.room.group`
  - `io.direxio.room.channel`
- `io.direxio.room.profile`
  - direct/group/channel 的产品元数据。
- `io.direxio.member.policy`
  - role、mute 等成员策略。
- `io.direxio.join_request`
  - public channel 申请审批状态。

投影规则：

- `io.direxio.room.profile` 投影到 groups/channels read model。
- direct invite 的 `io.direxio.room.profile` stripped state 投影为 inbound contact request。
- `io.direxio.member.policy` 投影成员角色与禁言。
- `io.direxio.join_request` 投影申请审批状态。
- Matrix `m.room.member membership=join` 是最终 joined 事实。
- 普通 Matrix timeline 不复制到 P2P 普通消息表；普通消息读写走 Matrix Client-Server API。唯一例外是配置的 agents room：其中的普通消息会投影为 `agent_room.message` SSE，供本地 gateway daemon 调用外部智能体并以 `@agent:<server>` 回写回复。

## 5. 用户请求生命周期

P2P action 生命周期：

1. HTTP route 接收 `/query` 或 `/command` envelope。
2. route 调用 `Service.Authorize`：
   - public action 直接放行；
   - protected action 校验 access token 或 Agent token 权限。
3. `Service.Handle` 分发到对应业务函数。
4. 业务函数校验参数、所有者/成员/策略权限。
5. 需要 Matrix 事实写入时调用 `p2p.Transport`。
6. Direxio Message Server roomserver 产生 output event。
7. `p2p.consumer` 调用 `ProjectRoomEvent` 更新 P2P read model。
8. `/_p2p/events` 发送产品投影事件；agents room 消息额外发送 `agent_room.message`，客户端或 gateway 刷新对应视图/触发智能体回复。
9. 客户端普通消息、历史、搜索、redaction 继续通过 Matrix Client-Server API。

Matrix Client-Server 写入生命周期：

1. 客户端调用 Matrix send/state/member/redaction API。
2. Direxio product policy 读取当前 Matrix state。
3. 如果房间是 Direxio product room，则校验 dissolved、member join、mute、role、join policy 等规则。
4. 合法事件进入 Direxio Message Server 原生 roomserver。
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
3. 频道主节点写 `io.direxio.join_request status=pending`。
4. 频道主节点 projection 中成员为 `pending`。

`channels.join_request.reject`：

1. 频道主节点写 `io.direxio.join_request status=rejected`。
2. 本地 projection 更新为 `reject`。
3. 如果申请人是远端用户，频道主节点调用申请人节点的 `channels.public.join_result`。
4. 申请人节点更新为 `rejected` 并发送 P2P event。

`channels.join_request.approve`：

1. 频道主节点写 `io.direxio.join_request status=approved`。
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
- profile update 同步 P2P profile/member projection，并写入 Matrix-facing profile storage。

Contacts：

- 发起联系人请求会创建 direct Matrix room，并邀请对方。
- inbound/outbound request 来自 Matrix invite/member projection。
- accept 通过 Matrix join 进入 direct room。
- delete 后保留原 direct room 身份用于恢复。删除方主动重新添加时，如果对方仍保留 accepted 关系，可以通过 `contacts.reactivate` 复用旧房间；被删除方重新申请时只能在旧房间形成 pending request，必须由删除方 accept 后才能重新聊天。
- reject/delete 只改变产品 projection 与对应 Matrix leave/kick 行为，不制造普通消息副本。

Groups：

- group create 写 Matrix room type 与 `io.direxio.room.profile`。
- invite/join/leave/remove/mute/unmute/dissolve 通过 `p2p.Transport` 与 native state 进入 Matrix。
- member list 来自 P2P projection，但最终事实是 Matrix membership。

Channels：

- channel create/update 写 Matrix room type 与 `io.direxio.room.profile`。
- public search/get 是只读发现，不创建占位记录。
- invite grant 用于私有或分享卡片加入。
- public join request 使用上面的申请审批自动 join 生命周期。
- channel member、mute、read marker、dissolve 都保持 Matrix-first。

Channel posts/comments/reactions：

- 仍是产品内容 projection。
- 使用 Matrix `m.room.message` 携带 `p2p_kind=channel_post` 或 `p2p_kind=channel_comment`。
- reaction 使用 Matrix reaction/内容字段投影到 P2P reaction read model。
- recall 通过 Matrix redaction。

Calls/Favorites/Follows/Reports：

- calls 是产品会话 read model，支持 create/incoming/get/list/active/event，持久化接通/结束时间、结束方和原因，并通过 `call.changed` P2P event 推送实时状态。
- favorites、follows、reports 是 P2P product state，使用 P2P store 持久化。

Agent/API permissions：

- Agent token 可按 action enable/disable。
- 服务初始化会创建真实私有 Matrix agents room，把 owner 和本地 `@agent:<server>` 加入同一房间，并把 `agent_room_id` 写入 bootstrap credentials；`portal.bootstrap`、`portal.auth`、`sync.bootstrap` 都会返回当前真实 `agent_room_id`，客户端可用它在重启后恢复 Agent 会话；部署和插件必须使用真实 room id，不使用 legacy `!agent:<domain>`。
- 新增 action 时必须同步默认权限、Postman、接口变更记录和相关测试。

Multi-node：

- 房间、成员、消息、redaction、state 通过 Matrix federation。
- public channel discovery 和 join request 使用 `remote_node_base_url` 显式指定频道主节点 P2P base URL。
- 后端校验远端 URL；本地自签名双节点开发可用 `P2P_REMOTE_NODE_INSECURE_SKIP_TLS_VERIFY=true`。

## 8. 配置与开发命令

单节点 Docker：

```bash
docker compose -f docker-compose.p2p.yml up --build
docker compose -f docker-compose.p2p.yml exec message-server cat /var/direxio-message-server/p2p/bootstrap.json
```

WSL2 多节点 regression：

```bash
export P2P_DUAL_PUBLIC_HOST="${P2P_DUAL_PUBLIC_HOST:-host.docker.internal}"
docker compose -f docker-compose.p2p-dual.yml up -d --force-recreate dendrite-a dendrite-b dendrite-c
python3 scripts/p2p-three-node-regression.py
```

常用验证：

```bash
gofmt -w <touched go files>
go test ./p2p ./internal/productpolicy -count=1
go test ./internal/httputil ./setup -count=1
go build ./cmd/direxio-message-server
python3 -m json.tool docs/postman/direxio-message-server.postman_collection.json >/dev/null
git diff --check
docker compose -f docker-compose.p2p-dual.yml config
```

## 9. 代码规范

- Go 代码必须 `gofmt`。
- 先从全局 Direxio server 视角梳理入口、鉴权、policy、storage、roomserver output、consumer/projection、sync/federation、CLI/docs 和验证路径，再把改动落在最小 owning package。
- 不新增 URL-shaped 产品接口；新增产品能力优先使用稳定 action 和 params schema。
- 不静默改变请求/响应字段；接口变化必须更新 `docs/api-interface-change-record.md`。
- 必须持久化的产品状态不得放内存-only；扩展 `p2p.Store` 和 migration。
- Matrix 侧房间、成员、消息、redaction 不绕过 `p2p.Transport`。
- remote public lookup 不从 room ID 推导 P2P URL，必须使用请求提供的 `remote_node_base_url`。
- public channel membership 不得在 Matrix join 前标记为 joined。
- local delete 与 recall 保持语义独立：local delete 是本地隐藏；recall 是 Matrix redaction。
- Postman JSON 必须保持可导入。
- 项目本地技能 `.codex/skills/*/SKILL.md` 与 AGENTS.md 必须随业务规则同步更新。
- 项目 skills 必须按全局工作面维护，不再按 P2P/Matrix/Direxio Message Server 层名拆分。当前全局技能是 `direxio-change-orchestrator`、`direxio-contract-sync`、`direxio-event-state-tracer`、`direxio-storage-migration-guard`、`direxio-targeted-verification` 和 `direxio-cli`。

## 10. 文档规则

- README/AGENTS 级文档只描述当前运行与开发规则。
- 本文件是当前项目事实源。
- `docs/api-interface-change-record.md` 记录接口变更审计。
- `docs/api-audit-and-optimization.md` 记录当前审计与优化结论。
- `docs/p2p-integrated-as-implementation.md` 记录实现细节。
- 不在活文档、Postman、技能规则或示例中保留旧接口作为当前可用能力。
