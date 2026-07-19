# Dirextalk Message Server

Dirextalk Message Server 是 Dirextalk 的后端合约权威。它在一个 Go monolith 中合并 Matrix 兼容 homeserver、Dirextalk ProductCore action API、产品策略、projection/read model、Native Agent、外部 MCP 访问和 PostgreSQL-backed 运行时存储。

本仓库基于 Element Dendrite，但维护目标是 Dirextalk 产品服务，而不是通用 Matrix homeserver 发行版。

[English README](README.md)

## 架构概览

每位用户拥有一个私有 Dirextalk 服务节点。个人节点之间直接联邦消息；Dirextalk Platform 仅负责激活、分发和经授权的公开发现元数据。

![Dirextalk 平台架构](docs/assets/dirextalk-platform-architecture.png)

每个个人节点内部：

- Matrix 是房间、成员关系、普通消息、媒体、历史、搜索、未读状态和 redaction 的事实源。
- ProductCore action 是产品语义 facade，负责鉴权、参数校验、远端转发、Matrix 写入编排和 projection 读取。
- P2P 表默认是 projection/read model，只有被当前合约文档明确声明时才作为权威业务表。
- Native Agent 和 `POST /mcp` 是后端拥有的能力，不通过 plugin 生命周期安装、配置或调用。

## 运行时

- 生产入口：`cmd/dirextalk-message-server`
- 兼容入口：`cmd/dendrite`
- Docker 镜像：`dirextalk/message-server:latest`
- Docker 默认配置路径：`/etc/dirextalk-message-server/message-server.yaml`
- Docker 默认数据路径：`/var/dirextalk-message-server`
- Go module：`github.com/YingSuiAI/dirextalk-message-server`
- Go 版本：`1.26.5`
- 服务端数据库：仅支持 PostgreSQL。SQLite/file DSN 不是受支持的服务端运行时。
- Docker 开发数据库：PostgreSQL 18

## API 入口

Matrix 协议路径保持在：

- `/_matrix/*`
- `/_synapse/*`
- `/_dendrite/*`
- `/.well-known/matrix/*`

Dirextalk 产品 API 使用 body-action 入口：

- `GET /_p2p/health`
- `POST /_p2p/query`
- `POST /_p2p/command`
- `POST /mcp`
- `GET /_p2p/ws`
- `GET /.well-known/portal/owner.json`

生成后的 ProductCore 合约是 [docs/product-action-contract.json](docs/product-action-contract.json)。当前版本包含 148 个 action：11 个 public action、136 个 owner action、1 个 agent action。

鉴权和传输边界：

- Owner product action 使用 `Authorization: Bearer <access_token>`。
- 登录后客户端优先通过 `GET /_p2p/ws` 的 `client.request`/`server.response` 调用；realtime 未 ready 时，可安全重复的 action 可回退到 `POST /_p2p/query` 或 `POST /_p2p/command`。
- `GET /_p2p/ws` 只接受短期 owner WebSocket ticket，不直接接受 bearer token。
- `agent_token` 只能通过 ProductCore body-action 调用 `agent.matrix_session.create`，并可访问标准 `POST /mcp` endpoint。
- 外部 MCP 客户端使用 `POST /mcp` 上的 JSON-RPC，目前支持 `initialize`、`tools/list`、`tools/call`。该 endpoint 接受 `Authorization: Bearer <agent_token>`；owner token 和 query-string bearer token 都会被拒绝。

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

## 合约事实源

修改客户端、部署工具、Agent 或 API 文档前，优先读取这些当前维护事实源：

- [生成后的 ProductCore action 合约](docs/product-action-contract.json)
- [当前项目文档](docs/current-project-documentation.md)
- [当前 Agent 和 MCP 合约](docs/agent-mcp-current-contract.md)
- [API 变更记录](docs/api-interface-change-record.md)
- [后端合约 / state / storage skill](.codex/skills/dirextalk-backend-contract-state-storage/SKILL.md)
- [Release skill](.codex/skills/dirextalk-message-server-release/SKILL.md)
- [Release notes](release/RELEASE_NOTES.md)

## 本地开发

在仓库根目录运行命令。Windows PowerShell、Linux Bash、macOS Bash/Zsh、WSL Bash 都可以使用；按当前 shell 选择对应命令格式。

PostgreSQL-backed 测试需要本机 PostgreSQL 实例。测试 helper 会创建相互隔离的 `dendrite_test_*` 数据库，并在对应测试结束后删除这些测试库。

构建服务：

```bash
go build ./cmd/dirextalk-message-server
go build ./cmd/dendrite
```

启动单节点 Docker 栈：

```bash
docker compose -f docker-compose.p2p.yml up --build
docker compose -f docker-compose.p2p.yml exec message-server cat /var/dirextalk-message-server/p2p/bootstrap.json
```

运行三节点回归。

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

使用本机 PostgreSQL 运行测试：

PowerShell：

```powershell
$env:POSTGRES_USER = "postgres"
$env:POSTGRES_PASSWORD = "123789"
$env:POSTGRES_HOST = "localhost"
$env:POSTGRES_PORT = "5432"
$env:POSTGRES_DB = "postgres"
go test ./p2p ./internal/productpolicy -count=1
```

Bash：

```bash
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=123789
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=postgres
go test ./p2p ./internal/productpolicy -count=1
```

提交文档改动前检查空白字符：

```bash
git diff --check
```

## 文档

当前维护文档保持精简。继承自 Dendrite 的站点文档、过时 tracker 和一次性实施计划不再作为本 fork 的维护文档。

- [当前项目文档](docs/current-project-documentation.md)
- [当前 Agent 和 MCP 合约](docs/agent-mcp-current-contract.md)
- [生成后的 ProductCore action 合约](docs/product-action-contract.json)
- [实现说明](docs/p2p-integrated-as-implementation.md)
- [API 变更记录](docs/api-interface-change-record.md)
- [API 审计与优化记录](docs/api-audit-and-optimization.md)
- [Release notes](release/RELEASE_NOTES.md)
- [Postman collection](docs/postman/dirextalk-message-server.postman_collection.json)
- [插件 Postman collection](docs/postman/dirextalk-plugins.postman_collection.json)
- [Docker 镜像说明](docs/dirextalk-message-server.md)
- [Push Gateway 合约](docs/dirextalk-push-gateway.md)

## License

本项目保留来自 Element Dendrite 的上游 license 与版权声明。见 [LICENSE](LICENSE) 和 [LICENSE-COMMERCIAL](LICENSE-COMMERCIAL)。
