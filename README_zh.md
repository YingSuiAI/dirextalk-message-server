# Direxio Message Server

Direxio Message Server 是 Direxio 后端服务，将 Matrix 兼容 homeserver 与 Direxio P2P 产品 API 合并在一个 Go monolith 中。

本仓库基于 Element Dendrite，但维护目标是 Direxio 产品服务，而不是通用 Matrix homeserver 发行版。

[English README](README.md)

## 运行时

- 生产入口：`cmd/direxio-message-server`
- 兼容入口：`cmd/dendrite`
- Docker 镜像：`direxio/message-server:latest`
- Docker 默认配置路径：`/etc/direxio-message-server/message-server.yaml`
- Docker 默认数据路径：`/var/direxio-message-server`
- Go module：`github.com/YingSuiAI/direxio-message-server`
- Go 版本：`1.26.4`

## API 入口

Matrix 协议路径保持在：

- `/_matrix/*`
- `/_synapse/*`
- `/_dendrite/*`
- `/.well-known/matrix/*`

Direxio 产品 API 使用 body-action 入口：

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

## 本地开发

在仓库根目录运行命令。Windows PowerShell、Linux Bash、macOS Bash/Zsh、WSL Bash 都可以使用；按当前 shell 选择对应命令格式。

构建服务：

```bash
go build ./cmd/direxio-message-server
go build ./cmd/dendrite
```

启动单节点 Docker 栈：

```bash
docker compose -f docker-compose.p2p.yml up --build
docker compose -f docker-compose.p2p.yml exec message-server cat /var/direxio-message-server/p2p/bootstrap.json
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

测试 helper 会创建相互隔离的 `dendrite_test_*` 数据库，并在对应测试结束后删除这些测试库。

## 文档

- [当前项目文档](docs/current-project-documentation.md)
- [实现说明](docs/p2p-integrated-as-implementation.md)
- [API 变更记录](docs/api-interface-change-record.md)
- [API 审计与优化记录](docs/api-audit-and-optimization.md)
- [Postman collection](docs/postman/direxio-message-server.postman_collection.json)
- [Docker 镜像说明](docs/direxio-message-server.md)

## License

本项目保留来自 Element Dendrite 的上游 license 与版权声明。见 [LICENSE](LICENSE) 和 [LICENSE-COMMERCIAL](LICENSE-COMMERCIAL)。
