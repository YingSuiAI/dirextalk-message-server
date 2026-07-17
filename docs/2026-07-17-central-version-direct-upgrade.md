# 中台驱动的版本直传升级实施清单

状态：开发与分支推送完成。此文档是本次跨仓实现的交付核对表；每个条目只有在代码、针对性测试和复核完成后才会标记为完成。

## 已完成的准备

- [x] 四个仓库均从 `origin/main` 创建 `feat/central-version-direct-upgrade` 分支。
- [x] Flutter 基线使用 `origin/main`，避免本地旧 `main`。

## 功能任务

- [x] Flutter：查询 `appId=1` 的 `google`/`ios` 与 `server` 当前版本，严格解析中台响应并比较 SemVer。
- [x] Flutter：实现客户端/服务端双向兼容矩阵、启动提示、商店跳转和关于页状态。
- [x] Flutter：删除公开 rollback 调用；兼容历史自动恢复终态与 restart。
- [x] Message server：新增 owner-only、HTTP-only `release.v2.status`。
- [x] Message server：新增 owner-only、HTTP-only `release.v2.apply`，仅接受目标版本和幂等参数，并复核中台 `server` 记录。
- [x] Updater：新增基于固定镜像仓库和 `target_version` 的直接单跳任务；拉取后固定实际 digest。
- [x] Updater：移除 GitHub discovery 活跃路径、公开 rollback 路由和 rollback operation，保留自动恢复与 restart。
- [x] Deployer：停止为新节点安装 discovery timer，并为已有节点提供幂等清理迁移（`57fc7a9`；bundle、安装和 S3 migration tests 已通过）。

## 交付任务

- [x] 覆盖客户端、中台错误、兼容矩阵、服务端鉴权与幂等、updater 自动恢复、deployer timer 迁移的测试。
- [x] 更新功能/接口/部署文档，并逐项复核本清单。
- [x] 对每个仓库运行适用测试、`git diff --check`，提交并推送分支。
- [x] 检查远端 CI 结果并记录最终状态。

## 已知发布约束

- 中台保持现有两个 GET；`url` 是字符串，且 iOS 当前 URL 为空。iOS 更新按钮在 URL 有效前不可执行。
- `preVersion` 在移动端表示最低服务端版本，在 `server` 记录表示最低客户端版本。
- 版本直传模式信任中台与镜像 tag；任务内仍固定拉取后解析出的 digest、禁止降级，并在失败时自动恢复。
- 该模式仅支持单跳更新；发布方必须确保受支持的历史服务端可直接迁移到目标镜像。

## 已执行验证

- Flutter：`flutter analyze --no-pub` 通过；升级 focused suite（中台响应、SemVer、兼容矩阵、HTTP-only v2、自动恢复、rollback 拒绝、关于页、启动提醒）142 项通过。全量 `flutter test --no-pub` 已运行至结束（1,857 项，116 项失败）；失败分布在本次未改动的既有 widget/资源场景，不能作为本次功能验收依据，focused suite 已覆盖本次改动。
- Message server：`go test ./internal/releasecontrol ./p2p/internal/release ./p2p/serviceapi` 通过（42 项），覆盖中央记录校验、HTTP-only/owner 权限、目标复核、最小客户端版本、非法字段和幂等。
- Updater：`go test ./...`（156 项）、`go test -race ./...`、`go vet ./...`、`go mod verify` 和 Linux amd64 构建冒烟均通过；真实 Compose 升级/自动恢复测试因本机已有 `dirextalk-p2p` 容器资源被保护逻辑安全跳过。
- Deployer：shell 语法检查、`updater_atomic_install_test.sh`、`updater_bundle_test.sh` 和 `s3_updater_integration_migration_test.sh` 均通过。

## 分支与远端 CI

- 四个仓库均已推送 `origin/feat/central-version-direct-upgrade`；本次仅按要求推送，未创建 PR。
- Updater CI（Ubuntu 22.04/24.04）全部通过：模块校验、格式、常规/`-race` 测试、`vet` 和构建冒烟均成功。
- Deployer CI：Windows 和 Ubuntu job 通过；macOS quick test 失败于 `scripts/lib/json.sh` 中 Bash 3.2 不支持的动态文件描述符语法。该文件与本次 diff 无交集，且 `origin/main` 的当前 CI 也在同一 macOS quick-test 步骤失败，因此未将无关基线故障混入本次功能改动。
- Flutter 工作流仅触发 `main` 或 PR；message-server CI 仅触发 `main`、`adam/**` 或 tag，因此本特性分支推送未生成对应 CI run。
