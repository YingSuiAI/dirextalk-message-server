# 中台驱动的版本直传升级实施清单

状态：进行中。此文档是本次跨仓实现的交付核对表；每个条目只有在代码、针对性测试和复核完成后才会标记为完成。

## 已完成的准备

- [x] 四个仓库均从 `origin/main` 创建 `feat/central-version-direct-upgrade` 分支。
- [x] Flutter 基线使用 `origin/main`，避免本地旧 `main`。

## 功能任务

- [ ] Flutter：查询 `appId=1` 的 `google`/`ios` 与 `server` 当前版本，严格解析中台响应并比较 SemVer。
- [ ] Flutter：实现客户端/服务端双向兼容矩阵、启动提示、商店跳转和关于页状态。
- [ ] Flutter：删除公开 rollback 调用；兼容历史自动恢复终态与 restart。
- [ ] Message server：新增 owner-only、HTTP-only `release.v2.status`。
- [ ] Message server：新增 owner-only、HTTP-only `release.v2.apply`，仅接受目标版本和幂等参数，并复核中台 `server` 记录。
- [ ] Updater：新增基于固定镜像仓库和 `target_version` 的直接单跳任务；拉取后固定实际 digest。
- [ ] Updater：移除 GitHub discovery 活跃路径、公开 rollback 路由和 rollback operation，保留自动恢复与 restart。
- [ ] Deployer：停止为新节点安装 discovery timer，并为已有节点提供幂等清理迁移。

## 交付任务

- [ ] 覆盖客户端、中台错误、兼容矩阵、服务端鉴权与幂等、updater 自动恢复、deployer timer 迁移的测试。
- [ ] 更新功能/接口/部署文档，并逐项复核本清单。
- [ ] 对每个仓库运行适用测试、`git diff --check`，提交并推送分支。
- [ ] 检查远端 CI 结果并记录最终状态。

## 已知发布约束

- 中台保持现有两个 GET；`url` 是字符串，且 iOS 当前 URL 为空。iOS 更新按钮在 URL 有效前不可执行。
- `preVersion` 在移动端表示最低服务端版本，在 `server` 记录表示最低客户端版本。
- 版本直传模式信任中台与镜像 tag；任务内仍固定拉取后解析出的 digest、禁止降级，并在失败时自动恢复。
- 该模式仅支持单跳更新；发布方必须确保受支持的历史服务端可直接迁移到目标镜像。
