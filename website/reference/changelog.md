# 更新日志

CKMAN 的完整版本变更历史维护在仓库根目录 [`CHANGELOG.md`](https://github.com/housepower/ckman/blob/main/CHANGELOG.md) 中。本页展示最近几个版本的关键变更摘要。

::: tip 完整列表
所有版本（含 v1.x 早期版本）请查阅 [GitHub CHANGELOG](https://github.com/housepower/ckman/blob/main/CHANGELOG.md)，或在仓库内 `git log --oneline` 查看更细的提交粒度。
:::

## 发布说明

- [**4.0.0**](/release-notes/4.0.0) — 备份恢复重构、用户体系、单节点 override、Nacos 热重载、SQLite 本地持久化、VitePress 文档站；含完整升级/降级/备份迁移注意事项。

## 近期亮点

以下是最近一段时间的主要功能与修复（具体版本号以发布 release 为准）：

### 场景化 AI 诊断

- 在集群资源、复制表、表指标、慢查询和分布式 DDL 页面原位发起诊断
- 先执行只读、确定性的系统表检查，再由可选 LLM 流式解释结构化证据
- 表健康诊断限定到指定表，减少无关扫描与模型上下文
- 诊断记录持久化到 local / MySQL / PostgreSQL / DM8，支持取消与中断恢复
- LLM 使用 OpenAI-compatible endpoint，API key 仅从环境变量或 secret file 读取

### 查询与安全

- Query 管理支持 SSE 分批返回、进度反馈与服务端查询取消
- 首次启动生成一次性管理员密码，并要求首次登录修改
- JWT 客户端 IP 绑定可按部署环境关闭
- 集群敏感字段支持带版本标记的 SM4-GCM 加密与存量密文迁移

### 用户与权限

- 用户管理面板支持新增 / 修改 / 删除 / 启停账号
- 角色权限模型完善：admin / ordinary / guest 三级 RBAC
- 用户管理页加入"角色说明" tooltip
- 移除文件系统用户存储，统一走 DB
- 登录时检查账号启用状态

### 数据备份

- 备份表单支持查看并勾选具体分区、一次性固定日期范围
- 支持主动停止排队中或运行中的备份/恢复 run
- 单分区读超时可配置；S3 备份支持按任务设置上传限速
- `data_manage` 重写为薄壁，全部委托 `service/backup`
- 增量备份去重
- 删除 run 时仅删 ckman 台账，不动 S3 数据
- 备份策略支持指定执行实例（避免跨区流量打满）
- S3 卷默认配置精简，tgz 路径安全加固

### 集群运维

- 新部署的 ClickHouse Keeper 在页面上只提供独立进程模式
- 增删节点时只更新其余节点的拓扑配置，避免重刷用户与自定义配置
- `/ck/rebalance_plan` 接口：均衡执行计划预演
- rebalance 改为通过 runner task 异步执行（修复锁泄漏与 error loss）
- 节点 override 配置接口（GET / PUT / DELETE）
- 任务支持 boot recovery 与真正的 cancellation
- AddNode 校验 SourceSchemaHost 并写入 d.Ext
- StartNode 切换到 PickAvailableSchemaSource

### 监控与可观测

- Prometheus `/metrics` 暴露
- HTTP service discovery 支持
- 从 ClickHouse 系统表直读指标的接口
- 后台线程池监控

### 持久层

- DM8 达梦数据库适配
- 持久层抽象重构

### 早期重要版本

#### v2.2.8

- migrate tool panic 修复
- 支持展示 uptime
- 支持 rename column（ALTER TABLE）
- 异常状态下不强制返回错误
- watch cluster status（tgz 部署）
- 修改 profiles / quotas 后重启集群
- 建表前的 dryrun 检查
- 分布式表 schema 自动同步定时任务
- 升级时权限拒绝问题修复

#### v2.2.7

- DM8 适配
- 前端 map 为 null 的处理

#### v2.2.6

- 部署集群时 Profiles / quotas 未填的错误
- 老版本部署的集群安装包类型未自动填充
- 密码加密支持 `password_double_sha1_hex` 与 `password_sha256_hex`
- 登录密码可视化
- 使用 scp 替代 sftp
- mysql 持久化失败修复
- 任务按 updatetime 排序

## 升级建议

- 跨大版本升级前请阅读对应版本的 changelog，确认是否有 schema 变更
- CKMAN 升级流程参见[升级 CKMAN](/deploy/upgrade)
- ClickHouse 集群升级见[升级集群](/features/cluster/upgrade)

## 反馈与贡献

- 报 bug：[GitHub Issues](https://github.com/housepower/ckman/issues)
- 提交 PR：欢迎社区贡献，请在 PR 描述中说明修改动机与影响范围
