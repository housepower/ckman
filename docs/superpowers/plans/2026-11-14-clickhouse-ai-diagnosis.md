# ClickHouse AI 诊断接入 Implementation Plan

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax for tracking. Implement task-by-task; do not start the Nova/LLM phase before the deterministic diagnosis vertical slice is green.

**Goal:** 将 `clickhouse-go-diagnose` 的结构化只读检查接入 ckman，在现有业务页面提供一次点击、一次流式结论、不可追问的 AI Native 诊断动作。

**Architecture:** ckman 自己持有集群拓扑、连接、任务、权限、持久化、严重度和审计；`clickhouse-go-diagnose` 只持有固定 SQL、证据归并、reason code 和服务端范围过滤；Nova 只把一份已完成、已裁剪的结构化报告流式解释为一次性结论。场景和采集范围由业务入口确定，LLM 不选检查、不调用工具、不接受追问。

**Tech Stack:** Go 1.26 / Gin / clickhouse-go v2.48 / clickhouse-go-diagnose v0.5.0 / Nova v0.4.1 / gorm-compatible repositories / Vue 2 + TypeScript + Element UI（`../ckman-fe`）

## Delivery Milestones

| 里程碑 | 内容 | LLM 依赖 |
| --- | --- | --- |
| M1 | 单检查适配、全副本 target、取消与预算 | 无 |
| M2 | 异步 run、持久化、API、RBAC、诊断抽屉 | 无 |
| M3 | 业务场景检查、按表/记录过滤、内部审计记录 | 无 |
| M4 | 自动触发的一次性 AI 解释、流式输出 | 可选 |

M1–M3 必须在 `LLM.Enabled=false`、无外网、无 API key 时完整可用。M4 失败不得改变 run 的 `Execution`、`Outcome`、`Severity`，也不得阻止用户查看原始结构化报告。

## Global Constraints

- 新 API 注册到 `/api/v1`：当前服务只装配 `router.InitRouterV1`，本计划不顺带创立半套 v2 响应协议。
- 后端依赖固定版本，不引用分支或伪版本：诊断库固定支持表级 scope 和五类业务场景的 `v0.5.0`；Nova 固定 `v0.4.1`。升级必须经过 contract test。
- 页面诊断由服务端 scene profile 决定检查，LLM 不得选择、删除或改变检查。
- 不提供诊断中心、通用聊天窗、追问、重试解释或 Agent 工具循环；历史 run 只用于审计、断线恢复和问题排查。
- 点击入口后自动完成采集并流式生成一轮 AI 结论，不要求用户再次点击“解释”。
- LLM 永远不能收到或指定 ClickHouse/SSH 凭据、任意 host、SQL、cluster 名切换、修复动作。
- 不注册任意 SQL、ALTER、KILL、SSH、restart、配置写入或文件工具。
- 严重度由 ckman 的纯规则计算；LLM 只能解释，回答必须标注“AI 解释”。
- `RunCheck` 同时返回 `report` 和 `error` 时先保存报告，再保存错误；不得因超时丢弃已有证据。
- `Execution` 与 `Outcome` 分开展示：`no_match` 只能显示“本检查未发现匹配”，不能显示“集群健康”。
- 诊断 targets 覆盖每个 shard 的每个 replica；不得复用 `CkService.InitCkService` 随机挑一个 host 的行为。
- 连接准备、pool health check、查询和 Rows 迭代必须共享同一个总 context budget。
- 诊断查询不自动重试。重试可能跨越采集窗口并混合不同时刻证据；用户显式重跑才生成新 run。
- 单集群默认只允许 1 个全量 run；单检查 run 可排队。检查默认串行，每个检查内部节点并发沿用库默认 4。
- 原始报告单 run 上限 2 MiB；进入模型前上限 32 KiB。超限必须结构化裁剪并保留总数与 `truncated` 标记。
- `last_exception`、mutation `command`、错误堆栈都视为不可信且可能敏感的数据：UI 文本转义；LLM 前默认脱敏/截断；不得拼进 system prompt。
- LLM API key 只从环境变量或 secret file 读取，不进入 Nacos/Apollo 正文、repository、API 返回或日志。
- ordinary/admin 可创建和查看诊断；guest 首期无诊断权限；admin 才可查看所有用户的历史和 LLM 状态。所有权校验不能只依赖 URL RBAC。
- 前端改动只在 `/data/root/go/src/github.com/housepower/ckman-fe/` 的有效分支完成，绝不直接编辑 `ckman/frontend/` 子模块。
- 所有 persistent interface/model/schema/backend 变更必须按 `repository/CLAUDE.md` 在真实 PostgreSQL 及可用的 MySQL/DM8 上验证；mock 或 SQLite 单测不能代替 DB 边界测试。

## Scene Profiles

| scene | 页面入口 | 固定检查 |
| --- | --- | --- |
| `resource_pressure` | 管理集群“AI 诊断资源异常” | CPU、内存、磁盘水位；按节点采集 |
| `replica_status` | 表 → 复制状态行操作“AI 诊断” | 指定 database/table 的副本状态与复制队列 |
| `table_health` | 表指标行操作“AI 诊断” | 指定 database/table 的 parts、detached parts、mutation |
| `slow_query` | 慢查询行操作“AI 诊断” | 指定 query_id 的 query_log、资源和 ProfileEvents 证据 |
| `distributed_ddl` | 分布式 DDL 行操作“AI 诊断” | 指定 query_id/queue entry 的 DDL 队列证据 |

请求只传 `scene` 和该业务行已有的稳定标识（database/table/query_id/queue entry），服务端是 scene→checks 的唯一事实源。database/table 必须作为参数下推到固定 SQL；不能只裁剪返回结果，更不能只在 prompt 中声明范围。表诊断默认只支持指定表，不提供全库/全表扫描入口。

## API Contract

```text
GET    /api/v1/diagnostics/checks
POST   /api/v1/diagnostics/:cluster/runs
GET    /api/v1/diagnostics/:cluster/runs/:runId
POST   /api/v1/diagnostics/:cluster/runs/:runId/cancel

POST   /api/v1/diagnostics/:cluster/runs/:runId/explain      # SSE, M4
```

`POST .../runs`：

```json
{
  "scene": "table_health",
  "context": {"database": "orders", "table": "events_local"}
}
```

返回 `202` 语义仍通过现有 `WrapMsg` 的 HTTP 200 envelope 表达，`entity` 至少包含 `run_id`、`status=queued`。不得为该功能单独改变全局响应格式。

统一 run 状态：`queued/running/complete/partial/failed/canceling/canceled/interrupted`。聚合规则：所有检查完整完成为 `complete`；任一检查 partial 为 `partial`；无可用报告为 `failed`；cancel 后保留报告并根据已有证据使用 `canceled`。

## Persistence Model

`DiagnosisRun` 使用独立表，不复用部署任务的 `Task.Message`：

```text
RunID, ClusterName, RequestedBy, Scene, ContextJSON, ChecksJSON,
Status, Execution, Outcome, Severity, ReportJSON, ErrorMessage,
OwnerInstance, CancelRequested, CreatedAtMS, StartedAtMS,
FinishedAtMS, UpdatedAtMS, ExpiresAtMS
```

不建立会话和消息模型。run 是唯一持久化单元；时间统一用 Unix milliseconds，避免各 DB 对 zero time/timezone 的差异。

## File Structure

| 文件 | 动作 | 职责 |
| --- | --- | --- |
| `common/ck.go`, `common/ck_cli.go` | 修改 | context-aware connect/ping |
| `service/diagnosis/adapter.go` | 创建 | `common.Conn` → `diagnose.Querier` |
| `service/diagnosis/targets.go` | 创建 | cluster topology → 全副本 targets |
| `service/diagnosis/profile.go` | 创建 | scene→checks/预算映射 |
| `service/diagnosis/severity.go` | 创建 | reason code→确定性严重度 |
| `service/diagnosis/sanitize.go` | 创建 | API/LLM DTO、脱敏、32 KiB 裁剪 |
| `service/diagnosis/orchestrator.go` | 创建 | run 生命周期、并发、取消、持久化 |
| `service/diagnosis/executor.go` | 创建 | 有界 worker、owner lease/heartbeat |
| `service/diagnosis/agent/*` | 创建 | ckman Agent 接口、会话、工具与事件 |
| `service/diagnosis/agent/nova/*` | 创建 | Nova v0.4.1 适配器 |
| `model/diagnosis.go` | 创建 | run/session/message 模型与 API DTO |
| `repository/*` | 修改 | 诊断持久化接口、backend model、migration、CRUD/CAS |
| `controller/diagnosis.go` | 创建 | HTTP、所有权校验、SSE |
| `router/v1.go` | 修改 | 注册 diagnostics 路由 |
| `server/enforce/*` | 修改 | ordinary/admin 诊断权限 |
| `config/config.go`, `config/remote_apply.go` | 修改 | LLM 非秘密配置与环境变量 |
| `statistics/statistics.go` | 修改 | diagnosis/LLM 指标 |
| `../ckman-fe/src/apis/diagnosis.ts` | 创建 | run/history/SSE API |
| `../ckman-fe/src/components/diagnosis/*` | 创建 | 统一诊断抽屉、报告、AI 面板 |

---

### Task 0: 稳定上游诊断库 JSON 契约

**Repositories:** `clickhouse-go-diagnose`（上游）与本仓库的依赖锁定

- [x] 给 `NodeResult.RowsRead/RowsKept/Truncated/ErrCategory/ErrMessage/StartedAt/FinishedAt` 及所有 observation/match 的裸字段补真实 struct tags（上游 commit `a363c0c`）。
- [x] 增加反射式报告 JSON contract test，并显式断言 `NodeResult` 只有 snake_case key。
- [x] 运行上游 `make check` 与 `make race`，核心库和 clickhouse-doctor 两个 module 全部通过。
- [x] 发布并推送核心库补丁 tag `v0.3.1`。
- [x] 在 ckman `go.mod` 固定精确版本；执行 `go mod tidy`，确认没有引入 doctor/chat 子模块。

**Gate:** `GET /diagnostics/checks` 与 report JSON contract test 未稳定前，不开始前端模型定义。

### Task 1: Context-aware ClickHouse 连接与诊断适配器

**Files:** `common/ck.go`, `common/ck_cli.go`, `service/diagnosis/adapter.go`, `service/diagnosis/targets.go` and tests

- [x] 为 `Conn` 增加 `PingWithCtx(ctx)`；HTTP 调 `db.PingContext`，native 调 `c.Ping(ctx)`。
- [x] 增加 `ConnectClickHouseWithCtx(ctx, host, database, opt)`；原 `ConnectClickHouse` 用 `context.Background()` 委托，保持兼容。
- [x] pool 命中时用调用方 ctx 做 Ping；新建 HTTP/native 连接的首次 Ping 也用同一 ctx。
- [x] 实现 lazy `ckQuerier.Query`：target 构建时只保存 host/connection options，真正执行 Query 时先用本次 node context 调 `ConnectClickHouseWithCtx`，再调 `QueryWithCtx`；不修改共享连接上的 `c.ctx`，不关闭共享 pool 连接。这样建连失败会被诊断库诚实记录为该 target 的 `query_failed`，同时建连时间计入 node/total budget。
- [x] 从 `conf.Shards` 逐 shard/replica 构建 targets；`ShardID=<cluster>/shard-<1-based>`，`NodeID=<host>:<effective-port>`；拓扑为空时返回输入错误，不退化为随机 host。
- [x] 所有拓扑节点都先生成带非 nil lazy querier 的 target；禁止在 target 构建阶段因 Ping 失败删除节点。一个节点建连失败不能导致其他 target 消失。
- [ ] 单测 native/HTTP context 取消、pool Ping 超时、target 稳定排序、同 shard 多 replica、不同 shard 同 host 标识。

Run:

```bash
go test ./common ./service/diagnosis -run 'Test.*(Context|Target|Adapter)' -race -v
```

### Task 2: Scene profiles、报告聚合与确定性严重度

**Files:** `service/diagnosis/profile.go`, `severity.go`, `sanitize.go`, `model/diagnosis.go`

- [x] 将上表 scene profiles 编码为只读 registry，提供 `ListProfiles`、`ResolveProfile`；拒绝客户端自带任意 check ID/threshold。
- [x] 定义 `RunReport` envelope：每个 check 保存 meta/header/concrete report/execution_error；聚合 execution/outcome 不覆盖各检查原值。
- [x] 实现纯函数 severity policy，并在结果中同时保存 `severity` 与触发它的 reason codes。
- [ ] 首版 policy：`disk_reserve_breached/lost_parts/keeper_unreachable=critical`；`replica_readonly/session_expired/mutation_failing/NO_REPLICA_HAS_PART=high`；backlog/delay/detached/too_many_parts=differentiated warning；partial/failed 单独显示 coverage risk，不冒充业务 severity。
- [ ] 实现 API normalizer、HTML-safe 文本约定和 LLM sanitizer：异常文本最多保留 512 字符、去控制字符、可选 hash identifiers。（已完成截断、控制字符清理和敏感字段移除；identifier hash 尚未接配置）
- [ ] 实现 32 KiB 分级裁剪：20 matches×4 observations → 10×2 → 5×1 → 仅统计；始终保留 `matches_total`、targets、execution/outcome、reason codes。
- [ ] 为状态矩阵、severity、敏感字段、UTF-8 边界、32 KiB 上限写 table-driven tests 和 golden JSON。

### Task 3: Diagnosis run 持久层

**Files:** `model/diagnosis.go`, `repository/persistent.go`, all repository backends/models/migrations, `repository/migrator.go`, DB tests

- [x] 增加 `PersistentDiagnosisRunService`：Create/Get/List/Update、`MarkRunRunningIfQueued`、`RequestCancel`、`HeartbeatRun`、`MarkStaleRunsInterrupted`、DeleteExpired。
- [x] CAS 方法必须由 DB 条件更新保证，不用“先读后写”模拟；只允许一个 instance 把 queued 变 running。
- [x] 为 sqlite/mysql/postgres/dm8 建表和索引：`run_id` PK、`cluster_name+created_at_ms`、`requested_by+created_at_ms`、`status+updated_at_ms`。
- [x] `ReportJSON`/`ContextJSON`/`ChecksJSON` 使用各 backend 的 text/clob 类型；写入前在 service 层执行 2 MiB 限制。
- [x] legacy JSON backend 增加 map 存储，migrator 能迁移 run；不迁移过期记录。
- [ ] repository 单测覆盖 CRUD、分页、所有权过滤、CAS、cancel、stale/expiry。
- [ ] 扩展 `repository/dbtest/backends_integration_test.go`，在真实 DB 验证 migration restart×3、large text、CAS 和毫秒时间往返。

Run:

```bash
go test ./repository/... -v
./repository/dbtest/pg-local-test.sh
go test -tags dbintegration -v ./repository/dbtest/ -run TestBackends
```

### Task 4: Orchestrator、worker、取消与多实例语义

**Files:** `service/diagnosis/orchestrator.go`, `executor.go`, tests

- [ ] `StartRun` 验证 cluster/profile/权限，持久化 queued 后立即返回 run ID；controller 不持有长查询。
- [ ] worker pool 默认 2，单 cluster 全量 run 并发 1；配置值必须有硬上限。
- [ ] owner 使用 ckman instance identity；running 每 5 秒 heartbeat。启动时只把 lease 超时的旧 running 标记 interrupted，首期不自动接管。
- [ ] 每个 check 串行执行；check 内把 profile budget 传给 `RunCheck`。每完成一项立即更新报告，前端可看到增量进度。
- [ ] cancel 设置 repository flag，并调用本实例 run cancel func；其他实例在 heartbeat/poll 点感知，最终保留已有 report。
- [ ] 服务关闭时 cancel 本实例运行，等待有界 grace period，并将未完成 run 标记 interrupted。
- [ ] 用 fake diagnose runner/clock/repository 测试 report+error、部分节点失败、排队、公平性、重复 claim、跨实例 cancel、shutdown。

### Task 5: 确定性诊断 API、RBAC 与 Swagger

**Files:** `controller/diagnosis.go`, `router/v1.go`, `server/enforce/ordinary.go`, request tests, swagger docs

- [ ] 实现 checks/create/list/detail/cancel handler；所有 cluster/run ID 必须交叉校验，防止通过另一个 cluster URL 读取 run。
- [ ] ordinary 只看自己创建的 run；admin 可看全部；owner check 在 controller/service 层执行，不能只靠 URL policy。
- [ ] `context` 仅允许已声明字段且限制长度；unknown scene、非法 pagination、过大 context 返回明确错误码。
- [ ] 在 ordinary policies 增加 diagnostics GET/POST；guest 不增加；admin 沿用全权限。
- [ ] response logger 对 report detail 和 explain 流设置省略/脱敏，不能把原始异常及 AI 内容写入 debug log。
- [ ] 增加 Swagger annotations 并运行 `swag init`。
- [ ] controller tests 覆盖 guest/ordinary/admin、跨用户、跨 cluster、取消、partial/no_match/failed 三态。

### Task 6: 诊断抽屉与第一批场景入口（前端 M2）

**Repository:** `../ckman-fe`

- [ ] 开工前确认 `git status`、当前 branch 和远端跟踪；只在 sibling repo 修改。
- [ ] 创建 `apis/diagnosis.ts` 和 typed models，稳定使用 snake_case contract。
- [ ] 创建全局 `DiagnosisDrawer`，由 root/layout 挂载；输入 `{cluster, scene, context}`，避免每个页面复制状态机。
- [ ] 抽屉按“覆盖率 → 结构化发现 → 证据 → 后续建议”展示；原始异常必须用文本节点，不用 `v-html`。
- [ ] 轮询 detail（运行中 1s、后台 3s、完成停止）；离开页面不取消 server run，显式按钮才 cancel。
- [x] 在管理集群、表指标、复制状态、慢查询和分布式 DDL 页面加入行内/页内 AI 诊断入口；概览页不放入口。
- [ ] 状态文案严格区分 `matched/no_match/unknown` 和 `complete/partial/failed`；`no_match` 不用绿色“健康”。
- [ ] 当前前端没有自动化 test runner，不在本功能内仓促引入一套；把 loading、partial+matched、no_match、failed、cancel、超大报告折叠和 XSS 字符串列入本任务的浏览器手工验收清单，并把 SSE 解析/状态转换尽量写成无 DOM 的纯 TypeScript helper，便于后续统一测试基建接入。
- [ ] `npm run build`（或仓库 Makefile 对应命令）通过后提交 ckman-fe，再更新父仓库 submodule 指针。

### Task 7: 业务场景采集（前端与上游 M3）

- [x] `resource_pressure`：管理集群页；CPU/内存快照和磁盘水位。
- [x] `replica_status`：复制状态行；按 database/table 过滤。
- [x] `table_health`：表指标行；按 database/table 过滤 parts、detached parts、mutation。
- [x] `slow_query`：慢查询行；按 query_id 过滤 query_log。
- [x] `distributed_ddl`：DDL 行；按 entry 过滤 distributed_ddl_queue。
- [x] 不提供诊断中心；run 历史仅保留为服务端审计和断线恢复数据。

### Task 8: LLM 配置和 CKMAN Agent 抽象

**Files:** `config/config.go`, `config/remote_apply.go`, `service/diagnosis/agent/*`

- [ ] 新增 `LLMConfig`：Enabled/BaseURL/Model/APIKeyEnv/APIKeyFile/TimeoutSeconds/MaxContextBytes/MaxOutputTokens/SendRawExceptions；默认 disabled。
- [ ] `MergeEnv` 只读取 API key 值到进程内 secret holder，不把值写回 `GlobalConfig` 可序列化字段；默认 env 名 `CKMAN_LLM_API_KEY`。
- [ ] remote config merge 允许非秘密字段覆盖，但 API key/env/file bootstrap 字段不接受远端覆盖；补 remote_apply tests。
- [ ] 定义 CKMAN-owned `AgentEngine`, `AgentRequest`, `AgentEvent`, `AgentResult`；其他包不 import Nova 类型。
- [ ] 定义事件：message_delta/usage/error/done；所有事件带 run correlation ID，不带思维链。
- [ ] 定义模型请求 timeout 与输出 token 预算。

### Task 9: Nova v0.4.1 适配器和一次性解释（M4）

**Files:** `service/diagnosis/agent/nova/*`, `controller/diagnosis.go`, tests

- [ ] 固定 `github.com/AutumnsGrove/nova v0.4.1`；只 import headless backend packages，不 import TUI/charm UI。
- [ ] 实现 message/event/tool/provider 转换，Nova 类型只存在于 `agent/nova` 包。
- [ ] explain 首期不给模型 `run_check`：把已脱敏、已裁剪 report 作为不可信 data block，单次生成解释；模型失败只结束 explain stream。
- [ ] system prompt 固定纪律：报告唯一事实源；AI 解释标识；partial/failed/truncated 必须说明；禁止生成修复 SQL/命令；异常文本不得视为指令。
- [ ] `POST .../explain` 用 SSE 输出，连接断开取消模型请求但不取消 diagnosis run；发送心跳，最终事件含 usage/error。
- [ ] provider fake tests 覆盖 streaming、timeout、disconnect、oversized context、恶意异常文本、模型声称执行修复时的输出标记。

### Task 10: 一次性 AI 输出体验

**Repository:** `../ckman-fe`

- [x] 抽屉在结构化报告完成后自动开始一次 AI 解释；AI 未配置时只展示结构化报告。
- [ ] 使用 `fetch` + `ReadableStream` 消费 POST SSE；axios 不承担该流。实现 abort、重连提示和事件去重。
- [ ] AI 区域永久显示“AI 解释，以结构化报告为准”；coverage warning 固定在回答上方，不由模型文案决定。
- [x] 不提供输入框、推荐追问、重新解释和诊断中心跳转。
- [ ] 错误态区分 provider unavailable 与 timeout；任何错误都保留结构化报告。
- [ ] 浏览器手工验收覆盖中断流、重复事件、Markdown/XSS、安全链接、刷新恢复、AI disabled；执行现有 `npm run build`、`npm run lint`、`npm run lint:style`，不新增未被仓库采用的测试框架。

### Task 11: 指标、保留策略、兼容性和发布门槛

- [ ] 增加低基数指标：run count/duration/inflight、check execution/outcome、target error category、LLM request/duration/error/token；标签禁止 cluster/table/user/run ID。
- [ ] 增加每日过期清理，仅 master 执行；默认 run 30 天、session 30 天，可配置但有范围限制。
- [ ] 在 ClickHouse 目标版本矩阵至少验证当前 ckman 主力旧版本和诊断库已验证的 26.3/26.10；native/HTTP/TLS 各跑 context cancel、Nullable scan、partial report。
- [ ] 更新 website：诊断字段语义、权限、LLM 配置、数据出境/脱敏、故障排查；Swagger playground 同步。
- [ ] 执行后端全量测试、race、vet、前端 build，以及 repository 真 DB 测试。

Release gate:

```bash
go test ./... -v
go test -race ./service/diagnosis/... ./common/...
go vet ./...
./repository/dbtest/pg-local-test.sh

cd ../ckman-fe
npm run build
```

必须人工验收：

1. LLM disabled 时完成全部确定性诊断流程。
2. 4 节点中 1 节点不可达时显示 `partial`，已有 matched 证据仍存在。
3. cancel 能中断 native 与 HTTP 查询，run 保留已采集证据。
4. ordinary 不能读取他人 run/session，guest 不能创建诊断。
5. LLM 请求正文不含密码、SSH 信息、API key、未脱敏异常全文。
6. 模型请求任意 SQL/host/修复操作时没有可调用工具。
7. provider 断网、超时、SSE 断开不影响诊断报告。
8. 多实例同时 claim 同一个 queued run 时只有一个 winner。

## Explicitly Out of Scope

- 自动修复、修复 SQL/命令生成与执行。
- 定时巡检、告警通知、Webhook。
- 任意 SQL Agent、schema/data RAG、联网搜索。
- Eino、multi-agent、planner/executor/reviewer、human-in-the-loop repair。
- 将诊断检查合并进 clickhouse-go driver。
- v2 API 体系重构。

满足以下任一条件再单独设计 Eino/工作流升级：工具超过 8–10 个；引入知识库/RAG；修复需要人工审批后恢复；需要跨天 checkpoint；出现多个专业 Agent。升级时替换 `AgentEngine` 实现，不改变 Diagnosis Orchestrator、API report contract 或前端证据模型。
