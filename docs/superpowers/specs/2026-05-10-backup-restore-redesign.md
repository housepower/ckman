# ckman 备份/恢复功能 重设计 Spec

- 日期：2026-05-10
- 状态：Draft（待用户 review）
- 范围：Backend (`controller/data_manage.go`, `service/clickhouse/data_manage.go`, `service/cron/cron_service.go`, `model/data_manage.go`, `repository/`) + Frontend (`frontend/src/views/data-manage/`, `frontend/src/apis/dataManage.ts`)

---

## 1. 背景与目标

ckman 现有备份/恢复功能在生产上暴露出两类核心痛点：

1. **调度规模问题**：三四百张表都用 cron 定时备份时，整点齐发会同时启动几百个 `BackupManage` goroutine；同一个 backup_id 上次未跑完时下次 cron 又触发，会出现两个 goroutine 并发改同一份数据库记录、并发对 ClickHouse 发 BACKUP TABLE 的"错乱"。
2. **台账模型问题**：当前的 `Backup` 表把"调度配置"和"执行历史"混在一条记录里，每次 cron 触发只是 update 同一记录的 partition 数组，看不到「某表过去 30 天每天的备份情况」「某 policy 跑了多少次/失败了多少次」。

此外现有实现还有若干已识别的 Bug 和健壮性缺口（详见 §2），适合一次性在重构里清理。

**目标**：

- 把"调度配置"与"执行历史"拆为两张表，让台账可按表 / 按任务双维度查询
- 引入全局 worker pool，限定并发，cron 仅做"提交到队列"
- HTTP 接口全部异步化，立即/定时/恢复 三类操作统一为「提交→ run_id→ 轮询」模型
- 支持 policy 编辑（含跨实例 failover）
- 修掉 §2 列出的所有真 Bug
- 不破坏 ckman 多实例 + 实例绑定的运行模型；wire JSON 字段名保持兼容

**非目标**（YAGNI）：

- 不引入第三方任务队列（NSQ/Redis）
- 不做分布式锁；ownership 仍靠 `policy.instance` 字段切片
- 不做自动重试、自动 retention 清理（后续按需要单独提）
- 不复用 `service/runner` 的 task 系统（评估后语义不合）

---

## 2. 当前实现的问题清单（要在重设计中修掉）

按严重度排序，编号沿用先前 review。这些都被新设计纳入修复。

| 编号 | 位置 | 问题 |
|---|---|---|
| #2 | `controller/data_manage.go:155` | `RestoreData` 校验失败用 `continue` 而非 `return`，导致响应被多次写入并继续创建恢复记录 |
| #3 | `service/clickhouse/data_manage.go:493` | `Check` 早 return 写在 for 循环里，多分区时只校验第一个 |
| #4 | `service/clickhouse/data_manage.go:155-178` | `Init` 在 `GetBackupByTable` 报错时丢弃新枚举的分区，run 静默"成功但啥都没备" |
| #5 | `service/clickhouse/data_manage.go:248-312` | `Prepare` checksum 阶段多 goroutine 写 `lastErr` 数据竞争；早 return 不关 `rows` |
| #6 | `controller/data_manage.go:103,173` | `BackupManage` 同步在 HTTP handler 里执行，大表数小时连接被拖死 |
| #7 | `service/clickhouse/data_manage.go:507-510` | `Close` 中 DROP PARTITION 错误被 `_ =` 静默吞，`Clean=true` 失败仍报 success |
| #8 | 全文件 | `SetStatus` / `repository.Ps.UpdateBackup` 错误普遍被忽略 |
| #9 | 多处 | `Local.Path`、库/表/分区名、S3 凭证拼到 SQL/Shell，存在注入面 |
| #11 | `controller/data_manage.go` | restore 不校验 cluster 是否与源 backup 一致 |
| #12 | `controller/data_manage.go:102-106` | 立即备份 controller 不判断 `instance == self`，多实例下任意节点都可执行 |
| 其它 | `service/clickhouse/data_manage.go:488` | `Check` 本地分支 `// todo` 未实现，本地备份开 checksum 实际不做校验仍标 success |

#1（JSON tag 与字段名错位）和 #2、#3 已在 brainstorming 之前完成的小修中处理。本 spec 覆盖剩余项。

---

## 3. 整体架构

### 3.1 分层

```
HTTP API (controller/data_manage.go)
      |    cron Scheduler
      |   /
      v  v
   BackupService (Submit / SubmitRestore / 查询)
      |
      v
   BackupRun (queued)  ← 持久化，启动时回填到内存队列
      |
      v
   Worker Pool (size = backup.max_concurrent)
      |
      v
   RunExecutor: init → prepare → backup/restore → check → close
      |
      v          v
 ClickHouse    Storage Backend (S3 / Local)
```

### 3.2 文件布局

```
controller/data_manage.go            // 仅参数解码 + 调 BackupService
service/backup/
    service.go                       // BackupService：Submit / SubmitRestore / 查询 / 重叠检测
    scheduler.go                     // cron 注册 + 60s reconcile loop
    pool.go                          // 队列 + worker pool（合并）
    executor.go                      // RunExecutor，状态机各阶段
    storage/
        storage.go                   // Storage interface
        s3.go                        // 复用现 S3Client，迁过来
        local.go                     // 新写：含路径白名单 + checksum
model/data_manage.go                 // BackupPolicy / BackupRun / BackupRunPartition
repository/                          // 各 backend 加 BackupPolicyService / BackupRunService
```

### 3.3 已抛弃的方案

- 拆 5 个文件（service/scheduler/queue/worker/executor/storage）：过头，Queue+Worker 合并
- 引入 `RunUpdater` 接口屏蔽 repository：过度设计；executor 直接调 `repository.Ps.UpdateBackupRun`
- 单纯渐进式（保留 Backup 单表 + 加 worker pool）：解决不了台账问题
- 复用 `service/runner` 的 task 系统：runner 主要承载一次性运维操作，与"周期可重叠备份"语义不合

---

## 4. 数据模型

### 4.1 BackupPolicy（调度配置）

```go
type BackupPolicy struct {
    PolicyID     string    // PK，uuid
    ClusterName  string
    Database     string
    Table        string
    ScheduleType string    // immediate | scheduled
    Crontab      string    // 仅 scheduled；最小间隔 1 小时
    Instance     string    // 哪个 ckman 跑（与现状一致）
    BackupStyle  string    // full | incremental
    BackupType   string    // partition | daily
    DaysBefore   int
    Partitions   []string  // 按分区时的分区清单
    TargetType   string    // s3 | local
    S3           TargetS3  // SecretAccessKey 入库前 AES 加密
    Local        TargetLocal
    Compression  string
    Checksum     bool
    Clean        bool
    Enabled      bool      // 软禁用（不删 policy 不丢历史）
    Deleted      bool      // 软删
    CreateTime   time.Time
    UpdateTime   time.Time
}
```

索引：PK(`policy_id`) + idx(`cluster_name`,`database`,`table`)

### 4.2 BackupRun（执行历史，不可变）

```go
type BackupRun struct {
    RunID        string                 // PK，uuid
    PolicyID     string                 // FK → BackupPolicy
    ClusterName  string                 // 冗余索引列：表台账查询
    Database     string                 // 冗余
    Table        string                 // 冗余
    Operation    string                 // backup | restore
    TriggerType  string                 // cron | manual_immediate | manual_restore | retry
    Instance     string                 // 实际执行实例
    Status       string                 // queued | running | success | failed | skipped | interrupted
    StatusReason string                 // skipped: overlap / disabled / queue_full；interrupted: ckman restart
    Partitions   []BackupRunPartition   // 内嵌 JSON
    StartedAt    time.Time
    FinishedAt   time.Time
    Elapsed      int                    // 秒
    ErrorMsg     string
    CreateTime   time.Time
}

type BackupRunPartition struct {
    Partition string
    Status    string                    // waiting | running | success | failed
    Size      uint64
    Rows      uint64
    FileNum   uint64
    Elapsed   int
    Msg       string
    PathInfo  map[string]PathInfo       // shard host → path/md5（仅 checksum=true）
}
```

索引：PK(`run_id`) + idx(`policy_id`,`started_at desc`) + idx(`cluster_name`,`database`,`table`,`started_at desc`) + idx(`status`,`instance`)

### 4.3 关键设计选择

1. **立即备份也建 policy**（`ScheduleType=immediate, Enabled=false`），统一台账查询入口
2. **restore 不建 policy**：run 上挂 `Operation=restore, PolicyID=源 policy_id`，源 policy 台账上能看到「备份过 + 恢复过」
3. **冗余 (cluster, database, table) 到 run 上**：表台账直接 WHERE，不 join policy
4. **partitions 内嵌 JSON**（不另起第三表）：单 run 上限 200 分区，JSON 表示足够；持久层（local/MySQL/PG/DM8）都能 LOB 字段存
5. **partitions 在 run 里不可变**：每次 cron 触发都写新 run；与现状"反复改写同一数组"的根本区别

---

## 5. 调度内核

### 5.1 BackupQueue + WorkerPool（合并在 `pool.go`）

- 队列：内存 `chan string`（run_id），容量 = `max_concurrent * 4`
- Worker 数 = `backup.max_concurrent`（配置项，默认 8）
- 启动时回填：`SELECT run_id FROM backup_run WHERE instance=self AND status='queued' ORDER BY create_time` → 推入 channel
- Worker 拉到 run_id 后：
  1. `UPDATE backup_run SET status='running', started_at=NOW() WHERE run_id=? AND status='queued'`（条件更新防重复执行）
  2. 影响行 = 0 → 跳过（已被其他 worker / 状态已非 queued）
  3. 调 `RunExecutor.Run(run)`
  4. 完成后 UPDATE status=success/failed, finished_at, elapsed, error_msg
- 关闭：context cancel；不打断进行中 run，不再拉新；超时 30s 强退（重启后会被标 interrupted）

### 5.2 队列满策略

`pool.Submit(run_id)` 用 non-blocking send：
- 成功 → 入队
- 失败（队列满） → 调用方写 `BackupRun(status=skipped, reason=queue_full)`；HTTP 调用方返回 503，cron 触发 silently 记台账

### 5.3 重叠检测（在 `BackupService.Submit` 内）

```sql
SELECT 1 FROM backup_run
 WHERE policy_id = ?
   AND status IN ('queued','running')
 LIMIT 1
```

命中即写 `BackupRun(status=skipped, reason=overlap, finished_at=NOW())`，**不**入队。

性能：任一时刻一个 policy 的 in-flight run 由设计保证 ≤ 1，索引 `(policy_id, status)` 命中即返回。即便最坏情况持久层不支持索引查（如 local backend）也是亚毫秒级全扫。

### 5.4 Scheduler Reconcile（核心新增）

每 60 秒跑一次：

```
1. policies := SELECT * FROM backup_policy
                WHERE enabled=true AND deleted=false
                  AND schedule_type='scheduled'
                  AND instance=self
2. cron_jobs := 当前 cron 表中已注册的 (policy_id → fn)
3. diff:
   - in policies, not in cron_jobs        → AddJob(policy_id, crontab, fn)
   - in cron_jobs, not in policies        → RemoveJob(policy_id)
   - in both, but crontab changed         → RemoveJob then AddJob
```

效果：
- 修改 crontab：≤60s 生效
- 故障转移：在活着的 ckman-02 上把 `policy.instance` 改成 ckman-02，下个 reconcile 周期 ckman-02 注册 cron；ckman-01 复活时它的 reconcile 发现 `instance != self` 自动移除 job
- 启用/禁用、删除：≤60s 生效

可选优化（不在本期）：编辑 API 返回前给本进程 scheduler 发内存信号触发立即 reconcile，把延迟从 60s 降到 < 1s。

### 5.5 启动顺序

```
main.Init()
  ├─ repository.Ps.Init()
  ├─ service.backup.Init()
  │    ├─ UPDATE backup_run SET status='interrupted', status_reason='ckman restart',
  │    │                         finished_at=NOW()
  │    │   WHERE instance=self AND status IN ('queued','running')
  │    ├─ pool.Start(max_concurrent)
  │    │    └─ 同时回填残留 queued runs
  │    └─ scheduler.Start()
  │         ├─ 立即 reconcile 一次
  │         └─ 启动 60s ticker
  └─ router.Init()              ← HTTP 才开始接请求
```

---

## 6. 状态机

### 6.1 Run 整体状态

```
        Submit
           │
           v
       ┌─────────┐
       │ queued  │
       └─┬───┬───┘
         │   │ 提交时检测到同 policy 已有 queued/running
         │   └────────────────► ┃ skipped (overlap) ┃
         │                       ┃ skipped (queue_full) ┃
         │                       ┃ skipped (disabled) ┃
         │  worker 拉取
         v
     ┌─────────┐
     │ running │
     └─┬─┬─┬───┘
       │ │ │ 任一阶段不可恢复错误
       │ │ └────────► ┃ failed ┃
       │ │
       │ │ ckman 重启时 instance==self
       │ └──────────► ┃ interrupted ┃
       │
       │ 全流程 OK
       v
    ┃ success ┃
```

不变量：
- skipped / interrupted / failed / success 是终态，不再修改
- 一个 run 进入 failed 时已 success 的分区**保留** success（部分成功），用户能挑成功分区做 restore

### 6.2 Partition 子状态（内嵌于 run.partitions）

```
waiting → running → success
                 └─► failed
```

partition 级失败**不传染**，其他分区继续；run 级失败（连不上 cluster、清旧目录失败、close 失败）立即终止后续阶段。

---

## 7. HTTP API

### 7.1 端点变更

| 现有 | 新形态 |
|---|---|
| `POST /api/v2/ck/backup/:cluster` | 参数同；响应 `{run_ids: [...]}`；立即备份 instance 强制 = self |
| `POST /api/v2/ck/restore/:cluster` | 入参 `{source_run_id, partitions}`（旧 `backup_id` 改名）；响应 `{run_id}` |
| `GET /api/v2/ck/backup/:cluster` | **重定义**：返回 policy 列表（按 cluster 范围） |
| `GET /api/v2/ck/backup/:backup_id` | `GET /api/v2/ck/backup/policy/:policy_id` |
| `DELETE /api/v2/ck/backup/:backup_id` | `DELETE /api/v2/ck/backup/policy/:policy_id`（软删） |

新增：

| 端点 | 用途 |
|---|---|
| `PUT /api/v2/ck/backup/policy/:policy_id` | 编辑 policy（详见 7.2） |
| `POST /api/v2/ck/backup/policy/:policy_id/trigger` | 「立即触发」一次（生成新 run） |
| `GET /api/v2/ck/backup/run/:run_id` | 单 run 详情 |
| `GET /api/v2/ck/backup/policy/:policy_id/runs?limit=&before=` | 任务维度台账 |
| `GET /api/v2/ck/backup/table/:cluster/:database/:table/runs?days=` | 表维度台账 |
| `GET /api/v2/ck/tables/:cluster/:database/summary` | 表的分区方式 + size，前端创建表单用 |

### 7.2 PUT policy 可编辑字段

| 字段 | 可编辑 |
|---|---|
| crontab / instance / enabled | ✓ |
| days_before / partitions / compression / checksum / clean | ✓ |
| s3.* / local.path | ✓（SecretAccessKey 留空表示保持原值） |
| backup_style / backup_type | ✓ |
| **cluster_name / database / table / schedule_type** | ✗ — 改这些等于不同 policy；要换就建新 policy（旧的软删但 runs 保留） |

修改最长 60s 后生效（reconcile 周期）。

### 7.3 限制

- `BackupRequest.Tables` 长度 ≤ 100，超出 400
- `BackupRequest.Partitions` 长度 ≤ 200，超出 400
- `Crontab` 最小间隔 ≥ 1 小时（提交时校验，cron 表达式解析后比较两次相邻触发时间差）

---

## 8. 错误处理与边界条件

### 8.1 提交阶段

| 场景 | 处理 |
|---|---|
| 表/分区不存在 | 400，不写 policy 不写 run |
| Restore 时源 run 状态非 success | 400 + return（修 #2） |
| Restore 时 cluster ≠ 源 run.cluster | 400（修 #11） |
| 写 policy 成功但写 run 失败 | 同事务则一并回滚；不支持事务的持久层显式 DELETE 已写 policy |
| 同 policy 已有 queued/running | 写 `skipped, reason=overlap` 的 run；cron 触发场景 silently 记台账，手动触发返回 409 |
| policy.enabled=false 时仍被 cron 触发（disable 与 reconcile 之间最多 60s 窗口） | 写 `skipped, reason=disabled` 的 run；不入队 |
| pool 队列满 | 写 `skipped, reason=queue_full`；HTTP 返 503 |
| 跨 instance 立即备份 | 立即备份 policy.instance 永远 = self（修 #12） |
| Daily 模式 + 表分区不兼容 | 整批 reject 400，错误信息列出哪些表为何不兼容（详见 8.5） |

### 8.2 执行阶段

| 阶段 | 失败 | 处理 |
|---|---|---|
| Init 连 shard | 任一 shard 全部 replica 不可达 | run failed |
| Init 列分区（修 #4） | `GetBackupByTable` 报错 | 视为无历史，使用本次计算的分区集，**不丢分区** |
| Prepare 行数/大小查询 | 单分区查询失败 | 该分区 failed，其他继续 |
| Prepare 远程 md5sum（修 #5） | 改用 `errgroup.Group`，单 host 失败该分区 failed | rows.Close defer 立即生效 |
| Prepare 清旧目录 | 失败 | run failed（旧文件残留会让 BACKUP 重名错） |
| Backup 单分区单 shard BACKUP TABLE | 失败 | 该分区 failed，其他继续 |
| Backup 全部分区失败 | run failed | |
| Backup 部分成功部分失败 | run failed，partitions 各保留终态；用户能 restore 成功子集 | |
| Check checksum 不匹配（修 #3） | run failed，error_msg 列出第一个不匹配的 partition + host | |
| Check 本地分支 | 不再 `// todo`：Local 实现内 SSH 远程比对 md5 | |
| Close DROP PARTITION 失败（修 #7） | run failed, status_reason="cleanup_failed: ..."；**不再静默** | |
| 任意 `repository.Ps.UpdateBackupRun` 失败（修 #8） | 记 error 日志并短路后续步骤，不忽略 | |

### 8.3 并发

| 风险 | 处理 |
|---|---|
| Prepare checksum 多 goroutine 写 lastErr（修 #5） | `errgroup.Group` + `errors.Join` |
| BackupData 多 goroutine 写 partitions[i].Status | 不再共享 slice 写；每 goroutine 把结果写 local；主 goroutine 串行汇总 |
| `Back.lock` + `wg` 跨阶段复用 | 每阶段独立 `sync.WaitGroup`，结束即丢 |
| 同 run_id 被两 worker 拉到 | 5.1 的条件 UPDATE 防重复执行 |

### 8.4 启动 / 退出

| 场景 | 处理 |
|---|---|
| 启动时 instance==self 的 queued/running run | 全部 → interrupted（5.5） |
| Graceful shutdown (SIGTERM) | pool 不再拉新；进行中 run 不打断；30s 超时强退 |
| 重复启动同一 ckman | 不防（运维责任）；同 instance 同时跑两进程会互覆盖状态 |

### 8.5 Daily 模式分区兼容性校验

「按日期 (daily)」模式假设 partition key 是 `YYYYMMDD` 字符串。其他场景全是踩坑，必须前置校验。

兼容性矩阵：

| 表分区方式 | 全量 | 按分区 | 按日期 |
|---|:---:|:---:|:---:|
| 按天 `toYYYYMMDD(t)` / `formatDateTime(t,'%Y%m%d')` | ✓ | ✓ | ✓ |
| 按月 `toYYYYMM(t)` | ✓ | ✓ | ✗ |
| 按周 / 按小时 | ✓ | ✓ | △（看格式） |
| 非时间分区 | ✓ | ✓ | ✗ |
| 无分区（隐式 `tuple()`） | ✓ | △（仅 'all'） | ✗ |
| 无时间字段 | ✓ | ✓ | ✗ |

实现：
1. **Backend Submit 校验**：`SELECT engine_full, partition_key FROM system.tables WHERE database=? AND name=?`，判断 partition_key 是否含 `toYYYYMMDD` / `toDate` / `formatDateTime(...,'%Y%m%d')`；再从 `system.parts` 抽样验证至少一个分区匹配 `^\d{8}$`。
2. **多表批量**：任一表不兼容 → 整批 reject 400，错误信息列出每张表的不兼容原因
3. **Frontend 智能置灰**：见 §10.2

`BACKUP_BY_DAILY` 常量改名为 `BACKUP_TYPE_DAILY_PARTITION`，让代码语义更准确。

---

## 9. 安全

| 面 | 处理 |
|---|---|
| `Local.Path` → `RemoteExecute("rm -fr ...")` 注入面（修 #9） | `storage/local.go` 强校验：路径必须匹配 `^/[a-zA-Z0-9_./\-]+$`，不允许 `..`，长度 ≤ 256；提交时拒绝 |
| 数据库/表/分区名拼到 SQL（修 #9） | identifier 用反引号包住，含反引号字符的 reject；分区名值用单引号包，含 `'` 的 reject |
| S3 endpoint/bucket/AK/SK 拼到 BACKUP SQL | endpoint 必须 `http(s)://` 前缀；bucket 用 S3 命名规则正则；AK/SK 单引号转义 |
| BackupPolicy.S3.SecretAccessKey 入库 | `common.AesEncryptECB`，executor 用前解密 |

---

## 10. 前端改动

### 10.1 API 客户端 (`frontend/src/apis/dataManage.ts`)

按 §7.1 调整。新增 `getRunDetail`, `listRunsByPolicy`, `listRunsByTable`, `updatePolicy`, `triggerPolicy`, `getTableSummary`。

### 10.2 表单页（`backup.vue` → 新建/编辑共用）

- **去掉** Cluster 字段（页面已在 cluster 上下文，breadcrumb + pill 显示）
- **Tables 选择**：可搜索多选下拉，前端 fuzzy match。下拉项展示分区方式 tag（按天 / 按月 / 无分区 / 自定义）+ 数据量；已选 chip 上展示分区 pill；分区不兼容的 chip 染黄
- **进表单页时一次性 batch 查询** `GET /api/v2/ck/tables/:cluster/:database/summary`，前端缓存几分钟，搜索/过滤纯前端
- **「按日期」radio 智能置灰**：所选表中存在不兼容时自动 disable，hover tooltip 显示原因
- **Crontab**：实时显示「下次触发时间」（cronstrue 库本地算）；最小间隔 1 小时校验
- **执行实例下拉**：标记健康状态（依赖 nacos 心跳暴露的内部 API；本期建议先实现仅显示列表 + 当前实例标记，健康状态留作 P2）
- **限制提示**：tables 计数器 `已选 N / 100`；partitions `N / 200`；超出阻止提交
- **Secret Key 输入框**（编辑场景）：`留空保持原值`
- **不可编辑字段**（编辑场景）：cluster / database / table / schedule_type 置灰

### 10.3 备份管理页（替代现 `history.vue`）

双 Tab 结构：

**Tab 1：任务列表**
- 表格列：database.table / 类型 / cron+实例 / 启用 / 最近一次（时间 + 状态 tag）/ 操作（立即触发 · 编辑 · 禁用 / 删除）
- 行可展开，展开区显示该 policy 最近 N 次 run（每行：触发时间 / 触发方式 / 状态 / 分区数 / 耗时 / 简述 / 查看）
- 上方筛选：状态 / database 下拉 + 表名搜索

**Tab 2：表台账**
- 筛选：database / table / 时间范围（默认 30 天）
- 主区：日历视图，每格显示状态 + 中文耗时（4分 / 5时）+ ✓/✗ 图标
  - 鼠标悬停显示完整 tooltip：`触发时间 · 状态 · 耗时 · 数据量 · 分区数`
  - 点击格子打开 Run 详情
- 右侧汇总卡：成功/失败/跳过/进行中数量 · 最近一次成功/失败 · 无备份天数

**Run 详情面板**（两 Tab 共用）：
- 元数据（run_id / policy / 触发方式 / 操作 / 实例 / 状态 / 时间）
- 分区明细表
- 错误日志（error_msg + 必要时 partition.msg）
- 操作：从此 run 恢复 · 重跑同 policy · 查看完整日志

### 10.4 提交后行为

不再页面跳转，弹绿色 toast：「✓ 已提交 N 个备份任务」+ 每个 run 的「查看」链接 + 5 秒自动消失。

### 10.5 轮询策略

- 列表页打开拉一次，**不**自动轮询
- Run 详情页若 status ∈ {queued, running}，每 5 秒轮一次直到终态后停

### 10.6 配色

- 主色：`#C9A100`（ckman 金黄，已存在于 `frontend/src/app/variables.scss`）
- 状态色：success `#67C23A` / warning `#E6A23C` / danger `#F56C6C` / info `#909399`（与 Element 默认一致）
- 中断特用色：`#ED8936`

---

## 11. 数据迁移

现有 `Backup` 单表 → `BackupPolicy` + `BackupRun` 双表。

**一次性迁移脚本**（`scripts/migrate-backup-v2.sql` / `.go`）：

```
for each row in backup:
    INSERT BackupPolicy (
        policy_id     = backup.backup_id（保留同 ID 便于追溯）
        cluster_name  = backup.cluster_name
        database      = backup.database
        table         = backup.table
        schedule_type = backup.schedule_type
        crontab       = backup.crontab
        instance      = backup.instance
        days_before   = backup.days_before
        target_type   = backup.target_type
        s3, local     = backup.s3, backup.local
        compression   = backup.compression
        checksum      = backup.checksum
        clean         = backup.clean
        enabled       = (backup.status != 'closed')
        backup_style  = (backup.partitions[0].partition == 'all') ? 'full' : 'incremental'
        backup_type   = 'partition' or 'daily'（启发式：days_before > 0 → daily）
        deleted       = false
        create_time   = backup.create_time
    )
    INSERT BackupRun (
        run_id        = uuid.New()
        policy_id     = backup.backup_id
        cluster/db/table = ...
        operation     = backup.operation
        trigger_type  = 'migrated'
        instance      = backup.instance
        status        = backup.status（映射）
        partitions    = backup.partitions（直接搬）
        started_at    = backup.create_time
        finished_at   = backup.update_time
        elapsed       = (finished_at - started_at).seconds
        create_time   = backup.create_time
    )
```

迁移完成后老 `backup` 表保留只读，下个版本删除。

---

## 12. 测试策略

### 12.1 单元测试 (`go test ./service/backup/...`)

| 组件 | 重点用例 |
|---|---|
| `BackupService.Submit` | 表/分区不存在 → 400；写 policy 后写 run 失败 → 回滚；overlap 命中 → skipped；overlap 未命中 → queued + 入队；queue 满 → 503；跨 instance 立即备份 → 拒绝；daily 模式 + 不兼容表 → 整批 reject |
| `BackupService.SubmitRestore` | 源 run 不存在 / 状态非 success / cluster 不一致 → 400；正常路径生成 partitions 子集 status 全 waiting |
| `Scheduler.Reconcile` | 启动时只加载 enabled+scheduled+instance==self；增/删/crontab 变化的 diff 各场景；非法 cron 跳过并 warn |
| `Pool` | 队列满 Submit 返 false；worker 数 = max_concurrent；shutdown 不拉新；同 run_id 二次推入由 worker 条件 UPDATE 跳过 |
| `Executor` 各阶段 | mock repository + mock storage；测部分分区失败 / checksum 不匹配 / Close 清理失败 等 |
| `storage/local.go` | 路径白名单（合法 / `..` / `;` / 空格 / 超长）；checksum 计算与 S3 对齐；Clean 路径不被注入 |
| `storage/s3.go` | endpoint/bucket/AK/SK 转义；bucket 自动建 |

测试基础设施：fake `repository.Ps`，fake `storage.Storage`，不依赖真 ClickHouse / S3。

### 12.2 集成测试（`build tag integration`，CI 默认不跑）

需要真 ClickHouse + minio：

- 立即备份 → success → restore 到新表 → 数据一致
- 定时备份连跑 3 次 → 3 条 run，partitions 互相独立
- 重叠：第一次跑时手动再 Submit → skipped run
- ckman kill -9 + 重启 → in-flight run 全 interrupted
- 大表（>100 GB）不阻塞 HTTP
- Clean=true + DROP PARTITION mock 失败 → run 标 failed（修 #7 验证）

### 12.3 手测/回归清单

- 数据迁移脚本：验证老备份历史在新 UI 上能正确呈现
- 立即备份单/多表
- policy 编辑：crontab、instance、enabled 三种修改 ≤60s 生效
- 跨 instance failover：手动停 ckman-01，UI 上把某 policy.instance 改到 ckman-02，验证下次 cron 触发到 ckman-02
- 三百表整点齐发：worker pool 排队，HTTP/UI 不卡

### 12.4 观测

加 Prometheus 指标：

- `ckman_backup_runs_total{status,trigger}` (counter)
- `ckman_backup_run_duration_seconds{operation}` (histogram)
- `ckman_backup_pool_queue_length` (gauge)
- `ckman_backup_pool_active_workers` (gauge)

Log 格式：`[backup][run=<id>][policy=<id>] phase=<...> ...`

---

## 13. 实施分期建议

| 阶段 | 内容 | 周期 |
|---|---|---|
| P0 | 数据模型 + repository 各 backend 实现（local/MySQL/PG/DM8）+ 迁移脚本 | 3 天 |
| P1 | `service/backup/` 4 文件骨架（service/scheduler/pool/executor）+ Pool + Scheduler.Reconcile + Submit/SubmitRestore 主路径 | 5 天 |
| P2 | `Executor` 各阶段从老代码搬过来 + 修 #3-#9 + storage 抽象（含 local 完整实现）| 5 天 |
| P3 | HTTP 端点重构 + 老 API 兼容期（一个版本同时支持两套） | 2 天 |
| P4 | Frontend 改造（创建表单 + 任务列表 + 表台账 + 编辑 modal） | 5 天 |
| P5 | 集成测试 + 手测回归 | 2 天 |
| **合计** | | **约 22 个工作日 / 3 人周** |

可并行：P3/P4（前后端各一人）、P5 前期可与 P4 后期并行。

---

## 14. 不做（YAGNI 明确）

- 第三方任务队列（NSQ / Redis / RabbitMQ）
- 分布式锁（ownership 靠 instance 字段）
- 自动重试（用户从 UI 点重跑即可）
- 自动 retention 清理（后端有 days_before 但不主动清 S3/Local 旧对象）
- batch_id / 一次提交多表的批组概念
- 跨 instance 立即备份发起
- partition 级实时进度（按 run 粒度更新足够）
- 日历视图的"备份依赖图 / 时间线可视化"
- 导出/报表

---

## 附录 A：相关链接

- 现有代码：`controller/data_manage.go`, `service/clickhouse/data_manage.go`, `service/cron/cron_service.go`, `model/data_manage.go`
- Mockup：`.superpowers/brainstorm/3685702-1778400890/content/`（backup-overview-v3.html / backup-create-v4.html / backup-edit-modal-v2.html）

## 附录 B：决策一览（brainstorming 过程中确认的所有点）

| # | 决策点 | 结果 |
|---|---|---|
| 1 | 产出形态 | 评估 + 改进方案文档 |
| 2 | 部署 | 多实例 + 实例绑定（保持现状） |
| 3 | 痛点优先级 | 调度规模 / 台账 |
| 4 | 台账维度 | 表 + 任务（不要分区维度） |
| 5 | 重叠触发 | 跳过本次，记 skipped run |
| 6 | 并发上限 | 全局 worker pool（max_concurrent 可配） |
| 7 | 重启 | 标 interrupted，不重试 |
| 8 | HTTP | 全异步 + 轮询 |
| 9 | 跨实例 | 立即备份强制 self；定时允许选 instance |
| 10 | 数据模型 | BackupPolicy + BackupRun，partition 内嵌 JSON |
| 11 | Policy 可编辑 | crontab/instance/enabled/style/type/target/credentials 可改；cluster/db/table/schedule_type 不可改 |
| 12 | Reconcile | 60s 周期，不立即推送 |
| 13 | Failover | 旧实例 in-flight run 跑完不打断；queued 标 interrupted；新实例下次 cron 接管 |
| 14 | 限制 | crontab ≥ 1 小时；单请求 ≤ 100 表；单 run ≤ 200 分区 |
| 15 | 表选择 | 可搜索下拉 + 分区方式 tag + 数据量 |
| 16 | Daily 兼容性 | 提交时校验，整批 reject；UI 智能置灰 |
| 17 | 配色 | #C9A100 ckman 主色 + Element 状态色 |
| 18 | 日历单位 | 中文 X分 / X时；hover 显示触发时间 · 状态 · 耗时 · 数据量 · 分区数 |
