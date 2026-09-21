# 备份 / 恢复任务终止（Run Cancel）

- 日期：2026-09-17
- 仓库：`housepower/ckman`（后端）+ `housepower/ckman-fe`（前端）
- 状态：方案已讨论，**未实现**

## 背景

一个大备份（多分区、单分区上百 GB）跑到一半时，目前没有办法终止：

- 备份路由只有查询 / 编辑 / 删除 / 触发，没有 cancel。集群任务的 `TaskController.StopTask` + `runner.Cancel` 没有接到备份上。
- worker 调执行器用的是 `context.Background()`（`service/backup/pool.go` `runOne`），分区循环（`executor.go` `realStages.Backup` / `Restore`）不检查 ctx，`execSQL` 的 `conn.Exec` 也不带 ctx。
- 在 CK 上手工 `KILL QUERY` 只会让**当前分区**失败，循环照常进入下一个分区。
- 重启 ckman 不行：`Pool.Stop` 会等进行中的 run 跑完；`kill -9` 后 run 被 Boot 标成 interrupted，但 CK 上的 BACKUP 不一定跟着停。

## 目标

1. 页面上对 queued / running 的 backup、restore run 提供「终止」。
2. 终止后不再开始新的分区。
3. CK 支持取消时（≥ 24.1），正在执行的分区立即停止。
4. 终止请求落到任意 ckman 实例都能生效，不需要实例间转发。

## 非目标

- 不支持 < 24.1 的 CK 上立即停止正在执行的分区（CK 本身做不到，见「版本差异」）。
- 不做断点续传：被终止的分区下次备份从头开始。

## 方案：两步终止

终止接口按顺序做两件事：**先改状态，再 kill**。

顺序不能反：先 kill 的话，当前分区报错返回后，循环可能在状态改掉之前就开始下一个分区。

### 第 1 步：条件更新 run 状态为 cancelled

- 新增状态 `model.BACKUP_STATUS_CANCELLED = "cancelled"`，分区状态新增 `BACKUP_PARTITION_STATUS_CANCELLED = "cancelled"`。
- 仓储层新增条件更新：只有 `status IN (queued, running)` 时才改为 cancelled，返回是否命中。写法照 `claimRun` 用的 markRunning（queued→running 的 CAS）。
  - 命中且原状态为 queued：worker 取到时 claim 失败，不会执行，**不需要 kill**。
  - 未命中：run 已结束，接口返回「任务已结束」。
- 涉及持久层接口，按 CLAUDE.md 要求跑 `repository/dbtest/` 真库回归。

### 第 2 步：按 query_id 前缀 kill

- 每条 BACKUP / RESTORE 用 `common.Conn.ExecWithQueryID` 执行，query_id 为：
  `ckman_backup_<run_id>_<partition>_<host>`（restore 用 `ckman_restore_` 前缀）。
  分区名需转义或哈希，保证是合法 query_id。
- 终止时对该集群每个 shard 的连接执行：
  `KILL QUERY WHERE query_id LIKE 'ckman_backup_<run_id>_%' SYNC`
  （或用 `clusterAllReplicas(..., system.processes)` 先查出 host 再逐个 kill）。
- 终止接口所在实例自己连集群执行，不依赖执行者实例。

### 执行器改动（否则第 1 步会被覆盖）

现有代码有两处会把 cancelled 写回去，必须改：

1. **分区循环整条覆盖**：`Backup` / `Restore` 循环前只 `GetRun` 一次，之后每个分区拿内存里的 `run` 整条 `UpdateRun`，status 仍是 running。
   - 每个分区开始前重新读 run 状态，是 cancelled 就把剩余 waiting 分区标 cancelled 并跳出。
   - 写回时不能覆盖 status：只更新分区字段，或写前确认 status 未变。
2. **收尾覆盖**：被 kill 的分区报错后，`Run` 会走 `markFailed`（重新读记录后置 failed，并把未完成分区标 failed）；`markSuccess` 同理。
   - 收尾时若已是 cancelled：status 保持 cancelled，未完成（waiting / running）分区标 cancelled，写 `FinishedAt` / `Elapsed`。
   - 被 kill 的那个分区标 cancelled 而不是 failed。

prepare / check 阶段同样在开始前检查一次状态。

### 被终止分区的残留数据

- 被 kill 的分区在 S3 / 本地会留下部分文件。下次备份该分区时 prepare 阶段会先清理 `<prefix>/<partition>/<db>.<table>/<host>`，不需要额外处理。
- 需确认：增量去重（`getLastRunPartitions`）只把 success 分区视为已备份，cancelled 分区下次会被重新纳入。

## 版本差异（关键）

| CK 版本 | 点「终止」之后 |
|---|---|
| ≥ 24.1 | 改状态 + KILL，当前分区立即停止 |
| < 24.1 | 改状态 + KILL（不生效），**当前分区在 CK 后台跑完**，之后不再开始新分区 |

依据：

- **24.1 起支持取消 BACKUP / RESTORE**：`system.backups` 新增 `BACKUP_CANCELLED` / `RESTORE_CANCELLED`（提交 085833aaae2，首个版本 v24.1.1.2048）。dev 集群（26.3.13.31）实测 `KILL QUERY ... SYNC` 立即返回 finished，备份随即结束。
- **23.8 不支持**（源码判断，未实测）：
  - `src/Backups/` 下没有任何检查查询取消标记的代码。
  - 同步 BACKUP 时 `doBackup` 直接在 TCPHandler 线程里执行（`BackupsWorker::startMakingBackup`），期间不读客户端 socket。
- **断开连接也没用**：服务端对断开的处理同样是置取消标记，和 KILL 等价；且 23.8 在备份结束前都发现不了断开。结果是 ckman 以为结束了，CK 仍在上传、占带宽，比 KILL 更具误导性，**不采用**。

### 页面提示

- 集群版本 ≥ 24.1：「终止后正在执行的分区会立即停止」。
- 集群版本 < 24.1：「当前分区会继续执行到完成（仍会占用带宽），之后不再执行剩余分区」。
- 终止按钮二次确认；run 列表 / 详情展示 cancelled 状态（与 failed 区分颜色）。

## 接口

`POST /api/v1/data_manage/backup/run/:run_id/cancel`

- 成功：返回原状态（queued / running）与是否执行了 kill、kill 结果（每个 host 的 kill_status）。
- run 已结束：返回错误「任务已结束」。
- 需要 swagger 注释并 `swag init`。

## 待验证

1. abc（23.8.9.54）上实测：BACKUP 执行中 `KILL QUERY ... SYNC` 的返回（预计 `waiting` 直到超时）与备份是否跑完；断开连接的表现。
2. 24.x / 25.x 上 RESTORE 的 KILL 是否同样立即生效。
3. KILL 后 CK 的报错码（预计 `QUERY_WAS_CANCELLED` 394），执行器是否要据此区分「被取消」与「真失败」。本方案以 run 状态为准，不依赖错误码。
4. 多实例：终止接口所在实例能连上目标集群（与执行者实例网络条件可能不同）。
