# 备份分区记录管理与空分区 run 修复 — 设计

日期:2026-06-05
状态:已与用户确认

## 背景

increment + daily 备份的去重基于 365 天内 run 历史中"最新终态 = success"的分区记录
(`successfulPartitionsFromRuns`,service/backup/clickhouse_adapter.go)。由此产生三个问题:

1. **空分区 run 误导**:窗口内无分区(表没新数据 / 全部已去重 / 窗口反转)时,run 照常
   走完全部阶段并标 `success` + 0 分区。生产库实锤:`aimeter22.event_metric` 自 5/15 起
   每天产生一条 success 空 run,无法与"真备份成功"区分。
2. **去重记录无法失效**:S3 上的备份数据被手动删除后,run 历史里的 success 记录仍然生效,
   该分区永远不会被重新备份。缺少删除分区记录的入口。
3. **页面现有删除按钮语义错误**:分区列表展开行的「删除」按钮删的是**整条 run**
   (一条 run 可能含几十个分区),且删除后更老 run 里同分区的 success 会复活,
   与用户"删这条分区记录"的预期完全不符。

附带发现(本次一并修):

4. **toDate 误判 daily 兼容**:`daily_compat.go` 的正则把 `toDate` 判为 daily 兼容,
   但 `PARTITION BY toDate(x)` 的分区值是 `2026-06-04`(带横线),与扫描 SQL 的
   `partition >= '20260604'` 字符串比较永不匹配 → 这类表 daily 备份永远扫不到分区。
5. **立即备份反转窗口不拦截**:立即备份 + 滚动窗口且 start_date > 今天−days_before 时,
   本次必然空跑,前端只有红字提示不阻断,后端也放行。

## 区间语义(确认结论,不改)

滚动窗口为左闭右闭 `[start_date, run.CreateTime − days_before]`;
固定区间为左闭右闭 `[range_start_date, range_end_date]`。
扫描 SQL:`partition >= from AND partition <= to`(字符串比较,分区值须为 YYYYMMDD)。

## 改动一:空分区 run 标 skipped + no_partitions(后端)

- `model` 新增 `REASON_NO_PARTITIONS = "no_partitions"`。
- `Executor.Run`:Init 成功后,若 `Operation == backup` 且 `len(run.Partitions) == 0`,
  将 run 标 `skipped`、`StatusReason = no_partitions`、写 FinishedAt/Elapsed,
  **不进入** Prepare/Backup/Check/Close 任何阶段,返回 nil(非错误)。
- 适用场景:daily 窗口空、窗口反转、partition 模式全部被去重。
  full 模式 0 分区已在 Init 报错,行为不变。
- restore run 不受影响(提交时已强制 partitions 非空)。

## 改动二:删除分区备份记录(后端新 API + 前端)

### 语义(用户确认)

- 删除粒度是**分区条目**,按分区名作用于该表 365 天内**所有终态 run**
  (必须全删,否则老 run 里的 success 复活继续去重)。
- run 本身保留;**若 run 的分区被删光,连空壳 run 一起删除**
  (避免留下 success + 0 分区的尸体记录)。
- 物理删除,无痕迹,不可恢复(用户接受)。
- 弹窗确认,带「同时清理 S3/存储上的备份数据」勾选项,**默认勾选**。
- 远端清理为 best-effort:失败收集为 warning 返回,记录照删
  (下次重备时 Prepare 阶段会再清一遍目标端残留,有兜底)。

### 后端

- API:`POST /api/v1/data_manage/backup/table/:clusterName/:database/:table/partitions/delete`
  body:`{"partitions": ["20260604", ...], "clean_remote": true}`
- Service 新增 `DeletePartitionRecords(cluster, db, table string, partitions []string, cleanRemote bool)`:
  1. 校验 partitions 非空,逐个 `ValidateIdentifier`。
  2. **守卫**:该表存在未结束 run(queued/running)时拒绝——Executor 持有 run 副本,
     并发 UpdateRun 会把删掉的条目写回(竞态)。
  3. 扫 `GetRunsByTable(cluster, db, table, 365)`,对每条终态 run:
     移除匹配分区条目 → 分区列表非空则 `UpdateRun`,删空则 `DeleteRun`。
  4. `cleanRemote` 时:按各 run 的 policy 组装 storage,对被删分区逐 shard 清理
     `JoinRunKey(storagePrefix, partition, db, table, host)`;错误记 warning 不中断。
  5. 返回:`{removed_records: N, deleted_runs: M, warnings: [...]}`。
- 去重逻辑 `successfulPartitionsFromRuns` 零改动。

### 前端(ckman-fe,在 ../ckman-fe 工作区开发)

入口:分区列表对话框 `partition-list-dialog.vue`(Tables 页签 → 点表进入):

- 底部新增「删除所选分区备份记录」按钮(复用现有多选框)。
- 点击弹确认框:列出待删分区名、红字警告
  "删除后这些分区将在下次备份时重新备份,历史记录不可恢复",
  勾选项「同时清理存储上的备份数据」默认勾选。
- **展开行的旧「删除」按钮改造**:由"删整条 run"改为"删除该分区的备份记录"
  (调用同一个新 API,单分区),确认文案同步修正。
  改造后 UI 不再有整条 run 的删除入口(现状中该按钮是唯一调用方);
  后端 `DELETE /backup/run/:run_id` API 保留不动。
- 成功后刷新 `fetchRuns()`。

## 改动三:立即备份反转窗口拦截

- **后端** `validateDailyRange`:立即备份(immediate)+ 滚动窗口时,
  若 `start_date > 今天 − days_before` 直接拒绝(本次必然空跑)。
  定时备份(scheduled)不拦——"从未来某天开始备份"是合法语义,前几天空窗是暂时的,
  且改动一会把这些空 run 标成 skipped,可观测。
- **前端** `backup-form-dialog.vue`:立即备份 + 滚动窗口反转时,把现有红字提示
  升级为表单阻断校验(rules);定时备份保持现有非阻断提示不变。

## 改动四:toDate 误判修复

- `daily_compat.go`:从 `dailyCompatibleRe` 移除 `toDate`(其分区值非 YYYYMMDD,
  与扫描 SQL 永不匹配)。受影响的表此前 daily 备份本来就扫不到分区,无行为退化。

## 测试计划(后端 TDD)

- 改动一:daily 空窗口 run → skipped/no_partitions/FinishedAt 置位,任何 stage 不执行;
  partition 模式全去重 → 同上;full 模式 0 分区 → 仍走 Init 报错(回归)。
- 改动二:分区散落多条 run → 全部删净,`GetLastRunPartitions` 不再返回该分区;
  有 in-flight run → 拒绝;run 删空 → 整条删除;clean 失败 → 记录仍删、warning 返回;
  partitions 含非法字符 → 拒绝。
- 改动三:immediate + 反转窗口 → 提交报错;scheduled + 未来 start_date → 放行。
- 改动四:`IsDailyCompatible("toDate(ts)")` → false;`toYYYYMMDD`/`formatDateTime %Y%m%d` → true。
- 前端改动以现有 lint + `make build`(ckman-fe)验证。
