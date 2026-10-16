package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/backup/storage"
	"golang.org/x/sync/errgroup"
)

// errRunCancelled 是用户主动停止（StopRun → Executor.Cancel）时各阶段返回的
// sentinel；Run 据此把台账收口为 interrupted 而不是 failed——连接是主动关的，
// 随之而来的报错是预期行为，不算备份失败。
var errRunCancelled = errors.New("run cancelled by user")

// queryResult is a minimal interface satisfied by *common.Rows and test fakes.
type queryResult interface{ Close() error }

// ExecRepo 暴露 Executor 需要的持久层操作。
type ExecRepo interface {
	GetRun(id string) (model.BackupRun, error)
	UpdateRun(r model.BackupRun) error
	GetPolicyForRun(policyID string) (model.BackupPolicy, error)
}

// shardConn 是 Executor 内部用的 shard 句柄。
// 真实实现持有 *common.Conn；测试用 fake 仅持 host（conn 为 nil）。
type shardConn struct {
	host    string
	conn    *common.Conn // 真实 adapter 注入；测试 fake 留 nil
	cluster string       // CH 集群名(remote_servers 里的),供分区发现走 clusterAllReplicas 用
}

// stages 让 Executor 把 5 个阶段分别可单测。
type stages interface {
	Prepare(ctx context.Context, e *Executor, runID string) error
	Backup(ctx context.Context, e *Executor, runID string) error
	Restore(ctx context.Context, e *Executor, runID string) error
	Check(ctx context.Context, e *Executor, runID string) error
	Close(ctx context.Context, e *Executor, runID string) error
}

type stagesStub struct{}

func (stagesStub) Prepare(context.Context, *Executor, string) error { return nil }
func (stagesStub) Backup(context.Context, *Executor, string) error  { return nil }
func (stagesStub) Restore(context.Context, *Executor, string) error { return nil }
func (stagesStub) Check(context.Context, *Executor, string) error   { return nil }
func (stagesStub) Close(context.Context, *Executor, string) error   { return nil }

// Executor 状态机：init → prepare → backup/restore → check → close
type Executor struct {
	repo                 ExecRepo
	connFactory          func(cluster string, maxBandwidthMB, maxReadSeconds int) ([]*shardConn, error)
	listPartitions       func(c *shardConn, db, table, fromYYYYMMDD, toYYYYMMDD string) ([]string, error)
	listAllPartitions    func(c *shardConn, db, table string) ([]string, error)
	getLastRunPartitions func(cluster, db, table string) ([]model.BackupRunPartition, error)
	stages               stages
	now                  func() time.Time

	// run-scoped（Init 阶段填，后续阶段读）
	conns   []*shardConn
	storage BackupStorage

	// connMu 保护 conns 的写（Init 赋值 / closeConns 清空）与 Cancel 的读：
	// StopRun 的 goroutine 可能赶在 Run 收尾的 closeConns 之前调 Cancel，
	// 两者会并发触碰 conns。阶段内的 execSQL 读发生在 closeConns 之前，无此问题。
	connMu sync.Mutex

	// cancelled 由 Cancel() 置位：用户主动停止。各阶段在分区间隙检查它提前
	// 退出（返回 errRunCancelled），Run 据此记 interrupted 而非 failed。
	cancelled atomic.Bool

	// run-scoped helpers（Init 期间不一定都注入；Task 21 真实 adapter 全部填）
	queryRows             func(host string) (queryResult, error)
	collectChecksumOnHost func(c *shardConn, run *model.BackupRun) error
	execSQL               func(host, sql string) error                                                   // 新增；Task 21 真实 adapter 注入
	queryPartitionStats   func(host, db, table, partition string) (rows, bytes, parts uint64, err error) // backup 前统计 partition 数据量
}

// BackupStorage 是 backup 包内对 storage.Storage 的本地镜像接口，
// 避免与 service/backup/storage 形成循环依赖。
// Task 21 真实 adapter 注入时会传入满足此接口的具体实现。
type BackupStorage interface {
	Init() error
	BackupSQL(database, table, partition, key string) string
	RestoreSQL(database, table, partition, key string) string
	CleanPartition(host, keyPrefix string) error
	CheckPartition(host, keyPrefix string, pathInfo map[string]model.PathInfo) error
	Type() string
}

// Run 是 worker 调用入口。
func (e *Executor) Run(ctx context.Context, runID string) error {
	if err := e.Init(ctx, runID); err != nil {
		e.closeConns()
		return e.failStage(runID, "init: "+err.Error())
	}
	defer e.closeConns()
	r, err := e.repo.GetRun(runID)
	if err != nil {
		return e.failStage(runID, "post-init get run: "+err.Error())
	}
	// 空分区:窗口内无分区 / 全部已去重 → skipped(no_partitions),不进任何阶段,
	// 避免产生「success + 0 分区」的误导记录(无法与真备份成功区分)。
	if r.Operation == model.OP_BACKUP && len(r.Partitions) == 0 {
		return e.markSkippedNoPartitions(runID)
	}
	st := e.resolveStages()
	if r.Operation == model.OP_BACKUP {
		if err := st.Prepare(ctx, e, runID); err != nil {
			return e.failStage(runID, "prepare: "+err.Error())
		}
		if err := st.Backup(ctx, e, runID); err != nil {
			return e.failStage(runID, "backup: "+err.Error())
		}
		policy, _ := e.repo.GetPolicyForRun(r.PolicyID)
		if policy.Checksum {
			if err := st.Check(ctx, e, runID); err != nil {
				return e.failStage(runID, "check: "+err.Error())
			}
		}
	} else if r.Operation == model.OP_RESTORE {
		if err := st.Restore(ctx, e, runID); err != nil {
			return e.failStage(runID, "restore: "+err.Error())
		}
	} else {
		return e.markFailed(runID, "unknown operation: "+r.Operation)
	}
	if err := st.Close(ctx, e, runID); err != nil {
		return e.failStage(runID, "close: "+err.Error())
	}
	return e.markSuccess(runID)
}

// Cancel 供 StopRun 调用：标记取消并关闭所有连接，促使在途 BACKUP/RESTORE
// 语句立即报错退出。与 closeConns 不同，这里不清空 e.conns——Backup/Restore
// 的执行 goroutine 可能正在读它；真正的清理由 Run 收尾时的 closeConns 做。
func (e *Executor) Cancel() {
	e.cancelled.Store(true)
	e.connMu.Lock()
	conns := e.conns
	e.connMu.Unlock()
	for _, c := range conns {
		if c != nil && c.conn != nil {
			_ = c.conn.Close()
		}
	}
}

// failStage 把阶段错误收口为台账终态：用户取消（连接是主动关的，报错属预期）
// 记 interrupted，其余记 failed。
func (e *Executor) failStage(runID, reason string) error {
	if e.cancelled.Load() {
		return e.markInterrupted(runID)
	}
	return e.markFailed(runID, reason)
}

// closeConns 关闭并清空 e.conns。backup 连接 BypassPool=true，用完显式 close，
// 防止任何 caller（包括 Executor 自己下次 Run）拿到一个已 close 的 conn。
func (e *Executor) closeConns() {
	e.connMu.Lock()
	defer e.connMu.Unlock()
	for _, c := range e.conns {
		if c != nil && c.conn != nil {
			_ = c.conn.Close()
		}
	}
	e.conns = nil
}

func (e *Executor) resolveStages() stages {
	if e.stages == nil {
		return stagesStub{}
	}
	return e.stages
}

func (e *Executor) clock() time.Time {
	if e.now != nil {
		return e.now()
	}
	return time.Now()
}

// Init 阶段：连 cluster + 生成本次 run 的 partition 列表。
func (e *Executor) Init(ctx context.Context, runID string) error {
	r, err := e.repo.GetRun(runID)
	if err != nil {
		return err
	}
	policy, err := e.repo.GetPolicyForRun(r.PolicyID)
	if err != nil {
		return err
	}
	conns, err := e.connFactory(policy.ClusterName, runBandwidthMB(r, policy), runMaxReadSeconds(policy))
	if err != nil {
		return fmt.Errorf("connect cluster %s: %w", policy.ClusterName, err)
	}
	if len(conns) == 0 {
		return fmt.Errorf("no shard reachable for cluster %s", policy.ClusterName)
	}
	e.connMu.Lock()
	e.conns = conns
	e.connMu.Unlock()

	// 装配 storage（如果尚未注入，且 policy 有目标类型则按 policy 创建）
	if e.storage == nil && policy.TargetType != "" {
		cluster, cerr := repository.Ps.GetClusterbyName(policy.ClusterName)
		if cerr != nil {
			return fmt.Errorf("get cluster %s: %w", policy.ClusterName, cerr)
		}
		e.storage = NewStorageForPolicy(policy, cluster)
		if e.storage == nil {
			return fmt.Errorf("unsupported target type: %s", policy.TargetType)
		}
		if err := e.storage.Init(); err != nil {
			return fmt.Errorf("storage init: %w", err)
		}
	}

	if err := e.resolveRunPartitions(&r, policy); err != nil {
		return err
	}

	r.StartedAt = e.clock()
	if err := e.repo.UpdateRun(r); err != nil {
		return err
	}
	return nil
}

// runBandwidthMB 返回本次 run 的 S3 上传限速（MB/s，集群总量）。
// 只有 backup 写 S3 才限；restore 和本地备份返回 0。
func runBandwidthMB(r model.BackupRun, policy model.BackupPolicy) int {
	if r.Operation != model.OP_BACKUP || policy.TargetType != model.BACKUP_S3 || policy.MaxBandwidthMB <= 0 {
		return 0
	}
	return policy.MaxBandwidthMB
}

// DefaultBackupMaxReadSeconds 是单分区备份语句读超时的默认值（6 小时）。
// clickhouse-go 在 exec 开始时一次性设置绝对读截止，单分区备份超过该时长会被
// i/o timeout 打断；大分区场景可通过 policy.MaxReadSeconds 调大。
const DefaultBackupMaxReadSeconds = 21600

// runMaxReadSeconds 返回本次 run 的单分区读超时（秒）；policy 未配置时用默认值。
func runMaxReadSeconds(policy model.BackupPolicy) int {
	if policy.MaxReadSeconds > 0 {
		return policy.MaxReadSeconds
	}
	return DefaultBackupMaxReadSeconds
}

// resolveRunPartitions 根据 policy 的 backup_style / backup_type 决定 run.Partitions 来源。
// 仅 backup 走这里；restore 的分区在 SubmitRestoreRequest 已经写好。
func (e *Executor) resolveRunPartitions(r *model.BackupRun, policy model.BackupPolicy) error {
	if r.Operation != model.OP_BACKUP {
		return nil
	}
	switch {
	case policy.BackupStyle == model.BACKUP_STYLE_FULL:
		// 全量：枚举表全部 active 分区，每次都重备
		if e.listAllPartitions == nil {
			return errors.New("full backup requires listAllPartitions hook")
		}
		all, err := e.listAllPartitions(e.conns[0], policy.Database, policy.Table)
		if err != nil {
			return fmt.Errorf("list all partitions: %w", err)
		}
		if len(all) == 0 {
			return fmt.Errorf("table %s.%s has no active partitions to backup", policy.Database, policy.Table)
		}
		r.Partitions = partitionsFromNames(all)
	case policy.BackupType == model.BACKUP_TYPE_PARTITION:
		// 增量 + 按分区名：用 policy.Partitions（用户提交时指定）
		if len(policy.Partitions) == 0 {
			return errors.New("partition mode requires explicit partition list")
		}
		// 跳过已经成功备份过的分区（避免重复备份），本次 run 只记录需要执行的分区。
		var prev []model.BackupRunPartition
		if e.getLastRunPartitions != nil {
			prev, _ = e.getLastRunPartitions(policy.ClusterName, policy.Database, policy.Table)
		}
		r.Partitions = excludeSuccessfulPartitions(prev, policy.Partitions)
	case policy.BackupType == model.BACKUP_TYPE_DAILY_PARTITION && (policy.DaysBefore > 0 || hasFixedDailyRange(policy)):
		// 增量 + 按时间段：
		// 1. 固定区间：[range_start_date, range_end_date]
		// 2. 滚动区间：[start_date, today-days_before]，start_date 为空时不限制下界
		fromPartition, toPartition := e.resolveDailyPartitionRange(policy, r.CreateTime)
		newPartitions, err := e.listPartitions(e.conns[0], policy.Database, policy.Table, fromPartition, toPartition)
		if err != nil {
			return err
		}
		var prev []model.BackupRunPartition
		if e.getLastRunPartitions != nil {
			prev, _ = e.getLastRunPartitions(policy.ClusterName, policy.Database, policy.Table)
		}
		r.Partitions = excludeSuccessfulPartitions(prev, newPartitions)
	default:
		return fmt.Errorf("unsupported backup mode: style=%s type=%s startDate=%s daysBefore=%d",
			policy.BackupStyle, policy.BackupType, policy.StartDate, policy.DaysBefore)
	}
	return nil
}

func hasFixedDailyRange(policy model.BackupPolicy) bool {
	return strings.TrimSpace(policy.RangeStartDate) != "" && strings.TrimSpace(policy.RangeEndDate) != ""
}

// resolveDailyPartitionRange 计算按时间段备份的分区范围。滚动窗口的上界
// `anchor - days_before` 锚定在 run 触发时刻（CreateTime）而非执行时刻：
// 大批量任务排队可能跨午夜，若按执行时刻计算，窗口会整体后移一天，与
// 用户在触发当天的预期范围不符。anchor 零值（老数据）回退到当前时钟。
func (e *Executor) resolveDailyPartitionRange(policy model.BackupPolicy, anchor time.Time) (string, string) {
	rangeStart := strings.TrimSpace(policy.RangeStartDate)
	rangeEnd := strings.TrimSpace(policy.RangeEndDate)
	if rangeStart != "" || rangeEnd != "" {
		return rangeStart, rangeEnd
	}
	if anchor.IsZero() {
		anchor = e.clock()
	}
	return strings.TrimSpace(policy.StartDate), anchor.AddDate(0, 0, -policy.DaysBefore).Format("20060102")
}

// partitionsFromNames 把分区名列表转换为 waiting 状态的 BackupRunPartition 切片。
func partitionsFromNames(names []string) []model.BackupRunPartition {
	out := make([]model.BackupRunPartition, 0, len(names))
	for _, n := range names {
		out = append(out, model.BackupRunPartition{
			Partition: n, Status: model.BACKUP_PARTITION_STATUS_WAITING,
		})
	}
	return out
}

// excludeSuccessfulPartitions 用历史 success 分区做排重，只返回本次需要执行的
// newest 分区，状态标 waiting。历史分区不进入当前 run，避免 check/clean/UI 把它们
// 当成本次执行结果处理。
func excludeSuccessfulPartitions(prev []model.BackupRunPartition, newest []string) []model.BackupRunPartition {
	done := map[string]bool{}
	for _, p := range prev {
		if p.Status == model.BACKUP_PARTITION_STATUS_SUCCESS {
			done[p.Partition] = true
		}
	}
	out := make([]model.BackupRunPartition, 0, len(newest))
	for _, n := range newest {
		if !done[n] {
			out = append(out, model.BackupRunPartition{
				Partition: n, Status: model.BACKUP_PARTITION_STATUS_WAITING,
			})
		}
	}
	return out
}

func (e *Executor) markFailed(runID, reason string) error {
	r, err := e.repo.GetRun(runID)
	if err != nil {
		log.Logger.Errorf("[exec] cannot mark failed, get run %s: %v", runID, err)
		return err
	}
	r.Status = model.BACKUP_STATUS_FAILED
	r.ErrorMsg = reason
	r.FinishedAt = e.clock()
	r.Elapsed = int(r.FinishedAt.Sub(r.StartedAt).Seconds())
	// 把还未走完的 partition (waiting/running) 一并标 failed，避免 partition.status
	// 与 run.status 不一致让 UI 出现「run 失败但分区显示等待」。已 success/skipped
	// 的不动——它们的阶段成果是真实的（如 check 阶段失败时，backup 阶段已上传的
	// 分区数据本身仍然成功）。
	for i := range r.Partitions {
		s := r.Partitions[i].Status
		if s == model.BACKUP_PARTITION_STATUS_WAITING || s == model.BACKUP_PARTITION_STATUS_RUNNING {
			r.Partitions[i].Status = model.BACKUP_PARTITION_STATUS_FAILED
			if r.Partitions[i].Msg == "" {
				r.Partitions[i].Msg = "run aborted: " + reason
			}
		}
	}
	if upErr := e.repo.UpdateRun(r); upErr != nil {
		log.Logger.Errorf("[exec] update failed run %s: %v", runID, upErr)
	}
	return fmt.Errorf("run %s failed: %s", runID, reason)
}

// markInterrupted 把 run 收口为用户取消的终态（StopRun → Cancel）。
// 未走完的 waiting/running 分区一并标 interrupted；已 success 的不动——
// 已上传/已恢复的成果是真实的，重跑时增量去重仍应跳过它们。
func (e *Executor) markInterrupted(runID string) error {
	r, err := e.repo.GetRun(runID)
	if err != nil {
		log.Logger.Errorf("[exec] cannot mark interrupted, get run %s: %v", runID, err)
		return err
	}
	r.Status = model.BACKUP_STATUS_INTERRUPTED
	r.StatusReason = model.REASON_CANCELLED
	r.ErrorMsg = "cancelled by user"
	r.FinishedAt = e.clock()
	if !r.StartedAt.IsZero() {
		r.Elapsed = int(r.FinishedAt.Sub(r.StartedAt).Seconds())
	}
	for i := range r.Partitions {
		s := r.Partitions[i].Status
		if s == model.BACKUP_PARTITION_STATUS_WAITING || s == model.BACKUP_PARTITION_STATUS_RUNNING {
			r.Partitions[i].Status = model.BACKUP_PARTITION_STATUS_INTERRUPTED
			if r.Partitions[i].Msg == "" {
				r.Partitions[i].Msg = "cancelled by user"
			}
		}
	}
	if upErr := e.repo.UpdateRun(r); upErr != nil {
		log.Logger.Errorf("[exec] update interrupted run %s: %v", runID, upErr)
	}
	return errRunCancelled
}

func (e *Executor) markSuccess(runID string) error {
	r, err := e.repo.GetRun(runID)
	if err != nil {
		return err
	}
	r.Status = model.BACKUP_STATUS_SUCCESS
	r.FinishedAt = e.clock()
	r.Elapsed = int(r.FinishedAt.Sub(r.StartedAt).Seconds())
	return e.repo.UpdateRun(r)
}

// markSkippedNoPartitions 把无分区可备份的 run 收口为 skipped 终态。
// 不是错误:表没新数据/窗口暂时为空是正常情形,返回 nil 让 worker 正常收尾。
func (e *Executor) markSkippedNoPartitions(runID string) error {
	r, err := e.repo.GetRun(runID)
	if err != nil {
		return err
	}
	r.Status = model.BACKUP_STATUS_SKIPPED
	r.StatusReason = model.REASON_NO_PARTITIONS
	r.FinishedAt = e.clock()
	r.Elapsed = int(r.FinishedAt.Sub(r.StartedAt).Seconds())
	return e.repo.UpdateRun(r)
}

// ── realStages ────────────────────────────────────────────────────────────────

// realStages implements the stages interface with real logic.
// Remaining methods (Backup/Restore/Check/Close) will be filled in subsequent tasks.
type realStages struct{}

func (realStages) Backup(ctx context.Context, e *Executor, runID string) error {
	run, err := e.repo.GetRun(runID)
	if err != nil {
		return err
	}
	var anyFail bool

	for i := range run.Partitions {
		p := &run.Partitions[i]
		if p.Status != model.BACKUP_PARTITION_STATUS_WAITING {
			continue
		}
		// 用户取消：剩余分区不再执行，交给 Run → markInterrupted 统一收口
		if e.cancelled.Load() {
			return errRunCancelled
		}
		p.Status = model.BACKUP_PARTITION_STATUS_RUNNING
		if err := e.repo.UpdateRun(run); err != nil {
			return fmt.Errorf("mark partition running: %w", err)
		}
		start := time.Now()

		// BACKUP 前累加每个 shard 上该分区的 rows / bytes / parts
		if e.queryPartitionStats != nil {
			var totalRows, totalBytes, totalParts uint64
			statsByHost := make([]string, 0, len(e.conns))
			for _, c := range e.conns {
				rows, bytes, parts, qerr := e.queryPartitionStats(c.host, run.Database, run.Table, p.Partition)
				if qerr != nil {
					log.Logger.Warnf("[exec] queryPartitionStats host=%s partition=%s: %v", c.host, p.Partition, qerr)
					statsByHost = append(statsByHost, fmt.Sprintf("%s:error=%v", c.host, qerr))
					continue
				}
				statsByHost = append(statsByHost, fmt.Sprintf("%s:rows=%d bytes=%d parts=%d", c.host, rows, bytes, parts))
				totalRows += rows
				totalBytes += bytes
				totalParts += parts
			}
			p.Rows = totalRows
			p.Size = totalBytes
			p.FileNum = totalParts
			if totalRows == 0 && totalBytes == 0 && totalParts == 0 {
				log.Logger.Warnf("[exec] partition stats empty run=%s table=%s.%s partition=%s stats=[%s]",
					run.RunID, run.Database, run.Table, p.Partition, strings.Join(statsByHost, "; "))
			}
		}

		// 跨 shard 并发执行 BACKUP TABLE，但状态汇总在主线程串行写，规避 race。
		type result struct {
			host string
			err  error
		}
		ch := make(chan result, len(e.conns))
		for _, c := range e.conns {
			c := c
			go func() {
				key := storage.JoinRunKey(run.StoragePrefix, p.Partition, run.Database, run.Table, c.host)
				sql := fmt.Sprintf("BACKUP TABLE `%s`.`%s`%s",
					run.Database, run.Table,
					e.storage.BackupSQL(run.Database, run.Table, p.Partition, key))
				ch <- result{c.host, e.execSQL(c.host, sql)}
			}()
		}
		var partErrs []string
		for range e.conns {
			r := <-ch
			if r.err != nil {
				partErrs = append(partErrs, fmt.Sprintf("[%s] %v", r.host, r.err))
			}
		}
		p.Elapsed = int(time.Since(start).Seconds())
		if len(partErrs) > 0 {
			if e.cancelled.Load() {
				// 连接是 Cancel 主动关的，报错是预期行为：该分区记 interrupted 而非 failed
				p.Status = model.BACKUP_PARTITION_STATUS_INTERRUPTED
				if p.Msg == "" {
					p.Msg = "cancelled by user"
				}
				if uerr := e.repo.UpdateRun(run); uerr != nil {
					log.Logger.Errorf("[exec] update cancelled partition run=%s: %v", runID, uerr)
				}
				return errRunCancelled
			}
			p.Status = model.BACKUP_PARTITION_STATUS_FAILED
			p.Msg = strings.Join(partErrs, "; ")
			anyFail = true
		} else {
			p.Status = model.BACKUP_PARTITION_STATUS_SUCCESS
		}
		if err := e.repo.UpdateRun(run); err != nil {
			return fmt.Errorf("update partition: %w", err)
		}
	}
	if anyFail {
		return errors.New("one or more partitions failed")
	}
	return nil
}

func (realStages) Restore(ctx context.Context, e *Executor, runID string) error {
	run, err := e.repo.GetRun(runID)
	if err != nil {
		return err
	}
	var anyFail bool
	for i := range run.Partitions {
		p := &run.Partitions[i]
		if p.Status != model.BACKUP_PARTITION_STATUS_WAITING {
			continue
		}
		// 用户取消：剩余分区不再执行，交给 Run → markInterrupted 统一收口
		if e.cancelled.Load() {
			return errRunCancelled
		}
		p.Status = model.BACKUP_PARTITION_STATUS_RUNNING
		if err := e.repo.UpdateRun(run); err != nil {
			return fmt.Errorf("mark partition running: %w", err)
		}
		start := time.Now()

		type result struct {
			host string
			err  error
		}
		ch := make(chan result, len(e.conns))
		for _, c := range e.conns {
			c := c
			go func() {
				key := storage.JoinRunKey(run.StoragePrefix, p.Partition, run.Database, run.Table, c.host)
				sql := fmt.Sprintf("RESTORE TABLE `%s`.`%s`%s SETTINGS allow_non_empty_tables=true",
					run.Database, run.Table,
					e.storage.RestoreSQL(run.Database, run.Table, p.Partition, key))
				ch <- result{c.host, e.execSQL(c.host, sql)}
			}()
		}
		var partErrs []string
		for range e.conns {
			r := <-ch
			if r.err != nil {
				partErrs = append(partErrs, fmt.Sprintf("[%s] %v", r.host, r.err))
			}
		}
		p.Elapsed = int(time.Since(start).Seconds())
		if len(partErrs) > 0 {
			if e.cancelled.Load() {
				// 连接是 Cancel 主动关的，报错是预期行为：该分区记 interrupted 而非 failed
				p.Status = model.BACKUP_PARTITION_STATUS_INTERRUPTED
				if p.Msg == "" {
					p.Msg = "cancelled by user"
				}
				if uerr := e.repo.UpdateRun(run); uerr != nil {
					log.Logger.Errorf("[exec] update cancelled partition run=%s: %v", runID, uerr)
				}
				return errRunCancelled
			}
			p.Status = model.BACKUP_PARTITION_STATUS_FAILED
			p.Msg = strings.Join(partErrs, "; ")
			anyFail = true
		} else {
			p.Status = model.BACKUP_PARTITION_STATUS_SUCCESS
		}
		if err := e.repo.UpdateRun(run); err != nil {
			return fmt.Errorf("update partition: %w", err)
		}
	}
	if anyFail {
		return errors.New("one or more partitions failed")
	}
	return nil
}

func (realStages) Check(ctx context.Context, e *Executor, runID string) error {
	run, err := e.repo.GetRun(runID)
	if err != nil {
		return err
	}
	var firstErr error
	for i := range run.Partitions {
		if e.cancelled.Load() {
			return errRunCancelled
		}
		p := &run.Partitions[i]
		if p.Status != model.BACKUP_PARTITION_STATUS_SUCCESS {
			continue
		}
		g, _ := errgroup.WithContext(ctx)
		for _, c := range e.conns {
			c := c
			pp := *p
			g.Go(func() error {
				keyPrefix := storage.JoinRunKey(run.StoragePrefix, pp.Partition, run.Database, run.Table, c.host)
				return e.storage.CheckPartition(c.host, keyPrefix, pp.PathInfo)
			})
		}
		if err := g.Wait(); err != nil {
			if e.cancelled.Load() {
				// 用户取消：产物本身未必坏，不能标 failed（否则增量去重会重备），
				// 交给 Run 记 interrupted
				return errRunCancelled
			}
			// 校验失败说明该分区在存储上的产物不可信：必须回写 failed，
			// 否则分区级增量去重会把它当成功永久跳过，坏备份再也不会重备。
			p.Status = model.BACKUP_PARTITION_STATUS_FAILED
			p.Msg = "checksum check failed: " + err.Error()
			if uerr := e.repo.UpdateRun(run); uerr != nil {
				log.Logger.Errorf("[exec] update check-failed partition run=%s: %v", runID, uerr)
			}
			if firstErr == nil {
				firstErr = err
			}
			// 关键：不 return；继续校验后续 partition（修 #3）
		}
	}
	return firstErr
}
func (realStages) Close(ctx context.Context, e *Executor, runID string) error {
	run, err := e.repo.GetRun(runID)
	if err != nil {
		return err
	}
	policy, err := e.repo.GetPolicyForRun(run.PolicyID)
	if err != nil {
		return err
	}
	// 清理动作只属于备份:把已成功备份的分区从源表删除。restore run 复用了 backup
	// 的 policy(可能 Clean=true),但恢复成功后绝不能再 DROP 刚恢复的数据。
	if run.Operation != model.OP_BACKUP {
		return nil
	}
	if !policy.Clean {
		return nil
	}

	// 防注入：identifier 强校验
	if err := ValidateIdentifier(run.Database); err != nil {
		return err
	}
	if err := ValidateIdentifier(run.Table); err != nil {
		return err
	}

	var dropErrs []error
	for _, p := range run.Partitions {
		if p.Status != model.BACKUP_PARTITION_STATUS_SUCCESS {
			continue
		}
		for _, c := range e.conns {
			sql := fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PARTITION '%s'",
				run.Database, run.Table, strings.ReplaceAll(p.Partition, "'", "''"))
			if err := e.execSQL(c.host, sql); err != nil {
				dropErrs = append(dropErrs, fmt.Errorf("[%s][%s] %w", c.host, p.Partition, err))
			}
		}
	}
	if len(dropErrs) > 0 {
		return fmt.Errorf("cleanup_failed: %w", errors.Join(dropErrs...))
	}
	return nil
}

// Prepare 阶段:
//  1. 若 policy.Checksum=true：用 errgroup 并发查各 host system.parts（修 #5 race）；
//     即便 goroutine 报错，已成功拿到的 rows 也通过 defer rows.Close() 释放（修泄漏）。
//  2. 清旧目标端残留分区：任一失败立即返回，不静默吞错。
func (realStages) Prepare(ctx context.Context, e *Executor, runID string) error {
	run, err := e.repo.GetRun(runID)
	if err != nil {
		return err
	}
	policy, err := e.repo.GetPolicyForRun(run.PolicyID)
	if err != nil {
		return err
	}

	// checksum 阶段：用 errgroup 替代旧 lastErr race
	if policy.Checksum {
		g, gctx := errgroup.WithContext(ctx)
		_ = gctx
		for _, c := range e.conns {
			c := c
			g.Go(func() error {
				rows, err := e.queryRows(c.host)
				if err != nil {
					return fmt.Errorf("[%s] query: %w", c.host, err)
				}
				defer rows.Close() // 即便后续报错也 close
				if e.collectChecksumOnHost != nil {
					return e.collectChecksumOnHost(c, &run)
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		if err := e.repo.UpdateRun(run); err != nil {
			return fmt.Errorf("update checksum info: %w", err)
		}
	}

	// 清旧目标端残留：失败立即终止 run（不静默）
	for _, p := range run.Partitions {
		if p.Status != model.BACKUP_PARTITION_STATUS_WAITING {
			continue
		}
		for _, c := range e.conns {
			keyPrefix := storage.JoinRunKey(run.StoragePrefix, p.Partition, run.Database, run.Table, c.host)
			if err := e.storage.CleanPartition(c.host, keyPrefix); err != nil {
				return fmt.Errorf("clean %s on %s: %w", p.Partition, c.host, err)
			}
		}
	}
	return nil
}
