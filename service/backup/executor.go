package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/backup/storage"
	"golang.org/x/sync/errgroup"
)

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
	host string
	conn *common.Conn // 真实 adapter 注入；测试 fake 留 nil
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
	connFactory          func(cluster string) ([]*shardConn, error)
	listPartitions       func(c *shardConn, db, table, beforeYYYYMMDD string) ([]string, error)
	listAllPartitions    func(c *shardConn, db, table string) ([]string, error)
	getLastRunPartitions func(cluster, db, table string) ([]model.BackupRunPartition, error)
	stages               stages
	now                  func() time.Time

	// run-scoped（Init 阶段填，后续阶段读）
	conns   []*shardConn
	storage BackupStorage

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
		return e.markFailed(runID, "init: "+err.Error())
	}
	defer e.closeConns()
	r, _ := e.repo.GetRun(runID)
	st := e.resolveStages()
	if r.Operation == model.OP_BACKUP {
		if err := st.Prepare(ctx, e, runID); err != nil {
			return e.markFailed(runID, "prepare: "+err.Error())
		}
		if err := st.Backup(ctx, e, runID); err != nil {
			return e.markFailed(runID, "backup: "+err.Error())
		}
		policy, _ := e.repo.GetPolicyForRun(r.PolicyID)
		if policy.Checksum {
			if err := st.Check(ctx, e, runID); err != nil {
				return e.markFailed(runID, "check: "+err.Error())
			}
		}
	} else if r.Operation == model.OP_RESTORE {
		if err := st.Restore(ctx, e, runID); err != nil {
			return e.markFailed(runID, "restore: "+err.Error())
		}
	} else {
		return e.markFailed(runID, "unknown operation: "+r.Operation)
	}
	if err := st.Close(ctx, e, runID); err != nil {
		return e.markFailed(runID, "close: "+err.Error())
	}
	return e.markSuccess(runID)
}

// closeConns 关闭并清空 e.conns。backup 连接 BypassPool=true，用完显式 close，
// 防止任何 caller（包括 Executor 自己下次 Run）拿到一个已 close 的 conn。
func (e *Executor) closeConns() {
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
	conns, err := e.connFactory(policy.ClusterName)
	if err != nil {
		return fmt.Errorf("connect cluster %s: %w", policy.ClusterName, err)
	}
	if len(conns) == 0 {
		return fmt.Errorf("no shard reachable for cluster %s", policy.ClusterName)
	}
	e.conns = conns

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
	case policy.BackupType == model.BACKUP_TYPE_DAILY_PARTITION && policy.DaysBefore > 0:
		// 增量 + 按时间段：枚举 partition <= today-N 的分区，排除上次 success run 已成功备份的分区。
		toPartition := e.clock().AddDate(0, 0, -policy.DaysBefore).Format("20060102")
		newPartitions, err := e.listPartitions(e.conns[0], policy.Database, policy.Table, toPartition)
		if err != nil {
			return err
		}
		var prev []model.BackupRunPartition
		if e.getLastRunPartitions != nil {
			prev, _ = e.getLastRunPartitions(policy.ClusterName, policy.Database, policy.Table)
		}
		r.Partitions = excludeSuccessfulPartitions(prev, newPartitions)
	default:
		return fmt.Errorf("unsupported backup mode: style=%s type=%s daysBefore=%d",
			policy.BackupStyle, policy.BackupType, policy.DaysBefore)
	}
	return nil
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
	for _, p := range run.Partitions {
		if p.Status != model.BACKUP_PARTITION_STATUS_SUCCESS {
			continue
		}
		g, _ := errgroup.WithContext(ctx)
		for _, c := range e.conns {
			c := c
			pp := p
			g.Go(func() error {
				keyPrefix := storage.JoinRunKey(run.StoragePrefix, pp.Partition, run.Database, run.Table, c.host)
				return e.storage.CheckPartition(c.host, keyPrefix, pp.PathInfo)
			})
		}
		if err := g.Wait(); err != nil && firstErr == nil {
			firstErr = err
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
