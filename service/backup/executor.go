package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"golang.org/x/sync/errgroup"
)

// queryResult is a minimal interface satisfied by *sql.Rows and test fakes.
type queryResult interface{ Close() }

// ExecRepo 暴露 Executor 需要的持久层操作。
type ExecRepo interface {
	GetRun(id string) (model.BackupRun, error)
	UpdateRun(r model.BackupRun) error
	GetPolicyForRun(policyID string) (model.BackupPolicy, error)
}

// shardConn 是 Executor 内部用的 shard 句柄。
// 真实实现持有 *common.Conn；测试用 fake 仅持 host。
type shardConn struct {
	host string
	// conn *common.Conn   // Task 21 真实 adapter 注入
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
	getLastRunPartitions func(cluster, db, table string) ([]model.BackupRunPartition, error)
	stages               stages
	now                  func() time.Time

	// run-scoped（Init 阶段填，后续阶段读）
	conns   []*shardConn
	storage BackupStorage

	// run-scoped helpers（Init 期间不一定都注入；Task 21 真实 adapter 全部填）
	queryRows             func(host string) (queryResult, error)
	collectChecksumOnHost func(c *shardConn, run *model.BackupRun) error
	execSQL               func(host, sql string) error // 新增；Task 21 真实 adapter 注入
}

// BackupStorage 是 backup 包内对 storage.Storage 的本地镜像接口，
// 避免与 service/backup/storage 形成循环依赖。
// Task 21 真实 adapter 注入时会传入满足此接口的具体实现。
type BackupStorage interface {
	Init() error
	BackupSQL(database, table, partition, key string) string
	RestoreSQL(database, table, partition, key string) string
	CleanPartition(database, table, host, partition string) error
	CheckPartition(host, database, table, partition string, pathInfo map[string]model.PathInfo) error
	Type() string
}

// Run 是 worker 调用入口。
func (e *Executor) Run(ctx context.Context, runID string) error {
	if err := e.Init(ctx, runID); err != nil {
		return e.markFailed(runID, "init: "+err.Error())
	}
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

	// 仅 daily 模式且 daysBefore > 0 时枚举分区
	if policy.BackupType == model.BACKUP_TYPE_DAILY_PARTITION && policy.DaysBefore > 0 {
		toPartition := e.clock().AddDate(0, 0, -policy.DaysBefore).Format("20060102")
		newPartitions, err := e.listPartitions(conns[0], policy.Database, policy.Table, toPartition)
		if err != nil {
			return err
		}
		// 修 #4：getLastRunPartitions 报错时仍采用 newPartitions，不丢分区
		var prev []model.BackupRunPartition
		if e.getLastRunPartitions != nil {
			prev, _ = e.getLastRunPartitions(policy.ClusterName, policy.Database, policy.Table)
		}
		r.Partitions = mergePartitionLists(prev, newPartitions)
	}

	r.StartedAt = e.clock()
	if err := e.repo.UpdateRun(r); err != nil {
		return err
	}
	return nil
}

// mergePartitionLists：保留 prev 的 success（避免重复备份），新增 newest 中尚未存在的分区
// 状态标 waiting。
func mergePartitionLists(prev []model.BackupRunPartition, newest []string) []model.BackupRunPartition {
	have := map[string]bool{}
	out := make([]model.BackupRunPartition, 0, len(prev)+len(newest))
	for _, p := range prev {
		out = append(out, p)
		have[p.Partition] = true
	}
	for _, n := range newest {
		if !have[n] {
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

		// 跨 shard 并发执行 BACKUP TABLE，但状态汇总在主线程串行写，规避 race。
		type result struct {
			host string
			err  error
		}
		ch := make(chan result, len(e.conns))
		for _, c := range e.conns {
			c := c
			go func() {
				key := fmt.Sprintf("%s/%s.%s/%s", p.Partition, run.Database, run.Table, c.host)
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
				key := fmt.Sprintf("%s/%s.%s/%s", p.Partition, run.Database, run.Table, c.host)
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
				return e.storage.CheckPartition(c.host, run.Database, run.Table, pp.Partition, pp.PathInfo)
			})
		}
		if err := g.Wait(); err != nil && firstErr == nil {
			firstErr = err
			// 关键：不 return；继续校验后续 partition（修 #3）
		}
	}
	return firstErr
}
func (realStages) Close(context.Context, *Executor, string) error   { return nil }

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
			if err := e.storage.CleanPartition(run.Database, run.Table, c.host, p.Partition); err != nil {
				return fmt.Errorf("clean %s on %s: %w", p.Partition, c.host, err)
			}
		}
	}
	return nil
}
