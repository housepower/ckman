package backup

import (
	"context"
	"fmt"
	"time"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

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
