package backup

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	servicecron "github.com/housepower/ckman/service/cron"
)

// 包级单例，供 controller 层通过 GetService() / GetChAdapter() 访问。
// 风格与 repository.Ps 一致。
var (
	globalService   *Service
	globalChAdapter *ClickHouseAdapter
)

// GetService 返回已初始化的 backup Service；未 Init 则返回 nil。
func GetService() *Service { return globalService }

// GetChAdapter 返回已初始化的 ClickHouseAdapter；未 Init 则返回 nil。
func GetChAdapter() *ClickHouseAdapter { return globalChAdapter }

// PersistentRepoAdapter 把 repository.Ps（PersistentMgr）桥接到 ServiceRepo 与 ExecRepo。
type PersistentRepoAdapter struct{}

// ---- ServiceRepo ----

func (PersistentRepoAdapter) CreatePolicy(p model.BackupPolicy) error {
	return repository.Ps.CreateBackupPolicy(p)
}
func (PersistentRepoAdapter) GetPolicy(id string) (model.BackupPolicy, error) {
	return repository.Ps.GetBackupPolicy(id)
}
func (PersistentRepoAdapter) UpdatePolicy(p model.BackupPolicy) error {
	return repository.Ps.UpdateBackupPolicy(p)
}
func (PersistentRepoAdapter) ListPoliciesByCluster(cluster string) ([]model.BackupPolicy, error) {
	return repository.Ps.GetBackupPoliciesByCluster(cluster)
}
func (PersistentRepoAdapter) CreateRun(r model.BackupRun) error {
	return repository.Ps.CreateBackupRun(r)
}
func (PersistentRepoAdapter) UpdateRun(r model.BackupRun) error {
	return repository.Ps.UpdateBackupRun(r)
}
func (PersistentRepoAdapter) DeleteRun(id string) error {
	return repository.Ps.DeleteBackupRun(id)
}
func (PersistentRepoAdapter) GetRun(id string) (model.BackupRun, error) {
	return repository.Ps.GetBackupRun(id)
}
func (PersistentRepoAdapter) InFlightRunsByPolicy(policyID string) []model.BackupRun {
	rs, err := repository.Ps.GetRunsInFlightByPolicy(policyID)
	if err != nil {
		log.Logger.Errorf("[backup] InFlightRunsByPolicy: %v", err)
		return nil
	}
	return rs
}
func (PersistentRepoAdapter) InFlightRunsByInstance(instance string) []model.BackupRun {
	rs, err := repository.Ps.GetRunsInFlightByInstance(instance)
	if err != nil {
		log.Logger.Errorf("[backup] InFlightRunsByInstance: %v", err)
		return nil
	}
	return rs
}

// ---- ExecRepo ----

func (PersistentRepoAdapter) GetPolicyForRun(policyID string) (model.BackupPolicy, error) {
	return repository.Ps.GetBackupPolicy(policyID)
}

// Compile-time interface checks.
var _ ServiceRepo = PersistentRepoAdapter{}
var _ ExecRepo = PersistentRepoAdapter{}

// PolicyRepoAdapter 桥接到 Scheduler.PolicyRepo。
type PolicyRepoAdapter struct{}

func (PolicyRepoAdapter) Active(instance string) []model.BackupPolicy {
	ps, err := repository.Ps.GetActiveScheduledPolicies(instance)
	if err != nil {
		log.Logger.Errorf("[backup] GetActiveScheduledPolicies: %v", err)
		return nil
	}
	return ps
}

var _ PolicyRepo = PolicyRepoAdapter{}

// ServiceCronAdapter 包装 service/cron 包级 AddJob/RemoveJob，
// 实现 backup.CronAdapter 接口。
type ServiceCronAdapter struct{}

func (ServiceCronAdapter) Add(id, spec string, fn func() error) {
	servicecron.AddJob(id, spec, fn)
}
func (ServiceCronAdapter) Remove(id string) {
	servicecron.RemoveJob(id)
}

var _ CronAdapter = ServiceCronAdapter{}

// claimRun 通过 CAS（queued→running）认领一次 run，返回是否应继续执行。
// 防同一 run 重复执行（重复入队 / 多实例误触发），同时把执行中的 run 状态
// 如实标为 running（台账与队列统计依赖它）。
// markRunning 失败（DB 故障）时保守不执行；run 停在 queued，重启后由 Boot
// 标 interrupted。
func claimRun(markRunning func(runID, instance string, startedAt time.Time) (bool, error), runID, self string) bool {
	ok, err := markRunning(runID, self, time.Now())
	if err != nil {
		log.Logger.Errorf("[backup] mark run %s running failed: %v, skip execution", runID, err)
		return false
	}
	if !ok {
		log.Logger.Warnf("[backup] run %s is not queued (already taken or finished), skip duplicate execution", runID)
		return false
	}
	return true
}

// newRealExecutor 为单次 run 装配一个独立 Executor。
// conns/storage 是 run-scoped 可变状态，hooks 闭包必须绑定到本实例（而非共享
// 单例），否则多 worker 并发执行时会互相覆盖、关闭对方正在使用的连接，
// storage 也会跨 policy 串台。
func newRealExecutor(repo ExecRepo, chAdapter *ClickHouseAdapter) *Executor {
	e := &Executor{
		repo:                  repo,
		connFactory:           chAdapter.ConnFactory,
		listPartitions:        chAdapter.ListPartitions,
		listAllPartitions:     chAdapter.ListAllPartitions,
		getLastRunPartitions:  chAdapter.GetLastRunPartitions,
		collectChecksumOnHost: chAdapter.CollectChecksumOnHost,
		stages:                realStages{},
	}

	// execSQL hook：从本实例 e.conns 按 host 找 conn 直接调 Exec
	e.execSQL = func(host, sql string) error {
		for _, c := range e.conns {
			if c.host == host && c.conn != nil {
				return c.conn.Exec(sql)
			}
		}
		return fmt.Errorf("no conn for host %s", host)
	}

	// queryRows hook：从本实例 e.conns 按 host 找 conn 直接调 Query
	e.queryRows = func(host string) (queryResult, error) {
		for _, c := range e.conns {
			if c.host == host && c.conn != nil {
				return c.conn.Query("SELECT path FROM system.parts WHERE active = 1 LIMIT 1")
			}
		}
		return nil, fmt.Errorf("no conn for host %s", host)
	}

	// queryPartitionStats hook：本 shard 上该 partition 的 rows / bytes / parts
	e.queryPartitionStats = func(host, db, table, partition string) (uint64, uint64, uint64, error) {
		var conn *common.Conn
		for _, c := range e.conns {
			if c.host == host && c.conn != nil {
				conn = c.conn
				break
			}
		}
		if conn == nil {
			return 0, 0, 0, fmt.Errorf("no conn for host %s", host)
		}
		sql := fmt.Sprintf(
			"SELECT coalesce(sum(rows),0), coalesce(sum(bytes_on_disk),0), count() FROM system.parts WHERE active=1 AND database='%s' AND `table`='%s' AND partition='%s'",
			strings.ReplaceAll(db, "'", "''"),
			strings.ReplaceAll(table, "'", "''"),
			strings.ReplaceAll(partition, "'", "''"),
		)
		rows, err := conn.Query(sql)
		if err != nil {
			return 0, 0, 0, err
		}
		defer rows.Close()
		var nrows, nbytes, nparts uint64
		if rows.Next() {
			if err := rows.Scan(&nrows, &nbytes, &nparts); err != nil {
				return 0, 0, 0, err
			}
		}
		return nrows, nbytes, nparts, nil
	}
	return e
}

// Init 初始化 backup 模块。在 main 中 repository.InitPersistent 之后调用。
//
// self: 本实例标识（host:port）。
// maxConcurrent: worker pool 大小，<=0 则使用默认值 8。
// chAdapter: ClickHouseAdapter，提供 ConnFactory / ListPartitions 等真实 hook。
//
// 返回 stop 函数，进程退出时调用以清理。
func Init(ctx context.Context, self string, maxConcurrent int, chAdapter *ClickHouseAdapter) (stop func(), err error) {
	if maxConcurrent <= 0 {
		maxConcurrent = 8
	}
	repo := PersistentRepoAdapter{}

	pool := NewPool(maxConcurrent, func(ctx context.Context, runID string) {
		if !claimRun(repository.Ps.MarkRunRunningIfQueued, runID, self) {
			return
		}
		// 每个 run 一个独立 Executor：conns/storage 等 run-scoped 状态互不串扰，
		// 多 worker 并发执行才安全（共享单例会互相覆盖/关闭对方的连接）。
		e := newRealExecutor(repo, chAdapter)
		if rerr := e.Run(ctx, runID); rerr != nil {
			log.Logger.Errorf("[backup] run %s failed: %v", runID, rerr)
		}
	})

	svc := NewService(self, repo, pool)
	if err := svc.Boot(); err != nil {
		return nil, err
	}
	pool.Start(ctx)

	sched := NewScheduler(self, ServiceCronAdapter{}, PolicyRepoAdapter{}, func(p model.BackupPolicy) {
		if _, err := svc.SubmitForPolicy(p, model.TRIGGER_CRON); err != nil {
			log.Logger.Errorf("[backup] cron submit policy=%s: %v", p.PolicyID, err)
		}
	})
	sched.Start(ctx)

	stop = func() {
		sched.Stop()
		// 短暂等待让最后一次 reconcile goroutine 安全停止
		time.Sleep(50 * time.Millisecond)
		pool.Stop()
	}

	// 暴露包级单例，供 controller 层读取
	globalService = svc
	globalChAdapter = chAdapter

	return stop, nil
}
