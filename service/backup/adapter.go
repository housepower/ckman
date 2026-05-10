package backup

import (
	"context"
	"time"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	servicecron "github.com/housepower/ckman/service/cron"
)

// PersistentRepoAdapter 把 repository.Ps（PersistentMgr）桥接到 ServiceRepo 与 ExecRepo。
type PersistentRepoAdapter struct{}

// ---- ServiceRepo ----

func (PersistentRepoAdapter) CreatePolicy(p model.BackupPolicy) error {
	return repository.Ps.CreateBackupPolicy(p)
}
func (PersistentRepoAdapter) GetPolicy(id string) (model.BackupPolicy, error) {
	return repository.Ps.GetBackupPolicy(id)
}
func (PersistentRepoAdapter) CreateRun(r model.BackupRun) error {
	return repository.Ps.CreateBackupRun(r)
}
func (PersistentRepoAdapter) UpdateRun(r model.BackupRun) error {
	return repository.Ps.UpdateBackupRun(r)
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

// Init 初始化 backup 模块。在 main 中 repository.InitPersistent 之后调用。
//
// self: 本实例标识（host:port）。
// maxConcurrent: worker pool 大小，<=0 则使用默认值 8。
//
// 返回 stop 函数，进程退出时调用以清理。
func Init(ctx context.Context, self string, maxConcurrent int) (stop func(), err error) {
	if maxConcurrent <= 0 {
		maxConcurrent = 8
	}
	repo := PersistentRepoAdapter{}

	// Pool 用占位 exec：Plan 1 阶段没有真实 run 流入；
	// Plan 2 接 HTTP 后换成真实 Executor.Run。
	pool := NewPool(maxConcurrent, func(_ context.Context, runID string) {
		log.Logger.Warnf("[backup] received run %s but executor not wired yet (Plan 2)", runID)
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
	return stop, nil
}
