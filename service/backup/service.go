package backup

import (
	"errors"
	"time"

	"github.com/go-basic/uuid"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

// ServiceRepo 暴露 Service 需要的持久层操作。
type ServiceRepo interface {
	CreatePolicy(p model.BackupPolicy) error
	GetPolicy(id string) (model.BackupPolicy, error)
	UpdatePolicy(p model.BackupPolicy) error
	ListPoliciesByCluster(cluster string) ([]model.BackupPolicy, error)
	CreateRun(r model.BackupRun) error
	UpdateRun(r model.BackupRun) error
	DeleteRun(id string) error
	GetRun(id string) (model.BackupRun, error)
	InFlightRunsByPolicy(policyID string) []model.BackupRun
	InFlightRunsByInstance(instance string) []model.BackupRun // 新增，Task 14
}

// ServicePool 暴露入队能力。Task 11 的 Pool 实现满足。
type ServicePool interface {
	Submit(runID string) bool
}

// Service 是 backup 提交的入口（HTTP / cron 都通过它）。
type Service struct {
	self string
	repo ServiceRepo
	pool ServicePool
	now  func() time.Time
}

func NewService(self string, repo ServiceRepo, pool ServicePool) *Service {
	return &Service{self: self, repo: repo, pool: pool, now: time.Now}
}

// SubmitForPolicy 给已存在的 policy 生成一次 run；trigger ∈ TRIGGER_*。
// 即便是 skipped 状态也返回 runID，便于台账定位。
func (s *Service) SubmitForPolicy(p model.BackupPolicy, trigger string) (string, error) {
	if p.PolicyID == "" {
		return "", errors.New("policy id empty")
	}

	now := s.now()
	run := model.BackupRun{
		RunID:         uuid.New(),
		PolicyID:      p.PolicyID,
		ClusterName:   p.ClusterName,
		Database:      p.Database,
		Table:         p.Table,
		Operation:     model.OP_BACKUP,
		TriggerType:   trigger,
		Instance:      p.Instance,
		Status:        model.BACKUP_STATUS_QUEUED,
		StoragePrefix: p.ClusterName, // 新 run 把 cluster 名落到 storage key 前缀；老 run 反序列化为 "" 兼容老路径
		CreateTime:    now,
	}

	// 1. policy.enabled=false 时仍被 cron 触发（disable→reconcile 之间最多 60s 窗口）
	if p.ScheduleType == model.BACKUP_SCHEDULED && !p.Enabled {
		run.Status = model.BACKUP_STATUS_SKIPPED
		run.StatusReason = model.REASON_DISABLED
		run.FinishedAt = now
		return run.RunID, s.repo.CreateRun(run)
	}

	// 2. 重叠检测
	inFlight := s.repo.InFlightRunsByPolicy(p.PolicyID)
	if len(inFlight) > 0 {
		run.Status = model.BACKUP_STATUS_SKIPPED
		run.StatusReason = model.REASON_OVERLAP
		run.FinishedAt = now
		return run.RunID, s.repo.CreateRun(run)
	}

	// 3. 持久化为 queued
	if err := s.repo.CreateRun(run); err != nil {
		return "", err
	}

	// 4. 入队；满则改 skipped(queue_full)
	if !s.pool.Submit(run.RunID) {
		run.Status = model.BACKUP_STATUS_SKIPPED
		run.StatusReason = model.REASON_QUEUE_FULL
		run.FinishedAt = s.now()
		log.Logger.Warnf("[backup] queue full, run %s skipped", run.RunID)
		if err := s.repo.UpdateRun(run); err != nil {
			log.Logger.Errorf("[backup] update queue_full run failed: %v", err)
			// 不冒泡，台账状态会停在 queued，启动时会被 Boot 标 interrupted
		}
	}

	return run.RunID, nil
}

// Boot 启动时调用：把本实例上所有 queued/running run 标 interrupted。
// 防止 ckman 崩溃 / 重启后留下假在跑的状态。spec §5.5。
func (s *Service) Boot() error {
	now := s.now()
	for _, r := range s.repo.InFlightRunsByInstance(s.self) {
		r.Status = model.BACKUP_STATUS_INTERRUPTED
		r.StatusReason = model.REASON_RESTART
		r.FinishedAt = now
		if err := s.repo.UpdateRun(r); err != nil {
			log.Logger.Errorf("[backup] boot mark interrupted run=%s: %v", r.RunID, err)
		}
	}
	return nil
}
