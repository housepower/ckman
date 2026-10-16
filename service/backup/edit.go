package backup

import (
	"errors"
	"fmt"
	"time"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

// UpdatePolicy 修改可编辑字段；cluster/db/table/schedule_type 不可改。
// crontab / instance / enabled / time range / target / credentials / style / type 可改。
func (s *Service) UpdatePolicy(p model.BackupPolicy) error {
	old, err := s.repo.GetPolicy(p.PolicyID)
	if err != nil {
		return err
	}
	if p.ClusterName != old.ClusterName ||
		p.Database != old.Database ||
		p.Table != old.Table ||
		p.ScheduleType != old.ScheduleType {
		return errors.New("cannot edit cluster/database/table/schedule_type; create a new policy instead")
	}
	if p.TaskID != old.TaskID {
		return errors.New("cannot edit task_id; create a new task instead")
	}
	if p.ScheduleType == model.BACKUP_SCHEDULED {
		if err := ValidateCrontabMinInterval(p.Crontab); err != nil {
			return fmt.Errorf("invalid crontab: %w", err)
		}
	}
	if err := validateDailyRange(p.ScheduleType, p.BackupStyle, p.BackupType, p.StartDate, p.RangeStartDate, p.RangeEndDate, p.DaysBefore); err != nil {
		return err
	}
	if err := validateBandwidth(p.TargetType, p.MaxBandwidthMB, func() (model.CKManClickHouseConfig, error) {
		return s.lookupCluster(p.ClusterName)
	}); err != nil {
		return err
	}
	if err := validateMaxReadSeconds(p.MaxReadSeconds); err != nil {
		return err
	}
	// MaxReadSeconds 0 视为「不传，保持原值」：老前端编辑表单不带该字段时不会
	// 把它意外重置为默认；想改回默认（0/6h）与「保持」语义冲突，编辑场景不区分，
	// 6h 默认值本身就是兜底。
	if p.MaxReadSeconds == 0 {
		p.MaxReadSeconds = old.MaxReadSeconds
	}
	// Sensitive 字段保留：前端编辑时通常不重传 secret，空值应解释为「不变」而非清空。
	if p.S3.SecretAccessKey == "" {
		p.S3.SecretAccessKey = old.S3.SecretAccessKey
	}
	p.CreateTime = old.CreateTime
	p.UpdateTime = time.Now()
	return s.repo.UpdatePolicy(p)
}

// DeletePolicy 物理删除 policy 及其名下所有 run 台账。
// 此前是软删(deleted=true),会在 tbl_backup_policy 永久残留脏数据;现改为物理删,
// 并级联物理删该 policy 的全部 run(否则 run 成为指向已删 policy 的孤儿)。
// 存在进行中(queued/running)run 时拒绝,避免删掉正在执行的台账(与前端阻断一致)。
// 远端 S3/本地备份文件是否清理由删任务流程的「同步删除」决定,与此无关。
func (s *Service) DeletePolicy(policyID string) error {
	if len(s.repo.InFlightRunsByPolicy(policyID)) > 0 {
		return fmt.Errorf("cannot delete policy %s with in-flight runs", policyID)
	}
	runs, err := s.repo.RunsByPolicy(policyID)
	if err != nil {
		return err
	}
	for _, r := range runs {
		if err := s.repo.DeleteRun(r.RunID); err != nil {
			return err
		}
	}
	return s.repo.HardDeletePolicy(policyID)
}

// DeleteRun 删除一次 run 的 ckman 台账记录。仅允许终态 run（success/failed/
// skipped/interrupted）；queued/running 拒绝，避免在跑的 worker 拿不到记录。
// 该操作不动 S3 / Local 上的备份数据，对应 backup 数据可能成为孤儿。
func (s *Service) DeleteRun(runID string) error {
	r, err := s.repo.GetRun(runID)
	if err != nil {
		return err
	}
	if r.Status == model.BACKUP_STATUS_QUEUED || r.Status == model.BACKUP_STATUS_RUNNING {
		return fmt.Errorf("cannot delete in-flight run (status=%s)", r.Status)
	}
	return s.repo.DeleteRun(runID)
}

// StopRun 主动停止一次备份/恢复 run，替代「重启 ckman」的粗暴中断：
//   - queued：从 Pool 队列移除并直接落 interrupted(cancelled) 台账；
//   - running：找到在途 Executor 调 Cancel()（关连接使 BACKUP/RESTORE 语句立即
//     报错退出），台账由 Run 收尾时落 interrupted(cancelled)；
//   - 终态 run 拒绝；run 绑定的执行实例不是本实例时拒绝（去对应实例上停）。
//
// queued→running 的认领窗口：Remove 失败且 registry 尚未登记时，短暂轮询等待
// executor 出现；超时就地落 interrupted 台账——claimRun 的 CAS(queued→running)
// 会因此失败而放弃执行，双保险下不会出「停了却还在跑」。
func (s *Service) StopRun(runID string) error {
	r, err := s.repo.GetRun(runID)
	if err != nil {
		return err
	}
	if r.Instance != "" && r.Instance != s.self {
		return fmt.Errorf("run %s belongs to instance %s, stop it on that instance", runID, r.Instance)
	}
	if r.Status == model.BACKUP_STATUS_QUEUED {
		if s.pool.Remove(runID) {
			r.Status = model.BACKUP_STATUS_INTERRUPTED
			r.StatusReason = model.REASON_CANCELLED
			r.FinishedAt = s.now()
			return s.repo.UpdateRun(r)
		}
		// worker 已取走，落入 running 处理
	}
	if r.Status != model.BACKUP_STATUS_RUNNING && r.Status != model.BACKUP_STATUS_QUEUED {
		return fmt.Errorf("run %s is not in-flight (status=%s)", runID, r.Status)
	}
	if e, ok := s.loadExecutorWithRetry(runID, 3*time.Second); ok {
		e.Cancel()
		return nil
	}
	// executor 始终未出现：直接落台账 interrupted；claimRun 的 CAS 会失败兜底
	log.Logger.Warnf("[backup] stop run %s: executor not found, mark interrupted in ledger directly", runID)
	r.Status = model.BACKUP_STATUS_INTERRUPTED
	r.StatusReason = model.REASON_CANCELLED
	r.ErrorMsg = "cancelled by user"
	r.FinishedAt = s.now()
	return s.repo.UpdateRun(r)
}

// loadExecutorWithRetry 轮询等待在途 executor 登记。认领（CAS queued→running）
// 到 execute 闭包注册 executor 之间有毫秒级窗口，StopRun 恰好落在窗口内时
// 稍等即可拿到；拿到前 run 的台账状态可能刚从 queued 翻成 running。
func (s *Service) loadExecutorWithRetry(runID string, timeout time.Duration) (*Executor, bool) {
	if s.executors == nil {
		return nil, false
	}
	deadline := time.Now().Add(timeout)
	for {
		if v, ok := s.executors.Load(runID); ok {
			return v.(*Executor), true
		}
		if time.Now().After(deadline) {
			return nil, false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TriggerPolicy 立即触发一次 run（不论 schedule_type）
func (s *Service) TriggerPolicy(policyID string) (string, error) {
	p, err := s.repo.GetPolicy(policyID)
	if err != nil {
		return "", err
	}
	return s.SubmitForPolicy(p, model.TRIGGER_MANUAL_IMMEDIATE)
}
