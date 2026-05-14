package backup

import (
	"errors"
	"fmt"
	"time"

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
	// Sensitive 字段保留：前端编辑时通常不重传 secret，空值应解释为「不变」而非清空。
	if p.S3.SecretAccessKey == "" {
		p.S3.SecretAccessKey = old.S3.SecretAccessKey
	}
	p.CreateTime = old.CreateTime
	p.UpdateTime = time.Now()
	return s.repo.UpdatePolicy(p)
}

// DeletePolicy 软删 policy
func (s *Service) DeletePolicy(policyID string) error {
	p, err := s.repo.GetPolicy(policyID)
	if err != nil {
		return err
	}
	p.Deleted = true
	p.Enabled = false
	p.UpdateTime = time.Now()
	return s.repo.UpdatePolicy(p)
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

// TriggerPolicy 立即触发一次 run（不论 schedule_type）
func (s *Service) TriggerPolicy(policyID string) (string, error) {
	p, err := s.repo.GetPolicy(policyID)
	if err != nil {
		return "", err
	}
	return s.SubmitForPolicy(p, model.TRIGGER_MANUAL_IMMEDIATE)
}
