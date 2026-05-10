package backup

import (
	"errors"
	"fmt"
	"time"

	"github.com/housepower/ckman/model"
)

// UpdatePolicy 修改可编辑字段；cluster/db/table/schedule_type 不可改。
// crontab / instance / enabled / days_before / target / credentials / style / type 可改。
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
	if p.ScheduleType == model.BACKUP_SCHEDULED {
		if err := ValidateCrontabMinInterval(p.Crontab); err != nil {
			return fmt.Errorf("invalid crontab: %w", err)
		}
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

// TriggerPolicy 立即触发一次 run（不论 schedule_type）
func (s *Service) TriggerPolicy(policyID string) (string, error) {
	p, err := s.repo.GetPolicy(policyID)
	if err != nil {
		return "", err
	}
	return s.SubmitForPolicy(p, model.TRIGGER_MANUAL_IMMEDIATE)
}
