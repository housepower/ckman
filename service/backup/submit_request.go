package backup

import (
	"fmt"

	"github.com/go-basic/uuid"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

// SubmitBackupRequest 把多表 BackupRequest 拆为 N 个 (policy, run) 对。
//
// 立即备份：建 policy(Enabled=false) + 立即提交一次 run；返回 run_ids。
// 定时备份：建 policy(Enabled=true) + 写入策略，由 scheduler 周期触发；
// 返回的 run_ids 为空切片（定时备份不立即创建 run）。
func (s *Service) SubmitBackupRequest(cluster string, req model.BackupRequest) ([]string, error) {
	if req.ScheduleType == model.BACKUP_SCHEDULED {
		if err := ValidateCrontabMinInterval(req.Crontab); err != nil {
			return nil, fmt.Errorf("invalid crontab: %w", err)
		}
	}
	var runIDs []string
	for _, table := range req.Tables {
		policy := model.BackupPolicy{
			PolicyID:     uuid.New(),
			ClusterName:  cluster,
			Database:     req.Database,
			Table:        table,
			ScheduleType: req.ScheduleType,
			Crontab:      req.Crontab,
			Instance:     req.Instance,
			BackupStyle:  req.BackupStyle,
			BackupType:   req.BackupType,
			DaysBefore:   req.DaysBefore,
			Partitions:   req.Partitions,
			TargetType:   req.Target,
			S3:           req.S3,
			Local:        req.Local,
			Compression:  req.Compression,
			Checksum:     req.Checksum,
			Clean:        req.Clean,
			Enabled:      req.ScheduleType == model.BACKUP_SCHEDULED,
		}
		if err := s.repo.CreatePolicy(policy); err != nil {
			return runIDs, fmt.Errorf("create policy for table %s: %w", table, err)
		}
		if req.ScheduleType == model.BACKUP_IMMEDIATE {
			runID, err := s.SubmitForPolicy(policy, model.TRIGGER_MANUAL_IMMEDIATE)
			if err != nil {
				log.Logger.Errorf("[backup] submit immediate run for table %s: %v", table, err)
				continue
			}
			runIDs = append(runIDs, runID)
		}
	}
	return runIDs, nil
}
