package backup

import (
	"errors"
	"fmt"
	"strings"

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
	// 通用校验：incremental + partition 模式必须显式给出 partitions
	if req.BackupStyle == model.BACKUP_STYLE_INCR && req.BackupType == model.BACKUP_TYPE_PARTITION && len(req.Partitions) == 0 {
		return nil, errors.New("incremental + partition mode requires at least one partition name")
	}
	if req.ScheduleType == model.BACKUP_SCHEDULED {
		if err := ValidateCrontabMinInterval(req.Crontab); err != nil {
			return nil, fmt.Errorf("invalid crontab: %w", err)
		}
		// 拦截：同一张表禁止挂在多个 active scheduled task 下。
		existing, err := s.repo.ListPoliciesByCluster(cluster)
		if err != nil {
			return nil, fmt.Errorf("list policies for conflict check: %w", err)
		}
		want := make(map[string]struct{}, len(req.Tables))
		for _, t := range req.Tables {
			want[t] = struct{}{}
		}
		var conflicts []string
		for _, p := range existing {
			if p.Deleted || !p.Enabled || p.ScheduleType != model.BACKUP_SCHEDULED {
				continue
			}
			if p.Database != req.Database {
				continue
			}
			if _, hit := want[p.Table]; !hit {
				continue
			}
			label := p.TaskName
			if label == "" {
				label = p.PolicyID
			}
			conflicts = append(conflicts, fmt.Sprintf("%s.%s (task=%s)", p.Database, p.Table, label))
		}
		if len(conflicts) > 0 {
			return nil, fmt.Errorf("以下表已存在启用中的定时备份任务，不允许重复添加: %s", strings.Join(conflicts, ", "))
		}
	}

	// 同次提交的所有 policy 共享一个 taskID；taskName 由用户指定或自动生成。
	taskID := uuid.New()
	taskName := req.TaskName
	if taskName == "" {
		if len(req.Tables) == 1 {
			taskName = fmt.Sprintf("%s.%s", req.Database, req.Tables[0])
		} else {
			taskName = fmt.Sprintf("%s.%s (+%d more)", req.Database, req.Tables[0], len(req.Tables)-1)
		}
	}

	var runIDs []string
	for _, table := range req.Tables {
		policy := model.BackupPolicy{
			PolicyID:     uuid.New(),
			TaskID:       taskID,
			TaskName:     taskName,
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
