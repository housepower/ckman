package backup

import (
	"errors"
	"fmt"

	"github.com/go-basic/uuid"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

type RestoreRequest struct {
	SourceRunID string   `json:"source_run_id"`
	Partitions  []string `json:"partitions"`
}

// SubmitRestore 创建 OP_RESTORE 类型的 BackupRun，从源 success run 复制 partition 元数据。
// 校验：源 run 存在 / status==success / cluster 一致 / 请求的每个 partition 在源中且为 success。
func (s *Service) SubmitRestore(cluster string, req RestoreRequest) (string, error) {
	if req.SourceRunID == "" {
		return "", errors.New("source_run_id empty")
	}
	if len(req.Partitions) == 0 {
		return "", errors.New("partitions empty")
	}
	src, err := s.repo.GetRun(req.SourceRunID)
	if err != nil {
		return "", fmt.Errorf("get source run: %w", err)
	}
	if src.ClusterName != cluster {
		return "", fmt.Errorf("source run cluster %s does not match request cluster %s", src.ClusterName, cluster)
	}
	if src.Status != model.BACKUP_STATUS_SUCCESS {
		return "", fmt.Errorf("source run status is %s, only success runs can be restored", src.Status)
	}

	srcByPart := map[string]model.BackupRunPartition{}
	for _, p := range src.Partitions {
		srcByPart[p.Partition] = p
	}
	var partitions []model.BackupRunPartition
	for _, p := range req.Partitions {
		sp, ok := srcByPart[p]
		if !ok || sp.Status != model.BACKUP_PARTITION_STATUS_SUCCESS {
			return "", fmt.Errorf("partition %s not in source run as success", p)
		}
		partitions = append(partitions, model.BackupRunPartition{
			Partition: p,
			Status:    model.BACKUP_PARTITION_STATUS_WAITING,
			Size:      sp.Size,
			Rows:      sp.Rows,
			FileNum:   sp.FileNum,
		})
	}

	now := s.now()
	run := model.BackupRun{
		RunID:       uuid.New(),
		PolicyID:    src.PolicyID,
		ClusterName: src.ClusterName,
		Database:    src.Database,
		Table:       src.Table,
		Operation:   model.OP_RESTORE,
		TriggerType: model.TRIGGER_MANUAL_RESTORE,
		Instance:    s.self,
		Status:      model.BACKUP_STATUS_QUEUED,
		Partitions:  partitions,
		CreateTime:  now,
	}
	if err := s.repo.CreateRun(run); err != nil {
		return "", err
	}
	if !s.pool.Submit(run.RunID) {
		run.Status = model.BACKUP_STATUS_SKIPPED
		run.StatusReason = model.REASON_QUEUE_FULL
		run.FinishedAt = s.now()
		log.Logger.Warnf("[backup] restore queue full, run %s skipped", run.RunID)
		_ = s.repo.UpdateRun(run)
	}
	return run.RunID, nil
}
