package backup

import (
	"time"

	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
)

// GetRun 返回 run 详情
func (s *Service) GetRun(runID string) (model.BackupRun, error) {
	return s.repo.GetRun(runID)
}

// ListRunsByPolicy 任务维度台账（按 policy 看历史 run）
// limit=0 表示不限；before=零值表示不限
func (s *Service) ListRunsByPolicy(policyID string, limit int, before time.Time) ([]model.BackupRun, error) {
	return repository.Ps.GetRunsByPolicy(policyID, limit, before)
}

// ListRunsByTable 表维度台账（按 cluster.db.table 看 N 天内 run）
func (s *Service) ListRunsByTable(cluster, database, table string, days int) ([]model.BackupRun, error) {
	return repository.Ps.GetRunsByTable(cluster, database, table, days)
}

// ListPoliciesByCluster cluster 下所有 policy（不含已软删）
func (s *Service) ListPoliciesByCluster(cluster string) ([]model.BackupPolicy, error) {
	return repository.Ps.GetBackupPoliciesByCluster(cluster)
}

// GetPolicy 单 policy 详情
func (s *Service) GetPolicy(policyID string) (model.BackupPolicy, error) {
	return s.repo.GetPolicy(policyID)
}

// BackupQueueStats cluster 维度的 in-flight run 统计，供前端展示排队深度。
type BackupQueueStats struct {
	Running int `json:"running"`
	Queued  int `json:"queued"`
}

// QueueStats 统计 cluster 下执行中 / 排队中的 run 数
func (s *Service) QueueStats(cluster string) (BackupQueueStats, error) {
	runs, err := repository.Ps.GetRunsInFlightByCluster(cluster)
	if err != nil {
		return BackupQueueStats{}, err
	}
	var st BackupQueueStats
	for _, r := range runs {
		switch r.Status {
		case model.BACKUP_STATUS_RUNNING:
			st.Running++
		case model.BACKUP_STATUS_QUEUED:
			st.Queued++
		}
	}
	return st, nil
}
