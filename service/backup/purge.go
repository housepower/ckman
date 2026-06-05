package backup

import (
	"errors"
	"fmt"

	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
)

// DeletePartitionRecordsResult 删除分区备份记录的执行结果。
type DeletePartitionRecordsResult struct {
	RemovedRecords int      `json:"removed_records"` // 删除的分区条目数
	DeletedRuns    int      `json:"deleted_runs"`    // 分区被删光后连带删除的 run 数
	Warnings       []string `json:"warnings"`        // best-effort 步骤(远端清理等)的警告
}

// purgeCleanItem 一条待清理的远端备份数据(在改写记录前收集)。
type purgeCleanItem struct {
	policyID      string
	storagePrefix string
	partition     string
}

// isTerminalRunStatus 判断 run 是否已进入终态(不会再被 Executor 改写)。
func isTerminalRunStatus(status string) bool {
	switch status {
	case model.BACKUP_STATUS_SUCCESS, model.BACKUP_STATUS_FAILED,
		model.BACKUP_STATUS_SKIPPED, model.BACKUP_STATUS_INTERRUPTED:
		return true
	}
	return false
}

// DeletePartitionRecords 按分区名删除该表 365 天内所有终态 run 中的分区条目,
// 让这些分区脱离增量去重、下次备份时重新备份。
//
//   - 必须全历史删除:同分区的 success 可能散落多条 run(含 migrated 老记录),
//     只删一条,更老的 success 会重新生效继续去重(见 successfulPartitionsFromRuns)。
//   - run 的分区被删光时连 run 一起删,避免留下 success+0 分区的尸体记录。
//   - cleanRemote=true 时 best-effort 清理远端备份数据:失败仅记 warning,
//     记录照删——下次重备时 Prepare 阶段会再清一遍目标端残留,有兜底。
func (s *Service) DeletePartitionRecords(cluster, database, table string, partitions []string, cleanRemote bool) (DeletePartitionRecordsResult, error) {
	var result DeletePartitionRecordsResult
	if len(partitions) == 0 {
		return result, errors.New("partitions required")
	}
	target := make(map[string]bool, len(partitions))
	for _, p := range partitions {
		if err := ValidateIdentifier(p); err != nil {
			return result, fmt.Errorf("invalid partition %q: %w", p, err)
		}
		target[p] = true
	}

	runs, err := s.fetchRunsByTable(cluster, database, table, 365)
	if err != nil {
		return result, fmt.Errorf("list runs: %w", err)
	}
	// 守卫(1):GetRunsByTable 返回的 run 中检查终态。
	// Executor 持有 run 副本,并发 UpdateRun 会把删掉的条目原样写回(竞态)。
	for _, r := range runs {
		if !isTerminalRunStatus(r.Status) {
			return result, fmt.Errorf("table %s.%s has in-flight run %s (status=%s), retry later",
				database, table, r.RunID, r.Status)
		}
	}
	// 守卫(2):queued run 的 StartedAt 为零值,被 GetRunsByTable 的 started_at
	// 过滤排除,上面的扫描看不见它——用 in-flight 专用查询补一刀。
	for _, r := range s.repo.InFlightRunsByCluster(cluster) {
		if r.Database == database && r.Table == table {
			return result, fmt.Errorf("table %s.%s has in-flight run %s (status=%s), retry later",
				database, table, r.RunID, r.Status)
		}
	}

	// 改写前收集远端清理项:storagePrefix 取自 run,target 配置取自 policy。
	var cleanItems []purgeCleanItem
	seenClean := map[string]bool{}

	for _, r := range runs {
		kept := make([]model.BackupRunPartition, 0, len(r.Partitions))
		removed := 0
		for _, p := range r.Partitions {
			if !target[p.Partition] {
				kept = append(kept, p)
				continue
			}
			removed++
			if r.Operation != model.OP_RESTORE { // restore 记录没有对应的远端备份数据
				k := r.PolicyID + "|" + r.StoragePrefix + "|" + p.Partition
				if !seenClean[k] {
					seenClean[k] = true
					cleanItems = append(cleanItems, purgeCleanItem{r.PolicyID, r.StoragePrefix, p.Partition})
				}
			}
		}
		if removed == 0 {
			continue
		}
		if len(kept) == 0 {
			if derr := s.repo.DeleteRun(r.RunID); derr != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("delete empty run %s: %v", r.RunID, derr))
			} else {
				result.RemovedRecords += removed
				result.DeletedRuns++
			}
			continue
		}
		r.Partitions = kept
		if uerr := s.repo.UpdateRun(r); uerr != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("update run %s: %v", r.RunID, uerr))
		} else {
			result.RemovedRecords += removed
		}
	}

	if cleanRemote && len(cleanItems) > 0 {
		s.cleanRemotePartitions(cluster, database, table, cleanItems, &result)
	}
	return result, nil
}

// cleanRemotePartitions best-effort 清理远端备份数据(Task 5 完整实现)。
func (s *Service) cleanRemotePartitions(cluster, database, table string, items []purgeCleanItem, result *DeletePartitionRecordsResult) {
}

// ── 可注入依赖(测试注入 fake;nil 时走生产默认) ────────────────────────

func (s *Service) fetchRunsByTable(cluster, db, table string, days int) ([]model.BackupRun, error) {
	if s.getRunsByTable != nil {
		return s.getRunsByTable(cluster, db, table, days)
	}
	return repository.Ps.GetRunsByTable(cluster, db, table, days)
}

func (s *Service) lookupCluster(name string) (model.CKManClickHouseConfig, error) {
	if s.getClusterByName != nil {
		return s.getClusterByName(name)
	}
	return repository.Ps.GetClusterbyName(name)
}

func (s *Service) makeStorage(policy model.BackupPolicy, cc model.CKManClickHouseConfig) BackupStorage {
	if s.storageFactory != nil {
		return s.storageFactory(policy, cc)
	}
	return NewStorageForPolicy(policy, cc)
}
