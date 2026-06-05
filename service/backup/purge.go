package backup

import (
	"errors"
	"fmt"

	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/backup/storage"
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

// cleanRemotePartitions best-effort 清理远端备份数据。备份 key 含执行当时的
// replica host(JoinRunKey),而 run 未记录该 host,故对全部副本 host 逐一清理
// (CleanPartition 对不存在的 key 是幂等 no-op)。任何失败只记 warning 不中断。
//
// 注意:Local 后端经 SSH 同步执行 rm,RemoteExecute 仅有连接超时(30s)而无
// 命令级 deadline——主机可连但命令挂住(如 NFS 假死)时本调用会阻塞 HTTP 请求。
// S3 后端为 server-side 删除,不受此影响。
//
// warning 数量上限 maxCleanWarnings(50):N item × M host 全失败时警告不膨胀;
// 超限后额外失败只累计计数,返回前补一条汇总。DeletePartitionRecords 主循环里
// 的 update/delete warning 不在此上限内(数量受 run 数约束且每条都重要)。
func (s *Service) cleanRemotePartitions(cluster, database, table string, items []purgeCleanItem, result *DeletePartitionRecordsResult) {
	const maxCleanWarnings = 50
	suppressed := 0
	warnf := func(format string, args ...interface{}) {
		if len(result.Warnings) >= maxCleanWarnings {
			suppressed++
			return
		}
		result.Warnings = append(result.Warnings, fmt.Sprintf(format, args...))
	}

	cc, err := s.lookupCluster(cluster)
	if err != nil {
		warnf("get cluster %s: %v; remote data not cleaned", cluster, err)
		return
	}
	var hosts []string
	for _, shard := range cc.Shards {
		for _, rep := range shard.Replicas {
			hosts = append(hosts, rep.Ip)
		}
	}
	if len(hosts) == 0 {
		warnf("cluster %s has no hosts; remote data not cleaned", cluster)
		return
	}
	storages := map[string]BackupStorage{} // policyID → storage;装配失败记 nil,同 policy 不重试
	for _, item := range items {
		st, ok := storages[item.policyID]
		if !ok {
			st = s.assembleStorageForPurge(item.policyID, cc, warnf)
			storages[item.policyID] = st
		}
		if st == nil {
			continue
		}
		for _, h := range hosts {
			key := storage.JoinRunKey(item.storagePrefix, item.partition, database, table, h)
			if cerr := st.CleanPartition(h, key); cerr != nil {
				warnf("clean %s on %s: %v", key, h, cerr)
			}
		}
	}
	if suppressed > 0 {
		result.Warnings = append(result.Warnings, fmt.Sprintf("...and %d more clean warnings suppressed", suppressed))
	}
}

// assembleStorageForPurge 按 policy 组装并初始化 storage;失败记 warning 返回 nil。
// warnf 由调用方提供,支持 warning 数量上限控制。
func (s *Service) assembleStorageForPurge(policyID string, cc model.CKManClickHouseConfig, warnf func(string, ...interface{})) BackupStorage {
	policy, perr := s.repo.GetPolicy(policyID)
	if perr != nil {
		warnf("policy %s not found; its remote data not cleaned", policyID)
		return nil
	}
	st := s.makeStorage(policy, cc)
	if st == nil {
		warnf("policy %s: unsupported target %q; remote data not cleaned", policyID, policy.TargetType)
		return nil
	}
	if ierr := st.Init(); ierr != nil {
		warnf("policy %s: storage init: %v; remote data not cleaned", policyID, ierr)
		return nil
	}
	return st
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
