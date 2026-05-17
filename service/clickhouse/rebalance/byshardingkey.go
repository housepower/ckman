package rebalance

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

// ByShardingKey rebalances data by re-hashing every row through a user-chosen
// column. It moves data to a temp table per host, then INSERTs back into the
// original table from cluster(), gated by `hash(key) % N = idx` so each host
// keeps exactly the rows that should land on it.
type ByShardingKey struct{}

func (ByShardingKey) Name() string { return model.RebalancePolicyShardingKey }

// Validate ensures the table actually has the requested sharding key column
// and pins down its ClickHouse type so the right hash expression can be built.
// The ctrlConn argument is used here rather than fishing a connection out of
// the pool: doing so makes the dependency explicit and removes the implicit
// ordering coupling with GetShardAvaliableHosts that would otherwise populate
// the pool entry for r.Hosts[0].
func (ByShardingKey) Validate(r *Rebalancer, ctrlConn *common.Conn) error {
	return getShardingType(&r.Shardingkey, ctrlConn)
}

// Plan reports the reshuffle's scale (rows, bytes, sharding key, shard count)
// without doing any of the actual work. Reuses InitCKConns(true) to fetch
// engine and oriCount which already populate r; one extra cluster()-wide
// query covers total bytes for the tmp-disk estimate.
func (ByShardingKey) Plan(r *Rebalancer) (model.TablePlan, error) {
	plan := model.TablePlan{
		Database: r.Database,
		Table:    r.Table,
		Strategy: model.RebalancePolicyShardingKey,
	}
	if err := r.InitCKConns(true); err != nil {
		return plan, err
	}
	plan.Engine = r.Engine

	var totalBytes, compressedBytes uint64
	bytesQuery := fmt.Sprintf(`SELECT
  toUInt64(coalesce(sum(data_uncompressed_bytes), 0)),
  toUInt64(coalesce(sum(data_compressed_bytes), 0))
FROM cluster('%s', 'system.parts') WHERE active AND database = '%s' AND table = '%s'`, r.Cluster, r.Database, r.Table)
	log.Logger.Debugf(bytesQuery)
	conn := common.GetConnection(r.Hosts[0])
	if conn == nil {
		return plan, fmt.Errorf("[rebalance]can't get connection: %s", r.Hosts[0])
	}
	if err := conn.QueryRow(bytesQuery).Scan(&totalBytes, &compressedBytes); err != nil {
		return plan, errors.Wrapf(err, "query %s failed", bytesQuery)
	}

	plan.Reshuffle = &model.ReshuffleSummary{
		TotalRows:       r.OriCount,
		TotalBytes:      totalBytes,
		CompressedBytes: compressedBytes,
		Shards:          len(r.Hosts),
		ShardingKey:     r.Shardingkey.ShardingKey,
	}
	return plan, nil
}

// Run executes the full lifecycle: open connections + metadata fetch, create
// the per-cluster tmp table, move the original data into tmp, verify count,
// re-insert filtered by hash modulo host index, verify count again, and
// optionally drop the tmp table.
//
// CheckCounts is retried once after 5s because the cluster() function uses a
// distributed query whose freshness lags slightly; a one-shot eventually-
// consistent retry covers that without inviting infinite retry loops.
func (ByShardingKey) Run(r *Rebalancer) error {
	start := time.Now()
	r.setStep(model.StepShardingFetch)
	log.Logger.Info("[rebalance] STEP InitCKConns")
	if err := r.InitCKConns(true); err != nil {
		log.Logger.Errorf("got error %+v", err)
		return err
	}
	r.setStep(model.StepShardingTmp)
	log.Logger.Info("[rebalance] STEP CreateTemporaryTable")
	if err := r.createTemporaryTable(); err != nil {
		return err
	}
	r.setStep(model.StepShardingMove)
	log.Logger.Info("[rebalance] STEP MoveBackup")
	if err := r.moveBackup(); err != nil {
		return err
	}
	r.setStep(model.StepShardingVerify)
	if err := r.checkCounts(r.TmpTable); err != nil {
		time.Sleep(5 * time.Second)
		if err := r.checkCounts(r.TmpTable); err != nil {
			return err
		}
	}
	r.setStep(model.StepShardingInsert)
	log.Logger.Info("[rebalance] STEP InsertPlan")
	if err := r.insertPlan(); err != nil {
		return errors.Wrapf(err, "table %s.%s rebalance failed, data can be corrupted, please move back from temp table[%s] manually", r.Database, r.Table, r.TmpTable)
	}
	r.setStep(model.StepShardingFinal)
	if err := r.checkCounts(r.Table); err != nil {
		time.Sleep(5 * time.Second)
		if err := r.checkCounts(r.Table); err != nil {
			return err
		}
	}
	if !r.SaveTemps {
		r.setStep(model.StepShardingClean)
		log.Logger.Info("[rebalance] STEP Cleanup")
		r.dropTmpTable()
	}
	log.Logger.Infof("[rebalance] DONE, Total counts: %d, Elapsed: %v sec", r.OriCount, time.Since(start).Seconds())
	return nil
}

// createTemporaryTable creates a tmp table on every shard mirroring the source
// table's engine_full, with the zookeeper path swapped so Replicated engines
// don't collide with the real table. Any pre-existing tmp table from a
// previous crashed run is dropped first.
func (r *Rebalancer) createTemporaryTable() error {
	r.dropTmpTable()
	create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE=%s",
		r.Database, r.TmpTable, r.Cluster, r.Database, r.Table, r.EngineFull)
	create = strings.ReplaceAll(create, fmt.Sprintf("/%s/", r.Table), fmt.Sprintf("/%s/", r.TmpTable)) // swap engine zoo path
	log.Logger.Debug(create)
	conn := common.GetConnection(r.Hosts[0])
	if conn == nil {
		return fmt.Errorf("[rebalance]can't get connection: %s", r.Hosts[0])
	}
	if err := conn.Exec(create); err != nil {
		return errors.Wrap(err, r.Hosts[0])
	}
	return nil
}

// dropTmpTable removes the tmp table on the whole cluster (ON CLUSTER ... SYNC).
// Failure is logged but not propagated; cleanup is best-effort.
func (r *Rebalancer) dropTmpTable() {
	conn := common.GetConnection(r.Hosts[0])
	if conn == nil {
		log.Logger.Warnf("[%s] can't get connection for cleanup", r.Hosts[0])
		return
	}
	cleanSQL := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER `%s` SYNC", r.Database, r.TmpTable, r.Cluster)
	log.Logger.Debugf("[%s]%s", r.Hosts[0], cleanSQL)
	if err := conn.Exec(cleanSQL); err != nil {
		log.Logger.Warnf("drop table %s.%s failed: %v", r.Database, r.TmpTable, err)
	}
}

// checkCounts compares the cluster-wide row count of tableName against the
// pre-rebalance baseline. The threshold incorporates AllowLossRate already
// (OriCount was multiplied by 1-rate during init), so we just check >=.
//
// Note: writes occurring during rebalance can legitimately push the count
// higher than the baseline; that's why we accept >= rather than ==.
func (r *Rebalancer) checkCounts(tableName string) error {
	var query string
	if strings.Contains(r.Engine, "Replacing") {
		query = fmt.Sprintf("SELECT count() FROM (SELECT DISTINCT %s FROM cluster('%s', '%s.%s') FINAL)", strings.Join(r.SortingKey, ","), r.Cluster, r.Database, tableName)
	} else {
		query = fmt.Sprintf("SELECT count() FROM cluster('%s', '%s.%s')", r.Cluster, r.Database, tableName)
	}
	log.Logger.Debugf("query: %s", query)
	conn := common.GetConnection(r.Hosts[0])
	if conn == nil {
		return fmt.Errorf("[rebalance]can't get connection: %s", r.Hosts[0])
	}
	rows, err := conn.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	var count uint64
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return err
		}
	}
	log.Logger.Infof("table: %s.%s, count: %d", r.Database, tableName, count)
	if count < r.OriCount {
		return fmt.Errorf("table %s count %d is less than original: %d", tableName, count, r.OriCount)
	}
	return nil
}

// insertPlan moves data from tmp back into the original table, partitioned
// across the N hosts by hash(key) % N == hostIdx. Each host pulls its own
// share via cluster() so the work is parallelized across the cluster.
//
// Note: lastError is written from multiple goroutines without synchronization,
// preserving the original behavior. Whichever assignment wins becomes the
// returned error; either way the caller learns the rebalance failed. A future
// PR can tighten this the way DoRebalanceByPart was tightened in Phase 1.
func (r *Rebalancer) insertPlan() error {
	var lastError error
	var wg sync.WaitGroup
	for idx, host := range r.Hosts {
		idx := idx
		host := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			conn := common.GetConnection(host)
			query := fmt.Sprintf(`SELECT distinct partition_id FROM cluster('%s', 'system.parts') WHERE database = '%s' AND table = '%s' AND active=1 ORDER BY partition_id DESC`, r.Cluster, r.Database, r.TmpTable)
			log.Logger.Debugf("[%s]%s", host, query)
			rows, err := conn.Query(query)
			if err != nil {
				lastError = errors.Wrap(err, host)
				return
			}
			partitions := make([]string, 0)
			for rows.Next() {
				var partitionID string
				if err := rows.Scan(&partitionID); err != nil {
					lastError = errors.Wrap(err, host)
					return
				}
				partitions = append(partitions, partitionID)
			}
			rows.Close()
			log.Logger.Debugf("host:[%s], parts: %v", host, partitions)

			for i, partition := range partitions {
				q := fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT * FROM cluster('%s', '%s.%s') WHERE _partition_id = '%s' AND %s %% %d = %d SETTINGS insert_deduplicate=false,max_execution_time=0,max_insert_threads=8",
					r.Database, r.Table, r.Cluster, r.Database, r.TmpTable, partition, ShardingFunc(r.Shardingkey), len(r.Hosts), idx)
				log.Logger.Debugf("[%s](%d/%d) %s", host, i+1, len(partitions), q)
				if err := conn.Exec(q); err != nil {
					lastError = errors.Wrap(err, host)
					return
				}
			}
		})
	}
	wg.Wait()
	return lastError
}

// moveBackup snapshots the original table into the tmp table by moving every
// partition (metadata-level, no data copy). After this step the original
// table is empty on every host and the tmp table holds the full dataset
// pending re-insertion.
//
// See insertPlan for a note on the racy lastError pattern preserved here.
func (r *Rebalancer) moveBackup() error {
	var wg sync.WaitGroup
	var lastError error
	for _, host := range r.Hosts {
		host := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			conn := common.GetConnection(host)
			query := fmt.Sprintf(`SELECT distinct partition_id FROM system.parts WHERE database = '%s' AND table = '%s' AND active=1 order by partition_id`, r.Database, r.Table)
			log.Logger.Debugf("[%s]%s", host, query)
			rows, err := conn.Query(query)
			if err != nil {
				lastError = errors.Wrap(err, host)
				return
			}
			partitions := make([]string, 0)
			for rows.Next() {
				var partitionID string
				if err := rows.Scan(&partitionID); err != nil {
					lastError = errors.Wrap(err, host)
					return
				}
				partitions = append(partitions, partitionID)
			}
			rows.Close()
			log.Logger.Debugf("host:[%s], partitions: %v", host, partitions)

			for idx, partition := range partitions {
				q := fmt.Sprintf("ALTER TABLE `%s`.`%s` MOVE PARTITION ID '%s' TO TABLE `%s`.`%s`", r.Database, r.Table, partition, r.Database, r.TmpTable)
				log.Logger.Debugf("[%s](%d/%d) %s", host, idx+1, len(partitions), q)
				if err := conn.Exec(q); err != nil {
					lastError = errors.Wrap(err, host)
					return
				}
			}
		})
	}
	wg.Wait()
	return lastError
}
