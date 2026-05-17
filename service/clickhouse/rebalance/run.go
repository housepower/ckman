package rebalance

import (
	"fmt"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

// Run executes a full cluster rebalance. For every requested (database, table)
// pair (after regex expansion via paddingKeys), it picks the appropriate
// strategy and runs it sequentially. exceptMaxShard, when true, first drains
// the last shard's data into the host with most free space and truncates the
// source, supporting cluster downsizing.
//
// onStep, if non-nil, is invoked at each top-level phase boundary so the
// caller can surface progress (the runner task handle wires this to update
// task.Step in the persistent layer). Pass nil if progress reporting is not
// needed.
//
// This is the public entry point invoked from the runner task handle. The
// thin wrapper at service/clickhouse.RebalanceCluster preserves the
// pre-refactor import path.
func Run(conf *model.CKManClickHouseConfig, rtables []model.RebalanceTables, exceptMaxShard bool, onStep func(model.Internationalization)) error {
	conf.Normalize() // default ports, mirror NewCkService's behavior
	conn, err := openControllerConn(conf)
	if err != nil {
		return err
	}

	// regex-expand the requested table list and validate sharding keys before
	// any data is moved
	rtables, err = paddingKeys(conn, conf.Cluster, rtables)
	if err != nil {
		return err
	}

	hosts, err := common.GetShardAvaliableHosts(conf)
	if err != nil {
		return err
	}
	emit(onStep, model.StepCheckTools)
	if err := checkBasicTools(conf, hosts, rtables); err != nil {
		return err
	}

	var exceptHost, target string
	if exceptMaxShard {
		exceptHost = hosts[len(hosts)-1]
		hosts = hosts[:len(hosts)-1]
		target, err = checkDiskSpace(hosts, exceptHost)
		if err != nil {
			return err
		}
	}

	log.Logger.Debugf("rebalance tables: %d, %#v", len(rtables), rtables)
	for _, rt := range rtables {
		if exceptMaxShard {
			emit(onStep, model.StepMoveExceptHost)
			if err := moveExceptToOthers(conf, exceptHost, target, rt.Database, rt.Table); err != nil {
				return err
			}
		}
		if err := runOneTable(conf, hosts, rt, conn, onStep); err != nil {
			return err
		}
	}
	return nil
}

// emit is the top-level (non-method) helper for Run/Info that don't have a
// *Rebalancer in scope yet. Mirrors Rebalancer.setStep semantics.
func emit(onStep func(model.Internationalization), step model.Internationalization) {
	if onStep != nil {
		onStep(step)
	}
}

// runOneTable selects and executes the strategy for a single table.
func runOneTable(conf *model.CKManClickHouseConfig, hosts []string, rt model.RebalanceTables, controllerConn *common.Conn, onStep func(model.Internationalization)) error {
	rebalancer := New(Config{
		Cluster:       conf.Cluster,
		Hosts:         hosts,
		Database:      rt.Database,
		Table:         rt.Table,
		TmpTable:      "tmp_" + rt.Table,
		DistTable:     rt.DistTable,
		DataDir:       conf.Path,
		OsUser:        conf.SshUser,
		OsPassword:    conf.SshPassword,
		OsPort:        conf.SshPort,
		ConnOpt:       conf.GetConnOption(),
		AllowLossRate: rt.AllowLossRate,
		SaveTemps:     rt.SaveTemps,
		IsReplica:     conf.IsReplica,
		// Shardingkey is harmless for ByPartition (ignored) and consumed by
		// ByShardingKey.Validate to look up the column's ClickHouse type.
		Shardingkey: rt,
		OnStep:      onStep,
	})
	defer rebalancer.Close()

	strategy := PickStrategy(rt)
	log.Logger.Infof("[rebalance]table %s.%s rebalance by %s", rt.Database, rt.Table, strategy.Name())

	if err := strategy.Validate(rebalancer, controllerConn); err != nil {
		return err
	}
	return strategy.Run(rebalancer)
}

// Info reports the current row/byte distribution for the requested tables,
// grouped per-table with per-shard breakdown plus table-wide summaries
// (engine, imbalance ratio, warnings) so the UI can render at-a-glance status
// without re-grouping rows client-side.
//
// One query per (table, shard) for the row/byte/partition stats, plus one
// query per table for engine — same O(tables × shards) order as the flat
// pre-2b-3 implementation.
func Info(conf *model.CKManClickHouseConfig, rtables []model.RebalanceTables) ([]model.TableRebalanceInfo, error) {
	conf.Normalize() // default ports, mirror NewCkService's behavior
	conn, err := openControllerConn(conf)
	if err != nil {
		return nil, err
	}
	rtables, err = paddingKeys(conn, conf.Cluster, rtables)
	if err != nil {
		return nil, err
	}

	hosts, err := common.GetShardAvaliableHosts(conf)
	if err != nil {
		return nil, err
	}

	infos := make([]model.TableRebalanceInfo, 0, len(rtables))
	for _, rt := range rtables {
		info, err := collectTableInfo(conn, hosts, rt)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// collectTableInfo gathers engine + per-shard footprint for one table and
// computes the table-wide imbalance ratio and warnings.
func collectTableInfo(ctrlConn *common.Conn, hosts []string, rt model.RebalanceTables) (model.TableRebalanceInfo, error) {
	out := model.TableRebalanceInfo{
		Database:  rt.Database,
		Table:     rt.Table,
		DistTable: rt.DistTable,
		Shards:    make([]model.ShardRebalanceInfo, 0, len(hosts)),
	}

	// Engine via control-plane conn (any host of the cluster works for system.tables).
	engineQuery := fmt.Sprintf("SELECT engine FROM system.tables WHERE database = '%s' AND table = '%s'", rt.Database, rt.Table)
	log.Logger.Debugf(engineQuery)
	if err := ctrlConn.QueryRow(engineQuery).Scan(&out.Engine); err != nil {
		return out, errors.Wrapf(err, "query %s failed", engineQuery)
	}

	// Per-shard footprint: rows, uncompressed bytes, compressed bytes, partition count.
	// coalesce(sum(...), 0) ensures empty tables (no active parts) return 0 rather
	// than NULL, which avoids driver-dependent NULL→uint64 scan behavior.
	shardQuery := fmt.Sprintf(`SELECT
  toUInt64(coalesce(sum(rows), 0)),
  toUInt64(coalesce(sum(data_uncompressed_bytes), 0)),
  toUInt64(coalesce(sum(data_compressed_bytes), 0)),
  uniqExact(partition)
FROM system.parts WHERE active AND database = '%s' AND table = '%s'`, rt.Database, rt.Table)
	log.Logger.Debugf(shardQuery)
	for idx, host := range hosts {
		hostConn := common.GetConnection(host)
		if hostConn == nil {
			return out, errors.Errorf("can't connect to %s", host)
		}
		var s model.ShardRebalanceInfo
		s.Host = host
		s.ShardNum = idx + 1
		if err := hostConn.QueryRow(shardQuery).Scan(&s.Rows, &s.Bytes, &s.CompressedBytes, &s.PartitionCount); err != nil {
			return out, errors.Wrapf(err, "query %s on %s failed", shardQuery, host)
		}
		out.Shards = append(out.Shards, s)
	}

	out.ImbalanceRatio = computeImbalance(out.Shards)
	out.Warnings = collectWarnings(out)
	return out, nil
}

// computeImbalance returns (max-avg)/avg across shards' row counts. Empty
// tables (avg == 0) report 0 — vacuously balanced. The chosen metric is the
// "extra load on the busiest shard relative to average", which is more
// discriminating in the typical slightly-imbalanced case than max/min.
func computeImbalance(shards []model.ShardRebalanceInfo) float64 {
	if len(shards) == 0 {
		return 0
	}
	var total, max uint64
	for _, s := range shards {
		total += s.Rows
		if s.Rows > max {
			max = s.Rows
		}
	}
	avg := float64(total) / float64(len(shards))
	if avg == 0 {
		return 0
	}
	return (float64(max) - avg) / avg
}

// collectWarnings produces UI hints about preconditions the strategies care
// about. partition-based rebalance needs enough partitions to actually move
// data around; an under-partitioned table is a footgun worth flagging early.
//
// Each warning is returned as an Internationalization pair (ZH + EN) so the
// frontend can render in the active locale without a string-key dictionary.
func collectWarnings(t model.TableRebalanceInfo) []model.Internationalization {
	var warns []model.Internationalization
	minParts := uint64(0)
	if len(t.Shards) > 0 {
		minParts = t.Shards[0].PartitionCount
		for _, s := range t.Shards[1:] {
			if s.PartitionCount < minParts {
				minParts = s.PartitionCount
			}
		}
	}
	if minParts <= 1 {
		warns = append(warns, model.Internationalization{
			ZH: fmt.Sprintf("最少分区的 shard 只有 %d 个分区，按分区均衡会无效", minParts),
			EN: fmt.Sprintf("only %d partition(s) on the least-partitioned shard; partition-based rebalance will be ineffective", minParts),
		})
	}
	return warns
}

// openControllerConn returns a pooled connection to some healthy host in the
// cluster, used as the "control plane" connection for metadata lookups
// (paddingKeys, sharding type, etc.) that don't need to fan out per shard.
// Mirrors the original NewCkService -> InitCkService behavior: hosts are
// shuffled and tried in order until one accepts a connection. This keeps the
// rebalance entry point tolerant of a single dead host.
func openControllerConn(conf *model.CKManClickHouseConfig) (*common.Conn, error) {
	if len(conf.Hosts) == 0 {
		return nil, fmt.Errorf("[rebalance]cluster has no hosts")
	}
	var lastErr error
	for _, host := range common.Shuffle(conf.Hosts) {
		if _, err := common.ConnectClickHouse(host, model.ClickHouseDefaultDB, conf.GetConnOption()); err != nil {
			lastErr = err
			continue
		}
		if conn := common.GetConnection(host); conn != nil {
			return conn, nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("[rebalance]no healthy controller host in %v", conf.Hosts)
	}
	return nil, lastErr
}
