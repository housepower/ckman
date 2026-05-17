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

// Info reports current per-host row counts and data sizes for the requested
// tables, used by the UI to visualize imbalance before deciding to execute.
func Info(conf *model.CKManClickHouseConfig, rtables []model.RebalanceTables) ([]*model.RebalanceInfo, error) {
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

	var infos []*model.RebalanceInfo
	for _, rt := range rtables {
		query := fmt.Sprintf("SELECT sum(rows), formatReadableSize(sum(data_uncompressed_bytes)) FROM system.parts WHERE active AND database = '%s' AND table = '%s'",
			rt.Database, rt.Table)
		log.Logger.Debugf(query)
		for idx, host := range hosts {
			hostConn := common.GetConnection(host)
			if hostConn == nil {
				return nil, errors.Errorf("can't connect to %s", host)
			}
			var rows uint64
			var size string
			if err := hostConn.QueryRow(query).Scan(&rows, &size); err != nil {
				return nil, errors.Wrapf(err, "query %s failed", query)
			}
			infos = append(infos, &model.RebalanceInfo{
				Database: rt.Database,
				Table:    rt.Table,
				Host:     host,
				ShardNum: idx + 1,
				Rows:     rows,
				Size:     size,
			})
		}
	}
	return infos, nil
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
