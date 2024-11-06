package cron

import (
	"errors"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/housepower/ckman/service/zookeeper"
)

func ClearZnodes() error {
	if !config.IsMasterNode() {
		return nil
	}
	log.Logger.Debugf("clear znodes task triggered")
	clusters, err := repository.Ps.GetAllClusters()
	if err != nil {
		return err
	}
	for _, cluster := range clusters {
		zkService, err := zookeeper.GetZkService(cluster.Cluster)
		if err != nil {
			log.Logger.Warnf("can't create zookeeper instance:%v", err)
			continue
		}
		for _, host := range cluster.Hosts {
			start := time.Now()
			conn, err := common.ConnectClickHouse(host, model.ClickHouseDefaultDB, cluster.GetConnOption())
			if err != nil {
				log.Logger.Warnf("can't connect to clickhouse:%s", host)
				continue
			}

			znodes, err := GetReplicaQueueZnodes(host, conn)
			if err != nil {
				log.Logger.Infof("[%s][%s]remove replica_queue from zookeeper failed: %v", cluster.Cluster, host, err)
			}
			deleted, notexist := RemoveZnodes(zkService, znodes, false)
			log.Logger.Infof("[%s][%s]remove [%d] replica_queue from zookeeper success, [%d] already deleted,[%d] failed. elapsed: %v",
				cluster.Cluster, host, deleted, notexist, len(znodes)-deleted-notexist, time.Since(start))
		}
	}
	return nil
}

func RemoveZnodes(zkService *zookeeper.ZkService, znodes []string, debug bool) (int, int) {
	var deleted, notexist int
	for i, znode := range znodes {
		retried := false
	RETRY:
		err := zkService.Delete(znode)
		if err != nil {
			if errors.Is(err, zk.ErrNoNode) {
				if debug {
					log.Logger.Debugf("[%d][%s]zookeeper node not exist: %v", i, znode, err)
				}
				notexist++
			} else if errors.Is(err, zk.ErrConnectionClosed) || errors.Is(err, zk.ErrSessionExpired) {
				log.Logger.Debugf("zk service session expired, should reconnect")
				if !retried {
					log.Logger.Debugf("[%s]zookeeper session expired, try to reconnect", znode)
					zkService.Conn.Close()
					if err = zkService.Reconnect(); err == nil {
						log.Logger.Debugf("reconnect zookeeper successfully")
						retried = true
						goto RETRY
					}
				}
				log.Logger.Debugf("[%d][%s]reconnect zookeeper failed: %v", i, znode, err)
			} else {
				log.Logger.Debugf("[%d][%s]remove replica_queue from zookeeper failed: %v", i, znode, err)
			}
		} else {
			if debug {
				log.Logger.Debugf("[%d][%s]remove replica_queue from zookeeper success", i, znode)
			}
			deleted++
		}
	}
	return deleted, notexist
}

func GetBlockNumberZnodes(ckService *clickhouse.CkService) ([]string, error) {
	query := `SELECT distinct concat(zk.block_numbers_path, zk.partition_id) FROM
	(
		SELECT r.database, r.table, zk.block_numbers_path, zk.partition_id, p.partition_id
		FROM 
		(
			SELECT path as block_numbers_path, name as partition_id
			FROM system.zookeeper
			WHERE path IN (
				SELECT concat(zookeeper_path, '/block_numbers/') as block_numbers_path
				FROM clusterAllReplicas('{cluster}',system.replicas)
			)
		) as zk 
		LEFT JOIN 
		(
				SELECT database, table, concat(zookeeper_path, '/block_numbers/') as block_numbers_path
				FROM clusterAllReplicas('{cluster}',system.replicas)
		)
		as r ON (r.block_numbers_path = zk.block_numbers_path) 
		LEFT JOIN 
		(
			SELECT DISTINCT partition_id, database, table
			FROM clusterAllReplicas('{cluster}',system.parts)
		)
		as p ON (p.partition_id = zk.partition_id AND p.database = r.database AND p.table = r.table)
		WHERE p.partition_id = ''  AND zk.partition_id <> 'all'
		ORDER BY r.database, r.table, zk.block_numbers_path, zk.partition_id, p.partition_id
	) t
	`
	rows, err := ckService.Conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var znodes []string
	for rows.Next() {
		var path string
		if err = rows.Scan(&path); err != nil {
			return nil, err
		}
		znodes = append(znodes, path)

	}
	log.Logger.Debugf("[%s]remove block_number from zookeeper: %v", ckService.Config.Cluster, len(znodes))
	return znodes, nil
}

func GetReplicaQueueZnodes(host string, conn *common.Conn) ([]string, error) {
	query := `SELECT DISTINCT concat(t0.replica_path, '/queue/', t1.node_name)
	FROM system.replicas AS t0,
	(
		SELECT
			database,
			table,
			replica_name,
			node_name
		FROM system.replication_queue
		WHERE (num_postponed > 0) AND (num_tries >= 0 OR create_time > addSeconds(now(), -86400))
	) AS t1
	WHERE (t0.database = t1.database) AND (t0.table = t1.table) AND (t0.replica_name = t1.replica_name)`
	log.Logger.Debugf("[%s]query:%s", host, query)
	rows, err := conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var znodes []string
	var cnt int
	for rows.Next() {
		var path string
		if err = rows.Scan(&path); err != nil {
			return nil, err
		}
		znodes = append(znodes, path)
		cnt++
		if cnt >= 1<<18 {
			break
		}
	}
	log.Logger.Debugf("[%s]remove replica_queue from zookeeper: %v", host, len(znodes))
	return znodes, nil
}
