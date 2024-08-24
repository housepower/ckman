package cron

import (
	"errors"

	"github.com/go-zookeeper/zk"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/housepower/ckman/service/zookeeper"
)

func ClearZnodes() error {
	log.Logger.Debugf("clear znodes task triggered")
	clusters, err := repository.Ps.GetAllClusters()
	if err != nil {
		return err
	}
	for _, cluster := range clusters {
		ckService := clickhouse.NewCkService(&cluster)
		if err = ckService.InitCkService(); err != nil {
			log.Logger.Warnf("[%s]init clickhouse service failed: %v", cluster.Cluster, err)
			continue
		}
		nodes, port := zookeeper.GetZkInfo(&cluster)
		zkService, err := zookeeper.NewZkService(nodes, port)
		if err != nil {
			log.Logger.Warnf("can't create zookeeper instance:%v", err)
			continue
		}
		// remove block_numbers in zookeeper
		// znodes, err := GetBlockNumberZnodes(ckService)
		// if err != nil {
		// 	log.Logger.Warnf("[%s]remove block_number from zookeeper failed: %v", cluster.Cluster, err)
		// }

		// deleted, notexist := RemoveZnodes(zkService, znodes)
		// log.Logger.Warnf("[%s]remove [%d] block_number from zookeeper success, %d already deleted", cluster.Cluster, deleted, notexist)

		// remove replica_queue in zookeeper
		znodes, err := GetReplicaQueueZnodes(ckService)
		if err != nil {
			log.Logger.Infof("[%s]remove replica_queue from zookeeper failed: %v", cluster.Cluster, err)
		}
		deleted, notexist := RemoveZnodes(zkService, znodes)
		log.Logger.Infof("[%s]remove [%d] replica_queue from zookeeper success, %d already deleted", cluster.Cluster, deleted, notexist)

	}
	return nil
}

func RemoveZnodes(zkService *zookeeper.ZkService, znodes []string) (int, int) {
	var deleted, notexist int
	for _, znode := range znodes {
		err := zkService.DeleteAll(znode)
		if err != nil {
			if errors.Is(err, zk.ErrNoNode) {
				notexist++
			}
		} else {
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

func GetReplicaQueueZnodes(ckService *clickhouse.CkService) ([]string, error) {
	query := `SELECT DISTINCT concat(t0.replica_path, '/queue/', t1.node_name)
	FROM clusterAllReplicas('{cluster}', system.replicas) AS t0,
	(
		SELECT
			database,
			table,
			replica_name,
			node_name,
			create_time,
			last_exception_time,
			num_tries
		FROM clusterAllReplicas('{cluster}', system.replication_queue)
		WHERE (num_postponed > 0) AND (num_tries > 100)
	) AS t1
	WHERE (t0.database = t1.database) AND (t0.table = t1.table) AND (t0.replica_name = t1.replica_name)`
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
	log.Logger.Debugf("[%s]remove replica_queue from zookeeper: %v", ckService.Config.Cluster, len(znodes))
	return znodes, nil
}
