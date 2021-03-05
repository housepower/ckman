package zookeeper

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"github.com/patrickmn/go-cache"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
	"sort"
	"strings"
	"time"
)

var ZkServiceCache *cache.Cache

type ZkService struct {
	ZkNodes []string
	ZkPort  int
	Conn    *zk.Conn
	Event   <-chan zk.Event
}

func NewZkService(nodes []string, port int) (*ZkService, error) {
	zkService := &ZkService{
		ZkNodes: nodes,
		ZkPort:  port,
	}

	servers := make([]string, len(nodes))
	for index, node := range nodes {
		servers[index] = fmt.Sprintf("%s:%d", node, port)
	}

	c, e, err := zk.Connect(servers, time.Second)
	if err != nil {
		return nil, err
	}

	zkService.Conn = c
	zkService.Event = e

	return zkService, nil
}

func GetZkService(clusterName string) (*ZkService, error) {
	zkService, ok := ZkServiceCache.Get(clusterName)
	if ok {
		return zkService.(*ZkService), nil
	} else {
		conf, ok := clickhouse.CkClusters.Load(clusterName)
		if ok {
			ckConfig := conf.(model.CKManClickHouseConfig)
			service, err := NewZkService(ckConfig.ZkNodes, ckConfig.ZkPort)
			if err != nil {
				return nil, err
			}
			ZkServiceCache.SetDefault(clusterName, service)
			return service, nil
		} else {
			return nil, fmt.Errorf("can't find cluster %s zookeeper service", clusterName)
		}
	}
}

func (z *ZkService) GetReplicatedTableStatus(conf *model.CKManClickHouseConfig) ([]model.ZkReplicatedTableStatus, error) {
	path := fmt.Sprintf("/clickhouse/tables/%s/1/%s", conf.Cluster, conf.DB)
	tables, _, err := z.Conn.Children(path)
	if err != nil {
		return nil, err
	}

	tableStatus := make([]model.ZkReplicatedTableStatus, len(tables))
	for tableIndex, tableName := range tables {
		status := model.ZkReplicatedTableStatus{
			Name: tableName,
		}
		shards := make([][]string, len(conf.Shards))
		status.Values = shards
		tableStatus[tableIndex] = status

		for shardIndex, shard := range conf.Shards {
			replicas := make([]string, len(shard.Replicas))
			shards[shardIndex] = replicas

			path = fmt.Sprintf("/clickhouse/tables/%s/%d/%s/%s/leader_election", conf.Cluster, shardIndex+1, conf.DB, tableName)
			leaderElection, _, err := z.Conn.Children(path)
			if err != nil {
				continue
			}
			sort.Strings(leaderElection)
			leaderBytes, _, _ := z.Conn.Get(fmt.Sprintf("%s/%s", path, leaderElection[0]))
			leader := strings.Split(string(leaderBytes), " ")[0]

			for replicaIndex, replica := range shard.Replicas {
				logPointer := ""
				if leader == replica.HostName {
					logPointer = "l"
				} else {
					logPointer = "f"
				}
				path = fmt.Sprintf("/clickhouse/tables/%s/%d/%s/%s/replicas/%s/log_pointer", conf.Cluster, shardIndex+1, conf.DB, tableName, replica.HostName)
				pointer, _, _ := z.Conn.Get(path)
				logPointer = logPointer + string(pointer)
				replicas[replicaIndex] = logPointer
			}
		}
	}

	return tableStatus, nil
}
