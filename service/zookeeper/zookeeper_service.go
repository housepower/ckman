package zookeeper

import (
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
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
		conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
		if ok {
			service, err := NewZkService(conf.ZkNodes, conf.ZkPort)
			if err != nil {
				return nil, err
			}
			ZkServiceCache.SetDefault(clusterName, service)
			return service, nil
		} else {
			return nil, errors.Errorf("can't find cluster %s zookeeper service", clusterName)
		}
	}
}

func (z *ZkService) GetReplicatedTableStatus(conf *model.CKManClickHouseConfig) ([]model.ZkReplicatedTableStatus, error) {
	err := clickhouse.GetReplicaZkPath(conf)
	if err != nil {
		return nil, err
	}

	tableStatus := make([]model.ZkReplicatedTableStatus, len(conf.ZooPath))
	tableIndex := 0
	for key, value := range conf.ZooPath {
		status := model.ZkReplicatedTableStatus{
			Name: key,
		}
		shards := make([][]string, len(conf.Shards))
		status.Values = shards
		tableStatus[tableIndex] = status

		for shardIndex, shard := range conf.Shards {
			replicas := make([]string, len(shard.Replicas))
			shards[shardIndex] = replicas

			zooPath := strings.Replace(value, "{shard}", fmt.Sprintf("%d", shardIndex+1), -1)
			zooPath = strings.Replace(zooPath, "{cluster}", conf.Cluster, -1)

			path := fmt.Sprintf("%s/leader_election", zooPath)
			leaderElection, _, err := z.Conn.Children(path)
			if err != nil {
				continue
			}
			sort.Strings(leaderElection)
			// fix out of range cause panic issue
			if len(leaderElection) == 0 {
				continue
			}
			leaderBytes, _, _ := z.Conn.Get(fmt.Sprintf("%s/%s", path, leaderElection[0]))
			if len(leaderBytes) == 0 {
				continue
			}
			leader := strings.Split(string(leaderBytes), " ")[0]

			for replicaIndex, replica := range shard.Replicas {
				logPointer := ""
				if leader == replica.HostName {
					logPointer = "L"
				} else {
					logPointer = "F"
				}
				path = fmt.Sprintf("%s/replicas/%s/log_pointer", zooPath, replica.HostName)
				pointer, _, _ := z.Conn.Get(path)
				logPointer = logPointer + string(pointer)
				replicas[replicaIndex] = logPointer
			}
		}
		tableIndex++
	}

	return tableStatus, nil
}

func (z *ZkService) DeleteAll(node string) (err error) {
	children, stat, err := z.Conn.Children(node)
	if err == zk.ErrNoNode {
		return nil
	} else if err != nil {
		err = errors.Wrap(err, "delete zk node: ")
		return
	}

	for _, child := range children {
		if err = z.DeleteAll(path.Join(node, child)); err != nil {
			err = errors.Wrap(err, "delete zk node: ")
			return
		}
	}

	return z.Conn.Delete(node, stat.Version)
}

func (z *ZkService) DeletePathUntilNode(path, endNode string) error {
	ok, _, _ := z.Conn.Exists(path)
	if !ok {
		return nil
	}

	for {
		node := filepath.Base(path)
		parent := filepath.Dir(path)
		if node == endNode {
			return z.DeleteAll(path)
		}
		if parent == "/clickhouse/tables" {
			return nil
		}
		path = parent
	}
}
