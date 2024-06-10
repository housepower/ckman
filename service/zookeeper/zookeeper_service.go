package zookeeper

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/repository"

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
		return nil, errors.Wrap(err, "")
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
		conf, err := repository.Ps.GetClusterbyName(clusterName)
		if err == nil {
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
	if !conf.IsReplica {
		return nil, nil
	}
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
				// the clickhouse version 20.5.x already Remove leader election, refer to : allow multiple leaders https://github.com/ClickHouse/ClickHouse/pull/11639
				const featureVersion = "20.5.x"
				logPointer := ""
				if common.CompareClickHouseVersion(conf.Version, featureVersion) >= 0 {
					logPointer = "ML"
				} else {
					if leader == replica.Ip {
						logPointer = "L"
					} else {
						logPointer = "F"
					}
				}
				path = fmt.Sprintf("%s/replicas/%s/log_pointer", zooPath, replica.Ip)
				pointer, _, _ := z.Conn.Get(path)
				logPointer = logPointer + fmt.Sprintf("[%s]", pointer)
				replicas[replicaIndex] = logPointer
			}
		}
		tableIndex++
	}

	return tableStatus, nil
}

func (z *ZkService) DeleteAll(node string) (err error) {
	children, stat, err := z.Conn.Children(node)
	if errors.Is(err, zk.ErrNoNode) {
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

func (z *ZkService) CleanZoopath(conf model.CKManClickHouseConfig, clusterName, target string, dryrun bool) error {
	root := fmt.Sprintf("/clickhouse/tables/%s", clusterName)
	return clean(z, root, target, dryrun)
}

func clean(svr *ZkService, znode, target string, dryrun bool) error {
	_, stat, err := svr.Conn.Get(znode)
	if errors.Is(err, zk.ErrNoNode) {
		return nil
	} else if err != nil {
		return errors.Wrap(err, znode)
	}
	base := path.Base(znode)
	if base == target {
		if dryrun {
			fmt.Println(znode)
		} else {
			err = svr.DeleteAll(znode)
			if err != nil {
				fmt.Printf("znode %s delete failed: %v\n", znode, err)
			} else {
				fmt.Printf("znode %s delete success\n", znode)
			}
		}
	} else if stat.NumChildren > 0 {
		children, _, err := svr.Conn.Children(znode)
		if err != nil {
			return errors.Wrap(err, znode)
		}

		for _, child := range children {
			subnode := path.Join(znode, child)
			err := clean(svr, subnode, target, dryrun)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// func ZkMetric(host string, port int, metric string) ([]byte, error) {
// 	url := fmt.Sprintf("http://%s:%d/commands/%s", host, port, metric)
// 	request, err := http.NewRequest("GET", url, nil)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "")
// 	}

// 	client := &http.Client{}
// 	response, err := client.Do(request)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "")
// 	}
// 	defer response.Body.Close()

// 	if response.StatusCode != 200 {
// 		return nil, errors.Errorf("%s", response.Status)
// 	}

// 	body, err := io.ReadAll(response.Body)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "")
// 	}

// 	return body, nil
// }

func ZkMetric(host string, port int, metric string) ([]byte, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_, err = conn.Write([]byte(metric))
	if err != nil {
		return nil, err
	}
	var b []byte
	for {
		buf := [8192]byte{}
		n, err := conn.Read(buf[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if n == 0 {
			break
		}
		b = append(b, buf[:n]...)
	}
	resp := make(map[string]interface{})
	lines := strings.Split(string(b), "\n")
	re := regexp.MustCompile(`zk_(\w+)\s+(.*)`)
	for _, line := range lines {
		matches := re.FindStringSubmatch(line)
		if len(matches) >= 3 {
			resp[matches[1]] = matches[2]
		}
	}
	return json.Marshal(resp)
}

func GetZkClusterNodes(host string, port int) ([]string, error) {
	b, err := ZkMetric(host, port, "voting_view")
	if err != nil {
		return nil, err
	}
	zkCluster := make(map[string]interface{})
	err = json.Unmarshal(b, &zkCluster)
	if err != nil {
		return nil, err
	}
	var nodes []string
	for _, v := range zkCluster["current_config"].(map[string]interface{}) {
		for _, value := range v.(map[string]interface{})["server_addresses"].([]interface{}) {
			nodes = append(nodes, strings.Split(value.(string), ":")[0])
		}
	}
	return nodes, nil
}
