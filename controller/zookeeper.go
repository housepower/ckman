package controller

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/housepower/ckman/service/zookeeper"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type ZookeeperController struct {
}

func NewZookeeperController() *ZookeeperController {
	ck := &ZookeeperController{}
	return ck
}

// @Summary Get Zookeeper cluster status
// @Description Get Zookeeper cluster status
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":[{"host":"192.168.102.116","version":"3.6.2","server_state":"follower","peer_state":"following - broadcast","avg_latency":0.4929,"approximate_data_size":141979,"znode_count":926}]}"
// @Router /api/v1/zk/status/{clusterName} [get]
func (zk *ZookeeperController) GetStatus(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	zkList := make([]model.ZkStatusRsp, len(conf.ZkNodes))
	for index, node := range conf.ZkNodes {
		tmp := model.ZkStatusRsp{
			Host: node,
		}
		body, err := getZkStatus(node, conf.ZkStatusPort)
		if err != nil {
			model.WrapMsg(c, model.GET_ZK_STATUS_FAIL, fmt.Sprintf("get zookeeper node %s satus fail: %v", node, err))
			return
		}
		_ = json.Unmarshal(body, &tmp)
		tmp.Version = strings.Split(strings.Split(tmp.Version, ",")[0], "-")[0]
		zkList[index] = tmp
	}

	model.WrapMsg(c, model.SUCCESS, zkList)
}

func getZkStatus(host string, port int) ([]byte, error) {
	url := fmt.Sprintf("http://%s:%d/commands/mntr", host, port)
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return nil, errors.Errorf("%s", response.Status)
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// @Summary Get replicated table in  Zookeeper status
// @Description Get replicated table in Zookeeper status
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":{"header":[["vm101106","vm101108"],["vm102114","vm101110"],["vm102116","vm102115"]],"tables":[{"name":"sensor_dt_result_online","values":[["l1846","f1846"],["l1845","f1845"],["l1846","f1846"]]}]}}"
// @Router /api/v1/zk/replicated_table/{clusterName} [get]
func (zk *ZookeeperController) GetReplicatedTableStatus(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	zkService, err := zookeeper.GetZkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.GET_ZK_TABLE_STATUS_FAIL, fmt.Sprintf("get zookeeper service fail: %v", err))
		return
	}

	tables, err := zkService.GetReplicatedTableStatus(&conf)
	if err != nil {
		model.WrapMsg(c, model.GET_ZK_TABLE_STATUS_FAIL, err)
		return
	}

	header := make([][]string, len(conf.Shards))
	for shardIndex, shard := range conf.Shards {
		replicas := make([]string, len(shard.Replicas))
		for replicaIndex, replica := range shard.Replicas {
			replicas[replicaIndex] = replica.HostName
		}
		header[shardIndex] = replicas
	}
	resp := model.ZkReplicatedTableStatusRsp{
		Header: header,
		Tables: tables,
	}

	model.WrapMsg(c, model.SUCCESS, resp)
}
