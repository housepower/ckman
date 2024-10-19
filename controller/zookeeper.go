package controller

import (
	"fmt"
	"strings"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/repository"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/housepower/ckman/service/zookeeper"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type ZookeeperController struct {
	Controller
}

func NewZookeeperController(wrapfunc Wrapfunc) *ZookeeperController {
	ck := &ZookeeperController{
		Controller: Controller{
			wrapfunc: wrapfunc,
		},
	}
	return ck
}

// @Summary 获取zookeeper状态
// @Description 访问8080端口，获取mntr信息
// @version 1.0
// @Security ApiKeyAuth
// @Tags zookeeper
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":[{\"host\":\"192.168.110.8\",\"version\":\"3.8.0\",\"server_state\":\"follower\",\"peer_state\":\"following - broadcast\",\"avg_latency\":0.1456,\"approximate_data_size\":1451273,\"znode_count\":6485,\"outstanding_requests\":0,\"watch_count\":146},{\"host\":\"192.168.110.12\",\"version\":\"3.8.0\",\"server_state\":\"leader\",\"peer_state\":\"leading - broadcast\",\"avg_latency\":0.1118,\"approximate_data_size\":1451273,\"znode_count\":6485,\"outstanding_requests\":0,\"watch_count\":1},{\"host\":\"192.168.110.16\",\"version\":\"3.8.0\",\"server_state\":\"follower\",\"peer_state\":\"following - broadcast\",\"avg_latency\":0.2062,\"approximate_data_size\":1451273,\"znode_count\":6485,\"outstanding_requests\":0,\"watch_count\":58}]}"
// @Failure 200 {string} json "{"code":"5202","msg":"cluster not exist","data":null}"
// @Failure 200 {string} json "{"code":"5080","msg":"get zk status fail","data":null}"
// @Router /api/v2/zk/status/{clusterName} [get]
func (controller *ZookeeperController) GetStatus(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	nodes, port := zookeeper.GetZkInfo(&conf)

	zkList := make([]model.ZkStatusRsp, len(nodes))
	for index, node := range nodes {
		tmp := model.ZkStatusRsp{
			Host: node,
		}
		body, err := zookeeper.ZkMetric(node, port, "mntr")
		if err != nil {
			// controller.wrapfunc(c, model.E_ZOOKEEPER_ERROR, fmt.Sprintf("get zookeeper node %s satus fail: %v", node, err))
			// return
			log.Logger.Warnf("get zookeeper node %s satus fail: %v", node, err)
			tmp.Version = "unknown"
			tmp.ServerState = "unknown"
			tmp.PeerState = "offline"
			zkList[index] = tmp
			continue
		}
		_ = json.Unmarshal(body, &tmp)
		tmp.Version = strings.Split(strings.Split(tmp.Version, ",")[0], "-")[0]
		zkList[index] = tmp
	}

	controller.wrapfunc(c, model.E_SUCCESS, zkList)
}

// @Summary 获取复制表状态
// @Description 获取ReplicatedMergeTree表的log_pointer状态
// @version 1.0
// @Security ApiKeyAuth
// @Tags zookeeper
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5202","msg":"cluster not exist","data":null}"
// @Failure 200 {string} json "{"code":"5080","msg":"get zk status fail","data":null}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":{"header":[["vm101106","vm101108"],["vm102114","vm101110"],["vm102116","vm102115"]],"tables":[{"name":"sensor_dt_result_online","values":[["l1846","f1846"],["l1845","f1845"],["l1846","f1846"]]}]}}"
// @Router /api/v2/zk/replicated-table/{clusterName} [get]
func (controller *ZookeeperController) GetReplicatedTableStatus(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	tables, err := clickhouse.GetReplicatedTableStatus(&conf)
	if err != nil {
		controller.wrapfunc(c, model.E_ZOOKEEPER_ERROR, err)
		return
	}

	header := make([][]string, len(conf.Shards))
	for shardIndex, shard := range conf.Shards {
		replicas := make([]string, len(shard.Replicas))
		for replicaIndex, replica := range shard.Replicas {
			replicas[replicaIndex] = replica.Ip
		}
		header[shardIndex] = replicas
	}
	resp := model.ZkReplicatedTableStatusRsp{
		Header: header,
		Tables: tables,
	}

	controller.wrapfunc(c, model.E_SUCCESS, resp)
}
