package prometheus

import (
	"fmt"

	"github.com/housepower/ckman/model"
)

const (
	ClickHouseMericPort = 9363
	ZookeeperMetricPort = 7000
	NodeExporterPort    = 9100
)

type Object struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

func GetObjects(clusters []model.CKManClickHouseConfig) map[string][]Object {
	objs := make(map[string][]Object)
	var clickhouse, zookeeper, node Object
	for _, conf := range clusters {
		for _, host := range conf.Hosts {
			clickhouse.Targets = append(clickhouse.Targets, fmt.Sprintf("%s:%d", host, ClickHouseMericPort))
			node.Targets = append(node.Targets, fmt.Sprintf("%s:%d", host, NodeExporterPort))
		}

		for _, host := range conf.ZkNodes {
			zookeeper.Targets = append(zookeeper.Targets, fmt.Sprintf("%s:%d", host, ZookeeperMetricPort))
		}
	}

	objs["clickhouse"] = []Object{clickhouse}
	objs["zookeeper"] = []Object{zookeeper}
	objs["node"] = []Object{node}
	return objs
}
