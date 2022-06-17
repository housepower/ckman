package ckconfig

import (
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

func GenerateMetrikaXML(filename string, conf *model.CKManClickHouseConfig) (string, error) {
	xml := common.NewXmlFile(filename)
	xml.Begin("yandex")
	xml.Append(GenZookeeperMetrika(xml.GetIndent(), conf))
	xml.Begin("remote_servers")
	xml.Append(GenLocalMetrika(xml.GetIndent(), conf))
	xml.End("remote_servers")
	xml.End("yandex")
	err := xml.Dump()
	if err != nil {
		return "", err
	}
	return filename, nil
}

func GenerateMetrikaXMLwithLogic(filename string, conf *model.CKManClickHouseConfig, logicMrtrika string) (string, error) {
	xml := common.NewXmlFile(filename)
	xml.Begin("yandex")
	xml.Append(GenZookeeperMetrika(xml.GetIndent(), conf))
	xml.Begin("remote_servers")
	xml.Append(GenLocalMetrika(xml.GetIndent(), conf))
	xml.Append(logicMrtrika)
	xml.End("remote_servers")
	xml.End("yandex")
	err := xml.Dump()
	if err != nil {
		return "", err
	}
	return filename, nil
}

func GenZookeeperMetrika(indent int, conf *model.CKManClickHouseConfig) string {
	xml := common.NewXmlFile("")
	xml.SetIndent(indent)
	xml.Begin("zookeeper")
	for index, zk := range conf.ZkNodes {
		xml.BeginwithAttr("node", []common.XMLAttr{{Key: "index", Value: index + 1}})
		xml.Write("host", zk)
		xml.Write("port", conf.ZkPort)
		xml.End("node")
	}
	xml.End("zookeeper")
	return xml.GetContext()
}

func GenLocalMetrika(indent int, conf *model.CKManClickHouseConfig) string {
	xml := common.NewXmlFile("")
	xml.SetIndent(indent)
	xml.Begin(conf.Cluster)
	secret := true
	if common.CompareClickHouseVersion(conf.Version, "20.10.3.30") < 0 {
		secret = false
	}
	if secret {
		xml.Comment(`Inter-server per-cluster secret for Distributed queries
                 default: no secret (no authentication will be performed)

                 If set, then Distributed queries will be validated on shards, so at least:
                 - such cluster should exist on the shard,
                 - such cluster should have the same secret.

                 And also (and which is more important), the initial_user will
                 be used as current user for the query.

                 Right now the protocol is pretty simple and it only takes into account:
                 - cluster name
                 - query

                 Also it will be nice if the following will be implemented:
                 - source hostname (see interserver_http_host), but then it will depends from DNS,
                   it can use IP address instead, but then the you need to get correct on the initiator node.
                 - target hostname / ip address (same notes as for source hostname)
                 - time-based security tokens`)
		xml.Write("secret", "foo")
	}
	for _, shard := range conf.Shards {
		xml.Begin("shard")
		xml.Write("internal_replication", conf.IsReplica)
		for _, replica := range shard.Replicas {
			xml.Begin("replica")
			xml.Write("host", replica.Ip)
			xml.Write("port", conf.Port)
			if !secret {
				xml.Write("user", conf.User)
				xml.Write("password", conf.Password)
			}
			xml.End("replica")
		}
		xml.End("shard")
	}
	xml.End(conf.Cluster)
	return xml.GetContext()
}
