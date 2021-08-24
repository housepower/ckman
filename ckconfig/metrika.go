package ckconfig

import (
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

func GenerateMetrikaXML(filename string, conf *model.CkDeployConfig)(string, error){
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

func GenerateMetrikaXMLwithLogic(filename string, conf *model.CkDeployConfig, logicMrtrika string)(string, error){
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

func GenZookeeperMetrika(indent int, conf *model.CkDeployConfig) string {
	xml := common.NewXmlFile("")
	xml.SetIndent(indent)
	xml.Begin("zookeeper")
	for index, zk := range conf.ZkNodes {
		xml.BeginwithAttr("node",  []common.XMLAttr{{Key: "index", Value:index+1}})
		xml.Write("host", zk)
		xml.Write("port", conf.ZkPort)
		xml.End("node")
	}
	xml.End("zookeeper")
	return xml.GetContext()
}

func GenLocalMetrika(indent int, conf *model.CkDeployConfig)string {
	xml := common.NewXmlFile("")
	xml.SetIndent(indent)
	xml.Begin(conf.ClusterName)
	for _, shard := range conf.Shards {
		xml.Begin("shard")
		xml.Write("internal_replication", conf.IsReplica)
		for _, replica := range shard.Replicas {
			xml.Begin("replica")
			xml.Write("host", replica.HostName)
			xml.Write("port", conf.CkTcpPort)
			xml.End("replica")
		}
		xml.End("shard")
	}
	xml.End(conf.ClusterName)
	return xml.GetContext()
}

