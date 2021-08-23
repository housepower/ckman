package ckconfig

import (
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

func GenerateMetrikaXML(filename string, conf *model.CkDeployConfig)(string, error){
	xml := common.NewXmlFile(filename)
	xml.XMLBegin("yandex", 0)
	xml.XMLAppend(GenZookeeperMetrika(conf))
	xml.XMLBegin("remote_servers", 1)
	xml.XMLAppend(GenLocalMetrika(conf))
	xml.XMLEnd("remote_servers", 1)
	xml.XMLEnd("yandex", 0)
	err := xml.XMLDump()
	if err != nil {
		return "", err
	}
	return filename, nil
}

func GenerateMetrikaXMLwithLogic(filename string, conf *model.CkDeployConfig, logicMrtrika string)(string, error){
	xml := common.NewXmlFile(filename)
	xml.XMLBegin("yandex", 0)
	xml.XMLAppend(GenZookeeperMetrika(conf))
	xml.XMLBegin("remote_servers", 1)
	xml.XMLAppend(GenLocalMetrika(conf))
	xml.XMLAppend(logicMrtrika)
	xml.XMLEnd("remote_servers", 1)
	xml.XMLEnd("yandex", 0)
	err := xml.XMLDump()
	if err != nil {
		return "", err
	}
	return filename, nil
}

func GenZookeeperMetrika( conf *model.CkDeployConfig) string {
	xml := common.NewXmlFile("")
	xml.XMLBegin("zookeeper", 1)
	for index, zk := range conf.ZkNodes {
		xml.XMLBeginwithAttr("node",  []common.XMLAttr{{Key:"index", Value:index+1}}, 2)
		xml.XMLWrite("host", zk, 3)
		xml.XMLWrite("port", conf.ZkPort, 3)
		xml.XMLEnd("node", 2)
	}
	xml.XMLEnd("zookeeper", 1)
	return xml.GetContext()
}

func GenLocalMetrika(conf *model.CkDeployConfig)string {
	xml := common.NewXmlFile("")
	xml.XMLBegin(conf.ClusterName, 2)
	for _, shard := range conf.Shards {
		xml.XMLBegin("shard", 3)
		xml.XMLWrite("internal_replication", conf.IsReplica, 4)
		for _, replica := range shard.Replicas {
			xml.XMLBegin("replica", 4)
			xml.XMLWrite("host", replica.HostName, 5)
			xml.XMLWrite("port", conf.CkTcpPort, 5)
			xml.XMLEnd("replica", 4)
		}
		xml.XMLEnd("shard", 3)
	}
	xml.XMLEnd(conf.ClusterName, 2)
	return xml.GetContext()
}

