package ckconfig

import (
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

func GenerateMacrosXML(filename string, conf *model.CkDeployConfig, host string)(string, error){
	shardIndex := 0
	hostName := ""
	for i, shard := range conf.Shards {
		for _, replica := range shard.Replicas {
			if host == replica.Ip {
				shardIndex = i + 1
				hostName = replica.HostName
				break
			}
		}
	}

	xml := common.NewXmlFile(filename)
	xml.Begin("yandex")
	xml.Begin("macros")
	xml.Write("cluster", conf.ClusterName)
	xml.Write("shard", shardIndex)
	xml.Write("replica", hostName)
	xml.End("macros")
	xml.End("yandex")
	err := xml.Dump()
	if err != nil {
		return "", err
	}
	return filename, nil
}
