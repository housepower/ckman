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
	xml.XMLBegin("yandex", 0)
	xml.XMLBegin("macros", 1)
	xml.XMLWrite("cluster", conf.ClusterName, 2)
	xml.XMLWrite("shard", shardIndex, 2)
	xml.XMLWrite("replica", hostName, 2)
	xml.XMLEnd("macros", 1)
	xml.XMLEnd("yandex", 0)
	err := xml.XMLDump()
	if err != nil {
		return "", err
	}
	return filename, nil
}
