package ckconfig

import (
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

func GenerateHostXML(filename string, conf *model.CKManClickHouseConfig, host string)(string, error){
	shardIndex := 0
	for i, shard := range conf.Shards {
		for _, replica := range shard.Replicas {
			if host == replica.Ip {
				shardIndex = i + 1
				break
			}
		}
	}

	xml := common.NewXmlFile(filename)
	xml.Begin("yandex")
	xml.Comment("This xml file contains every node's special configuration self.")
	xml.Write("interserver_http_host", host)
	xml.Begin("macros")
	xml.Write("cluster", conf.Cluster)
	xml.Write("shard", shardIndex)
	xml.Write("replica", host)
	xml.End("macros")
	xml.End("yandex")
	err := xml.Dump()
	if err != nil {
		return "", err
	}
	return filename, nil
}
