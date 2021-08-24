package ckconfig

import (
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

type HostInfo struct {
	MemoryTotal int
}

func GenerateUsersXML(filename string, conf *model.CkDeployConfig)(string, error){
	xml := common.NewXmlFile(filename)
	xml.Begin("yandex")
	xml.Begin("users")
	xml.Begin(conf.User)
	xml.Write("password", conf.Password)
	xml.Begin("networks")
	xml.Write("ip", "::/0")
	xml.End("networks")
	xml.Write("profile", "default")
	xml.Write("quota", "default")
	xml.End(conf.User)
	xml.End("users")
	xml.End("yandex")
	if err := xml.Dump(); err != nil {
		return filename, err
	}
	return filename, nil
}

func GenerateProfilesXML(filename string, hostinfo HostInfo)(string, error){
	xml := common.NewXmlFile(filename)
	xml.Begin("yandex")
	xml.Begin("profiles")
	xml.Begin("default")
	xml.Write("max_memory_usage", int64((hostinfo.MemoryTotal / 2) * 1e3))
	xml.Write("max_memory_usage_for_all_queries", int64(((hostinfo.MemoryTotal * 3) / 4) * 1e3))
	xml.Write("max_bytes_before_external_group_by", int64((hostinfo.MemoryTotal / 4) * 1e3))
	xml.Write("max_query_size",1073741824)
	xml.Write("distributed_aggregation_memory_efficient", 1)
	xml.Write("joined_subquery_requires_alias", 0)
	xml.Write("distributed_ddl_task_timeout", 15)
	xml.Comment("Use cache of uncompressed blocks of data. Meaningful only for processing many of very short queries.")
	xml.Write("use_uncompressed_cache", 0)
	xml.End("default")
	xml.End("profiles")
	xml.End("yandex")
	if err := xml.Dump(); err != nil {
		return filename, err
	}
	return filename, nil
}

