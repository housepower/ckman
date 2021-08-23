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
	xml.XMLBegin("yandex", 0)
	xml.XMLBegin("users", 1)
	xml.XMLBegin(conf.User, 2)
	xml.XMLWrite("password", conf.Password, 3)
	xml.XMLBegin("networks", 3)
	xml.XMLWrite("ip", "::/0", 4)
	xml.XMLEnd("networks", 3)
	xml.XMLWrite("profile", "default", 3)
	xml.XMLWrite("quota", "default", 3)
	xml.XMLEnd(conf.User, 2)
	xml.XMLEnd("users", 1)
	xml.XMLEnd("yandex", 0)
	if err := xml.XMLDump(); err != nil {
		return filename, err
	}
	return filename, nil
}

func GenerateProfilesXML(filename string, hostinfo HostInfo)(string, error){
	xml := common.NewXmlFile(filename)
	xml.XMLBegin("yandex", 0)
	xml.XMLBegin("profiles", 1)
	xml.XMLBegin("default", 2)
	xml.XMLWrite("max_memory_usage", int64((hostinfo.MemoryTotal / 2) * 1e3), 3)
	xml.XMLWrite("max_memory_usage_for_all_queries", int64(((hostinfo.MemoryTotal * 3) / 4) * 1e3), 3)
	xml.XMLWrite("max_bytes_before_external_group_by", int64((hostinfo.MemoryTotal / 4) * 1e3), 3)
	xml.XMLWrite("max_query_size",1073741824,3)
	xml.XMLWrite("distributed_aggregation_memory_efficient", 1, 3)
	xml.XMLWrite("joined_subquery_requires_alias", 0, 3)
	xml.XMLWrite("distributed_ddl_task_timeout", 15, 3)
	xml.XMLComment("Use cache of uncompressed blocks of data. Meaningful only for processing many of very short queries.", 3)
	xml.XMLWrite("use_uncompressed_cache", 0, 3)
	xml.XMLEnd("default", 2)
	xml.XMLEnd("profiles", 1)
	xml.XMLEnd("yandex", 0)
	if err := xml.XMLDump(); err != nil {
		return filename, err
	}
	return filename, nil
}

