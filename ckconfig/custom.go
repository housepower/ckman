package ckconfig

import (
	"fmt"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

func yandex(conf *model.CkDeployConfig)string {
	//yandex
	xml := common.NewXmlFile("")
	xml.XMLWrite("max_table_size_to_drop", 0, 1)
	xml.XMLWrite("max_partition_size_to_drop", 0, 1)
	xml.XMLWrite("default_replica_path", "/clickhouse/tables/{cluster}/{shard}/{database}/{table}", 1)
	xml.XMLWrite("default_replica_name", "{replica}", 1)
	xml.XMLWrite("tcp_port", conf.CkTcpPort, 1)
	if conf.Ipv6Enable {
		xml.XMLWrite("listen_host", "::", 1)
	} else {
		xml.XMLWrite("listen_host", "0.0.0.0", 1)
	}
	xml.XMLWrite("path", fmt.Sprintf("%sclickhouse/", conf.Path), 1)
	xml.XMLWrite("tmp_path", fmt.Sprintf("%sclickhouse/tmp/", conf.Path), 1)
	xml.XMLWrite("user_files_path", fmt.Sprintf("%sclickhouse/user_files/", conf.Path), 1)
	xml.XMLWrite("access_control_path", fmt.Sprintf("%sclickhouse/access/", conf.Path), 1)
	xml.XMLWrite("format_schema_path", fmt.Sprintf("%sclickhouse/format_schemas/", conf.Path), 1)
	return xml.GetContext()
}

func prometheus()string{
	//prometheus
	xml := common.NewXmlFile("")
	xml.XMLBegin("prometheus", 1)
	xml.XMLWrite("endpoint", "/metrics", 2)
	xml.XMLWrite("port", 9363, 2)
	xml.XMLWrite("metrics", true, 2)
	xml.XMLWrite("events", true, 2)
	xml.XMLWrite("asynchronous_metrics", true, 2)
	xml.XMLWrite("status_info", true, 2)
	xml.XMLEnd("prometheus", 1)
	return xml.GetContext()
}

func system_log()string{
	xml := common.NewXmlFile("")
	logLists := []string{
		"query_log", "trace_log", "query_thread_log", "query_views_log",
		"part_log", "text_log", "metric_log", "asynchronous_metric_log",
	}

	for _, logTable := range logLists {
		xml.XMLBegin(logTable, 1)
		xml.XMLWrite("ttl", "event_date + INTERVAL 30 DAY DELETE", 2)
		xml.XMLWrite("flush_interval_milliseconds", 30000, 2)
		xml.XMLEnd(logTable, 1)
	}

	return xml.GetContext()
}

func logger()string{
	xml := common.NewXmlFile("")
	xml.XMLBegin("logger", 1)
	xml.XMLWrite("level", "debug", 2)
	xml.XMLEnd("logger", 1)
	return xml.GetContext()
}

func distributed_ddl(cluster string)string{
	xml := common.NewXmlFile("")
	xml.XMLBegin("distributed_ddl", 1)
	xml.XMLWrite("path", fmt.Sprintf("/clickhouse/task_queue/ddl/%s", cluster), 2)
	xml.XMLEnd("distributed_ddl", 1)
	return xml.GetContext()
}

func merge_tree(mergetree *model.MergeTreeConf)string {
	if mergetree == nil {
		return ""
	}
	xml := common.NewXmlFile("")
	xml.XMLBegin("merge_tree", 1)
	for k, v := range mergetree.Expert {
		xml.XMLWrite(k, v, 2)
	}
	xml.XMLEnd("merge_tree", 1)
	return xml.GetContext()
}

func storage(storage *model.Storage)string{
	if storage == nil {
		return ""
	}
	xml := common.NewXmlFile("")
	xml.XMLBegin("storage_configuration",1 )
	if len(storage.Disks) > 0 {
		xml.XMLBegin("disks", 2)
		for _, disk := range storage.Disks {
			xml.XMLBegin(disk.Name, 3)
			xml.XMLWrite("type", disk.Type, 4)
			switch disk.Type {
			case "hdfs":
				xml.XMLWrite("endpoint", disk.DiskHdfs.Endpoint, 4)
			case "local":
				xml.XMLWrite("path", disk.DiskLocal.Path, 4)
				xml.XMLWrite("keep_free_space_bytes", disk.DiskLocal.KeepFreeSpaceBytes, 4)
			case "s3":
				xml.XMLWrite("endpoint", disk.DiskS3.Endpoint, 4)
				xml.XMLWrite("access_key_id", disk.DiskS3.AccessKeyID, 4)
				xml.XMLWrite("secret_access_key", disk.DiskS3.SecretAccessKey, 4)
				xml.XMLWrite("region", disk.DiskS3.Region, 4)
				xml.XMLWrite("use_environment_credentials", disk.DiskS3.UseEnvironmentCredentials, 4)
				for k, v := range disk.DiskS3.Expert {
					xml.XMLWrite(k, v, 4)
				}
			default:
				return ""
			}
			xml.XMLEnd(disk.Name, 3)
		}
		xml.XMLEnd("disks", 2)
	}
	if len(storage.Policies) > 0 {
		xml.XMLBegin("policies", 2)
		for _, policy := range storage.Policies {
			xml.XMLBegin(policy.Name, 3)
			xml.XMLBegin("volumes", 4)
			for _, vol := range policy.Volumns {
				xml.XMLBegin(vol.Name, 5)
				for _, disk := range vol.Disks {
					xml.XMLWrite("disk", disk, 6)
				}
				xml.XMLWrite("max_data_part_size_bytes", vol.MaxDataPartSizeBytes, 6)
				xml.XMLWrite("prefer_not_to_merge", vol.PreferNotToMerge, 6)
				xml.XMLEnd(vol.Name, 5)
			}
			xml.XMLWrite("move_factor", policy.MoveFactor, 5)
			xml.XMLEnd("volumes", 4)
			xml.XMLEnd(policy.Name, 3)
		}
		xml.XMLEnd("policies", 2)
	}

	xml.XMLEnd("storage_configuration", 1)
	return xml.GetContext()
}

func GenerateCustomXML(filename string, conf *model.CkDeployConfig)(string,error) {
	xml := common.NewXmlFile(filename)
	xml.XMLBegin("yandex", 0)
	xml.XMLAppend(yandex(conf))
	xml.XMLAppend(logger())
	xml.XMLAppend(system_log())
	xml.XMLAppend(distributed_ddl(conf.ClusterName))
	xml.XMLAppend(prometheus())
	xml.XMLAppend(merge_tree(conf.MergeTreeConf))
	xml.XMLAppend(storage(conf.Storage))
	xml.XMLEnd("yandex", 0)
	if err := xml.XMLDump(); err != nil {
		return filename, err
	}
	return filename, nil
}
