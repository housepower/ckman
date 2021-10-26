package ckconfig

import (
	"fmt"
	"strings"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

func yandex(indent int, conf *model.CkDeployConfig) string {
	//yandex
	xml := common.NewXmlFile("")
	xml.SetIndent(indent)
	xml.Write("max_table_size_to_drop", 0)
	xml.Write("max_partition_size_to_drop", 0)
	xml.Write("default_replica_path", "/clickhouse/tables/{cluster}/{database}/{table}/{shard}")
	xml.Write("default_replica_name", "{replica}")
	xml.Write("tcp_port", conf.CkTcpPort)
	if conf.Ipv6Enable {
		xml.Write("listen_host", "::")
	} else {
		xml.Write("listen_host", "0.0.0.0")
	}
	if !strings.HasSuffix(conf.Path, "/") {
		conf.Path += "/"
	}
	xml.Write("path", fmt.Sprintf("%sclickhouse/", conf.Path))
	xml.Write("tmp_path", fmt.Sprintf("%sclickhouse/tmp/", conf.Path))
	xml.Write("user_files_path", fmt.Sprintf("%sclickhouse/user_files/", conf.Path))
	xml.Write("access_control_path", fmt.Sprintf("%sclickhouse/access/", conf.Path))
	xml.Write("format_schema_path", fmt.Sprintf("%sclickhouse/format_schemas/", conf.Path))
	return xml.GetContext()
}

func prometheus(indent int) string {
	//prometheus
	xml := common.NewXmlFile("")
	xml.SetIndent(indent)
	xml.Begin("prometheus")
	xml.Write("endpoint", "/metrics")
	xml.Write("port", 9363)
	xml.Write("metrics", true)
	xml.Write("events", true)
	xml.Write("asynchronous_metrics", true)
	xml.Write("status_info", true)
	xml.End("prometheus")
	return xml.GetContext()
}

func system_log(indent int) string {
	xml := common.NewXmlFile("")
	xml.SetIndent(indent)
	logLists := []string{
		"query_log", "trace_log", "query_thread_log", "query_views_log",
		"part_log", "metric_log", "asynchronous_metric_log",
	}

	for _, logTable := range logLists {
		xml.Begin(logTable)
		xml.Write("partition_by", "toYYYYMMDD(event_date)")
		xml.Write("ttl", "event_date + INTERVAL 30 DAY DELETE")
		xml.Write("flush_interval_milliseconds", 30000)
		xml.End(logTable)
	}

	return xml.GetContext()
}

func logger(indent int) string {
	xml := common.NewXmlFile("")
	xml.SetIndent(indent)
	xml.Begin("logger")
	xml.Write("level", "debug")
	xml.End("logger")
	return xml.GetContext()
}

func distributed_ddl(indent int, cluster string) string {
	xml := common.NewXmlFile("")
	xml.SetIndent(indent)
	xml.Begin("distributed_ddl")
	xml.Write("path", fmt.Sprintf("/clickhouse/task_queue/ddl/%s", cluster))
	xml.End("distributed_ddl")
	return xml.GetContext()
}

func merge_tree(indent int, mergetree *model.MergeTreeConf) string {
	if mergetree == nil {
		return ""
	}
	xml := common.NewXmlFile("")
	xml.SetIndent(indent)
	xml.Begin("merge_tree")
	for k, v := range mergetree.Expert {
		xml.Write(k, v)
	}
	xml.End("merge_tree")
	return xml.GetContext()
}

func storage(indent int, storage *model.Storage) string {
	if storage == nil {
		return ""
	}
	xml := common.NewXmlFile("")
	xml.SetIndent(indent)
	xml.Begin("storage_configuration")
	if len(storage.Disks) > 0 {
		xml.Begin("disks")
		for _, disk := range storage.Disks {
			xml.Begin(disk.Name)
			xml.Write("type", disk.Type)
			switch disk.Type {
			case "hdfs":
				xml.Write("endpoint", disk.DiskHdfs.Endpoint)
			case "local":
				xml.Write("path", disk.DiskLocal.Path)
				xml.Write("keep_free_space_bytes", disk.DiskLocal.KeepFreeSpaceBytes)
			case "s3":
				xml.Write("endpoint", disk.DiskS3.Endpoint)
				xml.Write("access_key_id", disk.DiskS3.AccessKeyID)
				xml.Write("secret_access_key", disk.DiskS3.SecretAccessKey)
				xml.Write("region", disk.DiskS3.Region)
				xml.Write("use_environment_credentials", disk.DiskS3.UseEnvironmentCredentials)
				for k, v := range disk.DiskS3.Expert {
					xml.Write(k, v)
				}
			default:
				return ""
			}
			xml.End(disk.Name)
		}
		xml.End("disks")
	}
	if len(storage.Policies) > 0 {
		xml.Begin("policies")
		for _, policy := range storage.Policies {
			xml.Begin(policy.Name)
			xml.Begin("volumes")
			for _, vol := range policy.Volumns {
				xml.Begin(vol.Name)
				for _, disk := range vol.Disks {
					xml.Write("disk", disk)
				}
				xml.Write("max_data_part_size_bytes", vol.MaxDataPartSizeBytes)
				xml.Write("prefer_not_to_merge", vol.PreferNotToMerge)
				xml.End(vol.Name)
			}
			xml.Write("move_factor", policy.MoveFactor)
			xml.End("volumes")
			xml.End(policy.Name)
		}
		xml.End("policies")
	}

	xml.End("storage_configuration")
	return xml.GetContext()
}

func GenerateCustomXML(filename string, conf *model.CkDeployConfig) (string, error) {
	xml := common.NewXmlFile(filename)
	xml.Begin("yandex")
	indent := xml.GetIndent()
	xml.Append(yandex(indent, conf))
	xml.Append(logger(indent))
	xml.Append(system_log(indent))
	xml.Append(distributed_ddl(indent, conf.ClusterName))
	xml.Append(prometheus(indent))
	xml.Append(merge_tree(indent, conf.MergeTreeConf))
	xml.Append(storage(indent, conf.Storage))
	xml.End("yandex")
	if err := xml.Dump(); err != nil {
		return filename, err
	}
	return filename, nil
}
