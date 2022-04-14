package ckconfig

import (
	"fmt"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
	"github.com/imdario/mergo"
	"strings"
)

func yandex(conf *model.CKManClickHouseConfig, ipv6Enable bool) map[string]interface{} {
	output := make(map[string]interface{})
	output["max_table_size_to_drop"] = 0
	output["max_table_size_to_drop"] = 0
	output["max_partition_size_to_drop"] = 0
	output["default_replica_path"] = "/clickhouse/tables/{cluster}/{database}/{table}/{shard}"
	output["default_replica_name"] = "{replica}"
	output["tcp_port"] = conf.Port
	if ipv6Enable {
		output["listen_host"] = "::"
	} else {
		output["listen_host"] = "0.0.0.0"
	}
	if !strings.HasSuffix(conf.Path, "/") {
		conf.Path += "/"
	}
	output["path"] = fmt.Sprintf("%sclickhouse/", conf.Path)
	output["tmp_path"] = fmt.Sprintf("%sclickhouse/tmp/", conf.Path)
	output["user_files_path"] = fmt.Sprintf("%sclickhouse/user_files/", conf.Path)
	output["access_control_path"] = fmt.Sprintf("%sclickhouse/access/", conf.Path)
	output["format_schema_path"] = fmt.Sprintf("%sclickhouse/format_schemas/", conf.Path)
	return output
}

func prometheus() map[string]interface{} {
	output := make(map[string]interface{})
	output["prometheus"] = map[string]interface{}{
		"endpoint":             "/metrics",
		"port":                 9363,
		"metrics":              true,
		"events":               true,
		"asynchronous_metrics": true,
		"status_info":          true,
	}
	return output
}

func system_log() map[string]interface{} {
	logLists := []string{
		"query_log", "trace_log", "query_thread_log", "query_views_log",
		"part_log", "metric_log", "asynchronous_metric_log",
	}
	output := make(map[string]interface{})
	for _, logTable := range logLists {
		output[logTable] = map[string]interface{}{
			"partition_by":                "toYYYYMMDD(event_date)",
			"ttl":                         "event_date + INTERVAL 30 DAY DELETE",
			"flush_interval_milliseconds": 30000,
		}
	}

	return output
}

func logger() map[string]interface{} {
	output := make(map[string]interface{})
	output["logger"] = map[string]interface{}{
		"level": "debug",
	}
	return output
}

func distributed_ddl(cluster string) map[string]interface{} {
	output := make(map[string]interface{})
	output["distributed_ddl"] = map[string]interface{}{
		"path": fmt.Sprintf("/clickhouse/task_queue/ddl/%s", cluster),
	}
	return output
}

func storage(storage *model.Storage) map[string]interface{} {
	if storage == nil {
		return nil
	}
	output := make(map[string]interface{})
	storage_configuration := make(map[string]interface{})
	if len(storage.Disks) > 0 {
		disks := make(map[string]interface{})
		for _, disk := range storage.Disks {
			diskMapping := make(map[string]interface{})
			diskMapping["type"] = disk.Type
			switch disk.Type {
			case "hdfs":
				diskMapping["endpoint"] = disk.DiskHdfs.Endpoint
			case "local":
				diskMapping["path"] = disk.DiskLocal.Path
				diskMapping["keep_free_space_bytes"] = disk.DiskLocal.KeepFreeSpaceBytes
			case "s3":
				diskMapping["endpoint"] = disk.DiskS3.Endpoint
				diskMapping["access_key_id"] = disk.DiskS3.AccessKeyID
				diskMapping["secret_access_key"] = disk.DiskS3.SecretAccessKey
				diskMapping["region"] = disk.DiskS3.Region
				mergo.Merge(&diskMapping, disk.DiskS3.Expert)
			}
			disks[disk.Name] = diskMapping
		}
		storage_configuration["disks"] = disks
	}
	if len(storage.Policies) > 0 {
		policies := make(map[string]interface{})
		for _, policy := range storage.Policies {
			policyMapping := make(map[string]interface{})
			volumes := make(map[string]interface{})
			for _, vol := range policy.Volumns {
				volumes[vol.Name] = map[string]interface{}{
					"disk":                     vol.Disks,
					"max_data_part_size_bytes": vol.MaxDataPartSizeBytes,
					"prefer_not_to_merge":      vol.PreferNotToMerge,
				}
			}
			policyMapping["volumes"] = volumes
			policyMapping["move_factor"] = policy.MoveFactor
			policies[policy.Name] = policyMapping
		}
		storage_configuration["policies"] = policies
	}
	output["storage_configuration"] = storage_configuration
	return output
}

func expert(exp map[string]string) map[string]interface{} {
	output := make(map[string]interface{})
	for k, v := range exp {
		output[k] = v
	}
	// convert a.b.c:d => {a:{b:{c:d}}},beacuse we need to merge config with others
	return common.ConvertMapping(output)
}

func GenerateCustomXML(filename string, conf *model.CKManClickHouseConfig, ipv6Enable bool) (string, error) {
	custom := make(map[string]interface{})
	mergo.Merge(&custom, expert(conf.Expert)) //expert have the highest priority
	mergo.Merge(&custom, yandex(conf, ipv6Enable))
	mergo.Merge(&custom, logger())
	mergo.Merge(&custom, system_log())
	mergo.Merge(&custom, distributed_ddl(conf.Cluster))
	mergo.Merge(&custom, prometheus())
	mergo.Merge(&custom, storage(conf.Storage))
	xml := common.NewXmlFile(filename)
	xml.Begin("yandex")
	xml.Merge(custom)
	xml.End("yandex")
	if err := xml.Dump(); err != nil {
		return filename, err
	}
	return filename, nil
}
