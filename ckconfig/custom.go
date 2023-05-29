package ckconfig

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
	"github.com/imdario/mergo"
)

func root(conf *model.CKManClickHouseConfig, ipv6Enable bool) map[string]interface{} {
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
	output["database_atomic_delay_before_drop_table_sec"] = 0
	userDirectories := make(map[string]interface{})
	userDirectories["local_directory"] = map[string]interface{}{
		"path": fmt.Sprintf("%sclickhouse/access/", conf.Path),
	}
	output["user_directories"] = userDirectories

	max_concurrent_queries := 100 // 100 is default
	if v, ok := conf.Expert["max_concurrent_queries"]; ok {
		max_concurrent_queries, _ = strconv.Atoi(v)
	}
	output["max_concurrent_queries"] = max_concurrent_queries
	// at least leave 10% to insert
	output["max_concurrent_select_queries"] = int(max_concurrent_queries * 9 / 10)
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

func logger(conf *model.CKManClickHouseConfig) map[string]interface{} {
	output := make(map[string]interface{})
	loggerMap := make(map[string]interface{})
	loggerMap["level"] = "debug"
	if !conf.NeedSudo {
		loggerMap["log"] = path.Join(conf.Cwd, "log", "clickhouse-server", "clickhouse-server.log")
		loggerMap["errorlog"] = path.Join(conf.Cwd, "log", "clickhouse-server", "clickhouse-server.err.log")
	}
	output["logger"] = loggerMap
	return output
}

func distributed_ddl(cluster string) map[string]interface{} {
	output := make(map[string]interface{})
	output["distributed_ddl"] = map[string]interface{}{
		"path": fmt.Sprintf("/clickhouse/task_queue/ddl/%s", cluster),
	}
	return output
}

func storage(storage *model.Storage) (map[string]interface{}, map[string]interface{}) {
	if storage == nil {
		return nil, nil
	}
	backups := make(map[string]interface{})
	output := make(map[string]interface{})
	storage_configuration := make(map[string]interface{})
	if len(storage.Disks) > 0 {
		disks := make(map[string]interface{})
		var backup_disks []string
		for _, disk := range storage.Disks {
			diskMapping := make(map[string]interface{})
			diskMapping["type"] = disk.Type
			if disk.AllowedBackup {
				backup_disks = append(backup_disks, disk.Name)
			}
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
				mergo.Merge(&diskMapping, expert(disk.DiskS3.Expert))
			}
			disks[disk.Name] = diskMapping
		}
		storage_configuration["disks"] = disks
		if len(backup_disks) > 0 {
			backups["allowed_disk"] = backup_disks
		}
	}
	if len(storage.Policies) > 0 {
		var policies []map[string]interface{}
		for _, policy := range storage.Policies {
			policyMapping := make(map[string]interface{})
			var volumes []map[string]interface{}
			for _, vol := range policy.Volumns {
				volume := map[string]interface{}{
					"disk":                     vol.Disks,
					"max_data_part_size_bytes": vol.MaxDataPartSizeBytes,
					"prefer_not_to_merge":      vol.PreferNotToMerge,
				}
				volumes = append(volumes, map[string]interface{}{
					vol.Name: volume,
				})
			}
			policyMapping["volumes"] = volumes
			policyMapping["move_factor"] = policy.MoveFactor
			policies = append(policies, map[string]interface{}{
				policy.Name: policyMapping,
			})
		}
		storage_configuration["policies"] = policies
	}
	output["storage_configuration"] = storage_configuration
	return output, map[string]interface{}{
		"backups": backups,
	}
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
	rootTag := "yandex"
	if common.CompareClickHouseVersion(conf.Version, "22.x") >= 0 {
		rootTag = "clickhouse"
	}
	custom := make(map[string]interface{})
	mergo.Merge(&custom, expert(conf.Expert)) //expert have the highest priority
	mergo.Merge(&custom, root(conf, ipv6Enable))
	mergo.Merge(&custom, logger(conf))
	mergo.Merge(&custom, system_log())
	mergo.Merge(&custom, distributed_ddl(conf.Cluster))
	mergo.Merge(&custom, prometheus())
	storage_configuration, backups := storage(conf.Storage)
	mergo.Merge(&custom, storage_configuration)
	mergo.Merge(&custom, backups)
	xml := common.NewXmlFile(filename)
	xml.Begin(rootTag)
	xml.Merge(custom)
	xml.End(rootTag)
	if err := xml.Dump(); err != nil {
		return filename, err
	}
	return filename, nil
}
