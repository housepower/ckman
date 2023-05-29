package ckconfig

import (
	"testing"

	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
)

func TestGenerateCustomXML(t *testing.T) {
	var KeepFreeSpaceBytes int64 = 10000
	var UseEnvironmentCredentials bool = false
	var MoveFactor float32 = 0.3
	var MaxDataPartSizeBytes int64 = 1000000
	storage := model.Storage{
		Disks: []model.Disk{
			{Name: "hdfs1", Type: "hdfs", AllowedBackup: true, DiskHdfs: &model.DiskHdfs{
				Endpoint: "localhost:8020/admin/abc",
			}},
			{Name: "ssd", Type: "local", AllowedBackup: false, DiskLocal: &model.DiskLocal{
				Path:               "/data01/clickhouse",
				KeepFreeSpaceBytes: &KeepFreeSpaceBytes,
			}},
			{Name: "s3", Type: "s3", AllowedBackup: true, DiskS3: &model.DiskS3{
				Endpoint:                  "localhost:1200/var/s3",
				AccessKeyID:               "123456",
				SecretAccessKey:           "654321",
				Region:                    nil,
				UseEnvironmentCredentials: &UseEnvironmentCredentials,
				Expert: map[string]string{
					"cache_path":        "/var/lib/clickhouse/disks/s3/cache/",
					"skip_access_check": "false",
					"retry_attempts":    "10",
				},
			}},
		},
		Policies: []model.Policy{
			{
				Name: "hdfs_only",
				Volumns: []model.Volumn{{
					Name:                 "main",
					Disks:                []string{"hdfs1"},
					MaxDataPartSizeBytes: &MaxDataPartSizeBytes,
				}},
				MoveFactor: &MoveFactor,
			},
			{
				Name: "default",
				Volumns: []model.Volumn{{
					Name:  "hot",
					Disks: []string{"ssd"},
				}, {
					Name:  "cold",
					Disks: []string{"ssd2"},
				}},
			},
			{
				Name: "s3_only",
				Volumns: []model.Volumn{{
					Name:  "replica",
					Disks: []string{"s3"},
				}},
			},
		},
	}
	expert := map[string]string{
		"logger/level":                                          "information",
		"merge_tree/max_suspicious_broken_parts":                "5",
		"merge_tree/parts_to_throw_insert":                      "300",
		"merge_tree/parts_to_delay_insert":                      "150",
		"merge_tree/inactive_parts_to_throw_insert":             "0",
		"merge_tree/inactive_parts_to_delay_insert":             "0",
		"merge_tree/max_delay_to_insert":                        "1",
		"merge_tree/max_parts_in_total":                         "100000",
		"merge_tree/replicated_deduplication_window":            "100",
		"merge_tree/non_replicated_deduplication_window":        "0",
		"merge_tree/replicated_deduplication_window_seconds":    "604800",
		"merge_tree/replicated_fetches_http_connection_timeout": "0",
		"merge_tree/replicated_fetches_http_send_timeout":       "0",
		"merge_tree/replicated_fetches_http_receive_timeout":    "0",
		"merge_tree/old_parts_lifetime":                         "480",
		"merge_tree/max_bytes_to_merge_at_max_space_in_pool":    "161061273600",
		"merge_tree/max_bytes_to_merge_at_min_space_in_pool":    "1048576",
		"merge_tree/merge_max_block_size":                       "8192",
		"merge_tree/max_part_loading_threads":                   "auto",
		"merge_tree/max_partitions_to_read":                     "-1",
		"merge_tree/allow_floating_point_partition_key":         "0",
		"merge_tree/check_sample_column_is_correct":             "true",
		"merge_tree/allow_remote_fs_zero_copy_replication":      "1",
		"storage_configuration/disks/ssd/keep_free_space_bytes": "1024",
	}

	conf := &model.CKManClickHouseConfig{
		Cluster: "abc",
		Shards: []model.CkShard{
			{[]model.CkReplica{
				{Ip: "192.168.101.40", HostName: "vm10140"},
				{Ip: "192.168.101.41", HostName: "vm10141"},
			}},
			{[]model.CkReplica{
				{Ip: "192.168.101.42", HostName: "vm10142"},
				{Ip: "192.168.101.43", HostName: "vm10143"},
			}},
		},
		ZkNodes:   []string{"192.168.101.40", "192.168.101.41", "192.168.101.42"},
		ZkPort:    2181,
		Port:      9000,
		IsReplica: true,
		Storage:   &storage,
		Expert:    expert,
		Cwd:       "/home/eoi/clickhouse",
		NeedSudo:  false,
		Path:      "/data01/",
		Version:   "22.3.3.44",
	}
	_, err := GenerateCustomXML("custom_fake.xml", conf, true)
	assert.Nil(t, err)
}
