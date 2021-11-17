package ckconfig

import (
	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateCustomXML(t *testing.T) {
	var KeepFreeSpaceBytes int64 = 10000
	var UseEnvironmentCredentials bool = false
	var MoveFactor float32 = 0.3
	var MaxDataPartSizeBytes int64 = 1000000
	storage := model.Storage{
		Disks: []model.Disk{
			{Name: "hdfs1", Type: "hdfs", DiskHdfs: &model.DiskHdfs{
				Endpoint: "localhost:8020/admin/abc",
			}},
			{Name: "ssd", Type: "local", DiskLocal: &model.DiskLocal{
				Path:               "/data01/clickhouse",
				KeepFreeSpaceBytes: &KeepFreeSpaceBytes,
			}},
			{Name: "s3", Type: "s3", DiskS3: &model.DiskS3{
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
					Name:  "local",
					Disks: []string{"ssd"},
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
	mt := model.MergeTreeConf{
		Expert: map[string]string{
			"max_suspicious_broken_parts":                "5",
			"parts_to_throw_insert":                      "300",
			"parts_to_delay_insert":                      "150",
			"inactive_parts_to_throw_insert":             "0",
			"inactive_parts_to_delay_insert":             "0",
			"max_delay_to_insert":                        "1",
			"max_parts_in_total":                         "100000",
			"replicated_deduplication_window":            "100",
			"non_replicated_deduplication_window":        "0",
			"replicated_deduplication_window_seconds":    "604800",
			"replicated_fetches_http_connection_timeout": "0",
			"replicated_fetches_http_send_timeout":       "0",
			"replicated_fetches_http_receive_timeout":    "0",
			"old_parts_lifetime":                         "480",
			"max_bytes_to_merge_at_max_space_in_pool":    "161061273600",
			"max_bytes_to_merge_at_min_space_in_pool":    "1048576",
			"merge_max_block_size":                       "8192",
			"max_part_loading_threads":                   "auto",
			"max_partitions_to_read":                     "-1",
			"allow_floating_point_partition_key":         "0",
			"check_sample_column_is_correct":             "true",
			"allow_remote_fs_zero_copy_replication":      "1",
		},
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
		Port: 9000,
		IsReplica: true,
		Storage: &storage,
		MergeTreeConf:&mt,
	}
	_, err := GenerateCustomXML("custom.xml", conf, true)
	assert.Nil(t, err)
}
