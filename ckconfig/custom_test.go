package ckconfig

import (
	"os"
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
		Version:   "25.3.1.2703",
	}
	_, err := GenerateCustomXML("custom_fake.xml", conf, model.CkDeployExt{
		NumCPU:     48,
		Ipv6Enable: true,
	})
	assert.Nil(t, err)
}

func TestGenerateCustomXMLSecure(t *testing.T) {
	conf := &model.CKManClickHouseConfig{
		Cluster: "abc",
		Shards: []model.CkShard{
			{[]model.CkReplica{
				{Ip: "192.168.101.40", HostName: "vm10140"},
			}},
		},
		ZkNodes:   []string{"192.168.101.40"},
		ZkPort:    2181,
		Port:      9440,
		HttpPort:  8443,
		Secure:    true,
		IsReplica: true,
		Cwd:       "/home/eoi/clickhouse",
		NeedSudo:  false,
		Path:      "/data01/",
		Version:   "25.3.1.2703",
	}
	_, err := GenerateCustomXML("custom_secure_fake.xml", conf, model.CkDeployExt{
		NumCPU:     48,
		Ipv6Enable: false,
	})
	assert.Nil(t, err)

	data, err := os.ReadFile("custom_secure_fake.xml")
	assert.Nil(t, err)
	content := string(data)

	// 密文端口开启
	assert.Contains(t, content, "<tcp_port_secure>9440</tcp_port_secure>")
	assert.Contains(t, content, "<https_port>8443</https_port>")
	// 明文端口关闭
	assert.Contains(t, content, `<tcp_port remove="remove">`)
	assert.Contains(t, content, `<http_port remove="remove">`)
	// 不能再出现明文端口的实际值
	assert.NotContains(t, content, "<tcp_port>9440</tcp_port>")
	assert.NotContains(t, content, "<http_port>8443</http_port>")

	// 服务端证书
	assert.Contains(t, content, "<certificateFile>/home/eoi/clickhouse/etc/clickhouse-server/certs/server.crt</certificateFile>")
	assert.Contains(t, content, "<privateKeyFile>/home/eoi/clickhouse/etc/clickhouse-server/certs/server.key</privateKeyFile>")
	assert.Contains(t, content, "<verificationMode>none</verificationMode>")
	// client 段必须存在：分布式查询时本节点作为客户端连兄弟节点，
	// 自签证书下缺 AcceptCertificateHandler 会直接握手失败
	assert.Contains(t, content, "<name>AcceptCertificateHandler</name>")
}

func TestGenerateCustomXMLSecureNeedSudo(t *testing.T) {
	conf := &model.CKManClickHouseConfig{
		Cluster:  "abc",
		Shards:   []model.CkShard{{[]model.CkReplica{{Ip: "192.168.101.40"}}}},
		Port:     9440,
		HttpPort: 8443,
		Secure:   true,
		NeedSudo: true,
		Path:     "/data01/",
		Version:  "25.3.1.2703",
	}
	_, err := GenerateCustomXML("custom_secure_sudo_fake.xml", conf, model.CkDeployExt{NumCPU: 48})
	assert.Nil(t, err)

	data, err := os.ReadFile("custom_secure_sudo_fake.xml")
	assert.Nil(t, err)
	assert.Contains(t, string(data), "<certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>")
}

func TestGenerateCustomXMLPlaintextUnchanged(t *testing.T) {
	conf := &model.CKManClickHouseConfig{
		Cluster:  "abc",
		Shards:   []model.CkShard{{[]model.CkReplica{{Ip: "192.168.101.40"}}}},
		Port:     9000,
		HttpPort: 8123,
		Secure:   false,
		NeedSudo: true,
		Path:     "/data01/",
		Version:  "25.3.1.2703",
	}
	_, err := GenerateCustomXML("custom_plain_fake.xml", conf, model.CkDeployExt{NumCPU: 48})
	assert.Nil(t, err)

	data, err := os.ReadFile("custom_plain_fake.xml")
	assert.Nil(t, err)
	content := string(data)
	assert.Contains(t, content, "<tcp_port>9000</tcp_port>")
	assert.Contains(t, content, "<http_port>8123</http_port>")
	assert.NotContains(t, content, "openSSL")
	assert.NotContains(t, content, "remove")
}
