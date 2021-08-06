package deploy

import (
	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateStorageXML(t *testing.T) {
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
		Policies:[]model.Policy{
			{
				Name: "hdfs_only",
				Volumns:[]model.Volumn{{
					Name: "main",
					Disks: []string{"hdfs1"},
					MaxDataPartSizeBytes: &MaxDataPartSizeBytes,
				}},
				MoveFactor: &MoveFactor,
			},
			{
				Name: "default",
				Volumns: []model.Volumn{{
					Name: "local",
					Disks: []string{"ssd"},
				}},
			},
			{
				Name: "s3_only",
				Volumns: []model.Volumn{{
					Name: "replica",
					Disks: []string{"s3"},
				}},
			},
		},
	}

	_, err := GenerateStorageXML("storage.xml", storage)
	assert.Nil(t, err)
}


func TestGenerateMacrosXML(t *testing.T) {
	conf := &model.CkDeployConfig{
		ClusterName: "abc",
		Shards: []model.CkShard{
			{[]model.CkReplica{
				{Ip:"192.168.101.40", HostName: "vm10140"},
				{Ip:"192.168.101.41", HostName: "vm10141"},
			}},
			{[]model.CkReplica{
				{Ip:"192.168.101.42", HostName: "vm10142"},
				{Ip:"192.168.101.43", HostName: "vm10143"},
			}},
		},
	}
	_, err := GenerateMacrosXML("macros.xml", conf, "192.168.101.42")
	assert.Nil(t, err)
}

func TestGenerateMetrikaXML(t *testing.T) {
	conf := &model.CkDeployConfig{
		ClusterName: "abc",
		Shards: []model.CkShard{
			{[]model.CkReplica{
				{Ip:"192.168.101.40", HostName: "vm10140"},
				{Ip:"192.168.101.41", HostName: "vm10141"},
			}},
			{[]model.CkReplica{
				{Ip:"192.168.101.42", HostName: "vm10142"},
				{Ip:"192.168.101.43", HostName: "vm10143"},
			}},
		},
		ZkNodes:[]string{"192.168.101.40", "192.168.101.41", "192.168.101.42"},
		ZkPort: 2181,
		CkTcpPort: 9000,
		IsReplica: true,
	}
	_, err := GenerateMetrikaXML("metrika.xml", conf)
	assert.Nil(t, err)
}
