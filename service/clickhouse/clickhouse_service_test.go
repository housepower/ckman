package clickhouse

import (
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetReplicaZkPath(t *testing.T) {
	log.InitLoggerConsole()
	conf := &model.CKManClickHouseConfig{
		Hosts:    []string{"192.168.101.40", "192.168.101.41", "192.168.101.42"},
		Port:     9000,
		User:     "default",
		Password: "",
		Cluster:  "test",
		Shards: []model.CkShard{
			{
				[]model.CkReplica{
					{
						Ip: "192.168.101.40",
					},
					{
						Ip: "192.168.101.41",
					},
				},
			},
			{
				[]model.CkReplica{
					{
						Ip: "192.168.101.42",
					},
				},
			},
		},
	}
	err := GetReplicaZkPath(conf)
	assert.Nil(t, err)
	log.Logger.Infof("paths: %v", conf.ZooPath)
}
