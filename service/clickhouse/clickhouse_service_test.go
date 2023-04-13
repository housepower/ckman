package clickhouse

import (
	"testing"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
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

// func TestPaddingkeys(t *testing.T) {
// 	log.InitLoggerConsole()
// 	keys := []model.RebalanceShardingkey{
// 		{
// 			Database:    "default",
// 			Table:       "prom_metric",
// 			ShardingKey: "__series_id",
// 		},
// 	}

// 	conf := model.CKManClickHouseConfig{
// 		Cluster:   "abc",
// 		Port:      19000,
// 		IsReplica: true,
// 		Hosts:     []string{"192.168.110.6", "192.168.110.8"},
// 		User:      "default",
// 		Password:  "123456",
// 	}

// 	svc := NewCkService(&conf)
// 	err := svc.InitCkService()
// 	assert.Nil(t, err)

// 	keys, err = paddingKeys(keys, svc, false)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "dist_prom_metric", keys[0].DistTable)
// }
