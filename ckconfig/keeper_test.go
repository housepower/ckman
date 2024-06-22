package ckconfig

import (
	"testing"

	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
)

func TestGenerateKeeperXML(t *testing.T) {
	conf := model.CKManClickHouseConfig{
		KeeperConf: &model.KeeperConf{
			KeeperNodes:  []string{"192.168.101.102", "192.168.101.105", "192.168.101.107"},
			TcpPort:      9181,
			RaftPort:     9234,
			LogPath:      "/var/lib/clickhouse/coordination/log",
			SnapshotPath: "/var/lib/clickhouse/coordination/snapshots",
			Coordination: model.Coordination{
				OperationTimeoutMs: 10000,
				SessionTimeoutMs:   30000,
				ForceSync:          true,
			},
		},
	}
	_, err := GenerateKeeperXML("keeper_fake.xml", &conf, true, 2)
	assert.Nil(t, err)
}
