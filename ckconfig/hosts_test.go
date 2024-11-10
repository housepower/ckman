package ckconfig

import (
	"testing"

	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
)

func TestHosts(t *testing.T) {
	conf := model.CKManClickHouseConfig{
		Cluster: "test",
		Version: "23.8.9.54",
		Shards: []model.CkShard{
			{[]model.CkReplica{
				{HostName: "ck01", Ip: "192.168.0.1"},
				{HostName: "ck02", Ip: "192.168.0.2"},
			}},
			{[]model.CkReplica{
				{HostName: "ck03", Ip: "192.168.0.3"},
				{HostName: "ck04", Ip: "192.168.0.4"},
			}},
		},
	}
	_, err := GenerateHostXML("hosts_fake.xml", &conf, "192.168.0.3")
	assert.Nil(t, err)
}
