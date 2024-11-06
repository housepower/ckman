package ckconfig

import (
	"testing"

	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
)

func TestMetrika(t *testing.T) {
	logic := "logic_test"
	conf := model.CKManClickHouseConfig{
		Cluster: "test",
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
		Port:         9000,
		ZkNodes:      []string{"192.168.0.1", "192.168.0.2", "192.168.0.3"},
		ZkPort:       2181,
		IsReplica:    true,
		LogicCluster: &logic,
		Version:      "22.8.8.3",
		User:         "default",
		Password:     "123456",
	}
	_, err := GenerateMetrikaXML("metrika_fake.xml", &conf)
	assert.Nil(t, err)

	_, err = GenerateMetrikaXMLwithLogic("metrika_logic_fake.xml", &conf, "")
	assert.Nil(t, err)

	conf.IsReplica = false
	conf.Version = "20.3.8.5"
	_, err = GenerateMetrikaXML("metrika_fake2.xml", &conf)
	assert.Nil(t, err)

	_, err = GenerateMetrikaXMLwithLogic("metrika_logic_fake2.xml", &conf, "")
	assert.Nil(t, err)
}
