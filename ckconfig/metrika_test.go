package ckconfig

import (
	"os"
	"strings"
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

func TestMetrikaSecure(t *testing.T) {
	conf := model.CKManClickHouseConfig{
		Cluster: "test",
		Shards: []model.CkShard{
			{[]model.CkReplica{{HostName: "ck01", Ip: "192.168.0.1"}}},
		},
		Port:      9440,
		Secure:    true,
		ZkNodes:   []string{"192.168.0.1"},
		ZkPort:    2181,
		IsReplica: true,
		Version:   "22.8.8.3",
		User:      "default",
		Password:  "123456",
	}
	_, err := GenerateMetrikaXML("metrika_secure_fake.xml", &conf)
	assert.Nil(t, err)

	data, err := os.ReadFile("metrika_secure_fake.xml")
	assert.Nil(t, err)
	content := string(data)
	assert.Contains(t, content, "<secure>1</secure>")
	assert.Contains(t, content, "<port>9440</port>")
}

func TestMetrikaSecureOff(t *testing.T) {
	conf := model.CKManClickHouseConfig{
		Cluster:   "test",
		Shards:    []model.CkShard{{[]model.CkReplica{{Ip: "192.168.0.1"}}}},
		Port:      9000,
		Secure:    false,
		ZkNodes:   []string{"192.168.0.1"},
		ZkPort:    2181,
		IsReplica: true,
		Version:   "22.8.8.3",
	}
	_, err := GenerateMetrikaXML("metrika_plain_fake.xml", &conf)
	assert.Nil(t, err)

	data, err := os.ReadFile("metrika_plain_fake.xml")
	assert.Nil(t, err)
	assert.NotContains(t, string(data), "<secure>")
}

func TestGenLogicMetrikaMixedSecure(t *testing.T) {
	// 明文集群切 TLS 的中间态：逻辑集群内一个加密一个不加密，
	// XML 必须仍能正确生成，各成员按各自 Secure 写
	clusters := []model.CKManClickHouseConfig{
		{
			Cluster:   "secure_one",
			Shards:    []model.CkShard{{[]model.CkReplica{{Ip: "192.168.0.1"}}}},
			Port:      9440,
			Secure:    true,
			IsReplica: true,
		},
		{
			Cluster:   "plain_one",
			Shards:    []model.CkShard{{[]model.CkReplica{{Ip: "192.168.0.2"}}}},
			Port:      9000,
			Secure:    false,
			IsReplica: true,
		},
	}
	content := GenLogicMetrika("logic_test", clusters, true)
	assert.Contains(t, content, "<host>192.168.0.1</host>")
	assert.Contains(t, content, "<host>192.168.0.2</host>")
	// 加密成员有 secure 标记，明文成员没有
	assert.Equal(t, 1, strings.Count(content, "<secure>1</secure>"))
}
