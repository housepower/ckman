package discovery

import (
	"reflect"
	"testing"

	"github.com/housepower/ckman/config"
)

func TestNoneBackends(t *testing.T) {
	var cfg config.CKManConfig
	cfg.Registry.Type = config.DISCOVERY_NONE
	cfg.ConfigCenter.Type = config.DISCOVERY_NONE

	reg, err := NewRegistry(&cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var got []config.ClusterNode
	reg.OnChange(func(nodes []config.ClusterNode) { got = nodes })
	self := config.ClusterNode{Ip: "10.0.0.1", Port: 8808}
	if err := reg.Start(self); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []config.ClusterNode{self}) {
		t.Errorf("none registry members = %v, want [self]", got)
	}

	src, err := NewConfigSource(&cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if LoadInitial(src, &cfg) {
		t.Error("none config source should not change log config")
	}
}

func TestUnsupportedType(t *testing.T) {
	var cfg config.CKManConfig
	cfg.Registry.Type = "zk"
	cfg.ConfigCenter.Type = "etcd"
	if _, err := NewRegistry(&cfg, t.TempDir()); err == nil {
		t.Error("want error for unsupported registry type")
	}
	if _, err := NewConfigSource(&cfg, t.TempDir()); err == nil {
		t.Error("want error for unsupported config_center type")
	}
}

func TestBackendConfigValidation(t *testing.T) {
	newCfg := func() config.CKManConfig {
		var c config.CKManConfig
		c.Consul = config.CKManConsulConfig{
			Address: "127.0.0.1:8500", Scheme: "http",
			CheckInterval: "10s", CheckTTL: "30s", DeregisterAfter: "1m", WaitTime: "30s",
			KVKey: "ckman/ckman.hjson",
		}
		c.Apollo = config.CKManApolloConfig{Meta: "http://127.0.0.1:8080", AppId: "ckman", Cluster: "default", Namespace: "ckman.json"}
		return c
	}

	c := newCfg()
	c.Registry.Type = config.DISCOVERY_CONSUL
	if _, err := NewRegistry(&c, t.TempDir()); err != nil {
		t.Errorf("consul registry: %v", err)
	}
	c.Consul.CheckInterval = "30s" // 不小于 TTL
	if _, err := NewRegistry(&c, t.TempDir()); err == nil {
		t.Error("check_interval >= check_ttl 应报错")
	}

	c = newCfg()
	c.ConfigCenter.Type = config.DISCOVERY_CONSUL
	src, err := NewConfigSource(&c, t.TempDir())
	if err != nil {
		t.Errorf("consul config source: %v", err)
	} else {
		_ = src.Close()
	}
	c.Consul.KVKey = "ckman/config"
	if _, err := NewConfigSource(&c, t.TempDir()); err == nil {
		t.Error("kv_key 没有格式后缀应报错")
	}

	c = newCfg()
	c.ConfigCenter.Type = config.DISCOVERY_APOLLO
	if src, err := NewConfigSource(&c, t.TempDir()); err != nil {
		t.Errorf("apollo config source: %v", err)
	} else {
		_ = src.Close()
	}
	c.Apollo.Namespace = "application"
	if _, err := NewConfigSource(&c, t.TempDir()); err == nil {
		t.Error("properties namespace 应报错")
	}
}
