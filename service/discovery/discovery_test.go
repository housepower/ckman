package discovery

import (
	"os"
	"reflect"
	"testing"

	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
)

func TestMain(m *testing.M) {
	log.InitLoggerConsole()
	os.Exit(m.Run())
}

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

type fakeSource struct {
	noopSource
	fn func([]byte, string)
}

func (f *fakeSource) Watch(fn func([]byte, string)) error { f.fn = fn; return nil }

// 远端配置被删除时（Nacos 删 dataId、Consul KV 写成空值）回调收到空内容，不应当作一份空配置应用。
func TestWatchSkipsEmptyContent(t *testing.T) {
	t.Cleanup(config.ResetForTest)
	config.ResetForTest()
	applied := 0
	config.RegisterApplier("count", func(old, new *config.CKManConfig) { applied++ })

	src := &fakeSource{}
	if err := Watch(src); err != nil {
		t.Fatal(err)
	}
	src.fn(nil, config.FORMAT_HJSON)
	src.fn([]byte("  \n"), config.FORMAT_HJSON)
	if applied != 0 {
		t.Fatalf("空内容不应触发 applier，applied=%d", applied)
	}
	src.fn([]byte(`{ log: { level: "DEBUG" } }`), config.FORMAT_HJSON)
	if applied != 1 {
		t.Fatalf("有效内容应触发一次 applier，applied=%d", applied)
	}
}
