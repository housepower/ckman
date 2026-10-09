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
