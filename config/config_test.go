package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestGetClusterPeers(t *testing.T) {
	saved, savedNodes := GlobalConfig, ClusterNodes
	defer func() { GlobalConfig, ClusterNodes = saved, savedNodes }()

	GlobalConfig.Server.Ip = "10.0.0.1"
	GlobalConfig.Server.Port = 8808
	ClusterNodes = []ClusterNode{
		{Ip: "10.0.0.1", Port: 8808}, // self
		{Ip: "10.0.0.2", Port: 8808}, // 同端口不同 IP
		{Ip: "10.0.0.1", Port: 8809}, // 同 IP 不同端口
		{Ip: "10.0.0.3", Port: 9000},
	}

	got := GetClusterPeers()
	want := []ClusterNode{
		{Ip: "10.0.0.2", Port: 8808},
		{Ip: "10.0.0.1", Port: 8809},
		{Ip: "10.0.0.3", Port: 9000},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetClusterPeers() = %v, want %v", got, want)
	}
}

func TestNormalizeDiscovery(t *testing.T) {
	cases := []struct {
		name         string
		setup        func(c *CKManConfig)
		wantRegistry string
		wantCenter   string
		wantService  string
		wantWarn     bool
		wantErr      bool
	}{
		{
			name:         "旧配置：nacos 关闭",
			setup:        func(c *CKManConfig) {},
			wantRegistry: DISCOVERY_NONE, wantCenter: DISCOVERY_NONE, wantService: "ckman",
		},
		{
			name:         "旧配置：nacos 开启，sync_config 默认 true",
			setup:        func(c *CKManConfig) { c.Nacos.Enabled = true },
			wantRegistry: DISCOVERY_NACOS, wantCenter: DISCOVERY_NACOS, wantService: "ckman",
		},
		{
			name:         "旧配置：nacos 开启，sync_config=false",
			setup:        func(c *CKManConfig) { c.Nacos.Enabled = true; c.Nacos.SyncConfig = false },
			wantRegistry: DISCOVERY_NACOS, wantCenter: DISCOVERY_NONE, wantService: "ckman",
		},
		{
			name:         "旧配置：服务名沿用 data_id",
			setup:        func(c *CKManConfig) { c.Nacos.Enabled = true; c.Nacos.DataID = "ckman-prod" },
			wantRegistry: DISCOVERY_NACOS, wantCenter: DISCOVERY_NACOS, wantService: "ckman-prod",
		},
		{
			name: "新配置：显式指定，nacos.enabled 未写",
			setup: func(c *CKManConfig) {
				c.Registry.Type = DISCOVERY_NACOS
				c.Registry.ServiceName = "svc"
			},
			wantRegistry: DISCOVERY_NACOS, wantCenter: DISCOVERY_NONE, wantService: "svc",
		},
		{
			name: "新旧矛盾：以新段为准并告警",
			setup: func(c *CKManConfig) {
				c.Nacos.Enabled = true
				c.Registry.Type = DISCOVERY_NONE
				c.ConfigCenter.Type = DISCOVERY_NONE
			},
			wantRegistry: DISCOVERY_NONE, wantCenter: DISCOVERY_NONE, wantService: "ckman", wantWarn: true,
		},
		{
			name:    "非法 registry 类型",
			setup:   func(c *CKManConfig) { c.Registry.Type = "zk" },
			wantErr: true,
		},
		{
			name:    "非法 config_center 类型",
			setup:   func(c *CKManConfig) { c.ConfigCenter.Type = "etcd" },
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c CKManConfig
			fillDefault(&c)
			tc.setup(&c)
			warns, err := normalizeDiscovery(&c)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got nil (cfg=%+v %+v)", c.Registry, c.ConfigCenter)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if c.Registry.Type != tc.wantRegistry || c.ConfigCenter.Type != tc.wantCenter {
				t.Errorf("types = (%q, %q), want (%q, %q)",
					c.Registry.Type, c.ConfigCenter.Type, tc.wantRegistry, tc.wantCenter)
			}
			if c.Registry.ServiceName != tc.wantService {
				t.Errorf("service_name = %q, want %q", c.Registry.ServiceName, tc.wantService)
			}
			if (len(warns) > 0) != tc.wantWarn {
				t.Errorf("warns = %v, wantWarn %v", warns, tc.wantWarn)
			}
		})
	}
}

func TestParseConfigFileNormalizes(t *testing.T) {
	saved := GlobalConfig
	defer func() { GlobalConfig = saved }()
	GlobalConfig = CKManConfig{}

	p := filepath.Join(t.TempDir(), "ckman.hjson")
	if err := os.WriteFile(p, []byte(`{ nacos: { enabled: true, hosts: ["127.0.0.1"], port: 8848 } }`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ParseConfigFile(p, ""); err != nil {
		t.Fatal(err)
	}
	if GlobalConfig.Registry.Type != DISCOVERY_NACOS || GlobalConfig.ConfigCenter.Type != DISCOVERY_NACOS {
		t.Fatalf("got registry=%q config_center=%q", GlobalConfig.Registry.Type, GlobalConfig.ConfigCenter.Type)
	}
}

func TestIsMasterNode(t *testing.T) {
	saved, savedNodes := GlobalConfig, ClusterNodes
	defer func() { GlobalConfig, ClusterNodes = saved, savedNodes }()

	GlobalConfig = CKManConfig{}
	GlobalConfig.Server.Ip, GlobalConfig.Server.Port = "10.0.0.2", 8808
	ClusterNodes = nil
	if !IsMasterNode() {
		t.Error("registry 未启用时应视为主节点")
	}

	GlobalConfig.Registry.Type = DISCOVERY_NACOS
	if IsMasterNode() {
		t.Error("registry 启用但成员列表为空时不应是主节点")
	}

	SetClusterNodes([]ClusterNode{{Ip: "10.0.0.3", Port: 8808}, {Ip: "10.0.0.1", Port: 8808}})
	if IsMasterNode() {
		t.Error("10.0.0.1 排序在前，应为主节点")
	}
	SetClusterNodes([]ClusterNode{{Ip: "10.0.0.3", Port: 8808}})
	if !IsMasterNode() {
		t.Error("并入 self 后 10.0.0.2 排序在前，应为主节点")
	}
}

func TestSetClusterNodes(t *testing.T) {
	saved, savedNodes := GlobalConfig, ClusterNodes
	defer func() { GlobalConfig, ClusterNodes = saved, savedNodes }()

	GlobalConfig.Server.Ip, GlobalConfig.Server.Port = "10.0.0.2", 8808
	SetClusterNodes([]ClusterNode{
		{Ip: "10.0.0.3", Port: 8808},
		{Ip: "10.0.0.1", Port: 8809},
		{Ip: "10.0.0.1", Port: 8808},
		{Ip: "10.0.0.3", Port: 8808}, // 重复
	})
	want := []ClusterNode{
		{Ip: "10.0.0.1", Port: 8808},
		{Ip: "10.0.0.1", Port: 8809},
		{Ip: "10.0.0.2", Port: 8808}, // self
		{Ip: "10.0.0.3", Port: 8808},
	}
	if !reflect.DeepEqual(ClusterNodes, want) {
		t.Fatalf("ClusterNodes = %v, want %v", ClusterNodes, want)
	}
}

func TestDiscoveryEndpoints(t *testing.T) {
	var c CKManConfig
	fillDefault(&c)
	c.Nacos.Hosts = []string{"10.0.0.1", " 10.0.0.2 "}
	c.Nacos.Port = 8848

	if got := DiscoveryEndpoints(&c); len(got) != 0 {
		t.Errorf("均为 none 时应无探测目标，got %v", got)
	}

	c.Registry.Type = DISCOVERY_NACOS
	c.ConfigCenter.Type = DISCOVERY_NACOS
	want := map[string][]string{DISCOVERY_NACOS: {"10.0.0.1:8848", "10.0.0.2:8848"}}
	if got := DiscoveryEndpoints(&c); !reflect.DeepEqual(got, want) {
		t.Errorf("DiscoveryEndpoints = %v, want %v", got, want)
	}
}
