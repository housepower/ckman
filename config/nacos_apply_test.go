package config

import (
	"reflect"
	"testing"
)

func TestParseRemote_Hjson(t *testing.T) {
	t.Cleanup(ResetForTest)
	data := []byte(`{
  server: {
    session_timeout: 7200
  }
  log: {
    level: "DEBUG"
  }
}`)
	got, err := parseRemote(data, ".hjson")
	if err != nil {
		t.Fatalf("parseRemote returned error: %v", err)
	}
	if got.Server.SessionTimeout != 7200 {
		t.Errorf("SessionTimeout=%d, want 7200", got.Server.SessionTimeout)
	}
	if got.Log.Level != "DEBUG" {
		t.Errorf("Log.Level=%q, want DEBUG", got.Log.Level)
	}
}

func TestParseRemote_Yaml(t *testing.T) {
	t.Cleanup(ResetForTest)
	data := []byte("server:\n  session_timeout: 7200\nlog:\n  level: DEBUG\n")
	got, err := parseRemote(data, ".yaml")
	if err != nil {
		t.Fatalf("parseRemote returned error: %v", err)
	}
	if got.Server.SessionTimeout != 7200 {
		t.Errorf("SessionTimeout=%d, want 7200", got.Server.SessionTimeout)
	}
	if got.Log.Level != "DEBUG" {
		t.Errorf("Log.Level=%q, want DEBUG", got.Log.Level)
	}
}

func TestParseRemote_Json(t *testing.T) {
	t.Cleanup(ResetForTest)
	data := []byte(`{"server":{"session_timeout":7200},"log":{"level":"DEBUG"}}`)
	got, err := parseRemote(data, ".json")
	if err != nil {
		t.Fatalf("parseRemote returned error: %v", err)
	}
	if got.Server.SessionTimeout != 7200 {
		t.Errorf("SessionTimeout=%d, want 7200", got.Server.SessionTimeout)
	}
}

func TestParseRemote_UnknownFormat(t *testing.T) {
	t.Cleanup(ResetForTest)
	_, err := parseRemote([]byte("server: {}"), ".xml")
	if err == nil {
		t.Fatal("expected error for unsupported format, got nil")
	}
}

func newLocal() CKManConfig {
	var c CKManConfig
	c.ConfigFile = "/etc/ckman/conf/ckman.hjson"
	c.Version = "2.6.0"
	c.Server.Ip = "10.0.0.1"
	c.Server.Port = 8808
	c.Server.Https = false
	c.Server.PersistentPolicy = "local"
	c.Server.SessionTimeout = 3600
	c.PersistentConfig = map[string]map[string]interface{}{
		"local": {"format": "json"},
	}
	c.Nacos.Enabled = true
	c.Nacos.Hosts = []string{"10.0.0.99"}
	c.Nacos.Port = 8848
	c.Log.Level = "INFO"
	c.Log.MaxCount = 5
	c.ClickHouse.MaxOpenConns = 10
	c.Cron.Enabled = true
	c.Cron.SyncLogicSchema = "0 */5 * * * *"
	return c
}

func TestMergeFromNacos_NonBootstrapOverrides(t *testing.T) {
	t.Cleanup(ResetForTest)
	local := newLocal()
	remote := CKManConfig{
		Log:        CKManLogConfig{Level: "DEBUG", MaxCount: 9, MaxSize: 20, MaxAge: 7},
		Cron:       CronJob{Enabled: true, SyncLogicSchema: "0 0 * * * *"},
		ClickHouse: ClickHouseOpts{MaxOpenConns: 50, MaxIdleConns: 5, ConnMaxIdleTime: 30},
	}
	mergeFromNacos(&local, &remote)
	if local.Log.Level != "DEBUG" || local.Log.MaxCount != 9 {
		t.Errorf("Log not overridden: %+v", local.Log)
	}
	if local.Cron.SyncLogicSchema != "0 0 * * * *" {
		t.Errorf("Cron not overridden: %+v", local.Cron)
	}
	if local.ClickHouse.MaxOpenConns != 50 {
		t.Errorf("ClickHouse not overridden: %+v", local.ClickHouse)
	}
}

func TestMergeFromNacos_EmptyRemoteSectionsKeepLocal(t *testing.T) {
	t.Cleanup(ResetForTest)
	local := newLocal()
	snap := local
	remote := CKManConfig{}
	mergeFromNacos(&local, &remote)
	if !reflect.DeepEqual(local.Log, snap.Log) {
		t.Errorf("Log changed unexpectedly: got=%+v want=%+v", local.Log, snap.Log)
	}
	if !reflect.DeepEqual(local.Cron, snap.Cron) {
		t.Errorf("Cron changed unexpectedly: got=%+v want=%+v", local.Cron, snap.Cron)
	}
	if !reflect.DeepEqual(local.ClickHouse, snap.ClickHouse) {
		t.Errorf("ClickHouse changed unexpectedly")
	}
}

func TestMergeFromNacos_BootstrapNeverOverridden(t *testing.T) {
	t.Cleanup(ResetForTest)
	local := newLocal()
	remote := CKManConfig{
		Server: CKManServerConfig{
			Ip:               "1.2.3.4",
			Port:             9999,
			Https:            true,
			CertFile:         "/tmp/c",
			KeyFile:          "/tmp/k",
			PkgPath:          "/tmp/p",
			PersistentPolicy: "mysql",
		},
		PersistentConfig: map[string]map[string]interface{}{
			"mysql": {"host": "x"},
		},
		Nacos: CKManNacosConfig{Enabled: false, Password: "evil"},
	}
	mergeFromNacos(&local, &remote)
	if local.Server.Ip != "10.0.0.1" || local.Server.Port != 8808 || local.Server.Https {
		t.Errorf("Server bootstrap overridden: %+v", local.Server)
	}
	if local.Server.PersistentPolicy != "local" {
		t.Errorf("PersistentPolicy overridden: %s", local.Server.PersistentPolicy)
	}
	if _, ok := local.PersistentConfig["local"]; !ok {
		t.Errorf("PersistentConfig overridden: %+v", local.PersistentConfig)
	}
	if !local.Nacos.Enabled || local.Nacos.Password == "evil" {
		t.Errorf("Nacos section overridden: %+v", local.Nacos)
	}
}

func TestMergeFromNacos_PreservesConfigFileVersion(t *testing.T) {
	t.Cleanup(ResetForTest)
	local := newLocal()
	remote := CKManConfig{
		ConfigFile: "/wrong/path.hjson",
		Version:    "0.0.0",
		Log:        CKManLogConfig{Level: "DEBUG"},
	}
	mergeFromNacos(&local, &remote)
	if local.ConfigFile != "/etc/ckman/conf/ckman.hjson" {
		t.Errorf("ConfigFile overridden: %s", local.ConfigFile)
	}
	if local.Version != "2.6.0" {
		t.Errorf("Version overridden: %s", local.Version)
	}
}

func TestMergeFromNacos_ServerNonBootstrapSingleFieldOverride(t *testing.T) {
	t.Cleanup(ResetForTest)
	local := newLocal()
	remote := CKManConfig{
		Server: CKManServerConfig{
			SessionTimeout: 7200,
			SwaggerEnable:  true,
			Metric:         true,
			MetricPath:     "/m",
			Port:           9999,
		},
	}
	mergeFromNacos(&local, &remote)
	if local.Server.SessionTimeout != 7200 {
		t.Errorf("SessionTimeout not overridden: %d", local.Server.SessionTimeout)
	}
	if !local.Server.SwaggerEnable || !local.Server.Metric || local.Server.MetricPath != "/m" {
		t.Errorf("Server non-bootstrap fields not overridden: %+v", local.Server)
	}
	if local.Server.Port != 8808 {
		t.Errorf("Server.Port should remain 8808, got %d", local.Server.Port)
	}
}
