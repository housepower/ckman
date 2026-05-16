package config

import (
	"os"
	"reflect"
	"testing"

	"go.uber.org/zap"
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

func TestDiffBootstrap_DetectsAllFields(t *testing.T) {
	t.Cleanup(ResetForTest)
	local := newLocal()
	remote := newLocal()
	remote.Server.Ip = "x"
	remote.Server.Port = 1
	remote.Server.Https = true
	remote.Server.CertFile = "x"
	remote.Server.KeyFile = "x"
	remote.Server.PkgPath = "x"
	remote.Server.PersistentPolicy = "mysql"
	remote.PersistentConfig = map[string]map[string]interface{}{"mysql": {"x": 1}}
	remote.Nacos.Password = "new"

	diffs := diffBootstrap(&local, &remote)
	got := map[string]bool{}
	for _, d := range diffs {
		got[d.field] = true
	}
	expected := []string{
		"Server.Ip", "Server.Port", "Server.Https",
		"Server.CertFile", "Server.KeyFile", "Server.PkgPath",
		"Server.PersistentPolicy", "PersistentConfig", "Nacos",
	}
	for _, name := range expected {
		if !got[name] {
			t.Errorf("expected diff to contain %q, diffs=%+v", name, diffs)
		}
	}
}

func TestDiffBootstrap_NoChangeNoDiff(t *testing.T) {
	t.Cleanup(ResetForTest)
	local := newLocal()
	remote := newLocal()
	diffs := diffBootstrap(&local, &remote)
	if len(diffs) != 0 {
		t.Errorf("expected no diffs, got %+v", diffs)
	}
}

func TestMain(m *testing.M) {
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "console"
	l, _ := cfg.Build()
	SetNacosLogger(l.Sugar())
	os.Exit(m.Run())
}

func TestApplyNacosUpdate_SkipsOnSameHash(t *testing.T) {
	t.Cleanup(ResetForTest)
	GlobalConfig = newLocal()

	var calls int
	RegisterApplier("counter", func(_, _ *CKManConfig) { calls++ })

	data := []byte(`{ log: { level: "DEBUG" } }`)
	if err := ApplyNacosUpdate(data, ".hjson"); err != nil {
		t.Fatalf("first apply error: %v", err)
	}
	if calls != 1 {
		t.Errorf("first apply: applier calls=%d, want 1", calls)
	}
	if err := ApplyNacosUpdate(data, ".hjson"); err != nil {
		t.Fatalf("second apply error: %v", err)
	}
	if calls != 1 {
		t.Errorf("second apply (same hash) should not invoke applier, calls=%d", calls)
	}
}

func TestApplyNacosUpdate_ParseError(t *testing.T) {
	t.Cleanup(ResetForTest)
	GlobalConfig = newLocal()
	snap := GlobalConfig

	err := ApplyNacosUpdate([]byte("not a config"), ".hjson")
	if err == nil {
		t.Fatal("expected parse error, got nil")
	}
	if !reflect.DeepEqual(GlobalConfig, snap) {
		t.Errorf("GlobalConfig changed on parse error: got=%+v want=%+v", GlobalConfig, snap)
	}
}

func TestApplyNacosUpdate_ApplierPanicIsolated(t *testing.T) {
	t.Cleanup(ResetForTest)
	GlobalConfig = newLocal()
	var afterCalled bool
	RegisterApplier("boom", func(_, _ *CKManConfig) { panic("boom") })
	RegisterApplier("after", func(_, _ *CKManConfig) { afterCalled = true })

	if err := ApplyNacosUpdate([]byte(`{ log: { level: "DEBUG" } }`), ".hjson"); err != nil {
		t.Fatalf("apply error: %v", err)
	}
	if !afterCalled {
		t.Error("subsequent applier was not called after panic")
	}
}

func TestApplyNacosUpdate_AppliersInRegistrationOrder(t *testing.T) {
	t.Cleanup(ResetForTest)
	GlobalConfig = newLocal()
	var order []string
	RegisterApplier("a", func(_, _ *CKManConfig) { order = append(order, "a") })
	RegisterApplier("b", func(_, _ *CKManConfig) { order = append(order, "b") })
	RegisterApplier("c", func(_, _ *CKManConfig) { order = append(order, "c") })

	if err := ApplyNacosUpdate([]byte(`{ log: { level: "DEBUG" } }`), ".hjson"); err != nil {
		t.Fatalf("apply error: %v", err)
	}
	if !reflect.DeepEqual(order, []string{"a", "b", "c"}) {
		t.Errorf("applier order=%v, want [a b c]", order)
	}
}

func TestApplyInitialNacos_SetsHashSkipsAppliers(t *testing.T) {
	t.Cleanup(ResetForTest)
	cfg := newLocal()
	var calls int
	RegisterApplier("never", func(_, _ *CKManConfig) { calls++ })

	data := []byte(`{ log: { level: "DEBUG" } }`)
	if err := ApplyInitialNacos(data, ".hjson", &cfg); err != nil {
		t.Fatalf("ApplyInitialNacos error: %v", err)
	}
	if calls != 0 {
		t.Errorf("initial apply should not invoke applier, calls=%d", calls)
	}
	if cfg.Log.Level != "DEBUG" {
		t.Errorf("Log.Level=%q, want DEBUG", cfg.Log.Level)
	}

	GlobalConfig = cfg
	if err := ApplyNacosUpdate(data, ".hjson"); err != nil {
		t.Fatalf("ApplyNacosUpdate error: %v", err)
	}
	if calls != 0 {
		t.Errorf("same-content update after initial should be deduped, calls=%d", calls)
	}
}

// TestMergeFromNacos_OneWayBoolGate documents an intentional limitation:
// non-bootstrap Server bool fields (SwaggerEnable / Metric / Pprof) use a
// "non-zero override" rule, where Go's zero value for bool is false. As a
// consequence, a remote value of false cannot turn off a local true.
// Operators must edit ckman.hjson directly to disable a feature once enabled.
func TestMergeFromNacos_OneWayBoolGate(t *testing.T) {
	t.Cleanup(ResetForTest)
	local := newLocal()
	local.Server.Metric = true
	local.Server.Pprof = true
	local.Server.SwaggerEnable = true

	remote := CKManConfig{
		Server: CKManServerConfig{
			Metric:        false,
			Pprof:         false,
			SwaggerEnable: false,
		},
	}
	mergeFromNacos(&local, &remote)

	if !local.Server.Metric {
		t.Error("Metric: documented behaviour is that Nacos cannot turn off (false ignored)")
	}
	if !local.Server.Pprof {
		t.Error("Pprof: documented behaviour is that Nacos cannot turn off (false ignored)")
	}
	if !local.Server.SwaggerEnable {
		t.Error("SwaggerEnable: documented behaviour is that Nacos cannot turn off (false ignored)")
	}
}
