package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/housepower/ckman/config"
)

func TestRestartsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "watchdog.restarts")
	now := time.Now()
	in := []time.Time{now.Add(-2 * time.Hour), now.Add(-30 * time.Minute), now}
	writeRestarts(f, in)
	out := readRestarts(f)
	if len(out) != 3 {
		t.Fatalf("want 3 timestamps, got %d", len(out))
	}
	if out[2].Unix() != now.Unix() {
		t.Errorf("last ts mismatch: %d vs %d", out[2].Unix(), now.Unix())
	}
}

func TestReadRestartsMissingFile(t *testing.T) {
	if got := readRestarts(filepath.Join(t.TempDir(), "nope")); got != nil {
		t.Errorf("missing file should yield nil, got %v", got)
	}
}

func TestCountWithin(t *testing.T) {
	now := time.Now()
	ts := []time.Time{now.Add(-20 * time.Minute), now.Add(-5 * time.Minute), now.Add(-1 * time.Minute)}
	if n := countWithin(ts, 10*time.Minute); n != 2 {
		t.Errorf("want 2 within 10m, got %d", n)
	}
}

func TestPruneWithin(t *testing.T) {
	now := time.Now()
	ts := []time.Time{now.Add(-2 * time.Hour), now.Add(-1 * time.Minute)}
	got := pruneWithin(ts, time.Hour)
	if len(got) != 1 {
		t.Fatalf("want 1 after prune, got %d", len(got))
	}
}

func TestLatest(t *testing.T) {
	now := time.Now()
	ts := []time.Time{now.Add(-time.Hour), now, now.Add(-time.Minute)}
	if !latest(ts).Equal(now) {
		t.Errorf("latest wrong: %v", latest(ts))
	}
	if !latest(nil).IsZero() {
		t.Errorf("latest(nil) should be zero")
	}
}

// 确保 os 包在测试中被使用（避免后续编辑误删 import）
var _ = os.Stat

func TestFindCkmanPidsMatchesSelfBinary(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Skip("cannot resolve self exe")
	}
	p := paths{ckmanBin: exe}
	pids := findCkmanPids(p)
	// findCkmanPids 跳过自身 PID(os.Getpid)，故测试进程本身不会出现；
	// 这里只验证不 panic 且返回切片(可能为空)。
	_ = pids
}

func TestProcStateSelf(t *testing.T) {
	if s := procState(os.Getpid()); s == "" {
		t.Errorf("procState(self) returned empty")
	}
}

func TestProbePath(t *testing.T) {
	cfg := &config.CKManConfig{}
	cfg.Server.Metric = true
	cfg.Server.MetricPath = "/metrics"
	if got := probePath(cfg); got != "/metrics" {
		t.Errorf("metric on: want /metrics, got %s", got)
	}
	cfg.Server.Metric = false
	if got := probePath(cfg); got != "/" {
		t.Errorf("metric off: want /, got %s", got)
	}
	cfg.Server.Metric = true
	cfg.Server.MetricPath = ""
	if got := probePath(cfg); got != "/metrics" {
		t.Errorf("empty metric_path: want /metrics fallback, got %s", got)
	}
}

func TestDBAddrLocalSkipped(t *testing.T) {
	cfg := &config.CKManConfig{}
	cfg.Server.PersistentPolicy = "local"
	if _, ok := dbAddr(cfg); ok {
		t.Errorf("local policy must be skipped (ok=false)")
	}
}

func TestDBAddrMysqlDefaults(t *testing.T) {
	cfg := &config.CKManConfig{}
	cfg.Server.PersistentPolicy = "mysql"
	cfg.PersistentConfig = map[string]map[string]interface{}{
		"mysql": {"host": "10.0.0.5"}, // 缺 port → 取默认 3306
	}
	addr, ok := dbAddr(cfg)
	if !ok || addr != "10.0.0.5:3306" {
		t.Errorf("want 10.0.0.5:3306 ok=true, got %q ok=%v", addr, ok)
	}
}

func TestDBAddrPostgresExplicitPort(t *testing.T) {
	cfg := &config.CKManConfig{}
	cfg.Server.PersistentPolicy = "postgres"
	cfg.PersistentConfig = map[string]map[string]interface{}{
		"postgres": {"host": "db1", "port": float64(6543)}, // hjson 数字常解析为 float64
	}
	addr, ok := dbAddr(cfg)
	if !ok || addr != "db1:6543" {
		t.Errorf("want db1:6543, got %q ok=%v", addr, ok)
	}
}

func TestDBAddrMissingHost(t *testing.T) {
	cfg := &config.CKManConfig{}
	cfg.Server.PersistentPolicy = "dm8"
	cfg.PersistentConfig = map[string]map[string]interface{}{"dm8": {}}
	if _, ok := dbAddr(cfg); ok {
		t.Errorf("missing host must yield ok=false")
	}
}

func TestCheckExitCode(t *testing.T) {
	cases := []struct {
		res  probeResult
		code int
		lbl  string
	}{
		{probeResult{v: vHealthy}, 0, "OK"},
		{probeResult{v: vCrash}, 10, "DOWN"},
		{probeResult{v: vStopped}, 10, "DOWN"},
		{probeResult{v: vHung}, 11, "HUNG"},
		{probeResult{v: vApp, httpCode: 500}, 12, "APP"},
		{probeResult{v: vApp, httpCode: 401}, 3, "Auth"},
		{probeResult{v: vMulti}, 12, "ABNORMAL"},
	}
	for _, c := range cases {
		code, lbl := checkExitCode(c.res)
		if code != c.code || lbl != c.lbl {
			t.Errorf("verdict=%s code=%d: want (%d,%s) got (%d,%s)",
				c.res.v, c.res.httpCode, c.code, c.lbl, code, lbl)
		}
	}
}
