# Nacos 配置热生效 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 ckman 启动时尝试从 Nacos 拉取配置作为补充，运行时通过 `ListenConfig` 接收变更，对 Log / Cron / ClickHouse 连接池进行同进程选择性热重建；bootstrap 字段变更仅记录 WARN，不写回本地 hjson。

**Architecture:** 在 `config` 包新增 `nacos_apply.go`，提供 `mergeFromNacos` / `diffBootstrap` / `ApplyNacosUpdate` / `ApplyInitialNacos` 与 applier 注册表；`service/nacos/nacos.go` 新增 `PullAndMerge` 并把 `ListenConfigCallback` 改为调用 `config.ApplyNacosUpdate`；`main.go` 启动时调用 `PullAndMerge`，并在各服务 Start 后注册三个 applier（log / cron / ck pool）。

**Tech Stack:** Go 1.24, `github.com/hjson/hjson-go/v4`（已 vendor）, `gopkg.in/yaml.v3`（已 vendor）, `github.com/nacos-group/nacos-sdk-go/v2`（已 vendor）, 标准库 `crypto/sha256` / `reflect` / `sync`。

**参考实现：** `/root/go/src/gitlab.eoitek.net/DC/cell/nacos/nacos.go` 与 `cell/ctx.go`。

**Spec：** `docs/superpowers/specs/2026-05-16-nacos-config-hotreload-design.md`

---

## File Structure

**新建：**
- `config/nacos_apply.go` — Applier 类型、注册表、`parseRemote`、`mergeFromNacos`、`diffBootstrap`、`ApplyNacosUpdate`、`ApplyInitialNacos`、`ResetForTest`、包内 `lastAppliedHash`
- `config/nacos_apply_test.go` — 11 个单元测试

**修改：**
- `config/config.go` — `CKManNacosConfig` 添加 `SyncConfig` / `CfgNamespaceId` 字段；`fillDefault` 设 `SyncConfig=true`；新增 `var ConfigMutex sync.RWMutex`
- `service/nacos/nacos.go` — `InitNacosClient` 处理 `CfgNamespaceId`；新增方法 `PullAndMerge(cfg *config.CKManConfig) error`；新增方法 `listenConfigCallback`（小写）替换全局 `ListenConfigCallback` 调用；`Start` 中 `SyncConfig=false` 时跳过 `ListenConfig`
- `main.go` — 在 `nacos.InitNacosClient` 之后插入 `nacosClient.PullAndMerge(&config.GlobalConfig)`；改 `defer cronSvr.Stop()` 为闭包形式；在各服务 Start 之后追加三个 `config.RegisterApplier(...)`；定义 `logApplier` / `cronApplier` / `ckPoolApplier`

---

### Task 1: 扩展 CKManNacosConfig 与新增 ConfigMutex

**Files:**
- Modify: `config/config.go`

- [ ] **Step 1.1：在 `CKManNacosConfig` 中新增两个字段**

打开 `config/config.go`，找到 `CKManNacosConfig` 结构体（约 83-93 行），改为：

```go
type CKManNacosConfig struct {
	Enabled        bool
	Hosts          []string
	Port           uint64
	UserName       string `yaml:"user_name" json:"user_name"`
	Password       string
	NamespaceId    string `yaml:"namespace_id" json:"namespace_id"`
	CfgNamespaceId string `yaml:"cfg_namespace_id" json:"cfg_namespace_id"`
	Group          string
	DataID         string `yaml:"data_id" json:"data_id"`
	BeatInterval   int64  `yaml:"beat_interval" json:"beat_interval"`
	SyncConfig     bool   `yaml:"sync_config" json:"sync_config"`
}
```

- [ ] **Step 1.2：在 `fillDefault` 设置 `SyncConfig=true`**

`fillDefault` 函数（约 95-118 行）末尾添加一行（紧跟 `c.Nacos.BeatInterval = 5000`）：

```go
	c.Nacos.SyncConfig = true
```

- [ ] **Step 1.3：在 `config` 包内新增 `ConfigMutex` 全局变量**

在文件顶部 `var ClusterMutex sync.RWMutex`（约 21 行）下面追加：

```go
var ConfigMutex sync.RWMutex
```

- [ ] **Step 1.4：构建验证**

Run: `go build ./...`
Expected: 成功无错误。

- [ ] **Step 1.5：commit**

```bash
git add config/config.go
git commit -m "$(cat <<'EOF'
feat(config): extend CKManNacosConfig with SyncConfig and CfgNamespaceId

Adds two fields to support pulling/listening config from Nacos and to allow
the config client to use a different namespace from naming. Introduces
ConfigMutex used by upcoming nacos_apply.go.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: 创建 nacos_apply.go 骨架（Applier 类型、注册表、ResetForTest）

**Files:**
- Create: `config/nacos_apply.go`

- [ ] **Step 2.1：写文件内容**

```go
package config

import (
	"sync"
)

// Applier 是热生效回调。old 为 merge 前的 GlobalConfig 快照，new 为 merge 后的 &GlobalConfig。
// Applier 必须自行 recover panic，必须 idempotent，不应假设并发调用。
type Applier func(old, new *CKManConfig)

type namedApplier struct {
	name string
	fn   Applier
}

var (
	appliersMu sync.Mutex
	appliers   []namedApplier

	// lastAppliedHash 保存上次成功 merge 的 sha256，用于去重 SDK 启动回放与重复推送。
	lastAppliedHash string
)

// RegisterApplier 注册热生效回调，按注册顺序串行执行。
func RegisterApplier(name string, fn Applier) {
	appliersMu.Lock()
	defer appliersMu.Unlock()
	appliers = append(appliers, namedApplier{name: name, fn: fn})
}

// ResetForTest 仅供测试使用，复位 GlobalConfig、applier 注册表与 lastAppliedHash。
func ResetForTest() {
	ConfigMutex.Lock()
	defer ConfigMutex.Unlock()
	appliersMu.Lock()
	defer appliersMu.Unlock()
	GlobalConfig = CKManConfig{}
	appliers = nil
	lastAppliedHash = ""
}
```

- [ ] **Step 2.2：构建验证**

Run: `go build ./...`
Expected: 成功。

- [ ] **Step 2.3：commit**

```bash
git add config/nacos_apply.go
git commit -m "$(cat <<'EOF'
feat(config): add applier registry skeleton for nacos hot reload

Introduces Applier type, RegisterApplier registry and a test reset helper.
Concrete merge/apply logic lands in subsequent commits.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: parseRemote — 按 fmt 解析 Nacos 内容

**Files:**
- Modify: `config/nacos_apply.go`
- Modify: `config/nacos_apply_test.go`（新建）

- [ ] **Step 3.1：写失败的测试**

创建 `config/nacos_apply_test.go`：

```go
package config

import (
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
```

- [ ] **Step 3.2：运行测试验证失败**

Run: `go test ./config/ -run TestParseRemote -v`
Expected: FAIL，`parseRemote` 未定义。

- [ ] **Step 3.3：在 `nacos_apply.go` 实现 `parseRemote`**

在 `nacos_apply.go` 顶部 import 部分补充并新增函数（追加到文件尾）：

```go
import (
	"fmt"
	"sync"

	"github.com/hjson/hjson-go/v4"
	"gopkg.in/yaml.v3"
)

// parseRemote 按本地配置文件后缀解析 Nacos 推送的内容。
// fmt 应为 ".hjson" / ".json" / ".yaml"；其他后缀返回错误。
func parseRemote(data []byte, fmtExt string) (CKManConfig, error) {
	var out CKManConfig
	switch fmtExt {
	case FORMAT_HJSON, FORMAT_JSON:
		if err := hjson.Unmarshal(data, &out); err != nil {
			return CKManConfig{}, err
		}
	case FORMAT_YAML:
		if err := yaml.Unmarshal(data, &out); err != nil {
			return CKManConfig{}, err
		}
	default:
		return CKManConfig{}, fmt.Errorf("unsupported config format %q", fmtExt)
	}
	return out, nil
}
```

注意：合并 import 时保留 `"sync"`，最终 import 块应为：

```go
import (
	"fmt"
	"sync"

	"github.com/hjson/hjson-go/v4"
	"gopkg.in/yaml.v3"
)
```

- [ ] **Step 3.4：运行测试验证通过**

Run: `go test ./config/ -run TestParseRemote -v`
Expected: PASS（4 个用例）。

- [ ] **Step 3.5：commit**

```bash
git add config/nacos_apply.go config/nacos_apply_test.go
git commit -m "$(cat <<'EOF'
feat(config): parse Nacos config content by local file extension

Adds parseRemote that delegates to hjson/yaml unmarshal based on the local
ConfigFile suffix and rejects unsupported formats.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: mergeFromNacos — Section 合并 + bootstrap 保留

**Files:**
- Modify: `config/nacos_apply.go`
- Modify: `config/nacos_apply_test.go`

- [ ] **Step 4.1：写失败的测试**

在 `config/nacos_apply_test.go` 追加：

```go
import "reflect"

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
	remote := CKManConfig{} // 全零值
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
			Port:           9999, // bootstrap，应被忽略
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
```

- [ ] **Step 4.2：运行测试验证失败**

Run: `go test ./config/ -run TestMergeFromNacos -v`
Expected: FAIL，`mergeFromNacos` 未定义。

- [ ] **Step 4.3：在 `nacos_apply.go` 实现 `mergeFromNacos`**

追加到 `nacos_apply.go` 文件尾（同时 import 增加 `"reflect"`）：

```go
// bootstrap 字段：merge 时跳过，由 diffBootstrap 单独检测变更。
// 这些字段一旦初始化即冻结，热覆盖会让内存与运行态不一致。

// mergeFromNacos 将 remote 中非 bootstrap、非保留段的非零字段覆盖到 local。
// 规则见 docs/superpowers/specs/2026-05-16-nacos-config-hotreload-design.md。
func mergeFromNacos(local, remote *CKManConfig) {
	// 1. Log：DeepEqual 零值判定后整段覆盖
	if !reflect.DeepEqual(remote.Log, CKManLogConfig{}) {
		local.Log = remote.Log
	}

	// 2. Cron：同上
	if !reflect.DeepEqual(remote.Cron, CronJob{}) {
		local.Cron = remote.Cron
	}

	// 3. ClickHouse：同上
	if !reflect.DeepEqual(remote.ClickHouse, ClickHouseOpts{}) {
		local.ClickHouse = remote.ClickHouse
	}

	// 4. Server 段内非 bootstrap 字段：单字段非零时覆盖（不能整段 DeepEqual）。
	if remote.Server.SessionTimeout != 0 {
		local.Server.SessionTimeout = remote.Server.SessionTimeout
	}
	if remote.Server.SwaggerEnable {
		local.Server.SwaggerEnable = remote.Server.SwaggerEnable
	}
	if remote.Server.PublicKey != "" {
		local.Server.PublicKey = remote.Server.PublicKey
	}
	if remote.Server.TaskInterval != 0 {
		local.Server.TaskInterval = remote.Server.TaskInterval
	}
	if remote.Server.Metric {
		local.Server.Metric = remote.Server.Metric
	}
	if remote.Server.MetricPath != "" {
		local.Server.MetricPath = remote.Server.MetricPath
	}
	if remote.Server.Pprof {
		local.Server.Pprof = remote.Server.Pprof
	}

	// ConfigFile / Version / Nacos / Server.Ip,Port,Https,Cert,Key,PkgPath / PersistentPolicy /
	// PersistentConfig 永不覆盖。
}
```

- [ ] **Step 4.4：运行测试验证通过**

Run: `go test ./config/ -run TestMergeFromNacos -v`
Expected: PASS（5 个用例）。

- [ ] **Step 4.5：commit**

```bash
git add config/nacos_apply.go config/nacos_apply_test.go
git commit -m "$(cat <<'EOF'
feat(config): section-level merge of Nacos remote into GlobalConfig

Implements mergeFromNacos: Log/Cron/ClickHouse use DeepEqual zero-value test
for whole-section override; Server non-bootstrap fields use per-field
non-zero override; ConfigFile/Version/Nacos and bootstrap fields are never
overridden.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: diffBootstrap — 检测 bootstrap 字段变更

**Files:**
- Modify: `config/nacos_apply.go`
- Modify: `config/nacos_apply_test.go`

- [ ] **Step 5.1：写失败的测试**

在 `config/nacos_apply_test.go` 追加：

```go
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
```

- [ ] **Step 5.2：运行测试验证失败**

Run: `go test ./config/ -run TestDiffBootstrap -v`
Expected: FAIL，`diffBootstrap` 未定义。

- [ ] **Step 5.3：在 `nacos_apply.go` 实现 `diffBootstrap`**

追加到 `nacos_apply.go` 文件尾：

```go
// bootstrapDiff 描述一个被忽略的 bootstrap 字段变更。
type bootstrapDiff struct {
	field string
	old   interface{}
	new   interface{}
}

// diffBootstrap 返回 remote 想改、但 merge 不会覆盖的 bootstrap 字段列表。
// 用于在 ApplyNacosUpdate 中输出 WARN 提示运维手动重启。
func diffBootstrap(local, remote *CKManConfig) []bootstrapDiff {
	var diffs []bootstrapDiff
	add := func(field string, oldV, newV interface{}) {
		diffs = append(diffs, bootstrapDiff{field: field, old: oldV, new: newV})
	}

	if local.Server.Ip != remote.Server.Ip && remote.Server.Ip != "" {
		add("Server.Ip", local.Server.Ip, remote.Server.Ip)
	}
	if local.Server.Port != remote.Server.Port && remote.Server.Port != 0 {
		add("Server.Port", local.Server.Port, remote.Server.Port)
	}
	if local.Server.Https != remote.Server.Https {
		add("Server.Https", local.Server.Https, remote.Server.Https)
	}
	if local.Server.CertFile != remote.Server.CertFile && remote.Server.CertFile != "" {
		add("Server.CertFile", local.Server.CertFile, remote.Server.CertFile)
	}
	if local.Server.KeyFile != remote.Server.KeyFile && remote.Server.KeyFile != "" {
		add("Server.KeyFile", local.Server.KeyFile, remote.Server.KeyFile)
	}
	if local.Server.PkgPath != remote.Server.PkgPath && remote.Server.PkgPath != "" {
		add("Server.PkgPath", local.Server.PkgPath, remote.Server.PkgPath)
	}
	if local.Server.PersistentPolicy != remote.Server.PersistentPolicy && remote.Server.PersistentPolicy != "" {
		add("Server.PersistentPolicy", local.Server.PersistentPolicy, remote.Server.PersistentPolicy)
	}
	if len(remote.PersistentConfig) > 0 && !reflect.DeepEqual(local.PersistentConfig, remote.PersistentConfig) {
		add("PersistentConfig", "<map>", "<map>")
	}
	if !reflect.DeepEqual(remote.Nacos, CKManNacosConfig{}) && !reflect.DeepEqual(local.Nacos, remote.Nacos) {
		add("Nacos", "<section>", "<section>")
	}
	return diffs
}
```

- [ ] **Step 5.4：运行测试验证通过**

Run: `go test ./config/ -run TestDiffBootstrap -v`
Expected: PASS（2 个用例）。

- [ ] **Step 5.5：commit**

```bash
git add config/nacos_apply.go config/nacos_apply_test.go
git commit -m "$(cat <<'EOF'
feat(config): detect bootstrap field changes for WARN logging

Adds diffBootstrap that compares bootstrap fields (HTTP listener, persistent
backend, Nacos section) between local and remote. Used by ApplyNacosUpdate
to emit one WARN per ignored change without overwriting GlobalConfig.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: ApplyNacosUpdate / ApplyInitialNacos — 主入口

**Files:**
- Modify: `config/nacos_apply.go`
- Modify: `config/nacos_apply_test.go`

- [ ] **Step 6.1：写失败的测试**

在 `config/nacos_apply_test.go` 追加：

```go
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
```

- [ ] **Step 6.2：运行测试验证失败**

Run: `go test ./config/ -run "TestApplyNacosUpdate|TestApplyInitialNacos" -v`
Expected: FAIL，`ApplyNacosUpdate` / `ApplyInitialNacos` 未定义。

- [ ] **Step 6.3：实现 `ApplyNacosUpdate` 与 `ApplyInitialNacos`**

把 `nacos_apply.go` 的 import 块扩为：

```go
import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sync"

	"github.com/hjson/hjson-go/v4"
	"github.com/housepower/ckman/log"
	"gopkg.in/yaml.v3"
)
```

追加到文件尾：

```go
// hashContent 返回 data 的 sha256 十六进制串，用于内容去重。
func hashContent(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ApplyInitialNacos 在启动阶段使用：把 Nacos 返回的内容合并到 cfg，并设置 lastAppliedHash。
// 不触发 applier（启动期各服务尚未 Start）。
func ApplyInitialNacos(data []byte, fmtExt string, cfg *CKManConfig) error {
	remote, err := parseRemote(data, fmtExt)
	if err != nil {
		return fmt.Errorf("parse nacos config: %w", err)
	}
	ConfigMutex.Lock()
	defer ConfigMutex.Unlock()
	diffs := diffBootstrap(cfg, &remote)
	mergeFromNacos(cfg, &remote)
	lastAppliedHash = hashContent(data)
	for _, d := range diffs {
		log.Logger.Warnf(
			"nacos config: bootstrap field %s differs from local (local=%v, remote=%v), ignored; local value will be used",
			d.field, d.old, d.new,
		)
	}
	log.Logger.Infof("nacos config initial merge done, sha=%s", lastAppliedHash)
	return nil
}

// ApplyNacosUpdate 在 Nacos OnChange 回调中使用：解析、合并、触发 applier。
// 解析失败立即返回，不进入 merge；内容与上次相同则跳过。
func ApplyNacosUpdate(data []byte, fmtExt string) error {
	remote, err := parseRemote(data, fmtExt)
	if err != nil {
		return fmt.Errorf("parse nacos config: %w", err)
	}
	hash := hashContent(data)

	ConfigMutex.Lock()
	if hash == lastAppliedHash {
		ConfigMutex.Unlock()
		log.Logger.Debugf("nacos config unchanged (sha=%s), skip", hash)
		return nil
	}
	var oldSnapshot CKManConfig
	oldSnapshot = GlobalConfig
	diffs := diffBootstrap(&GlobalConfig, &remote)
	mergeFromNacos(&GlobalConfig, &remote)
	lastAppliedHash = hash
	newSnapshot := GlobalConfig
	// 复制 appliers 列表到本地变量，避免持锁期间用户 RegisterApplier 死锁
	appliersMu.Lock()
	current := make([]namedApplier, len(appliers))
	copy(current, appliers)
	appliersMu.Unlock()
	ConfigMutex.Unlock()

	log.Logger.Infof("nacos config received, sha=%s", hash)
	for _, d := range diffs {
		log.Logger.Warnf(
			"nacos config: bootstrap field %s changed (local=%v, remote=%v), "+
				"ignored in memory; restart ckman manually to apply",
			d.field, d.old, d.new,
		)
	}
	for _, a := range current {
		runApplier(a, &oldSnapshot, &newSnapshot)
	}
	log.Logger.Infof("nacos config applied successfully")
	return nil
}

// runApplier 在独立的 deferred recover 中执行 a.fn。
func runApplier(a namedApplier, old, new *CKManConfig) {
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("applier %s panic: %v", a.name, r)
		}
	}()
	a.fn(old, new)
}
```

注意：测试使用的 `log.Logger` 来自 `github.com/housepower/ckman/log`，单元测试中需要 logger 至少能写。继续往下看，测试用 `TestMain` 初始化 logger。

- [ ] **Step 6.4：在测试文件添加 TestMain 初始化 logger**

在 `config/nacos_apply_test.go` 顶部 import 区域追加：

```go
import (
	"os"
	"reflect"
	"testing"

	"github.com/housepower/ckman/log"
)
```

并在文件中追加：

```go
func TestMain(m *testing.M) {
	log.InitLoggerConsole()
	os.Exit(m.Run())
}
```

- [ ] **Step 6.5：运行测试验证通过**

Run: `go test ./config/ -v`
Expected: PASS（11 用例：4 parseRemote + 5 merge + 2 diff + 4 ApplyNacosUpdate + 1 ApplyInitial = 16，覆盖率高于 spec 列出的 11 项）。

如果有失败，注意 `ApplyNacosUpdate_ApplierPanicIsolated` 测试中 panic 后期 logger 输出 ERROR 是允许的。

- [ ] **Step 6.6：commit**

```bash
git add config/nacos_apply.go config/nacos_apply_test.go
git commit -m "$(cat <<'EOF'
feat(config): ApplyNacosUpdate and ApplyInitialNacos entry points

Wires parseRemote/mergeFromNacos/diffBootstrap together. ApplyInitialNacos
is used at startup to merge once and seed lastAppliedHash without firing
appliers. ApplyNacosUpdate is the OnChange entry point: sha256 dedup, parse
then merge then bootstrap WARN then serial applier dispatch with per-call
recover.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: NacosClient.PullAndMerge + 替换 ListenConfigCallback + CfgNamespaceId

**Files:**
- Modify: `service/nacos/nacos.go`

- [ ] **Step 7.1：在 `InitNacosClient` 中支持 `CfgNamespaceId`**

打开 `service/nacos/nacos.go`，找到 `clientConfig := constant.ClientConfig{...}`（约 37-47 行）保持原样。

找到创建 `configClient` 的位置（约 71-80 行）：

```go
		// Create config client for dynamic configuration
		configClient, err := clients.NewConfigClient(
			vo.NacosClientParam{
				ClientConfig:  &clientConfig,
				ServerConfigs: serverConfigs,
			},
		)
```

替换为（在创建 configClient **之前**克隆 clientConfig 并按需覆盖 NamespaceId）：

```go
		// Config client may use a different namespace (借鉴 cell).
		cfgClientConfig := clientConfig
		if config.CfgNamespaceId != "" {
			if config.CfgNamespaceId == "public" {
				cfgClientConfig.NamespaceId = ""
			} else {
				cfgClientConfig.NamespaceId = config.CfgNamespaceId
			}
		}
		// Create config client for dynamic configuration
		configClient, err := clients.NewConfigClient(
			vo.NacosClientParam{
				ClientConfig:  &cfgClientConfig,
				ServerConfigs: serverConfigs,
			},
		)
```

- [ ] **Step 7.2：新增 `PullAndMerge` 方法**

在文件尾追加：

```go
// PullAndMerge 在启动阶段从 Nacos 拉取配置并合并到 cfg。
// 任何错误（client 未启用、SyncConfig=false、Nacos 不可达、内容为空、解析失败）
// 都以 WARN/ERROR 形式记录，但**不会导致启动失败**——启动绝不阻塞。
func (c *NacosClient) PullAndMerge(cfg *config.CKManConfig) error {
	if !c.Enabled || c.Config == nil {
		return nil
	}
	if !cfg.Nacos.SyncConfig {
		log.Logger.Infof("nacos sync_config disabled, skip pulling remote config")
		return nil
	}
	content, err := c.Config.GetConfig(vo.ConfigParam{
		DataId: c.DataId,
		Group:  c.GroupName,
	})
	if err != nil {
		log.Logger.Warnf("nacos GetConfig failed, fallback to local: %v", err)
		return nil
	}
	if content == "" {
		log.Logger.Warnf("nacos config empty (dataId=%s, group=%s), fallback to local",
			c.DataId, c.GroupName)
		return nil
	}
	ext := filepath.Ext(cfg.ConfigFile)
	if err := config.ApplyInitialNacos([]byte(content), ext, cfg); err != nil {
		log.Logger.Errorf("apply initial nacos config failed: %v", err)
		return nil
	}
	return nil
}
```

确保文件顶部 import 包含 `"path/filepath"`（应已存在）。

- [ ] **Step 7.3：替换 `ListenConfigCallback`**

把现有：

```go
func ListenConfigCallback(namespace, group, dataId, data string) {
}
```

替换为：

```go
// ListenConfigCallback 是 Nacos config_client 的 OnChange 回调。
// 解析格式按本地 ConfigFile 后缀确定。
func ListenConfigCallback(namespace, group, dataId, data string) {
	ext := filepath.Ext(config.GlobalConfig.ConfigFile)
	if err := config.ApplyNacosUpdate([]byte(data), ext); err != nil {
		log.Logger.Errorf("nacos config apply failed: %v", err)
	}
}
```

- [ ] **Step 7.4：在 `Start` 中按 `SyncConfig` 跳过 `ListenConfig` 注册**

找到 `Start` 方法（约 142 行）的内容：

```go
func (c *NacosClient) Start(ipHttp string, portHttp int) error {
	if !c.Enabled {
		return nil
	}

	err := c.Subscribe()
	if err != nil {
		return errors.Wrap(err, "")
	}

	err = c.ListenConfig()
	if err != nil {
		return errors.Wrap(err, "")
	}
	...
}
```

把 `err = c.ListenConfig()` 块改为：

```go
	if config.GlobalConfig.Nacos.SyncConfig {
		err = c.ListenConfig()
		if err != nil {
			return errors.Wrap(err, "")
		}
	}
```

- [ ] **Step 7.5：构建并跑既有测试**

Run: `go build ./... && go test ./config/ ./service/nacos/ -v`
Expected: 构建成功；config 测试 PASS；service/nacos 包若无测试文件就显示 `no test files`。

- [ ] **Step 7.6：commit**

```bash
git add service/nacos/nacos.go
git commit -m "$(cat <<'EOF'
feat(nacos): pull config at startup and dispatch OnChange to config package

Adds NacosClient.PullAndMerge (used by main.go right after InitNacosClient)
to seed GlobalConfig from Nacos at startup. ListenConfigCallback now
delegates to config.ApplyNacosUpdate. Honours CfgNamespaceId for splitting
config-center namespace from naming, and skips ListenConfig registration
when Nacos.SyncConfig is false.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: main.go 接线 — PullAndMerge 与三个 applier

**Files:**
- Modify: `main.go`

- [ ] **Step 8.1：在 `nacosClient.Start` 之前调用 `PullAndMerge`**

打开 `main.go`，找到（约 114-121 行）：

```go
	nacosClient, err := nacos.InitNacosClient(&config.GlobalConfig.Nacos, LogFilePath)
	if err != nil {
		log.Logger.Fatalf("Failed to init nacos client, %v", err)
	}
	err = nacosClient.Start(config.GlobalConfig.Server.Ip, config.GlobalConfig.Server.Port)
	if err != nil {
		log.Logger.Fatalf("Failed to start nacos client, %v", err)
	}
```

在 `InitNacosClient` 之后、`Start` 之前插入：

```go
	if err := nacosClient.PullAndMerge(&config.GlobalConfig); err != nil {
		log.Logger.Warnf("pull nacos config at startup: %v", err)
	}
```

- [ ] **Step 8.2：注册 `log` applier（在 InitLogger 之后即可，但要在 PullAndMerge 之后，避免启动期触发）**

> 注：启动期 `ApplyInitialNacos` 不触发 applier，所以这里在 PullAndMerge 之后注册不会有副作用。我们把所有 RegisterApplier 集中到 nacosClient.Start 之前一段。

在 `nacosClient.Start(...)` **之前**（且在前面的 `PullAndMerge` 之后）插入：

```go
	config.RegisterApplier("log", logApplier)
```

`logApplier` 在 main.go 末尾新增（见 Step 8.5）。

- [ ] **Step 8.3：把 `defer cronSvr.Stop()` 改为闭包形式并注册 `cron` applier**

找到（约 149-153 行）：

```go
	cronSvr := cron.NewCronService(config.GlobalConfig)
	if err = cronSvr.Start(); err != nil {
		log.Logger.Fatalf("Failed to start cron service, %v", err)
	}
	defer cronSvr.Stop()
```

替换为：

```go
	cronSvr := cron.NewCronService(config.GlobalConfig)
	if err = cronSvr.Start(); err != nil {
		log.Logger.Fatalf("Failed to start cron service, %v", err)
	}
	defer func() { cronSvr.Stop() }()
	config.RegisterApplier("cron", cronApplier(&cronSvr))
	config.RegisterApplier("ck", ckPoolApplier)
```

- [ ] **Step 8.4：把 `RegisterApplier("log", ...)` 调整到 Step 8.2 中描述的位置**

最终顺序应为：

```
nacos.InitNacosClient → nacosClient.PullAndMerge → config.RegisterApplier("log", logApplier)
→ ...其他既有 Start... → cronSvr Start → defer cronSvr.Stop()
→ config.RegisterApplier("cron", cronApplier(&cronSvr))
→ config.RegisterApplier("ck", ckPoolApplier)
→ nacosClient.Start
```

注意 `nacosClient.Start` 必须在三个 applier 都注册之后调用，因为 `Start` 中的 `ListenConfig` 一旦注册到 SDK，下一次 OnChange 就会触发 applier 调用。

把 main.go 中 `nacosClient.Start(...)` 的位置移动到 `RegisterApplier("ck", ...)` **之后**。

具体改动顺序示意：

```go
nacosClient, err := nacos.InitNacosClient(&config.GlobalConfig.Nacos, LogFilePath)
if err != nil {
    log.Logger.Fatalf("Failed to init nacos client, %v", err)
}
if err := nacosClient.PullAndMerge(&config.GlobalConfig); err != nil {
    log.Logger.Warnf("pull nacos config at startup: %v", err)
}
config.RegisterApplier("log", logApplier)

zookeeper.ZkServiceCache = cache.New(time.Hour, time.Minute)
zookeeper.ZkServiceCache.OnEvicted(...)

// backup/runner/svr 启动... (既有)

cronSvr := cron.NewCronService(config.GlobalConfig)
if err = cronSvr.Start(); err != nil {
    log.Logger.Fatalf("Failed to start cron service, %v", err)
}
defer func() { cronSvr.Stop() }()
config.RegisterApplier("cron", cronApplier(&cronSvr))
config.RegisterApplier("ck", ckPoolApplier)

// nacosClient.Start 必须在所有 applier 注册之后调用
err = nacosClient.Start(config.GlobalConfig.Server.Ip, config.GlobalConfig.Server.Port)
if err != nil {
    log.Logger.Fatalf("Failed to start nacos client, %v", err)
}

handleSignal(signalCh)
```

- [ ] **Step 8.5：在 main.go 末尾新增三个 applier 函数**

在文件末尾（`func DumpConfig` 之后）追加：

```go
// logApplier 在 Log 段变更时重新初始化 logger。
func logApplier(old, new *config.CKManConfig) {
	if reflect.DeepEqual(old.Log, new.Log) {
		return
	}
	log.InitLogger(LogFilePath, &new.Log)
	log.Logger.Infof("log config reloaded: %+v", new.Log)
}

// cronApplier 在 Cron 段变更时停止旧调度器并启动新调度器。
// svrPtr 是 main.go 中 cronSvr 变量的地址，applier 内通过它替换实例。
func cronApplier(svrPtr **cron.CronService) config.Applier {
	return func(old, new *config.CKManConfig) {
		if reflect.DeepEqual(old.Cron, new.Cron) {
			return
		}
		(*svrPtr).Stop()
		ns := cron.NewCronService(*new)
		if err := ns.Start(); err != nil {
			log.Logger.Errorf("cron reload failed: %v", err)
			return
		}
		*svrPtr = ns
		log.Logger.Infof("cron reloaded")
	}
}

// ckPoolApplier 在 ClickHouse 段变更时关闭现有连接，下次取连接走新参数。
func ckPoolApplier(old, new *config.CKManConfig) {
	if reflect.DeepEqual(old.ClickHouse, new.ClickHouse) {
		return
	}
	var hosts []string
	common.ConnectPool.Range(func(k, _ interface{}) bool {
		hosts = append(hosts, k.(string))
		return true
	})
	common.CloseConns(hosts)
	log.Logger.Infof("clickhouse pool reloaded: %+v", new.ClickHouse)
}
```

- [ ] **Step 8.6：补充 main.go 顶部 import**

确保 import 块包含：

```go
	"reflect"
```

（其他需要的 `common` / `cron` / `config` / `log` 已存在）

- [ ] **Step 8.7：构建验证**

Run: `make backend` 或 `go build ./...`
Expected: 成功无错误。

- [ ] **Step 8.8：跑 config 测试确认未回归**

Run: `go test ./config/ -v`
Expected: PASS。

- [ ] **Step 8.9：commit**

```bash
git add main.go
git commit -m "$(cat <<'EOF'
feat(main): wire up Nacos pull-and-merge and three hot-reload appliers

PullAndMerge runs after InitNacosClient and before nacosClient.Start so that
the first GlobalConfig snapshot used by all services reflects Nacos. Three
appliers (log/cron/ck pool) are registered after their respective services
are up; nacosClient.Start is moved last so the first OnChange callback only
fires once everything is ready.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: 全量构建 + 单元测试 + 人工冒烟清单（验收）

**Files:** 无代码改动。

- [ ] **Step 9.1：完整构建**

Run: `make build`
Expected: 成功。

- [ ] **Step 9.2：跑所有测试**

Run: `make test`
Expected: 所有包通过；至少 `config` 包新增的 16 个 test PASS。

- [ ] **Step 9.3：lint**

Run: `make lint`
Expected: 无新告警。

- [ ] **Step 9.4：人工冒烟（验收，不写为自动化测试）**

按以下顺序逐项确认（部署环境需要一个可用的 Nacos）：

```
1. 不开 Nacos（Enabled=false 或本地无 Nacos 段）：启动行为如旧。
2. Enabled=true，Nacos 上无对应 dataId：日志 WARN
   "nacos config empty (dataId=ckman, group=DEFAULT_GROUP), fallback to local"。
3. Nacos 上发布完整 ckman.hjson，启动期日志：
   "nacos config initial merge done, sha=..."；Log/Cron/ClickHouse 段使用 Nacos 值。
4. 运行期 Nacos 改 log.level INFO → DEBUG：
   日志包含 "log config reloaded"，新一行已是 DEBUG。
5. 运行期 Nacos 改 cron.watch_cluster_status：
   日志包含 "cron reloaded"，新表达式按时调度。
6. 运行期 Nacos 改 server.port=9999：
   日志包含 "nacos config: bootstrap field Server.Port changed
   (local=8808, remote=9999), ignored in memory; restart ckman manually
   to apply"；HTTP 仍监听 8808。
7. 运行期 Nacos 改 nacos.password：
   日志包含 "bootstrap field Nacos changed ... ignored"；Nacos 心跳不重连。
8. 运行期重复推送同一份内容：
   日志包含 "nacos config unchanged (sha=...), skip"；不重建 log/cron/ck。
9. Nacos.SyncConfig=false 启动：
   日志包含 "nacos sync_config disabled, skip pulling remote config"；
   naming 服务发现仍工作（GetInstances 返回 self+其他实例）。
```

- [ ] **Step 9.5：（可选）打 tag 或 PR 描述更新**

如果工作流要求，在 PR 描述中粘贴 §9.4 的冒烟结果。无代码改动则跳过。

---

## Self-Review

**1. Spec coverage：**

| Spec 章节 | 对应任务 |
|---|---|
| §「CKManNacosConfig 扩展 / fillDefault / ConfigMutex」 | Task 1 |
| §「Applier 类型 / registry / ResetForTest」 | Task 2 |
| §「parseRemote」 | Task 3 |
| §「mergeFromNacos 与 Section 规则」 | Task 4 |
| §「diffBootstrap」 | Task 5 |
| §「ApplyNacosUpdate / ApplyInitialNacos / sha256 去重 / applier 调度 / panic 隔离」 | Task 6 |
| §「PullAndMerge / ListenConfigCallback 替换 / CfgNamespaceId / SyncConfig 跳过 ListenConfig」 | Task 7 |
| §「main.go 时序 / 三个内置 applier」 | Task 8 |
| §「单元测试 11 项 / 人工冒烟」 | Task 3-6 + Task 9 |
| §「错误与边界」 | 散落于 Task 6/7（解析失败、空内容、SDK 断连交由 SDK） |
| §「安全考量 / 兼容性」 | 自然满足（无 syscall.Exec；SyncConfig 默认 true 但 Enabled=false 时整条短路） |

无遗漏。

**2. Placeholder 扫描：** 无 TBD / TODO / "add appropriate error handling" / "similar to Task N" 等。

**3. 类型一致性：**
- `Applier = func(old, new *CKManConfig)` 三处一致（Task 2 定义、Task 6 调用、Task 8 使用）。
- `cronApplier(svrPtr **cron.CronService) config.Applier` 三处一致。
- `ApplyNacosUpdate(data []byte, fmtExt string) error` 在 Task 6 定义与 Task 7 调用一致。
- `ApplyInitialNacos(data []byte, fmtExt string, cfg *CKManConfig) error` 在 Task 6 定义与 Task 7 调用一致。
- `PullAndMerge(cfg *config.CKManConfig) error` 在 Task 7 定义与 Task 8 调用一致。
- `bootstrapDiff` 字段名 `field/old/new` 在 Task 5 定义与 Task 6 引用一致。
- `lastAppliedHash` 在 Task 2 声明、Task 6 读写。

无不一致。
